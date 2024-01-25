///! JSON Query Parser
use crate::db;
use crate::idl;
use crate::result::EgResult;
use crate::util;
use json::JsonValue;
use std::sync::Arc;

const DEFAULT_LOCALE: &str = "en-US";

#[derive(Debug, Clone, Copy)]
pub enum JoinOp {
    And,
    Or,
}

impl From<JoinOp> for &str {
    fn from(j: JoinOp) -> &'static str {
        match j {
            JoinOp::And => "AND",
            JoinOp::Or => "OR",
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceDef {
    is_base_class: bool,
    classname: String,
    tablename: String,
    alias: Option<String>,
}

impl SourceDef {
    /// String used to prefix column names, parameters, etc.
    /// E.g. SELECT "aou".id FROM ...
    fn prefix(&self) -> &str {
        self.alias.as_deref().unwrap_or(self.classname.as_str())
    }
}

#[derive(Debug)]
pub struct ParamDef {
    /// Parameter value.
    /// Valid options are strings, numbers, bools, and null.
    value: JsonValue,

    /// 0-based offset of this parameter in the list of parameters.
    /// This is used when passing the query to the DB backend
    /// for ?-based variable replacements.
    index: usize,
}

#[derive(Debug)]
pub struct JsonQueryCompiler {
    /// So we can see how classes relate to each other.
    idl: Arc<idl::Parser>,

    /// Used for oils_i18n_xlate() if set.
    /// If unset, use the default.
    locale: Option<String>,

    /// Avoid calling oils_i18n_xlate()
    disable_i18n: bool,

    /// I.e. EG service name.  Compare to 'suppress_controller' values
    /// to see of this instance can view selected fields.
    controllername: Option<String>,

    /// If set, we're pulling values from a DB function instead of
    /// SELECTing from a table.
    from_function: Option<String>,

    /// All tables (IDL classes) included in our query.
    /// Basically FROM + JOINs.
    sources: Vec<SourceDef>,

    /// Final compiled SQL string
    query_string: Option<String>,

    /// Query parameters whose values are replaced at query execution time.
    params: Option<Vec<ParamDef>>,

    /// Global parameter index.  This value increases by one with
    /// every WHERE/transform parameter added so that each has a
    /// unique value.
    param_index: usize,

    /// TODO
    group_by: Vec<usize>,

    /// TODO
    select_index: usize,
}

impl JsonQueryCompiler {
    pub fn new(idl: Arc<idl::Parser>) -> Self {
        Self {
            idl,
            locale: None,
            controllername: None,
            sources: Vec::new(),
            from_function: None,
            query_string: None,
            disable_i18n: false,
            params: None,
            param_index: 1,
            group_by: Vec::new(),
            select_index: 0,
        }
    }

    pub fn params(&self) -> Option<&Vec<ParamDef>> {
        self.params.as_ref()
    }

    /// Returns a JSON array of parameter values; primarily for debugging.
    pub fn param_values(&self) -> JsonValue {
        let mut array = json::array![];
        if let Some(params) = self.params.as_ref() {
            for param in params {
                array.push(param.value.clone());
            }
        }
        array
    }

    /// Set the locale for use with oils_i18n_xlate().
    pub fn set_locale(&mut self, locale: &str) -> EgResult<()> {
        if locale.chars().any(|b| !b.is_ascii_alphabetic() && b != '-') {
            return Err(format!("Invalid locale: '{locale}'").into());
        }
        self.locale = Some(locale.to_string());
        Ok(())
    }

    pub fn query_string(&self) -> Option<&str> {
        self.query_string.as_deref()
    }

    /// Get the IDL classname linked to a table alias.
    /// The alias may also be the classname.
    fn get_alias_classname(&self, alias: &str) -> EgResult<&str> {
        self.sources
            .iter()
            .filter(|c| {
                if let Some(als) = c.alias.as_ref() {
                    alias == als
                } else {
                    alias == &c.classname
                }
            })
            .map(|c| c.classname.as_str())
            .next()
            .ok_or(format!("No such class alias: {alias}").into())
    }

    fn get_base_source(&self) -> EgResult<&SourceDef> {
        self.sources
            .iter()
            .filter(|s| s.is_base_class)
            .next()
            .ok_or(format!("No bass class has been set").into())
    }

    /// Returns the IDL classname of the base class, i.e. the root
    /// class of the FROM clause.
    fn get_base_classname(&self) -> EgResult<&str> {
        self.sources
            .iter()
            .filter(|s| s.is_base_class)
            .map(|s| s.classname.as_ref())
            .next()
            .ok_or(format!("No bass class has been set").into())
    }

    /// Returns option of IDL field if the field is valid exists on the
    /// class, isn't virtual, and may be viewed by this module.
    fn field_may_be_selected(&self, name: &str, class: &str) -> bool {
        let idl_class = match self.idl.classes().get(class) {
            Some(c) => c,
            None => return false,
        };

        let idl_field = match idl_class.fields().get(name) {
            Some(f) => f,
            None => return false,
        };

        if idl_field.is_virtual() {
            return false;
        }

        if let Some(suppress) = idl_field.suppress_controller() {
            if let Some(module) = self.controllername.as_ref() {
                if suppress.contains(module) {
                    // Field is not visible to this module.
                    return false;
                }
            }
        }

        true
    }

    /// Compile a json_query structure into its constituent parts.
    pub fn compile(&mut self, query: &JsonValue) -> EgResult<()> {
        if !query.is_object() {
            return Err(format!("json_query must be a JSON hash").into());
        }

        // TODO union, intersect, except
        // TODO wholly separate the FROM/array compilation

        if query["from"].is_array() {
            // TODO compile function calls separately to avoid
            // complicating the main code path.
            return Ok(());
        }

        // Clone the source to avoid a number of parellel mut's below.
        let base_source = self.set_base_source(&query["from"])?.clone();
        let cname = &base_source.classname;

        // Compile JOINs first so we can populate our sources.
        let join_str = self.compile_joins_for_class(cname, &query["from"][cname])?;

        let sel_str = self.compile_selects(&query["select"])?;

        // TODO WHERE
        // TODO GROUP BY (reminder: aggregates / distinct)
        // TODO ORDER BY

        self.query_string = Some(format!(
            r#"SELECT {sel_str} FROM {} AS "{}" {join_str}"#,
            self.force_valid_ident(&base_source.tablename)?,
            self.force_valid_ident(base_source.alias.as_deref().unwrap_or(cname))?,
        ));

        Ok(())
    }

    fn compile_selects(&mut self, select_def: &JsonValue) -> EgResult<String> {
        if select_def.is_null() {
            let cn = self.get_base_classname()?.to_string(); // parallel mutes

            // If we have no SELECT clause at all, just select the default fields.
            return self.build_default_select_list(&cn);
        } else if !select_def.is_object() {
            // The root SELECT clause is a map of classname (or alias) to field list
            return Err(format!("Invalid SELECT clause: {}", select_def.dump()).into());
        }

        let mut sql = String::new();
        for (alias, payload) in select_def.entries() {
            sql += " ";
            sql += &self.compile_selects_for_class(alias, payload)?;
        }

        if sql.len() > 0 {
            // Remove first space.
            sql.remove(0);
        }

        // remove final trailing ","
        sql.pop();

        Ok(sql)
    }

    fn compile_selects_for_class(
        &mut self,
        class_alias: &str,
        select_def: &JsonValue,
    ) -> EgResult<String> {
        if select_def.is_null() {
            return self.build_default_select_list(class_alias);
        }

        let classname = self.get_alias_classname(class_alias)?.to_string(); // mut's

        if let Some(col) = select_def.as_str() {
            if col == "*" {
                // Wildcard queries use the default select list.
                return self.build_default_select_list(class_alias);
            } else {
                // Selecting a single column by name.

                if self.field_may_be_selected(col, &classname) {
                    return Ok(format!(
                        "{},",
                        self.select_one_field(class_alias, None, col, None)?
                    ));
                }
            }
        }

        if !select_def.is_array() {
            return Err(format!("SELECT must be string, null, or array").into());
        }

        let mut sql = String::new();

        for field_struct in select_def.members() {
            if let Some(column) = field_struct.as_str() {
                // Field entry is a string field name.

                if self.field_may_be_selected(column, &classname) {
                    sql += " ";
                    sql += &self.select_one_field(class_alias, None, column, None)?;
                    sql += ",";
                }

                continue;
            }

            let column = field_struct["column"].as_str().ok_or_else(|| {
                format!("SELECT hash requires a 'column': {}", field_struct.dump())
            })?;

            if !self.field_may_be_selected(column, &classname) {
                continue;
            }

            sql += " ";
            sql += &self.select_one_field(
                class_alias,
                field_struct["alias"].as_str(),
                column,
                Some(field_struct),
            )?;

            sql += ",";
        }

        if sql.len() > 0 {
            // remove opening space
            sql.remove(0);
        }

        Ok(sql)
    }

    fn build_default_select_list(&mut self, alias: &str) -> EgResult<String> {
        let classname = self.get_alias_classname(alias)?.to_string(); // mut's

        // If we have an alias it's known to be valid
        let idl_class = self.idl.classes().get(&classname).unwrap();

        let mut sql = String::new();

        let field_names: Vec<String> = idl_class
            .real_fields_sorted()
            .iter()
            .filter(|f| self.field_may_be_selected(f.name(), &classname))
            .map(|f| f.name().to_string())
            .collect();

        for field_name in field_names.iter() {
            sql += " ";
            sql += &self.select_one_field(alias, None, field_name, None)?;
            sql += ","
        }

        Ok(sql)
    }

    fn select_one_field(
        &mut self,
        class_alias: &str,
        field_alias: Option<&str>,
        field_name: &str,
        field_def: Option<&JsonValue>,
    ) -> EgResult<String> {
        let idl_class = self
            .idl
            .classes()
            .get(self.get_alias_classname(class_alias)?)
            .ok_or_else(|| format!("Invalid alias: {class_alias}"))?;

        let idl_field = idl_class
            .fields()
            .get(field_name)
            .ok_or_else(|| format!("Invalid field {}::{field_name}", idl_class.classname()))?;

        // TODO maybe some dedupe / refactoring here.

        if let Some(fdef) = field_def {
            // If we have a field_def, it may mean the field has extended
            // properties, like a transform or other flags.

            if let Some(xform) = fdef["transform"].as_str() {
                let mut sql = String::new();

                sql += &format!(" {}(", &self.force_valid_ident(xform)?.to_uppercase());

                if util::json_bool(&fdef["distinct"]) {
                    sql += "DISTINCT ";
                }

                // Avoid sending the field alias here since any alias
                // should apply to our transform as a whole.
                sql += &self.format_one_select_field(class_alias, idl_class, None, idl_field)?;

                for param in fdef["params"].members() {
                    let index = self.add_param(param);
                    sql += &format!(", ${index}");
                }

                sql += ")";

                if let Some(rfield) = fdef["result_field"].as_str() {
                    // Append (...).xform_result_field.
                    sql = format!(r#"({sql})."{}""#, self.force_valid_ident(rfield)?);
                } else if let Some(alias) = field_alias {
                    sql += &format!(r#" AS "{}""#, self.force_valid_ident(alias)?);
                }

                return Ok(sql);
            }
        }

        self.format_one_select_field(class_alias, idl_class, field_alias, idl_field)
    }

    /// Format the SELECT component for a single field, adding the
    /// oils_i18n_xlate() where needed.
    fn format_one_select_field(
        &self,
        class_alias: &str,
        idl_class: &idl::Class,
        field_alias: Option<&str>,
        idl_field: &idl::Field,
    ) -> EgResult<String> {
        let mut sql;

        if !idl_field.i18n() || self.disable_i18n {
            sql = format!(
                r#""{}".{}"#,
                self.force_valid_ident(class_alias)?,
                self.force_valid_ident(idl_field.name())?
            );
        } else {
            let locale = self.locale.as_deref().unwrap_or(DEFAULT_LOCALE);

            let pkey = idl_class
                .pkey()
                .ok_or_else(|| format!("{} has no primary key", idl_class.classname()))?;

            let tablename = idl_class
                .tablename()
                .ok_or_else(|| format!("{} has no table name", idl_class.classname()))?;

            // Our 'locale' string format is validated at set time.

            sql = format!(
                r#"oils_i18n_xlate('{}', '{}', '{}', '{}', "{}".{}::TEXT, '{locale}')"#,
                self.force_valid_ident(tablename)?,
                self.force_valid_ident(class_alias)?,
                self.force_valid_ident(idl_field.name())?,
                self.force_valid_ident(pkey)?,
                self.force_valid_ident(class_alias)?,
                self.force_valid_ident(pkey)?,
            );
        }

        if let Some(alias) = field_alias {
            sql += &format!(r#" AS "{}""#, self.force_valid_ident(alias)?);
        }

        Ok(sql)
    }

    /// Unpack the JOIN clauses into their constituent parts.
    fn compile_joins_for_class(&mut self, left_alias: &str, joins: &JsonValue) -> EgResult<String> {
        let mut sql = String::new();

        let class_to_hash = |c| {
            // Sometimes we JOIN to a class with no additional info beyond
            // the classname.  Put that info into a json object for consistency.
            let mut hash = json::object! {};
            hash[c] = JsonValue::Null;
            hash
        };

        let mut join_binding;

        let join_list = if let JsonValue::Array(list) = joins {
            list.iter().collect::<Vec<&JsonValue>>()
        } else if let Some(class) = joins.as_str() {
            join_binding = class_to_hash(class);
            vec![&join_binding]
        } else {
            vec![joins]
        };

        for join_entry in join_list {
            let mut hash_binding;

            let hash_ref = if let Some(class) = join_entry.as_str() {
                hash_binding = class_to_hash(class);
                &hash_binding
            } else {
                join_entry
            };

            for (right_alias, join_def) in hash_ref.entries() {
                sql += " ";
                sql += &self.add_one_join(left_alias, right_alias, join_def)?;
            }
        }

        if sql.len() > 0 {
            sql.remove(0);
        }

        Ok(sql)
    }

    fn add_one_join(
        &mut self,
        left_alias: &str,
        right_alias: &str,
        join_def: &JsonValue,
    ) -> EgResult<String> {
        let right_class = if let Some(class) = join_def["class"].as_str() {
            class
        } else {
            // If there's no "class" in the hash, the alias is the classname
            right_alias
        };

        let right_idl_class = self
            .idl
            .classes()
            .get(right_class)
            .ok_or_else(|| format!("No such IDL class in JOIN: {right_class}"))?;

        let tablename = right_idl_class
            .tablename()
            .ok_or_else(|| format!("Cannot join to a class with no table: {right_class}"))?;

        let left_class = self.get_alias_classname(left_alias)?;
        let left_idl_class = self.idl.classes().get(left_class).unwrap();

        // Field on the left/source table to JOIN on. Optional.
        let mut left_join_field = join_def["fkey"].as_str();

        // Field on the right/target table to JOIN on. Optional.
        let mut right_join_field = join_def["field"].as_str();

        // Find the left and right field names from the IDL via links.

        if right_join_field.is_some() && left_join_field.is_none() {
            let rfield_name = right_join_field.as_deref().unwrap(); // verified

            // Find the link definition that points from the target/joined
            // class to the left/source class.
            let idl_link = right_idl_class
                .links()
                .get(rfield_name)
                .ok_or_else(|| format!("No such link  for class '{right_class}'"))?;

            let reltype = idl_link.reltype();

            let maybe_left_class = idl_link.class();
            if reltype != idl::RelType::HasMany {
                if maybe_left_class == left_class {
                    left_join_field = Some(idl_link.key());
                }
            }

            if left_join_field.is_none() {
                return Err(format!(
                    "No link defined from {right_class}::{rfield_name} to {maybe_left_class}"
                )
                .into());
            }
        } else if right_join_field.is_none() && left_join_field.is_some() {
            let lfield_name = left_join_field.as_deref().unwrap(); // verified above.

            let idl_link = left_idl_class
                .links()
                .get(lfield_name)
                .ok_or_else(|| format!("No such link {lfield_name} for class {left_class}"))?;

            let reltype = idl_link.reltype();

            let maybe_right_class = idl_link.class();
            if reltype != idl::RelType::HasMany {
                if maybe_right_class == right_class {
                    right_join_field = Some(idl_link.key());
                }
            }

            if right_join_field.is_none() {
                return Err(format!(
                    "No link defined from {left_class}::{lfield_name} to {maybe_right_class}"
                )
                .into());
            }
        } else if right_join_field.is_none() && left_join_field.is_none() {
            // See if we can determine the left and right join fields
            // based solely on the 2 tables being joined.

            for (link_key, cur_link) in left_idl_class.links() {
                let maybe_right_class = cur_link.class();

                if maybe_right_class == right_class {
                    let reltype = cur_link.reltype();
                    if reltype != idl::RelType::HasMany {
                        left_join_field = Some(link_key);
                        right_join_field = Some(cur_link.key());
                        break;
                    }
                }
            }

            // Do another search with the classes reversed.
            if right_join_field.is_none() && left_join_field.is_none() {
                for (link_key, cur_link) in right_idl_class.links() {
                    let maybe_left_class = cur_link.class();

                    if maybe_left_class == left_class {
                        let reltype = cur_link.reltype();
                        if reltype != idl::RelType::HasMany {
                            left_join_field = Some(link_key);
                            right_join_field = Some(cur_link.key());
                            break;
                        }
                    }
                }
            }

            if right_join_field.is_none() && left_join_field.is_none() {
                return Err(format!(
                    "Could not find link between classes {left_class} and {right_class}"
                )
                .into());
            }
        }

        let tablename = right_idl_class
            .tablename()
            .ok_or_else(|| format!("JOINed class has no table name: {right_class}"))?;

        // Add this new class to our list of sources.
        let mut source_def = SourceDef {
            classname: right_class.to_string(),
            tablename: tablename.to_string(),
            alias: None,
            is_base_class: false,
        };

        if right_alias != right_class {
            // No need to allocate/track an alias if it's the same
            // as the classname.
            source_def.alias = Some(right_alias.to_string());
        }

        self.sources.push(source_def);

        let join_type = if let Some(jtype) = join_def["type"].as_str() {
            match jtype {
                "left" => "LEFT JOIN",
                "right" => "RIGHT JOIN",
                "full" => "FULL JOIN",
                _ => "INNER JOIN",
            }
        } else {
            "INNER JOIN"
        };

        let mut sql = format!(
            r#"{} {} AS "{}" ON ("{}".{} = "{}".{}"#,
            join_type,
            self.force_valid_ident(tablename)?,
            self.force_valid_ident(right_alias)?,
            self.force_valid_ident(right_alias)?,
            self.force_valid_ident(right_join_field.as_deref().unwrap())?,
            self.force_valid_ident(left_alias)?,
            self.force_valid_ident(left_join_field.as_deref().unwrap())?,
        );

        // Some JOINS have filters, which are mini WHERE clauses tacked
        // on to the JOIN.
        let filter = &join_def["filter"];
        if !filter.is_null() {
            let mut op = " AND ";
            if let Some(filter_op) = filter["filter_op"].as_str() {
                if filter_op == "or" {
                    op = " OR ";
                }
            }
            sql += op;
            sql += &self.compile_where_for_class(filter, right_alias, JoinOp::And)?;
        }

        sql += ")";

        // Add nested JOINs if we have any
        let sub_join = &join_def["join"];
        if !sub_join.is_null() {
            sql += " ";
            sql += &self.compile_joins_for_class(right_alias, sub_join)?;
        }

        Ok(sql)
    }

    fn compile_where_for_class(
        &mut self,
        where_def: &JsonValue,
        class_alias: &str,
        join_op: JoinOp,
    ) -> EgResult<String> {
        let mut sql = String::new();
        let and_or: &str = join_op.into();

        if where_def.is_array() {
            if where_def.len() == 0 {
                return Err(format!("Invalid WHERE clause / empty array").into());
            }

            let mut first = true;
            for part in where_def.members() {
                if first {
                    first = false;
                } else {
                    sql += &format!(" {and_or} ");
                }
                let sub_pred = self.compile_where_for_class(part, class_alias, join_op)?;
                sql += &format!("({sub_pred})");
            }

            return Ok(sql);
        } else if where_def.is_object() {
            if where_def.is_empty() {
                return Err(format!("Invalid predicate structure: empty JSON object"))?;
            }

            let mut first = true;
            for (key, sub_blob) in where_def.entries() {
                if first {
                    first = false;
                } else {
                    sql += &format!(" {and_or} ");
                }

                if key.starts_with("+") && key.len() > 1 {
                    // Class alias
                    // E.g. {"+aou": {"shortname": "BR1"}}

                    let alias = &key[1..];
                    let classname = self.get_alias_classname(class_alias)?;

                    if let Some(field) = sub_blob.as_str() {
                        // We verified above this is a valid classname.
                        // Now verif it's a valid field name.
                        if !self
                            .idl
                            .classes()
                            .get(classname)
                            .unwrap()
                            .has_real_field(field)
                        {
                            return Err(
                                format!("Class {classname} has no field named {field}").into()
                            );
                        }

                        // {"+aou": "shortname"} ?
                        // Does this really happen?  I'm missing something.
                        sql += &format!(r#" "{alias}".{field} "#);
                    } else {
                        //

                        let sub_pred = self.compile_where_for_class(sub_blob, alias, join_op)?;
                        sql += &format!("({sub_pred})");
                    }
                } else if key.starts_with("-") {
                    if key == "-or" {
                        let sub_pred =
                            self.compile_where_for_class(sub_blob, class_alias, JoinOp::Or)?;
                        sql += &format!("({sub_pred})");
                    } else if key == "-and" {
                        let sub_pred =
                            self.compile_where_for_class(sub_blob, class_alias, JoinOp::And)?;
                        sql += &format!("({sub_pred})");
                    } else if key == "-not" {
                        let sub_pred =
                            self.compile_where_for_class(sub_blob, class_alias, JoinOp::And)?;
                        sql += &format!("NOT ({sub_pred})");
                    } else if key == "-exists" {
                        // TODO this needs to build a whole new parser
                        //let sub_pred = self.compile_where_for_class(sub_blob, class_alias, JoinOp::And)?;
                        //sql += &format!("EXISTS ( {sub_pred} )");
                    } else if key == "-not-exists" {
                        // TODO this needs to build a whole new parser
                        //let sub_pred = self.compile_where_for_class(sub_blob, class_alias, JoinOp::And)?;
                        //sql += &format!("NOT EXISTS ( {sub_pred} )");
                    }
                } else {
                    // key is assumed to be a field name

                    let classname = self.get_alias_classname(class_alias)?;

                    // classname verified above.
                    // Make sure it's a valid field name
                    if !self
                        .idl
                        .classes()
                        .get(classname)
                        .unwrap()
                        .has_real_field(key)
                    {
                        return Err(format!("Class {classname} has no field called {key}").into());
                    }

                    sql += &self.search_predicate(class_alias, key, sub_blob)?;
                }
            }
        } else {
            return Err(format!("Invalid WHERE structure: {}", where_def.dump()).into());
        }

        Ok(sql)
    }

    fn search_predicate(
        &mut self,
        class_alias: &str,
        field_name: &str,
        field_def: &JsonValue,
    ) -> EgResult<String> {
        if field_def.is_array() {
            // Equality IN search
            self.search_in_predicate(class_alias, field_name, field_def)
        } else if field_def.is_object() {
            if field_def.len() > 1 {
                return Err(format!("Multiple predicates for field: {}", field_def.dump()).into());
            }

            // TODO TODO oils_sql.c:3279

            Ok(String::new()) // TODO
        } else {
            self.simple_search_predicate("=", class_alias, field_name, field_def)
        }
    }

    fn simple_search_predicate(
        &mut self,
        mut operator: &str,
        class_alias: &str,
        field: &str,
        value: &JsonValue,
    ) -> EgResult<String> {
        if !db::is_supported_operator(operator) {
            return Err(format!("Operator '{operator}' not supported").into());
        }

        if value.is_object() || value.is_array() {
            return Err(format!("Invalid simple search predicate: {}", value.dump()).into());
        }

        let prefix = format!(r#""{class_alias}".{field}"#);

        if value.is_null() {
            let val_str = if operator == "=" || operator.to_uppercase() == "IS" {
                "NULL"
            } else {
                "NOT NULL"
            };

            return Ok(format!("{prefix} IS {val_str}"));
        } else if let Some(b) = value.as_bool() {
            let val_str = if b { "TRUE" } else { "FALSE" };

            return Ok(format!("{prefix} {operator} {val_str}"));
        }

        // Numbers and strings from here on out.

        // If the field in question is non-numeric, then we need
        // to treat it as a replaceable parameter.

        let classname = self.get_alias_classname(class_alias)?;

        let idl_class = self
            .idl
            .classes()
            .get(classname)
            .ok_or_else(|| format!("IDL class {classname} not found"))?;

        let idl_field = idl_class
            .get_field(field)
            .ok_or_else(|| format!("IDL class {classname} has no field named {field}"))?;

        if idl_field.datatype().is_numeric() {
            if let Some(num) = value.as_number() {
                // No need to quote numeric parameters for numeric columns.
                Ok(format!("{prefix} {operator} {num}"))
            } else {
                return Err(format!(
                    "Field {field} is numeric, but query value isn't: {}",
                    value.dump()
                )
                .into());
            }
        } else {
            // IDL field is non-numeric but may still contain numeric
            // values.  Quote 'em.

            let idx = self.add_param(value);
            Ok(format!("{prefix} {operator} ${idx}"))
        }
    }

    fn search_in_predicate(
        &mut self,
        class_alias: &str,
        field_name: &str,
        field_def: &JsonValue,
    ) -> EgResult<String> {
        let mut sql = String::new();

        // TODO
        Ok(sql)
    }

    /// Verify the provided string may act as a valid PG identifier.
    fn force_valid_ident<'a>(&'a self, s: &'a str) -> EgResult<&str> {
        if db::is_identifier(s) {
            return Ok(s);
        } else {
            return Err(format!("Value is not a valid identifier: {s}").into());
        }
    }

    /// Adds a query parameter to the pile and increments our
    /// param index.
    fn add_param(&mut self, value: &JsonValue) -> usize {
        let index = self.param_index;
        self.param_index += 1;

        let def = ParamDef {
            index,
            value: value.clone(),
        };

        if let Some(list) = self.params.as_mut() {
            list.push(def);
        } else {
            self.params = Some(vec![def]);
        }

        return index;
    }

    /// Determine the core IDL class from the main FROM clause.
    fn set_base_source(&mut self, from_blob: &JsonValue) -> EgResult<&SourceDef> {
        let classname = if from_blob.is_object() && from_blob.len() == 1 {
            // "from":{"aou": ...}
            let (class, _) = from_blob.entries().next().unwrap();
            class.to_string()
        } else if let Some(class) = from_blob.as_str() {
            // "from": "aou"
            class.to_string()
        } else {
            return Err(format!("Invalid FROM clause: {}", from_blob.dump()).into());
        };

        // Sanity check our results.

        let idl_class = self
            .idl
            .classes()
            .get(&classname)
            .ok_or_else(|| format!("Invalid IDL class: {classname}"))?;

        let tablename = idl_class
            .tablename()
            .ok_or_else(|| format!("Base class requires a tablename"))?;

        // Add our first source
        self.sources.push(SourceDef {
            classname,
            tablename: tablename.to_string(),
            // Base classes cannot have aliases.  (right?)
            alias: None,
            is_base_class: true,
        });

        Ok(self.sources.get(0).unwrap())
    }
}
