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
    ///
    /// Note we only need concern ourselves with Strings because
    /// all other parameter types are included as bare values (bool,
    /// numbers, null) or futher decomposed into number and strings.
    value: String,

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

    /// True if one or more fields have the "aggregate" flag set.
    has_aggregate: bool,

    /// List of fields (by position) to add to the GROUP BY clause.
    group_by: Vec<usize>,

    /// Current index into the list of SELECT'ed fields.
    select_index: usize,
}

impl Clone for JsonQueryCompiler {
    fn clone(self: &JsonQueryCompiler) -> JsonQueryCompiler {
        let mut new = JsonQueryCompiler::new(self.idl.clone());

        new.locale = self.locale.clone();
        new.disable_i18n = self.disable_i18n;
        new.controllername = self.controllername.clone();

        new
    }
}

impl JsonQueryCompiler {
    pub fn new(idl: Arc<idl::Parser>) -> Self {
        Self {
            idl,
            locale: None,
            controllername: None,
            sources: Vec::new(),
            query_string: None,
            disable_i18n: false,
            params: None,
            param_index: 1,
            group_by: Vec::new(),
            has_aggregate: false,
            select_index: 0,
        }
    }

    /// Returns a list of parameter values as strs.
    ///
    /// Note we only have to concern ourselves with Strings because
    /// all other parameter types are included as bare values (numbers)
    /// or futher decomposed into number and strings.
    pub fn query_params(&self) -> Vec<&str> {
        if let Some(params) = self.params.as_ref() {
            params.iter().map(|p| p.value.as_str()).collect()
        } else {
            vec![]
        }
    }

    /// Stringified JSON array of parameter indexes and values.
    pub fn debug_params(&self) -> String {
        let mut array = json::array![];
        if let Some(params) = self.params.as_ref() {
            for param in params {
                let mut obj = json::object! {};
                obj[format!("${}", param.index)] = json::from(param.value.as_str());
                array.push(obj);
            }
        }

        array.dump()
    }

    /// KLUDGE: Generates the (likely) SQL that will run on the server.
    ///
    /// Parameter replacement for executed queries occurs in the PG
    /// server, which this module does not have direct access to.
    /// This is for debugging purpose only.
    pub fn debug_query_kludge(&self) -> String {
        let mut sql = match self.query_string.as_ref() {
            Some(s) => s.to_string(),
            None => return String::new(),
        };

        if let Some(params) = self.params.as_ref() {
            // Iterate params in reverse so we're replacing larger
            // paramters indexes first.  This way replace('$1') does not
            // affect $10, $11, etc. values.
            for param in params.iter().rev() {
                let target = format!("${}", param.index);

                // Parameters will always be numbers or strings.
                let mut value = param.value.to_string();

                value = value.replace("'", "''"); // pesky single quotes

                sql = sql.replace(&target, &format!("'{value}'"));
            }
        }

        sql
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

    pub fn take_query_string(&mut self) -> Option<String> {
        self.query_string.take()
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
            .ok_or_else(|| format!("No such class alias: {alias}").into())
    }

    fn get_idl_class(&self, classname: &str) -> EgResult<&idl::Class> {
        self.idl
            .classes()
            .get(classname)
            .ok_or_else(|| format!("Invalid IDL class: {classname}").into())
    }

    fn get_base_source(&self) -> EgResult<&SourceDef> {
        self.sources
            .iter()
            .filter(|s| s.is_base_class)
            .next()
            .ok_or_else(|| format!("No bass class has been set").into())
    }

    /// Returns the IDL classname of the base class, i.e. the root
    /// class of the FROM clause.
    fn get_base_classname(&self) -> EgResult<&str> {
        self.sources
            .iter()
            .filter(|s| s.is_base_class)
            .map(|s| s.classname.as_ref())
            .next()
            .ok_or_else(|| format!("No bass class has been set").into())
    }

    /// Returns option of IDL field if the field is valid exists on the
    /// class, isn't virtual, and may be viewed by this module.
    fn field_may_be_selected(&self, name: &str, class: &str) -> bool {
        let idl_class = match self.get_idl_class(class) {
            Ok(c) => c,
            Err(_) => return false,
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

    /// Entry point for compiling JSON-query into SQL.
    ///
    /// The resulting SQL may be found in self.query_string() and
    /// the resulting query parameters may be found in self.query_params();
    pub fn compile(&mut self, query: &JsonValue) -> EgResult<()> {
        if !query.is_object() {
            return Err(format!("json_query must be a JSON hash").into());
        }

        if util::json_bool(&query["no_i18n"]) {
            self.disable_i18n = true;
        }

        if util::json_bool(&query["distinct"]) {
            self.has_aggregate = true;
        }

        // TODO union, intersect, except

        if query["from"].is_array() {
            let func_str = self.compile_function_query(&query["from"])?;
            self.query_string = Some(func_str);
            return Ok(());
        }

        // Clone the source to avoid a number of parellel mut's below.
        let base_source = self.set_base_source(&query["from"])?.clone();
        let cname = &base_source.classname;

        // Compile JOINs first so we can populate our sources.
        let from_str = self.compile_joins_for_class(cname, &query["from"][cname])?;

        let sel_str = self.compile_selects(&query["select"])?;
        let where_str = self.compile_where_for_class(&query["where"], cname, JoinOp::And)?;

        let mut sql = format!(
            r#"SELECT {sel_str} FROM {} AS "{}" {from_str} WHERE {where_str}"#,
            self.check_identifier(&base_source.tablename)?,
            self.check_identifier(base_source.alias.as_deref().unwrap_or(cname))?,
        );

        if self.has_aggregate {
            let positions: Vec<String> = self.group_by.iter().map(|n| format!("{n}")).collect();
            sql += &format!(" GROUP BY {}", positions.join(", "));
        }

        self.query_string = Some(sql);

        Ok(())
    }

    fn compile_selects(&mut self, select_def: &JsonValue) -> EgResult<String> {
        if select_def.is_null() {
            let cn = self.get_base_classname()?.to_string(); // parallel mutes

            // If we have no SELECT clause at all, just select the default fields.
            return self.build_default_select_list(&cn);
        } else if !select_def.is_object() {
            // The root SELECT clause is a map of classname (or alias) to field list
            return Err(format!("Invalid SELECT clause: {select_def}").into());
        }

        let mut sql = String::new();
        for (alias, payload) in select_def.entries() {
            sql += " ";
            sql += &self.compile_selects_for_class(alias, payload)?;
            sql += ",";
        }

        if sql.len() > 0 {
            sql.remove(0); // first space
            sql.pop(); // final comma
        }

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
                        "{}",
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

            let column = field_struct["column"]
                .as_str()
                .ok_or_else(|| format!("SELECT hash requires a 'column': {field_struct}"))?;

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
            sql.remove(0); // first space
            sql.pop(); // final comma
        }

        Ok(sql)
    }

    fn build_default_select_list(&mut self, alias: &str) -> EgResult<String> {
        let classname = self.get_alias_classname(alias)?.to_string(); // mut's

        // If we have an alias it's known to be valid
        let idl_class = self.get_idl_class(&classname)?;

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

        if sql.len() > 0 {
            sql.remove(0); // first space
            sql.pop(); // final comma
        }

        Ok(sql)
    }

    // TODO a way to call this function without appending to the group-by
    // since it's also called e.g. when building predicates.
    fn select_one_field(
        &mut self,
        class_alias: &str,
        field_alias: Option<&str>,
        field_name: &str,
        field_def: Option<&JsonValue>,
    ) -> EgResult<String> {
        self.select_index += 1; // TODO

        let idl_class = self.get_idl_class(self.get_alias_classname(class_alias)?)?;
        let idl_class = idl_class.clone(); // TODO parallel mut's

        let idl_field = idl_class
            .fields()
            .get(field_name)
            .ok_or_else(|| format!("Invalid field {}::{field_name}", idl_class.classname()))?;

        let mut is_aggregate = false;

        if let Some(fdef) = field_def {
            // If we have a field_def, it may mean the field has extended
            // properties, like a transform or other flags.

            // Do we support aggregate functions?  Maybe.
            is_aggregate = util::json_bool(&fdef["aggregate"]);

            if let Some(xform) = fdef["transform"].as_str() {
                let mut sql = String::new();

                sql += &self.check_identifier(xform)?;
                sql += "(";

                if util::json_bool(&fdef["distinct"]) {
                    sql += "DISTINCT ";
                }

                // Avoid sending the field alias here since any alias
                // should apply to our transform as a whole.
                sql += &self.format_one_select_field(class_alias, &idl_class, None, idl_field)?;

                for param in fdef["params"].members() {
                    let index = self.add_param(param)?;
                    sql += &format!(", ${index}");
                }

                sql += ")";

                if let Some(rfield) = fdef["result_field"].as_str() {
                    // Append (...).xform_result_field.
                    sql = format!(r#"({sql})."{}""#, self.check_identifier(rfield)?);
                } else if let Some(alias) = field_alias {
                    sql += &format!(r#" AS "{}""#, self.check_identifier(alias)?);
                }

                if is_aggregate {
                    self.has_aggregate = true;
                } else {
                    self.group_by.push(self.select_index);
                }

                return Ok(sql);
            }
        }

        if is_aggregate {
            self.has_aggregate = true;
        } else {
            self.group_by.push(self.select_index);
        }

        self.format_one_select_field(class_alias, &idl_class, field_alias, idl_field)
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
                self.check_identifier(class_alias)?,
                self.check_identifier(idl_field.name())?
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
                self.check_identifier(tablename)?,
                self.check_identifier(class_alias)?,
                self.check_identifier(idl_field.name())?,
                self.check_identifier(pkey)?,
                self.check_identifier(class_alias)?,
                self.check_identifier(pkey)?,
            );
        }

        if let Some(alias) = field_alias {
            sql += &format!(r#" AS "{}""#, self.check_identifier(alias)?);
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
        // If there's no "class" in the hash, the alias is the classname
        let right_class = join_def["class"].as_str().unwrap_or(right_alias);
        let right_idl_class = self.get_idl_class(right_class)?;

        let left_class = self.get_alias_classname(left_alias)?;
        let left_idl_class = self.get_idl_class(left_class)?;

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
            .ok_or_else(|| format!("JOINed class has no table name: {right_class}"))?
            .to_string();

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
            self.check_identifier(&tablename)?,
            self.check_identifier(right_alias)?,
            self.check_identifier(right_alias)?,
            self.check_identifier(right_join_field.as_deref().unwrap())?,
            self.check_identifier(left_alias)?,
            self.check_identifier(left_join_field.as_deref().unwrap())?,
        );

        // ----
        // Add this new class to our list of sources before we
        // potentially start adding recursive JOINs.
        let mut source_def = SourceDef {
            classname: right_class.to_string(),
            tablename,
            alias: None,
            is_base_class: false,
        };

        if right_alias != right_class {
            // No need to allocate/track an alias if it's the same
            // as the classname.
            source_def.alias = Some(right_alias.to_string());
        }

        self.sources.push(source_def);
        // ----

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
                    sql += " ";
                    sql += and_or;
                    sql += " ";
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
                    sql += " ";
                    sql += and_or;
                    sql += " ";
                }

                if key.starts_with("+") && key.len() > 1 {
                    // Class alias
                    // E.g. {"+aou": {"shortname": "BR1"}}

                    let alias = &key[1..];
                    let classname = self.get_alias_classname(class_alias)?;

                    if let Some(field) = sub_blob.as_str() {
                        // {"+aou": "shortname"} ?
                        // Does this really happen?  I'm missing something.

                        if !self.get_idl_class(classname)?.has_real_field(field) {
                            return Err(
                                format!("Class {classname} has no field named {field}").into()
                            );
                        }

                        sql += &format!(r#" "{alias}".{field} "#);
                    } else {
                        // {"+aou": {"shortname": ...}}

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
                    } else if key == "-exists" || key == "-not-exists" {
                        // EXIST queries run atop a fully formed json query
                        // object.  Collect the SQL via a single-use sub-compiler.
                        let mut compiler = self.clone();

                        compiler.compile(sub_blob)?;

                        let sub_sql = compiler.take_query_string().ok_or_else(|| {
                            format!("EXISTS clause produced no SQL: {}", sub_blob)
                        })?;

                        // Absorb parameter information collected by
                        // our sub-compiler.
                        if let Some(params) = compiler.params.as_ref() {
                            for param in params {
                                self.add_param_string(param.value.clone());
                            }
                        }

                        let question = if key.contains("not") {
                            "NOT EXISTS"
                        } else {
                            "EXISTS"
                        };

                        sql += &format!("{question} ({sub_sql})");
                    }
                } else {
                    // key is assumed to be a field name

                    let classname = self.get_alias_classname(class_alias)?;

                    // classname verified above.
                    // Make sure it's a valid field name
                    if !self.get_idl_class(classname)?.has_real_field(key) {
                        return Err(format!("Class {classname} has no field called {key}").into());
                    }

                    sql += &self.search_predicate(class_alias, key, sub_blob)?;
                }
            }
        } else {
            return Err(format!("Invalid WHERE structure: {where_def}").into());
        }

        Ok(sql)
    }

    /// Does the provided field match some value?
    /// Value may be a simple thing, like a string, or a more complex
    /// comparison (IN list, between, etc.)
    ///
    /// Examples:
    ///
    /// {"shortname": "BR1"}
    /// {"shortname": ["BR1", "BR2"]}
    /// {"shortname": {"in": ["BR1", "BR2"]}}
    /// {"shortname": {"not in": {"select": ...}}}
    fn search_predicate(
        &mut self,
        class_alias: &str,
        field_name: &str,
        value_def: &JsonValue,
    ) -> EgResult<String> {
        if value_def.is_array() {
            // Equality IN search
            self.search_in_predicate(class_alias, field_name, value_def, false)
        } else if value_def.is_object() {
            if value_def.len() != 1 {
                return Err(format!(
                    "Invalid search predicate for field: {field_name} {value_def}",
                )
                .into());
            }

            let (key, sub_def) = value_def.entries().next().unwrap(); // above

            if key == "between" {
                self.search_between_predicate(class_alias, field_name, sub_def)
            } else if key == "in" || key == "not in" {
                self.search_in_predicate(class_alias, field_name, sub_def, key.contains("not"))
            } else if sub_def.is_array() {
                self.search_function_predicate(key, class_alias, field_name, sub_def)
            } else if sub_def.is_object() {
                self.search_field_transform_predicate(key, class_alias, field_name, sub_def)
            } else {
                self.simple_search_predicate(key, class_alias, field_name, sub_def)
            }
        } else {
            self.simple_search_predicate("=", class_alias, field_name, value_def)
        }
    }

    /// Compiles a variety of somefield-someoprator-somevalue scenarios.
    ///
    /// Examples (the inner {...}):
    ///
    /// {"label": {">=": {"transform": "oils_text_as_bytea", "value": ["oils_text_as_bytea", "ABC"]}}
    fn search_field_transform_predicate(
        &mut self,
        operator: &str,
        class_alias: &str,
        field_name: &str,
        value_def: &JsonValue,
    ) -> EgResult<String> {
        let field_str = self.select_one_field(class_alias, None, field_name, Some(value_def))?;

        println!(
            "search_field_transform_predicate() {field_name} {operator} {}",
            value_def.dump()
        );

        let value_obj = &value_def["value"];

        let mut extra_parens = false;

        let value_str = if value_obj.is_null() {
            extra_parens = true;
            self.compile_where_for_class(value_def, class_alias, JoinOp::And)?
        } else if value_obj.is_array() {
            self.compile_function_from(value_obj)?
        } else if value_obj.is_object() {
            extra_parens = true;
            self.compile_where_for_class(value_obj, class_alias, JoinOp::And)?
        } else if value_obj.is_string() || value_obj.is_number() {
            self.scalar_param_as_string(class_alias, field_name, value_obj)?
        } else {
            return Err(format!(
                "Invalid predicate for field transform for {field_name}: {}",
                value_obj.dump()
            )
            .into());
        };

        let left_parens = if extra_parens { "(" } else { "" };
        let right_parens = if extra_parens { ")" } else { "" };

        Ok(format!(
            r#"{}{} {} {}{}{}{}"#,
            left_parens,
            field_str,
            self.check_operator(operator)?,
            left_parens,
            value_str,
            right_parens,
            right_parens
        ))
    }

    /// Encode a function call as the right-hand part of a WHERE entry.
    ///
    /// Examples:
    ///
    /// ["actor.org_unit_ancestor_setting_batch", "4", "{circ.course_materials_opt_in}"]
    ///
    /// Output:
    ///
    /// "aou".id = some.function()
    fn search_function_predicate(
        &mut self,
        operator: &str,
        class_alias: &str,
        field_name: &str,
        value_def: &JsonValue,
    ) -> EgResult<String> {
        let func_str = self.compile_function_from(value_def)?;

        Ok(format!(
            r#""{class_alias}".{field_name} {} {func_str}"#,
            self.check_operator(operator)?,
        ))
    }

    /// Compiles a BETWEEN search.
    ///
    /// Examples (but really just the array part):
    ///
    /// {"somefield": {"between": [123, 456]}}
    fn search_between_predicate(
        &mut self,
        class_alias: &str,
        field_name: &str,
        value_def: &JsonValue,
    ) -> EgResult<String> {
        let value_def = if !value_def["value"].is_null() {
            // Could be a field transformed w/ a function
            &value_def["value"]
        } else {
            value_def
        };

        if !value_def.is_array() || value_def.len() != 2 {
            return Err(format!("Invalid BETWEEN clause for {field_name}: {value_def}").into());
        }

        Ok(format!(
            "{} BETWEEN {} AND {}",
            self.select_one_field(class_alias, None, field_name, Some(value_def))?,
            self.scalar_param_as_string(class_alias, field_name, &value_def[0])?,
            self.scalar_param_as_string(class_alias, field_name, &value_def[1])?
        ))
    }

    /// This is your class a.b = 'c' scenario.
    ///
    /// Examples:
    ///
    /// {"somefield": {"is not": null}}
    /// {"somefield": "foobar"}
    /// {"somefield": true}
    fn simple_search_predicate(
        &mut self,
        mut operator: &str,
        class_alias: &str,
        field_name: &str,
        value: &JsonValue,
    ) -> EgResult<String> {
        if value.is_object() || value.is_array() {
            return Err(format!("Invalid simple search predicate: {}", value.dump()).into());
        }

        let prefix = format!(r#""{class_alias}".{field_name}"#);

        if value.is_null() {
            let val_str = if operator == "=" || operator.to_uppercase() == "IS" {
                "NULL"
            } else {
                "NOT NULL"
            };

            return Ok(format!("{prefix} IS {val_str}"));
        } else if let Some(b) = value.as_bool() {
            let val_str = if b { "TRUE" } else { "FALSE" };

            let oper_str = if operator == "=" || operator.to_uppercase() == "IS" {
                "IS"
            } else {
                "IS NOT"
            };

            return Ok(format!("{prefix} {oper_str} {val_str}"));
        }

        let param_str = self.scalar_param_as_string(class_alias, field_name, value)?;

        // Numbers and strings from here on out.
        Ok(format!(
            "{prefix} {} {param_str}",
            self.check_operator(operator)?,
        ))
    }

    /// Encode a String or Number parameter value as a String suitable
    /// for including in the main SQL string.
    ///
    /// Values that requires quoting are added as replaceable parameters.
    ///
    /// Results in an error if the value is not appropriate for the
    /// field, e.g. a numeric field compared to a non-numeric string value.
    ///
    /// Examples:
    /// 1
    /// "1" -- will be parameterized and eventually quoted
    fn scalar_param_as_string(
        &mut self,
        class_alias: &str,
        field_name: &str,
        value: &JsonValue,
    ) -> EgResult<String> {
        if !value.is_string() && !value.is_number() {
            return Err(format!("Invalid scalar value for field {field_name}: {value}").into());
        }

        // If the field in question is non-numeric, then we need
        // to treat it as a replaceable parameter.
        let classname = self.get_alias_classname(class_alias)?;
        let idl_class = self.get_idl_class(classname)?;

        let idl_field = idl_class
            .get_field(field_name)
            .ok_or_else(|| format!("IDL class {classname} has no field named {field_name}"))?;

        if idl_field.datatype().is_numeric() {
            // No need to quote numeric parameters for numeric columns.

            if let Some(num) = value.as_number() {
                Ok(num.to_string())
            } else if let Ok(num) = util::json_int(&value) {
                // Handle cases where we receive numeric values as JSON strings.
                Ok(num.to_string())
            } else if let Ok(num) = util::json_float(&value) {
                // Handle cases where we receive numeric values as JSON strings.
                Ok(num.to_string())
            } else {
                return Err(format!(
                    "Field {field_name} is numeric, but query value isn't: {value}",
                )
                .into());
            }
        } else {
            // IDL field is non-numeric.  Quote the param.
            Ok(format!("${}", self.add_param(value)?))
        }
    }

    /// Compiles an IN clause.
    ///
    /// Examples:
    ///
    /// {"somefield": [1, 2, 3, 4]}
    /// {"somefield": {"not in": [1, 2, 3, 4]}}
    /// {"somefield": {"in": {"select": {"au":["id"]}, "from", ...}}}
    fn search_in_predicate(
        &mut self,
        class_alias: &str,
        field_name: &str,
        value_def: &JsonValue,
        is_not_in: bool,
    ) -> EgResult<String> {
        println!("search_in_predicate() {field_name} = {}", value_def.dump());
        let field_str = self.select_one_field(class_alias, None, field_name, Some(value_def))?;
        let in_str = self.search_in_list(class_alias, field_name, value_def)?;

        Ok(format!(
            "{field_str} {} ({in_str})",
            if is_not_in { "NOT IN" } else { "IN" }
        ))
    }

    /// Compiles right-hand part of an IN clause.
    ///
    /// Examples (minus the outermost container):
    ///
    /// {"somefield": [1, 2, 3, 4]}
    /// {"somefield": {"not in": [1, 2, 3, 4]}}
    /// {"somefield": {"in": {"select": {"au":["id"]}, "from", ...}}}
    fn search_in_list(
        &mut self,
        class_alias: &str,
        field_name: &str,
        value_def: &JsonValue,
    ) -> EgResult<String> {
        if !value_def.is_object() && !value_def.is_array() {
            return Err(format!("Unexpected IN clause: {value_def}").into());
        }

        let value_def = if !value_def["value"].is_null() {
            &value_def["value"]
        } else {
            value_def
        };

        println!("search_in_list() {field_name} = {}", value_def.dump());

        if value_def.is_object() {
            // Some IN queries run atop a fully formed json query
            // object.  Collect the SQL via a single-use sub-compiler.
            let mut compiler = self.clone();

            compiler.compile(value_def)?;

            let sub_sql = compiler
                .take_query_string()
                .ok_or_else(|| format!("IN clause produced no SQL: {}", value_def))?;

            // Absorb parameter information collected by
            // our sub-compiler.
            if let Some(params) = compiler.params.as_ref() {
                for param in params {
                    self.add_param_string(param.value.clone());
                }
            }

            return Ok(sub_sql);
        }

        if value_def.len() == 0 {
            return Err(format!("Empty IN list for field {field_name}"))?;
        }

        let mut sql = String::new();

        for value in value_def.members() {
            sql += " ";
            sql += &self.scalar_param_as_string(class_alias, field_name, value)?;
            sql += ","
        }

        if sql.len() > 0 {
            sql.remove(0); // first space
            sql.pop(); // final comma
        }

        Ok(sql)
    }

    /// Verify the provided string may act as a valid PG identifier.
    ///
    /// Returns the source value on success for convenience.
    fn check_identifier<'a>(&'a self, s: &'a str) -> EgResult<&str> {
        if db::is_identifier(s) {
            Ok(s)
        } else {
            Err(format!("Value is not a valid identifier: {s}").into())
        }
    }

    /// Verify the provided string may act as a valid SQL operator
    ///
    /// Returns the source value on success for convenience.
    fn check_operator<'a>(&'a self, operator: &'a str) -> EgResult<&str> {
        if db::is_supported_operator(operator) {
            Ok(operator)
        } else {
            Err(format!("Invalid operator: {operator}").into())
        }
    }

    /// See add_param_string()
    ///
    /// The value parameter Must be a String or Number.
    fn add_param(&mut self, value: &JsonValue) -> EgResult<usize> {
        let s = util::json_string(value)?;
        Ok(self.add_param_string(s))
    }

    /// Adds a new query parameter and increments our param index.
    ///
    /// At SQL compile time, parameter values that require escaping
    /// (i.e. String-ish things) are encoded as numeric placeholders
    /// ($1, $2, ...).
    ///
    /// At query execution time, parameter values are passed to the
    /// DB for runtime compilation and string quoting.
    fn add_param_string(&mut self, value: String) -> usize {
        let index = self.param_index;
        self.param_index += 1;

        let def = ParamDef {
            index,
            value: value,
        };

        if let Some(list) = self.params.as_mut() {
            list.push(def);
        } else {
            self.params = Some(vec![def]);
        }

        return index;
    }

    /// Get the core IDL class from the main FROM clause.
    ///
    /// Examples:
    ///
    /// {"acp": {"acn": {"join": {"bre": ... }}}
    fn set_base_source(&mut self, from_blob: &JsonValue) -> EgResult<&SourceDef> {
        let classname = if from_blob.is_object() && from_blob.len() == 1 {
            // "from":{"aou": ...}
            let (class, _) = from_blob.entries().next().unwrap();
            class.to_string()
        } else if let Some(class) = from_blob.as_str() {
            // "from": "aou"
            class.to_string()
        } else {
            return Err(format!("Invalid FROM clause: {from_blob}").into());
        };

        // Sanity check our results.

        let idl_class = self.get_idl_class(&classname)?;

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

    /// Compile a (sub-)query which is simply a function call.
    ///
    /// Examples:
    ///
    /// {"from": ["actor.org_unit_ancestor_setting_batch", "4", "{circ.course_materials_opt_in}"]}
    fn compile_function_query(&mut self, from_def: &JsonValue) -> EgResult<String> {
        let from_str = self.compile_function_from(from_def)?;

        // This is verified in compile_function_from().
        let func_name = from_def[0].as_str().unwrap();

        Ok(format!(r#"SELECT * FROM {from_str} AS "{func_name}""#))
    }

    /// Compiles the FROM component of a function call array.
    ///
    /// Examples:
    ///
    /// ["actor.org_unit_ancestor_setting_batch", "4", "{circ.course_materials_opt_in}"]
    fn compile_function_from(&mut self, from_def: &JsonValue) -> EgResult<String> {
        if from_def.len() == 0 || !from_def.is_array() {
            return Err(format!("Invalid FROM function spec: {}", from_def.dump()).into());
        }

        let mut func_name = match from_def[0].as_str() {
            Some(f) => self.check_identifier(f)?.to_string(),
            None => return Err(format!("Invalid function name: {}", from_def[0].dump()).into()),
        };

        let mut param_str = String::new();

        if from_def.len() > 1 {
            let mut first = true;

            for value in from_def.members() {
                if first {
                    // Function name
                    first = false;
                    continue;
                }

                param_str += " ";

                if value.is_null() {
                    param_str += "NULL";
                } else if let Some(b) = value.as_bool() {
                    param_str += if b { "TRUE" } else { "FALSE" };
                } else if let Some(s) = value.as_str() {
                    let index = self.add_param(&value)?;
                    param_str += &format!("${index}");
                } else if value.is_number() {
                    param_str += &format!("{}", value.dump());
                } else {
                    return Err(format!("Invalid function parameter: {}", value.dump()).into());
                };

                param_str += ",";
            }

            if param_str.len() > 0 {
                param_str.remove(0); // first space
                param_str.pop(); // final comma
            }
        }

        Ok(format!("{func_name}({param_str})"))
    }
}
