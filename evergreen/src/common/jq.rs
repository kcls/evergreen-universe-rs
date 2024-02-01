///! JSON Query Parser
use crate::db;
use crate::idl;
use crate::result::EgResult;
use crate::util;
use json::JsonValue;
use std::sync::Arc;

/// See set_locale()
const DEFAULT_LOCALE: &str = "en-US";
const JOIN_WITH_AND: &str = "AND";
const JOIN_WITH_OR: &str = "OR";

/// Models an IDL class used as a data source.
///
/// Data for a class may come from its associated databsase table
/// or from an inline SQL query.
#[derive(Debug, Clone)]
pub struct SourceDef {
    classname: String,
    alias: Option<String>,
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
    params: Option<Vec<String>>,

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

/// Translates JSON-Query into SQL.
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
            // Every parameter should have a value at compile/execute time.
            params.iter().map(|s| s.as_str()).collect()
        } else {
            vec![]
        }
    }

    /// Stringified JSON array of parameter indexes and values.
    pub fn debug_params(&self) -> String {
        let mut array = json::array![];
        if let Some(params) = self.params.as_ref() {
            for (idx, value) in params.iter().enumerate() {
                let mut obj = json::object! {};

                // Every parameter should have a value at compile/execute time.
                obj[format!("${}", idx + 1)] = json::from(value.as_str());

                array.push(obj).expect("Array is too big??");
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
            // affect $10, $11, etc.
            let mut idx = params.len();
            for value in params.iter().rev() {
                let target = format!("${idx}");

                // Counting down from the top.
                idx -= 1;

                if value.contains("'") {
                    // Escape single quotes
                    let escaped = value.replace("'", "''");
                    sql = sql.replace(&target, &format!("'{escaped}'"));
                } else {
                    sql = sql.replace(&target, &format!("'{value}'"));
                }
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

    /// The final compiled SQL string
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

    /// Get an IDL Class object from its classname.
    fn get_idl_class(&self, classname: &str) -> EgResult<&idl::Class> {
        self.idl
            .classes()
            .get(classname)
            .ok_or_else(|| format!("Invalid IDL class: {classname}").into())
    }

    /// Returns the IDL classname of the base class, i.e. the root
    /// class of the FROM clause.
    fn get_base_classname(&self) -> EgResult<&str> {
        // The base class is the first source.
        self.sources
            .get(0)
            .map(|s| s.classname.as_ref())
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

    /// Entry point for compiling the JSON-Query.
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

        if query["from"].is_array() {
            // {"from": ["actor.org_unit_ancestors", 2, 1]

            let func_str = self.compile_function_query(&query["from"])?;
            self.query_string = Some(func_str);
            return Ok(());
        }

        if query.has_key("union") || query.has_key("except") || query.has_key("intersect") {
            let combo_str = self.compile_combo_query(&query)?;
            self.query_string = Some(combo_str);
            return Ok(());
        }

        self.set_base_source(&query["from"])?;
        let cname = self.get_base_classname()?.to_string(); // mut's

        // Compile JOINs first so we can populate our data sources.
        let join_str = self.compile_joins_for_class(&cname, &query["from"][&cname])?;

        let sel_str = self.compile_selects(&query["select"])?;

        let source_str = self.class_table_or_source_def(&cname)?;

        let where_str = self.compile_where_for_class(&query["where"], &cname, JOIN_WITH_AND)?;

        let mut sql = format!(
            r#"SELECT {sel_str} FROM {source_str} AS "{cname}" {join_str} WHERE {where_str}"#
        );

        if self.has_aggregate {
            let positions: Vec<String> = self.group_by.iter().map(|n| format!("{n}")).collect();
            sql += &format!(" GROUP BY {}", positions.join(", "));
        }

        self.query_string = Some(sql);

        Ok(())
    }

    /// Compiles a UNION, INTERSECT, or EXCEPT query.
    fn compile_combo_query(&mut self, query: &JsonValue) -> EgResult<String> {
        let all = util::json_bool(&query["all"]);
        let qtype;

        let query_array = if query["union"].is_array() {
            qtype = "UNION";
            &query["union"]
        } else if query["except"].is_array() {
            qtype = "EXCEPT";
            &query["except"]
        } else if query["intersect"].is_array() {
            qtype = "INTERSECT";
            &query["intersect"]
        } else {
            return Err(format!("Invalid UNION/INTERSECT/EXCEPT query: {}", query.dump()).into());
        };

        if !query["order_by"].is_null() {
            return Err(format!("ORDER BY not supported for query type: {}", query.dump()).into());
        }

        // At this point we're guaranteed it's an array.
        if query_array.len() < 2 {
            return Err(format!("Invalid query array for query type: {}", query.dump()).into());
        }

        if qtype == "EXCEPT" && query_array.len() > 2 {
            return Err(format!(
                "EXCEPT operator has too many query operands: {}",
                query.dump()
            )
            .into());
        }

        let mut sql = String::new();
        for (idx, hash) in query_array.members().enumerate() {
            if !hash.is_object() {
                return Err(format!("Invalid sub-query for query type: {}", query.dump()).into());
            }

            if idx > 0 {
                sql += " ";
                sql += qtype;
                if all {
                    sql += " ALL ";
                }
            }

            sql += &self.compile_sub_query(hash)?;
        }

        Ok(sql)
    }

    /// Compile a wholly-formed subquery and absorb its parameter values.
    fn compile_sub_query(&mut self, query: &JsonValue) -> EgResult<String> {
        let mut compiler = self.clone();

        compiler.compile(query)?;

        let sub_sql = compiler
            .take_query_string()
            .ok_or_else(|| format!("Sub-query produced no SQL: {}", query.dump()))?;

        if let Some(params) = compiler.params.as_mut() {
            for value in params.drain(..) {
                self.add_param_string(value);
            }
        }

        Ok(sub_sql)
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
                        self.select_one_field(class_alias, None, col, None, true)?
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
                    sql += &self.select_one_field(class_alias, None, column, None, true)?;
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
                true,
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
            sql += &self.select_one_field(alias, None, field_name, None, true)?;
            sql += ","
        }

        if sql.len() > 0 {
            sql.remove(0); // first space
            sql.pop(); // final comma
        }

        Ok(sql)
    }

    /// Format a field, with transform if needed, for inclusion in a
    /// SELECT or WHERE clause entry.
    fn select_one_field(
        &mut self,
        class_alias: &str,
        field_alias: Option<&str>,
        field_name: &str,
        field_def: Option<&JsonValue>,
        // Fields within a query predicate (e.g. WHERE "aou".id = 1)
        // are not part of the SELECT clause and cannot be grouped on.
        handle_group_by: bool,
    ) -> EgResult<String> {
        if handle_group_by {
            self.select_index += 1;
        }

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
                sql += &self.format_one_select_field(class_alias, None, field_name)?;

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

                if handle_group_by {
                    if is_aggregate {
                        self.has_aggregate = true;
                    } else {
                        self.group_by.push(self.select_index);
                    }
                }

                return Ok(sql);
            }
        }

        if handle_group_by {
            if is_aggregate {
                self.has_aggregate = true;
            } else {
                self.group_by.push(self.select_index);
            }
        }

        self.format_one_select_field(class_alias, field_alias, field_name)
    }

    /// Format the SELECT component for a single field, adding the
    /// oils_i18n_xlate() where needed.
    fn format_one_select_field(
        &self,
        class_alias: &str,
        field_alias: Option<&str>,
        field_name: &str,
    ) -> EgResult<String> {
        let mut sql;

        let classname = self.get_alias_classname(class_alias)?;
        let idl_class = self.get_idl_class(classname)?;

        let idl_field = idl_class
            .fields()
            .get(field_name)
            .ok_or_else(|| format!("Invalid field {}::{field_name}", idl_class.classname()))?;

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

            // i18n fields must come from a proper table; no source defs.
            // TODO very has tablename
            let source_str = self.class_table_or_source_def(idl_class.classname())?;

            // Our 'locale' string format is validated at set time.

            sql = format!(
                r#"oils_i18n_xlate('{source_str}', '{}', '{}', '{}', "{}".{}::TEXT, '{locale}')"#,
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

        let join_binding;

        let join_list = if let JsonValue::Array(list) = joins {
            list.iter().collect::<Vec<&JsonValue>>()
        } else if let Some(class) = joins.as_str() {
            join_binding = class_to_hash(class);
            vec![&join_binding]
        } else {
            vec![joins]
        };

        for join_entry in join_list {
            let hash_binding;

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
                            right_join_field = Some(link_key);
                            left_join_field = Some(cur_link.key());
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

        // Table name or subquery wrapped in parens.
        let source_str = self.class_table_or_source_def(right_idl_class.classname())?;

        let mut sql = format!(
            r#"{} {} AS "{}" ON ("{}".{} = "{}".{}"#,
            join_type,
            source_str,
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
            alias: None,
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
            sql += &self.compile_where_for_class(filter, right_alias, JOIN_WITH_AND)?;
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

    /// Returns the SQL representing the data source for an IDL
    /// class.
    ///
    /// Typically this will be a DB table name, but for classes with
    /// a source definition, it will be source SQL wrappen in parens
    /// for inclusion in a containing query.
    fn class_table_or_source_def(&self, classname: &str) -> EgResult<String> {
        if let Some(idl_class) = self.idl.classes().get(classname) {
            if let Some(tablename) = idl_class.tablename() {
                return Ok(self.check_identifier(&tablename)?.to_string());
            } else if let Some(source_def) = idl_class.source_definition() {
                // Wrap the source def in params since it's sub-query.
                return Ok(format!("({source_def})"));
            }
        }

        Err(format!("Class {classname} has no table or source definition").into())
    }

    fn compile_where_for_class(
        &mut self,
        where_def: &JsonValue,
        class_alias: &str,
        join_op: &str,
    ) -> EgResult<String> {
        let mut sql = String::new();

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
                    sql += join_op;
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
                    sql += join_op;
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
                            self.compile_where_for_class(sub_blob, class_alias, JOIN_WITH_OR)?;
                        sql += &format!("({sub_pred})");
                    } else if key == "-and" {
                        let sub_pred =
                            self.compile_where_for_class(sub_blob, class_alias, JOIN_WITH_AND)?;
                        sql += &format!("({sub_pred})");
                    } else if key == "-not" {
                        let sub_pred =
                            self.compile_where_for_class(sub_blob, class_alias, JOIN_WITH_AND)?;
                        sql += &format!("NOT ({sub_pred})");
                    } else if key == "-exists" || key == "-not-exists" {
                        let sub_sql = self.compile_sub_query(sub_blob)?;

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
        } else if where_def.is_null() {
            // A query with no WHERE is valid, but return something to the
            // caller so they don't have to make a special case for, say,
            // an empty string.
            sql = "TRUE".to_string();
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
        let field_str =
            self.select_one_field(class_alias, None, field_name, Some(value_def), false)?;

        let value_obj = &value_def["value"];

        let mut extra_parens = false;

        let value_str = if value_obj.is_null() {
            extra_parens = true;
            self.compile_where_for_class(value_def, class_alias, JOIN_WITH_AND)?
        } else if value_obj.is_array() {
            self.compile_function_from(value_obj)?
        } else if value_obj.is_object() {
            extra_parens = true;
            self.compile_where_for_class(value_obj, class_alias, JOIN_WITH_AND)?
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
            self.select_one_field(class_alias, None, field_name, Some(value_def), false)?,
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
        operator: &str,
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
        let field_str =
            self.select_one_field(class_alias, None, field_name, Some(value_def), false)?;
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

        if value_def.is_object() {
            return self.compile_sub_query(value_def);
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

    /// Adds a new query parameter and returns the index of the new
    /// param for SQL variable replacement.
    ///
    /// At SQL compile time, parameter values that require escaping
    /// (i.e. Strings) are encoded as numeric placeholders
    /// ($1, $2, ...).
    ///
    /// Query parameter indexes are 1-based.
    fn add_param_string(&mut self, value: String) -> usize {
        if let Some(list) = self.params.as_mut() {
            list.push(value);
            list.len()
        } else {
            self.params = Some(vec![value]);
            1
        }
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

        // Add our first source
        self.sources.push(SourceDef {
            classname,
            alias: None,
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

        let func_name = match from_def[0].as_str() {
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
                } else if value.is_string() {
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
