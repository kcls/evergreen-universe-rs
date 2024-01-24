///! JSON Query Parser
use crate::db;
use crate::idl;
use crate::util;
use crate::result::EgResult;
use json::JsonValue;
use std::sync::Arc;

const DEFAULT_LOCALE: &str = "en-US";

#[derive(Debug)]
pub struct SourceDef {
    is_base_class: bool,
    classname: String,
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
            params: None,
            param_index: 1,
        }
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
            return Ok(())
        }

        self.set_base_class(&query["from"])?;

        // Compile JOINs first so we can collect the remaining
        // table sources.
        let join_str = self.compile_joins(&query["from"][self.get_base_classname()?])?;

        /*
        if let Some(classname) = self.base_class.as_ref() {
            let classname = classname.clone(); // parallel mutables
            self.compile_joins(&query["from"][&classname], &classname)?;
        }
        */

        /*

        if let Some(from_func) = self.from_function.as_ref() {
            // maybe shortcut this and exit this function early.
            //
            // TODO self.compile_from_select()?;
            // TODO searchValueTransform
        } else {
            self.compile_select(&query["select"])?;
            sql += &self.selects_to_sql()?;

            // base_class with a tablename is guaranteed here.
            let cc = self.get_base_class();
            sql += &format!(
                r#" FROM {} AS "{}""#,
                cc.tablename().as_ref().unwrap(),
                cc.classname()
            );
        }

        if let Some(join_sql) = self.joins_to_sql()? {
            sql += &join_sql;
        }

        if !query["where"].is_null() && self.base_class.is_some() {
            let alias = self.get_base_classname().to_string();

            // TODO separate WHERE unpacking and compilation.
            sql += " WHERE ";
            sql += &self.compile_where(&query["where"], &alias, WhereJoinOp::And)?;
        }

        // TODO GROUP BY (reminder: aggregates)
        // TODO ORDER BY

        */

        self.query_string = Some(
            format!(" ... {join_str}")
        );

        Ok(())
    }

    /// Unpack the JOIN clauses into their constituent parts.
    fn compile_joins(&mut self, joins: &JsonValue) -> EgResult<String> {
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

        let left_alias = self.get_base_classname()?.to_string();
        for join_entry in join_list {
            let mut hash_binding;

            let hash_ref = if let Some(class) = join_entry.as_str() {
                hash_binding = class_to_hash(class);
                &hash_binding
            } else {
                join_entry
            };

            for (right_alias, join_def) in hash_ref.entries() {
                sql += &self.add_one_join(&left_alias, right_alias, join_def)?;
            }
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
                .ok_or_else(||
                    format!("No such link {rfield_name} for class {right_class}")
                )?;

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

            let idl_link = left_idl_class.links().get(lfield_name)
                .ok_or_else(|| {
                    format!("No such link {lfield_name} for class {left_class}")
                })?;

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
                return Err(
                    format!("Could not find link between classes {left_class} and {right_class}").into(),
                );
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

        // TODO filters / params

        sql += ") ";

        // Add this new class to our list of sources.
        let mut source_def = SourceDef {
            classname: right_class.to_string(),
            alias: None,
            is_base_class: false,
        };

        if right_alias != right_class {
            // No need to allocate/track an alias if it's the same
            // as the classname.
            source_def.alias = Some(right_alias.to_string());
        }

        self.sources.push(source_def);

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

    /// Returns option of IDL field if the field is valid for the class,
    /// isn't virtual, and may be viewed by this module.
    fn field_may_be_selected(&self, name: &str, class: &str) -> Option<&idl::Field> {
        let idl_class = match self.idl.classes().get(class) {
            Some(c) => c,
            None => return None,
        };

        let idl_field = match idl_class.fields().get(name) {
            Some(f) => f,
            None => return None,
        };

        if idl_field.is_virtual() {
            return None;
        }

        if let Some(suppress) = idl_field.suppress_controller() {
            if let Some(module) = self.controllername.as_ref() {
                if suppress.contains(module) {
                    // Field is not visible to this module.
                    return None;
                }
            }
        }

        Some(idl_field)
    }

    /// Determine the core IDL class from the main FROM clause.
    fn set_base_class(&mut self, from_blob: &JsonValue) -> EgResult<()> {

        let classname =  if from_blob.is_object() && from_blob.len() == 1 {
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

        if self.idl.classes().get(&classname).is_none() {
            return Err(format!("Invalid IDL class: {classname}").into());
        }

        self.sources.push(SourceDef {
            classname,
            // Base classes cannot have aliases.  (right?)
            alias: None,
            is_base_class: true,
        });

        Ok(())
    }


    /// Creates a default list of columns to select from an alias'ed IDL
    /// class.
    fn add_default_select_list(&mut self, alias: &str) -> EgResult<()> {
        /*
        let classname = self
            .find_alias(alias)
            .ok_or_else(|| format!("Unknown SELECT alias: {alias}"))?;

        // If we have an alias it's known to be valid
        let idl_class = self.idl.classes().get(classname).unwrap();

        let def = SelectDef {
            classname: idl_class.classname().to_string(),
            alias: alias.to_string(),
            fields: idl_class
                .real_fields_sorted()
                .iter()
                .filter(|f| self.field_may_be_selected(f.name(), classname).is_some())
                .map(|f| SelectFieldDef {
                    name: f.name().to_string(),
                    alias: None,
                    i18n_required: f.i18n(),
                    aggregate: false,
                    distinct: false,
                    transform: None,
                    transform_result_field: None,
                    transform_params: None,
                })
                .collect(),
        };

        self.add_select(def);
        */

        Ok(())
    }

}
