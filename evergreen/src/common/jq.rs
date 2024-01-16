use crate::db;
///! JSON Query Parser
use crate::idl;
use crate::result::EgResult;
use crate::util;
use json::JsonValue;
use std::fmt;
use std::sync::Arc;

const DEFAULT_LOCALE: &str = "en-US";

/// SQL joins
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Left,
    Right,
    Full,
    Inner,
}

impl From<&JoinType> for &str {
    fn from(jt: &JoinType) -> &'static str {
        match *jt {
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "RIGHT JOIN",
            JoinType::Full => "FULL JOIN",
            JoinType::Inner => "INNER JOIN",
        }
    }
}

#[derive(Debug)]
pub struct JoinDef {
    /// IDL classname, e.g. "aou"
    classname: String,

    /// Schema-qualified database table name.
    tablename: String,

    /// Alias for the joined table.  This is typically the same as
    /// the IDL classname, but can be another value, esp. when
    /// joing to the same table multiple times.
    alias: String,

    /// Alias of the joined-to table.
    left_alias: String,

    /// Classname of the joined-to table.
    left_class: String,

    /// Name of the field on the joined table used in the join filter.
    field: Option<String>,

    /// Name of the field on the joined-to table used in the join filter.
    fkey: Option<String>,
    join_type: JoinType,
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

/// A SELECTED field.
#[derive(Debug)]
pub struct FieldDef {
    name: String,
    alias: Option<String>,
    /// True if this is a string that must be loaded via oils_i18n_xlate()
    i18n_required: bool,
    aggregate: bool,
    distinct: bool,

    /// Transform the value with this function
    transform: Option<String>,

    /// Collect the value from this column returned by the function.
    transform_result_field: Option<String>,

    /// Parameters to pass to the transform function.
    transform_params: Option<Vec<ParamDef>>,
}

#[derive(Debug)]
pub struct SelectDef {
    /// IDL classname, e.g. "aou"
    classname: String,
    /// Table alias.
    alias: String,
    /// What fields do we want?
    fields: Vec<FieldDef>,
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

    /// Root IDL class of a JSON query.
    core_class: Option<String>,

    /// If set, we're pulling values from a DB function instead of
    /// SELECTing from a table.
    from_function: Option<String>,

    /// Final compiled SQL string
    query_string: Option<String>,

    /// Parameters passed to the WHERE clause and transform functions.
    params: Option<Vec<String>>,

    /// Unpacked collection of SELECT field lists.
    selects: Option<Vec<SelectDef>>,

    /// Unpacked collection of table JOINs
    joins: Option<Vec<JoinDef>>,

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
            core_class: None,
            from_function: None,
            query_string: None,
            params: None,
            selects: None,
            joins: None,
            param_index: 0,
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

    pub fn params(&self) -> Option<&Vec<String>> {
        self.params.as_ref()
    }

    /// Compile a json_query structure into its constituent parts.
    pub fn compile(&mut self, query: &JsonValue) -> EgResult<()> {
        if !query.is_object() {
            return Err(format!("json_query must be a JSON hash").into());
        }

        // TODO union, intersect, except

        self.set_core_class(&query["from"])?;

        if let Some(classname) = self.core_class.as_ref() {
            let classname = classname.clone(); // parallel mutables
            self.compile_joins(&query["from"][&classname], &classname)?;
        }

        let mut sql = String::new();

        if let Some(from_func) = self.from_function.as_ref() {
            // TODO self.compile_from_select()?;
            // TODO searchValueTransform

        } else {

            self.compile_select(&query["select"])?;
            sql += &self.selects_to_sql()?;

            // core_class with a tablename is guaranteed here.
            let cc = self.get_core_class();
            sql += &format!(
                " FROM {} AS \"{}\"",
                cc.tablename().as_ref().unwrap(),
                cc.classname()
            );
        }

        if !query["where"].is_null() {
            self.compile_where(&query["where"])?;
        }

        if let Some(join_sql) = self.joins_to_sql()? {
            sql += &join_sql;
        }

        // TODO GROUP BY (reminder: aggregates)
        // TODO ORDER BY

        self.query_string = Some(sql);

        Ok(())
    }

    /// Unpacks the WHERE clause into its constituent parts.
    fn compile_where(&mut self, where_struct: &JsonValue) -> EgResult<()> {
        // TODO
        Ok(())
    }

    /// Panics if our core_class is unset or we have an invalid core class.
    /// Generally, core_class is unset if we're compiling a from-func.
    fn get_core_class(&self) -> &idl::Class {
        self.idl
            .classes()
            .get(self.get_core_classname())
            .expect("get_core_class() has no class")
    }

    /// Panics if our core_class is unset
    fn get_core_classname(&self) -> &str {
        self.core_class
            .as_ref()
            .expect("get_core_classname() has no class")
    }

    /// Determine the core IDL class from the main FROM clause.
    /// If this is a function call instead, no core class is set,
    /// and the name of the function is stored.
    fn set_core_class(&mut self, from_blob: &JsonValue) -> EgResult<()> {
        if from_blob.is_object() && from_blob.len() == 1 {
            // "from":{"aou": ...}

            let (class, _) = from_blob.entries().next().unwrap();
            self.core_class = Some(class.to_string());
        } else if from_blob.is_array() {
            // "from": ["my.func", ... ]

            if let Some(func) = from_blob[0].as_str() {
                self.from_function = Some(func.to_string());
            }
        } else if let Some(class) = from_blob.as_str() {
            // "from": "aou"

            self.core_class = Some(class.to_string());
        }

        // Sanity check our results.

        if let Some(class) = self.core_class.as_ref() {
            if self.idl.classes().get(class).is_none() {
                return Err(format!("Invalid IDL class: {class}").into());
            }
        } else if self.from_function.is_none() {
            return Err(format!("Malformed FROM clause: {}", from_blob.dump()).into());
        }

        Ok(())
    }

    /// Unpack the JOIN clauses into their constituent parts.
    fn compile_joins(&mut self, from_blob: &JsonValue, base_classname: &str) -> EgResult<()> {
        let mut join_list: JsonValue;

        if from_blob.is_array() {
            join_list = from_blob.clone();
        } else {
            join_list = json::array![];

            let sub_hash = if let Some(from) = from_blob.as_str() {
                let mut h = json::object! {};
                h[from] = JsonValue::Null;
                h
            } else if from_blob.is_object() {
                from_blob.clone()
            } else {
                return Err(format!(
                    "JOIN failed; expected JSON object/string: {}",
                    from_blob.dump()
                )
                .into());
            };

            join_list.push(sub_hash);
        }

        let left_class = base_classname;
        for list_entry in join_list.members() {
            let mut sub_hash;
            let mut sub_hash_ref = list_entry;

            if let Some(class) = list_entry.as_str() {
                sub_hash = json::object! {};
                sub_hash[class] = JsonValue::Null;
                sub_hash_ref = &sub_hash;
            }

            let mut left_alias;
            let mut left_alias_ref = left_class;
            for (key, val) in sub_hash_ref.entries() {
                left_alias = self.add_one_join(left_class, left_alias_ref, key, val)?;
                left_alias_ref = left_alias.as_ref();
            }
        }

        Ok(())
    }

    /// Unpack one JOIN clause.
    fn add_one_join(
        &mut self,
        left_class: &str,
        left_alias: &str,
        join_alias: &str,
        join_body: &JsonValue,
    ) -> EgResult<String> {
        let join_classname = if let Some(class) = join_body["class"].as_str() {
            class
        } else {
            // If there's no "class" in the hash, the alias is the classname
            join_alias
        };

        let join_class = self
            .idl
            .classes()
            .get(join_classname)
            .ok_or_else(|| format!("No such IDL class in JOIN: {join_classname}"))?;

        let tablename = join_class
            .tablename()
            .ok_or_else(|| format!("Cannot join to a class with no table: {join_classname}"))?;

        let mut join_def = JoinDef {
            classname: join_classname.to_string(),
            alias: join_alias.to_string(),
            left_alias: left_alias.to_string(),
            left_class: left_class.to_string(),
            tablename: tablename.to_string(),
            join_type: JoinType::Inner,
            field: join_body["field"].as_str().map(|s| s.to_string()),
            fkey: join_body["fkey"].as_str().map(|s| s.to_string()),
        };

        if join_def.field.is_some() && join_def.fkey.is_none() {
            // Look up the corresponding join column in the IDL.  The
            // link must be defined in the joined table, and point to
            // the source table.

            let field_name = join_def.field.as_ref().unwrap();
            let idl_link = join_class
                .links()
                .get(field_name)
                .ok_or_else(|| format!("No such link {field_name}"))?;

            let reltype = idl_link.reltype();

            let other_class = idl_link.class();
            if reltype != idl::RelType::HasMany {
                if other_class == left_class {
                    join_def.fkey = Some(idl_link.key().to_string());
                }
            }

            if join_def.fkey.is_none() {
                return Err(format!(
                    "No link defined from {join_classname}::{field_name} to {other_class}"
                )
                .into());
            }
        } else if join_def.field.is_none() && join_def.fkey.is_some() {
            // TODO refactor / duplication

            let fkey_name = join_def.fkey.as_ref().unwrap();
            let left_idl_class = self.idl.classes().get(left_class).unwrap();

            let idl_link = left_idl_class.links().get(fkey_name).ok_or_else(|| {
                format!(
                    "No such link {fkey_name} for class {}",
                    left_idl_class.classname()
                )
            })?;

            let reltype = idl_link.reltype();

            let other_class = idl_link.class();
            if reltype != idl::RelType::HasMany {
                if other_class == join_classname {
                    join_def.field = Some(idl_link.key().to_string());
                }
            }

            if join_def.field.is_none() {
                return Err(format!(
                    "No link defined from {join_classname}::{fkey_name} to {other_class}"
                )
                .into());
            }
        } else if join_def.field.is_none() && join_def.fkey.is_none() {
            let left_idl_class = self.idl.classes().get(left_class).unwrap();

            for (link_key, cur_link) in left_idl_class.links() {
                let other_class = cur_link.class();

                if other_class == join_classname {
                    let reltype = cur_link.reltype();
                    if reltype != idl::RelType::HasMany {
                        join_def.fkey = Some(link_key.to_string());
                        join_def.field = Some(cur_link.key().to_string());
                        break;
                    }
                }
            }

            // Do another search with the classes reversed.
            if join_def.field.is_none() && join_def.fkey.is_none() {
                for (link_key, cur_link) in join_class.links() {
                    let other_class = cur_link.class();

                    if other_class == left_class {
                        let reltype = cur_link.reltype();
                        if reltype != idl::RelType::HasMany {
                            join_def.fkey = Some(link_key.to_string());
                            join_def.field = Some(cur_link.key().to_string());
                            break;
                        }
                    }
                }
            }

            if join_def.field.is_none() && join_def.fkey.is_none() {
                return Err(
                    format!("No link defined between {left_class} and {join_classname}").into(),
                );
            }
        }

        if let Some(join_type) = join_body["type"].as_str() {
            join_def.join_type = match join_type {
                "left" => JoinType::Left,
                "right" => JoinType::Right,
                "full" => JoinType::Full,
                _ => JoinType::Inner,
            };
        }

        if self.joins.is_none() {
            self.joins = Some(vec![join_def]);
        } else {
            self.joins.as_mut().unwrap().push(join_def);
        }

        // TODO filter

        if join_body["join"].is_object() {
            // Add sub-joins
            self.compile_joins(&join_body["join"], join_classname)?;
        }

        Ok(left_alias.to_string())
    }

    /// Verify the provided string may act as a valid PG identifier.
    fn force_valid_ident<'a>(&'a self, s: &'a str) -> EgResult<&str> {
        if db::is_identifier(s) {
            return Ok(s);
        } else {
            return Err(format!("Value is not a valid identifier: {s}").into());
        }
    }

    /// Collect all of our SelectDef entries into a single SQL string.
    fn selects_to_sql(&mut self) -> EgResult<String> {
        let mut sql = format!("SELECT");

        // At this point we have to have something to select.
        let selects = self
            .selects
            .as_ref()
            .ok_or_else(|| format!("selects_to_sql() has no selects"))?;

        for select in selects {
            let idl_class = self.idl.classes().get(&select.classname).unwrap();

            let pkey = idl_class
                .pkey()
                .ok_or_else(|| format!("{} has no primary key", select.classname))?;

            for field in &select.fields {
                if let Some(xform) = field.transform.as_ref() {
                    if field.transform_result_field.is_some() {
                        // So later we can append (...).xform_field.
                        sql += "(";
                    }

                    sql += &format!(" {}(", &self.force_valid_ident(xform)?.to_uppercase());

                    if field.distinct {
                        sql += "DISTINCT ";
                    }

                    // TODO this should also do the i18n dance.  The C code
                    // doesn't either, so I'm guessing it just hasn't come up.

                    sql += &format!(
                        "\"{}\".{}",
                        self.force_valid_ident(&select.alias)?,
                        self.force_valid_ident(&field.name)?
                    );

                    if let Some(params) = field.transform_params.as_ref() {
                        if params.len() > 0 {
                            for _ in params {
                                // Values will be replaced at query execution time.
                                sql += ", ?";
                            }
                        }
                    }

                    sql += ")";

                    if let Some(xform_field) = field.transform_result_field.as_ref() {
                        // Append (...).xform_field.
                        sql += &format!(").\"{}\"", self.force_valid_ident(xform_field)?);
                    }

                    sql += ",";

                    continue;
                }

                if field.i18n_required {
                    let locale = self.locale.as_deref().unwrap_or(DEFAULT_LOCALE);

                    sql += &format!(
                        " oils_i18n_xlate('{}', '{}', '{}', '{}', \"{}\".{}::TEXT, '{}') AS \"{}\",",
                        self.force_valid_ident(&select.classname)?,
                        self.force_valid_ident(&select.alias)?,
                        self.force_valid_ident(&field.name)?,
                        self.force_valid_ident(pkey)?,
                        self.force_valid_ident(&select.alias)?,
                        self.force_valid_ident(pkey)?,
                        locale, // e.g. en-US
                        self.force_valid_ident(field.alias.as_ref().unwrap_or(&field.name))?
                    );

                    continue;
                }

                sql += &format!(
                    " \"{}\".{},",
                    self.force_valid_ident(&select.alias)?,
                    self.force_valid_ident(&field.name)?,
                );
            }
        }

        sql.pop(); // remove final ","

        Ok(sql)
    }

    /// Collect all of our JoinDef entries into a single SQL string.
    fn joins_to_sql(&mut self) -> EgResult<Option<String>> {
        let join_list = match self.joins.as_ref() {
            Some(v) => v,
            None => return Ok(None),
        };

        let mut sql = String::new();

        for join in join_list {
            let fieldname = join
                .field
                .as_ref()
                .ok_or_else(|| format!("JOIN requires a field name"))?;

            let fkey = join
                .fkey
                .as_ref()
                .ok_or_else(|| format!("JOIN requires a fkey value"))?;

            sql += " ";
            sql += (&join.join_type).into(); // Into<&str>

            sql += &format!(
                " {} AS \"{}\" ON ( \"{}\".{} = \"{}\".{}",
                self.force_valid_ident(&join.tablename)?,
                self.force_valid_ident(&join.alias)?,
                self.force_valid_ident(&join.alias)?,
                self.force_valid_ident(&fieldname)?,
                self.force_valid_ident(&join.left_alias)?,
                self.force_valid_ident(fkey)?,
            );

            // TODO JOIN FILTER
            sql += " ) ";
        }

        Ok(Some(sql))
    }

    /// Unpack and generate the SELECT field lists.
    ///
    /// If no SELECT fields are provided, uses the default set.
    fn compile_select(&mut self, select: &JsonValue) -> EgResult<()> {
        if select.is_null() {
            let cn = self.core_class.as_ref().unwrap().to_string(); // parallel mutes
            return self.add_default_select_list(&cn);
        } else if !select.is_object() {
            return Err(format!("Invalid SELECT clause: {}", select.dump()).into());
        }

        for (alias, payload) in select.entries() {
            self.add_selects_for_class(alias, payload)?;
        }

        Ok(())
    }

    /// Generate the SELECT list for one component -- a potentially aliased
    /// IDL class -- of the SELECT clause.
    fn add_selects_for_class(&mut self, alias: &str, payload: &JsonValue) -> EgResult<()> {
        let classname = self
            .find_alias(alias)
            .ok_or_else(|| format!("Unknown SELECT alias: {alias}"))?
            .to_string(); // parallel mutes

        if payload.is_null() {
            return self.add_default_select_list(&classname);
        }

        let mut select_def = SelectDef {
            classname: classname.to_string(),
            alias: alias.to_string(),
            fields: Vec::new(),
        };

        if let Some(col) = payload.as_str() {

            if col == "*" {
                // Wildcard queries use the default select list.
                return self.add_default_select_list(&classname);

            } else {
                // Selecting a single column by name.

                if let Some(idl_field) = self.field_may_be_selected(col, &classname) {
                    select_def.fields.push(FieldDef {
                        name: col.to_string(),
                        alias: None,
                        i18n_required: idl_field.i18n(),
                        aggregate: false,
                        distinct: false,
                        transform: None,
                        transform_result_field: None,
                        transform_params: None,
                    });
                }

                self.add_select(select_def);
            }

            return Ok(());
        }

        if !payload.is_array() {
            return Err(format!("SELECT must be string, null, or array").into());
        }

        // Columns to select are packed in an array.

        for field_struct in payload.members() {

            if let Some(column) = field_struct.as_str() {
                // Field entry is a string field name.

                if let Some(idl_field) = self.field_may_be_selected(column, &classname) {
                    select_def.fields.push(FieldDef {
                        name: column.to_string(),
                        alias: None,
                        i18n_required: idl_field.i18n(),
                        aggregate: false,
                        distinct: false,
                        transform: None,
                        transform_result_field: None,
                        transform_params: None,
                    });
                }
                continue;
            }

            // Here we have a column definition HASH with more SELECT
            // requirements than a simple column name.

            let column = field_struct["column"].as_str().ok_or_else(|| {
                format!("SELECT hash requires a 'column': {}", field_struct.dump())
            })?;

            let idl_field = self
                .field_may_be_selected(column, &classname)
                .ok_or_else(|| {
                    format!(
                    "Field '{column}' does not exist in class '{classname}' or may not be selected")
                })?;

            let i18n_required = idl_field.i18n();

            // Determine the column alias.

            let alias = if let Some(a) = field_struct["alias"].as_str() {
                Some(a.to_string())
            } else if let Some(a) = field_struct["result_field"].as_str() {
                Some(a.to_string())
            } else {
                None
            };

            let mut params: Option<Vec<ParamDef>> = None;
            if field_struct["params"].is_array() {
                let mut list = Vec::new();
                for param in field_struct["params"].members() {
                    let def = ParamDef {
                        value: param.clone(),
                        index: self.param_index,
                    };
                    self.param_index += 1;
                    list.push(def);
                }
                params = Some(list);
            }

            let field_def = FieldDef {
                name: column.to_string(),
                alias,
                i18n_required,
                aggregate: util::json_bool(&field_struct["aggregate"]),
                distinct: util::json_bool(&field_struct["distinct"]),
                transform: field_struct["transform"].as_str().map(|s| s.to_string()),
                transform_result_field: field_struct["result_field"]
                    .as_str()
                    .map(|s| s.to_string()),
                transform_params: params,
            };

            select_def.fields.push(field_def);
        }

        self.add_select(select_def);

        Ok(())
    }

    /// Add a collection of SELECT fields to our list in progress.
    fn add_select(&mut self, select: SelectDef) {
        if let Some(selects) = self.selects.as_mut() {
            selects.push(select);
        } else {
            self.selects = Some(vec![select]);
        }
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

    /// Creates a default list of columns to select from an alias'ed IDL
    /// class.
    fn add_default_select_list(&mut self, alias: &str) -> EgResult<()> {
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
                .map(|f| FieldDef {
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

        Ok(())
    }

    /// Get the IDL classname linked to a table alias.
    fn find_alias(&self, alias: &str) -> Option<&str> {
        if let Some(cl) = self.core_class.as_ref() {
            if cl == alias {
                return Some(cl);
            }
        }

        if let Some(joins) = self.joins.as_ref() {
            joins
                .iter()
                .filter(|j| j.alias == alias)
                .map(|j| j.classname.as_ref())
                .next()
        } else {
            None
        }
    }
}
