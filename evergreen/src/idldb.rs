///! Tools for translating between IDL objects and Database rows.
use super::db;
use super::idl;
use super::util;
use super::util::Pager;
use chrono::prelude::*;
use json::JsonValue;
use pg::types::ToSql;
use postgres as pg;
use rust_decimal::Decimal;
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

const SUPPORTED_OPERANDS: &[&'static str] = &[
    "IS", "IS NOT", "IN", "NOT IN", "LIKE", "ILIKE", "<", "<=", ">", ">=", "<>", "!=", "~", "=",
    "!~", "!~*", "~*",
];

#[derive(Debug, Clone, PartialEq)]
pub enum OrderByDir {
    Asc,
    Desc,
}

impl fmt::Display for OrderByDir {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                OrderByDir::Asc => "ASC",
                _ => "DESC",
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    field: String,
    dir: OrderByDir,
}

impl OrderBy {
    pub fn new(field: &str, dir: OrderByDir) -> Self {
        OrderBy {
            dir,
            field: field.to_string(),
        }
    }
}

/// Models a request to create a set of IDL objects of a given class.
pub struct IdlClassCreate {
    pub classname: String,
    // Outer Vec is our list of value collections.
    // Inner list is a single set of values to create.
    pub values: Vec<Vec<(String, JsonValue)>>,
}

impl IdlClassCreate {
    pub fn new(classname: &str) -> Self {
        IdlClassCreate {
            classname: classname.to_string(),
            values: vec![vec![]],
        }
    }
}

/// Models a request to update a set of values on a set of IDL objects
/// of a given class.
pub struct IdlClassUpdate {
    pub classname: String,
    pub values: Vec<(String, JsonValue)>,
    pub filter: Option<JsonValue>,
}

impl IdlClassUpdate {
    pub fn new(classname: &str) -> Self {
        IdlClassUpdate {
            classname: classname.to_string(),
            values: Vec::new(),
            filter: None,
        }
    }
    pub fn reset(&mut self) {
        self.values = Vec::new();
        self.filter = None;
    }
    pub fn values(&self) -> &Vec<(String, JsonValue)> {
        &self.values
    }

    pub fn add_value(&mut self, field: &str, value: &JsonValue) {
        self.values.push((field.to_string(), value.clone()));
    }

    pub fn filter(&self) -> &Option<JsonValue> {
        &self.filter
    }

    pub fn set_filter(&mut self, f: JsonValue) {
        self.filter = Some(f);
    }
}

/// Models a request to search for a set of IDL objects of a given class.
pub struct IdlClassSearch {
    pub classname: String,
    pub filter: Option<JsonValue>,
    pub order_by: Option<Vec<OrderBy>>,
    pub pager: Option<Pager>,
}

impl IdlClassSearch {
    pub fn new(classname: &str) -> Self {
        IdlClassSearch {
            classname: classname.to_string(),
            filter: None,
            order_by: None,
            pager: None,
        }
    }

    pub fn classname(&self) -> &str {
        &self.classname
    }

    pub fn filter(&self) -> &Option<JsonValue> {
        &self.filter
    }

    pub fn set_filter(&mut self, f: JsonValue) {
        self.filter = Some(f);
    }

    pub fn order_by(&self) -> &Option<Vec<OrderBy>> {
        &self.order_by
    }

    pub fn set_order_by(&mut self, v: Vec<OrderBy>) {
        self.order_by = Some(v);
    }

    pub fn pager(&self) -> &Option<Pager> {
        &self.pager
    }

    pub fn set_pager(&mut self, pager: Pager) {
        self.pager = Some(pager);
    }
}

/// Manages the translation to / from IDL objects and database queries.
pub struct Translator {
    idl: Arc<idl::Parser>,
    db: Rc<RefCell<db::DatabaseConnection>>,
}

impl Translator {
    pub fn new(idl: Arc<idl::Parser>, db: Rc<RefCell<db::DatabaseConnection>>) -> Self {
        Translator { idl, db }
    }

    pub fn idl(&self) -> &Arc<idl::Parser> {
        &self.idl
    }

    /// Start a new database transaction
    pub fn xact_begin(&mut self) -> Result<(), String> {
        self.db.borrow_mut().xact_begin()
    }

    /// Commit an in-progress transaction.
    pub fn xact_commit(&mut self) -> Result<(), String> {
        self.db.borrow_mut().xact_commit()
    }

    /// Roll back an in-progress transaction.
    pub fn xact_rollback(&mut self) -> Result<(), String> {
        self.db.borrow_mut().xact_rollback()
    }

    /// Verify a query operand provided by the caller is allowed.
    pub fn is_supported_operand(op: &str) -> bool {
        SUPPORTED_OPERANDS.contains(&op.to_uppercase().as_str())
    }

    /// Retrieve an IDL object via pkey lookup.
    ///
    /// Numeric pkey values should be passed as strings.  They will be
    /// numerified withih before the query is issued.
    pub fn get_idl_object_by_pkey(
        &self,
        classname: &str,
        pkey: &JsonValue,
    ) -> Result<Option<JsonValue>, String> {
        let idl_class = self
            .idl()
            .classes()
            .get(classname)
            .ok_or(format!("No such IDL class: {classname}"))?;

        let pkey_field = idl_class
            .pkey_field()
            .ok_or(format!("Class {classname} has no primary key field"))?;

        let mut filter = JsonValue::new_object();
        filter.insert(pkey_field.name(), pkey.clone()).unwrap();

        let mut search = IdlClassSearch::new(classname);
        search.set_filter(filter);

        let list = self.idl_class_search(&search)?;

        match list.len() {
            0 => Ok(None),
            1 => Ok(Some(list[0].to_owned())),
            _ => Err(format!(
                "Pkey query for {classname} returned {} results",
                list.len()
            )),
        }
    }

    /// Get the IDL Class representing to the provided object.
    pub fn get_idl_class_from_object(&self, obj: &JsonValue) -> Result<&idl::Class, String> {
        let classname = obj[idl::CLASSNAME_KEY]
            .as_str()
            .ok_or(format!("Not an IDL object: {}", obj.dump()))?;

        self.idl()
            .classes()
            .get(classname)
            .ok_or(format!("No such IDL class: {classname}"))
    }

    /// Create an IDL object in the database
    ///
    /// Returns the created value
    pub fn create_idl_object(&self, obj: &JsonValue) -> Result<JsonValue, String> {
        let idl_class = self.get_idl_class_from_object(obj)?;

        let mut create = IdlClassCreate::new(idl_class.classname());
        let values = &mut create.values[0]; // list of lists

        for field in idl_class.real_fields_sorted() {
            values.push((field.name().to_string(), obj[field.name()].clone()));
        }

        let values = self.idl_class_create(&create)?;

        if let Some(v) = values.get(0) {
            Ok(v.to_owned())
        } else {
            // Should encounter an error before we get here, but just
            // to cover our bases.
            Err(format!(
                "Could not create new value for class: {}",
                idl_class.classname()
            ))
        }
    }

    /// Create one or more IDL objects in the database.
    ///
    /// Returns the created rows.
    pub fn idl_class_create(&self, create: &IdlClassCreate) -> Result<Vec<JsonValue>, String> {
        if create.values.len() == 0 {
            Err(format!("No values to create in idl_class_create()"))?;
        }

        if !self.db.borrow().in_transaction() {
            Err(format!("idl_class_create() requires a transaction"))?;
        }

        let classname = &create.classname;

        let idl_class = self
            .idl()
            .classes()
            .get(classname)
            .ok_or(format!("No such IDL class: {classname}"))?;

        let tablename = idl_class.tablename().ok_or(format!(
            "Cannot query an IDL class that has no tablename: {classname}"
        ))?;

        let pkey_field = idl_class
            .pkey()
            .ok_or(format!("Cannot create rows that have no primary key"))?;

        let mut query = format!("INSERT INTO {tablename} (");

        // Add the column names
        query += &idl_class
            .real_fields_sorted()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<&str>>()
            .join(", ");

        query += ") VALUES ";

        // Now add the sets of values to insert
        let mut param_index: usize = 1;
        let mut param_list: Vec<String> = Vec::new();
        let mut strings: Vec<String> = Vec::new();
        for values in &create.values {
            strings.push(self.compile_class_create(
                &idl_class,
                &values,
                &mut param_index,
                &mut param_list,
            )?);
        }

        query += &strings.join(", ");

        // And finally, tell PG to return the primary keys just created.
        query += &format!(" RETURNING {pkey_field}");

        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for p in param_list.iter() {
            params.push(p);
        }

        log::debug!("create() executing query: {query}; params=[{param_list:?}]");

        let query_res = self
            .db
            .borrow_mut()
            .client()
            .query(&query[..], params.as_slice());

        if let Err(ref e) = query_res {
            log::error!("DB Error: {e} query={query} param={params:?}");
            Err(format!("DB query failed. See error logs"))?;
        }

        // Use the primary key values reported by PG to find the
        // newly created rows.
        let mut results: Vec<JsonValue> = Vec::new();
        for row in query_res.unwrap() {
            let pkey_value = self.col_value_to_json_value(&row, 0)?;
            if let Some(pkv) = self.get_idl_object_by_pkey(idl_class.classname(), &pkey_value)? {
                results.push(pkv);
            } else {
                Err(format!("Could not recover newly created value from the DB"))?;
            }
        }

        Ok(results)
    }

    /// Update one IDL object in the database.
    pub fn update_idl_object(&self, obj: &JsonValue) -> Result<u64, String> {
        let idl_class = self.get_idl_class_from_object(obj)?;

        let mut update = IdlClassUpdate::new(idl_class.classname());
        for field in idl_class.real_fields_sorted() {
            update.add_value(field.name(), &obj[field.name()]);
        }

        let (pkey_field, pkey_value) = self
            .idl
            .get_pkey_info(obj)
            .ok_or(format!("Object has no primary key field"))?;

        let mut filter = JsonValue::new_object();
        filter
            .insert(pkey_field.name(), pkey_value.clone())
            .unwrap();

        update.set_filter(filter);

        self.idl_class_update(&update)
    }

    /// Update one or more IDL objects in the database.
    ///
    /// Returns Result of the number of rows modified.
    pub fn idl_class_update(&self, update: &IdlClassUpdate) -> Result<u64, String> {
        if update.values.len() == 0 {
            Err(format!("No values to update in idl_class_update()"))?;
        }

        if !self.db.borrow().in_transaction() {
            Err(format!("idl_class_update() requires a transaction"))?;
        }

        let classname = &update.classname;

        let class = self
            .idl()
            .classes()
            .get(classname)
            .ok_or(format!("No such IDL class: {classname}"))?;

        let tablename = class.tablename().ok_or(format!(
            "Cannot query an IDL class that has no tablename: {classname}"
        ))?;

        let mut param_list: Vec<String> = Vec::new();
        let mut param_index: usize = 1;
        let updates =
            self.compile_class_update(&class, &update.values, &mut param_index, &mut param_list)?;

        let mut query = format!("UPDATE {tablename} {updates}");

        if let Some(filter) = update.filter() {
            query +=
                &self.compile_class_filter(&class, filter, &mut param_index, &mut param_list)?;
        }

        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for p in param_list.iter() {
            params.push(p);
        }

        log::debug!("update() executing query: {query}; params=[{param_list:?}]");

        self.execute_one(&query, params.as_slice())
    }

    /// Execute a single db command and return the number of rows affected.
    fn execute_one(&self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, String> {
        log::debug!("update() executing query: {query}; params=[{params:?}]");

        let query_res = self.db.borrow_mut().client().execute(&query[..], params);

        match query_res {
            Ok(v) => {
                log::debug!("Update modified {v} rows");
                Ok(v)
            }
            Err(e) => {
                log::error!("DB Error: {e} query={query} param={params:?}");
                Err(format!("DB query failed. See error logs"))
            }
        }
    }

    /// Delete one IDL object via its primary key.
    ///
    /// Returns a Result of the number of rows affected.
    pub fn delete_idl_object_by_pkey(
        &self,
        classname: &str,
        pkey: &JsonValue,
    ) -> Result<u64, String> {
        if !self.db.borrow().in_transaction() {
            Err(format!("delete_idl_object_by_pkey requires a transaction"))?;
        }

        let class = self
            .idl()
            .classes()
            .get(classname)
            .ok_or(format!("No such IDL class: {classname}"))?;

        let tablename = class.tablename().ok_or(format!(
            "Cannot query an IDL class that has no tablename: {classname}"
        ))?;

        let pkey_field = class
            .pkey_field()
            .ok_or(format!("IDL class {classname} has no primary key field"))?;

        let mut param_list: Vec<String> = Vec::new();
        let mut param_index: usize = 1;

        let mut query = format!("DELETE FROM {tablename} WHERE {} ", pkey_field.name());

        query += &self.append_json_literal(
            &mut param_index,
            &mut param_list,
            pkey_field,
            pkey,
            Some("="),
            false,
        )?;

        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for p in param_list.iter() {
            params.push(p);
        }

        self.execute_one(&query, params.as_slice())
    }

    /// Search for IDL objects in the database.
    ///
    /// Returns a Vec of the found IDL objects.
    pub fn idl_class_search(&self, search: &IdlClassSearch) -> Result<Vec<JsonValue>, String> {
        let mut results: Vec<JsonValue> = Vec::new();
        let classname = &search.classname;

        let class = self
            .idl()
            .classes()
            .get(classname)
            .ok_or(format!("No such IDL class: {classname}"))?;

        let tablename = class.tablename().ok_or(format!(
            "Cannot query an IDL class that has no tablename: {classname}"
        ))?;

        let select = self.compile_class_select(&class);

        let mut query = format!("{select} FROM {tablename}");

        // Some parameters require binding within the DB statement.
        // Put them here.
        let mut param_list: Vec<String> = Vec::new();
        let mut param_index: usize = 1;

        if let Some(filter) = &search.filter {
            query +=
                &self.compile_class_filter(&class, filter, &mut param_index, &mut param_list)?;
        }

        if let Some(order) = &search.order_by {
            query += &self.compile_class_order_by(order);
        }

        if let Some(pager) = &search.pager {
            query += &self.compile_pager(pager);
        }

        log::debug!("search() executing query: {query}");

        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for p in param_list.iter() {
            params.push(p);
        }

        let query_res = self
            .db
            .borrow_mut()
            .client()
            .query(&query[..], params.as_slice());

        if let Err(ref e) = query_res {
            log::error!("DB Error: {e} query={query} param={params:?}");
            Err(format!("DB query failed. See error logs"))?;
        }

        for row in query_res.unwrap() {
            results.push(self.row_to_idl(&class, &row)?);
        }

        Ok(results)
    }

    /// Create a query ORDER BY string.
    fn compile_class_order_by(&self, order: &Vec<OrderBy>) -> String {
        let mut sql = String::new();
        let mut count = order.len();

        if count > 0 {
            sql += " ORDER BY";
            for order_by in order {
                sql += &format!(" {} {}", &order_by.field, &order_by.dir);
                count -= 1;
                if count > 0 {
                    sql += ",";
                }
            }
        }

        sql
    }

    /// Translate numeric IDL field values from JSON Strings into JSON
    /// Numbers.
    ///
    /// Sometimes numbers are passed as strings in the wild west of JSON,
    /// but the database doesn't want strings for, say, numeric primary key
    /// matches.  Numerify if we should and can.
    ///
    /// JSON Null values are ignored.
    fn try_translate_numeric(
        &self,
        idl_field: &idl::Field,
        value: &JsonValue,
    ) -> Result<Option<JsonValue>, String> {
        if !value.is_string() {
            return Ok(None);
        }

        if !idl_field.datatype().is_numeric() {
            return Ok(None);
        }

        // Try to create a int, then try a float.
        match util::json_int(&value) {
            Ok(n) => Ok(Some(json::from(n))),
            Err(_) => match util::json_float(&value) {
                Ok(n) => Ok(Some(json::from(n))),
                Err(_) => Err(format!(
                    "Numeric value cannot be coerced int a number: {value}"
                ))?,
            },
        }
    }

    /// Create the values lists of an SQL create command.
    fn compile_class_create(
        &self,
        class: &idl::Class,
        values: &Vec<(String, JsonValue)>,
        param_index: &mut usize,
        param_list: &mut Vec<String>,
    ) -> Result<String, String> {
        let mut sql = String::from("(");
        let mut strings = Vec::new();

        for kvp in values {
            let field = &kvp.0;
            let value = &kvp.1;

            let idl_field = class.get_real_field(field).ok_or(format!(
                "No such real field '{field}' on class '{}'",
                class.classname()
            ))?;

            strings.push(self.append_json_literal(
                param_index,
                param_list,
                idl_field,
                value,
                None,
                true,
            )?);
        }

        sql += &strings.join(", ");

        sql += ")";

        Ok(sql)
    }

    /// Create the SET portion of an SQL update command.
    fn compile_class_update(
        &self,
        class: &idl::Class,
        values: &Vec<(String, JsonValue)>,
        param_index: &mut usize,
        param_list: &mut Vec<String>,
    ) -> Result<String, String> {
        let mut sql = String::from("SET");
        let mut strings = Vec::new();

        for kvp in values {
            let field = &kvp.0;
            let value = &kvp.1;

            let idl_field = class.get_real_field(field).ok_or(format!(
                "No such real field '{field}' on class '{}'",
                class.classname()
            ))?;

            strings.push(format!(
                " {field} {}",
                self.append_json_literal(
                    param_index,
                    param_list,
                    idl_field,
                    value,
                    Some("="),
                    false,
                )?
            ));
        }

        sql += &strings.join(", ");

        Ok(sql)
    }

    /// Create the SELECT clause for a search query.
    fn compile_class_select(&self, class: &idl::Class) -> String {
        let mut sql = String::from("SELECT");

        for (name, field) in class.fields() {
            if !field.is_virtual() {
                sql += &format!(" {name},");
            }
        }

        String::from(&sql[0..sql.len() - 1]) // Trim final ","
    }

    /// Create the limit/offset part of the query string.
    fn compile_pager(&self, pager: &Pager) -> String {
        format!(" LIMIT {} OFFSET {}", pager.limit(), pager.offset())
    }

    /// Generate a WHERE clause from a JSON query object.
    fn compile_class_filter(
        &self,
        class: &idl::Class,
        filter: &JsonValue,
        param_index: &mut usize,
        param_list: &mut Vec<String>,
    ) -> Result<String, String> {
        if !filter.is_object() {
            return Err(format!(
                "Translator class filter must be an object: {}",
                filter.dump()
            ));
        }

        let mut filters = Vec::new();
        for (field, subq) in filter.entries() {
            log::trace!("compile_class_filter adding filter on field: {field}");

            let idl_field = class.get_real_field(field).ok_or(format!(
                "No such real field '{field}' on class '{}'",
                class.classname()
            ))?;

            let filter = match subq {
                JsonValue::Array(_) => self.compile_class_filter_array(
                    param_index,
                    param_list,
                    idl_field,
                    &subq,
                    "IN",
                )?,
                JsonValue::Object(_) => {
                    self.compile_class_filter_object(param_index, param_list, idl_field, &subq)?
                }
                JsonValue::Number(_) | JsonValue::String(_) | JsonValue::Short(_) => self
                    .append_json_literal(
                        param_index,
                        param_list,
                        idl_field,
                        subq,
                        Some("="),
                        false,
                    )?,
                JsonValue::Boolean(_) | JsonValue::Null => self.append_json_literal(
                    param_index,
                    param_list,
                    idl_field,
                    subq,
                    Some("IS"),
                    false,
                )?,
            };

            filters.push(format!(" {field} {filter}"));
        }

        Ok(format!(" WHERE {}", filters.join(" AND")))
    }

    /// Add a JSON literal (scalar) value to a query.
    ///
    /// If the value is a JSON String, add it to the param_list for
    /// query binding.  Otherwise, add it directly to the compiled
    /// SQL string.
    fn append_json_literal(
        &self,
        param_index: &mut usize,
        param_list: &mut Vec<String>,
        idl_field: &idl::Field,
        obj: &JsonValue,
        operand: Option<&str>,
        use_default: bool,
    ) -> Result<String, String> {
        if obj.is_object() || obj.is_array() {
            return Err(format!("Cannot format array/object as a literal: {obj:?}"));
        }

        if use_default && obj.is_null() {
            return Ok(format!("DEFAULT"));
        }

        let opstr = match operand {
            Some(op) => format!("{op} "),
            None => String::new(),
        };

        // We may need to coerce a JSON String into a JSON Number
        let new_obj = self.try_translate_numeric(idl_field, obj)?;
        let obj = new_obj.as_ref().unwrap_or(obj);

        // Track String parameters so we can use query binding on the
        // them in the final query.  All other types, being derived
        // from JsonValue, have a known shape and size (number/bool/null),
        // so query binding is less critical from a sql-injection
        // perspective.
        if obj.is_string() {
            let s = format!("{opstr}${param_index}");
            param_list.push(obj.to_string());
            *param_index += 1;
            Ok(s)
        } else {
            // obj here is a bool, number, or null
            Ok(format!("{opstr}{}", obj))
        }
    }

    /// Turn an object-based subquery into part of the WHERE AND.
    fn compile_class_filter_object(
        &self,
        param_index: &mut usize,
        param_list: &mut Vec<String>,
        idl_field: &idl::Field,
        obj: &JsonValue,
    ) -> Result<String, String> {
        // A filter object may only contain a single operand => value combo
        let (key, val) = obj
            .entries()
            .next()
            .ok_or(format!("Invalid query object; {obj:?}"))?;

        let operand = key.to_uppercase();

        if !Translator::is_supported_operand(&operand) {
            Err(format!("Unsupported operand: {operand} : {obj}"))?;
        }

        if val.is_array() {
            // E.g. NOT IN (a, b, c, ...)

            self.compile_class_filter_array(
                param_index,
                param_list,
                idl_field,
                val,
                operand.as_str(),
            )
        } else {
            self.append_json_literal(
                param_index,
                param_list,
                idl_field,
                val,
                Some(&operand),
                false,
            )
        }
    }

    /// Turn an array-based subquery into part of the WHERE AND.
    ///
    /// This creates a list of values to compare to, e.g. IN list.
    fn compile_class_filter_array(
        &self,
        param_index: &mut usize,
        param_list: &mut Vec<String>,
        idl_field: &idl::Field,
        arr: &JsonValue,
        operand: &str,
    ) -> Result<String, String> {
        let operand = operand.to_uppercase();
        if !Translator::is_supported_operand(&operand) {
            Err(format!("Unsupported operand: {operand} : {arr}"))?;
        }

        let mut filters: Vec<String> = Vec::new();
        for val in arr.members() {
            filters.push(self.append_json_literal(
                param_index,
                param_list,
                idl_field,
                val,
                None,
                false,
            )?);
        }

        Ok(format!("{operand} ({})", filters.join(", ")))
    }

    /// Maps a PG row into an IDL-based JsonValue;
    fn row_to_idl(&self, class: &idl::Class, row: &pg::Row) -> Result<JsonValue, String> {
        let mut obj = JsonValue::new_object();
        obj[idl::CLASSNAME_KEY] = json::from(class.classname());

        let mut index = 0;

        for (name, _) in class.fields().iter().filter(|(_, f)| !f.is_virtual()) {
            obj[name] = self.col_value_to_json_value(row, index)?;
            index += 1;
        }

        Ok(obj)
    }

    /// Translate a PG-typed row value into a JsonValue
    fn col_value_to_json_value(&self, row: &pg::Row, index: usize) -> Result<JsonValue, String> {
        let col_type = row.columns().get(index).map(|c| c.type_().name()).unwrap();

        match col_type {
            // JsonValue has From<Option<T>>
            "bool" => {
                let v: Option<bool> = row.get(index);
                Ok(json::from(v))
            }
            "interval" => {
                let v: Option<pg_interval::Interval> = row.get(index);
                let s = match v {
                    Some(val) => val.to_postgres(),
                    None => return Ok(JsonValue::Null),
                };
                Ok(json::from(s))
            }
            "varchar" | "char(n)" | "text" | "name" => {
                let v: Option<String> = row.get(index);
                Ok(json::from(v))
            }
            "date" => {
                let v: Option<chrono::NaiveDate> = row.get(index);
                let s = match v {
                    Some(val) => val.format("%F").to_string(),
                    None => return Ok(JsonValue::Null),
                };
                Ok(json::from(s))
            }
            "timestamp" | "timestamptz" => {
                let v: Option<chrono::DateTime<Utc>> = row.get(index);
                let s = match v {
                    Some(val) => val.format("%FT%T%z").to_string(),
                    None => return Ok(JsonValue::Null),
                };
                Ok(json::from(s))
            }
            "int2" | "smallserial" | "smallint" => {
                let v: Option<i16> = row.get(index);
                Ok(json::from(v))
            }
            "int" | "int4" | "serial" => {
                let v: Option<i32> = row.get(index);
                Ok(json::from(v))
            }
            "int8" | "bigserial" | "bigint" => {
                let v: Option<i64> = row.get(index);
                Ok(json::from(v))
            }
            "float4" | "real" => {
                let v: Option<f32> = row.get(index);
                Ok(json::from(v))
            }
            "float8" | "double precision" => {
                let v: Option<f64> = row.get(index);
                Ok(json::from(v))
            }
            "numeric" => {
                let decimal: Option<Decimal> = row.get(index);
                match decimal {
                    Some(d) => Ok(json::from(d.to_string())),
                    None => Ok(JsonValue::Null),
                }
            }
            "tsvector" => Ok(JsonValue::Null),
            _ => Err(format!("Unsupported column type: {col_type}")),
        }
    }
}
