//! Tools for managing IDL-classed objects/values via SQL.
use crate as eg;
use chrono::prelude::*;
use eg::db;
use eg::idl;
use eg::util::Pager;
use eg::EgResult;
use eg::EgValue;
use pg::types::ToSql;
use postgres as pg;
use rust_decimal::Decimal;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

pub const MAX_FLESH_DEPTH: i16 = 100;

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
    pub values: Vec<Vec<(String, EgValue)>>,
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
    pub values: Vec<(String, EgValue)>,
    pub filter: Option<EgValue>,
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
    pub fn values(&self) -> &Vec<(String, EgValue)> {
        &self.values
    }

    pub fn add_value(&mut self, field: &str, value: &EgValue) {
        self.values.push((field.to_string(), value.clone()));
    }

    pub fn filter(&self) -> &Option<EgValue> {
        &self.filter
    }

    pub fn set_filter(&mut self, f: EgValue) {
        self.filter = Some(f);
    }
}

#[derive(Debug, Clone)]
pub struct FleshDef {
    pub fields: HashMap<String, Vec<String>>,
    /// Depth of <0 means flesh to maximum.
    pub depth: i16,
}

impl FleshDef {
    /// Creates a FleshDef from a JSON options hash/object.
    ///
    /// ```
    /// use evergreen as eg;
    /// use eg::idldb::FleshDef;
    /// use json;
    ///
    /// let obj = eg::hash! {
    ///   "flesh": -1, "flesh_fields": {"au": ["home_ou", "profile"]}
    /// };
    ///
    /// let flesh_def = FleshDef::from_eg_value(&obj).expect("Parsed Flesh");
    /// assert_eq!(flesh_def.depth, evergreen::idldb::MAX_FLESH_DEPTH);
    /// assert_eq!(flesh_def.fields.len(), 1);
    /// assert_eq!(flesh_def.fields.get("au").expect("Has an au").len(), 2);
    /// ```
    pub fn from_eg_value(obj: &EgValue) -> EgResult<Self> {
        let mut fields = HashMap::new();

        for (classname, field_names) in obj["flesh_fields"].entries() {
            let mut list = Vec::new();
            for name in field_names.members() {
                let n = name
                    .as_str()
                    .ok_or_else(|| format!("Invalid flesh definition: {}", obj.dump()))?;
                list.push(n.to_string());
            }

            fields.insert(classname.to_string(), list);
        }

        let depth = if let Some(num) = obj["flesh"].as_i16() {
            if num > MAX_FLESH_DEPTH || num < 0 {
                MAX_FLESH_DEPTH
            } else {
                num
            }
        } else {
            0
        };

        Ok(FleshDef { depth, fields })
    }
}

/// Models a request to search for a set of IDL objects of a given class.
#[derive(Debug)]
pub struct IdlClassSearch {
    pub classname: String,
    pub filter: Option<EgValue>,
    pub order_by: Option<Vec<OrderBy>>,
    pub pager: Option<Pager>,
    pub flesh: Option<FleshDef>,
}

impl IdlClassSearch {
    pub fn new(classname: &str) -> Self {
        IdlClassSearch {
            classname: classname.to_string(),
            filter: None,
            order_by: None,
            pager: None,
            flesh: None,
        }
    }

    pub fn set_flesh(&mut self, flesh_def: FleshDef) {
        self.flesh = Some(flesh_def);
    }

    pub fn classname(&self) -> &str {
        &self.classname
    }

    pub fn filter(&self) -> &Option<EgValue> {
        &self.filter
    }

    pub fn set_filter(&mut self, f: EgValue) {
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
    db: Rc<RefCell<db::DatabaseConnection>>,
}

impl Translator {
    pub fn new(db: Rc<RefCell<db::DatabaseConnection>>) -> Self {
        Translator { db }
    }

    /// Start a new database transaction
    pub fn xact_begin(&mut self) -> EgResult<()> {
        self.db.borrow_mut().xact_begin()
    }

    /// Commit an in-progress transaction.
    pub fn xact_commit(&mut self) -> EgResult<()> {
        self.db.borrow_mut().xact_commit()
    }

    /// Roll back an in-progress transaction.
    pub fn xact_rollback(&mut self) -> EgResult<()> {
        self.db.borrow_mut().xact_rollback()
    }

    /// Retrieve an IDL object via pkey lookup.
    ///
    /// Numeric pkey values should be passed as strings.  They will be
    /// numerified withih before the query is issued.
    pub fn get_idl_object_by_pkey(
        &self,
        classname: &str,
        pkey: &EgValue,
        flesh_def: Option<FleshDef>,
    ) -> EgResult<Option<EgValue>> {
        let idl_class = idl::get_class(classname)?;

        let pkey_field = match idl_class.pkey_field() {
            Some(f) => f,
            None => return Err(format!("Class {classname} has no primary key field").into()),
        };

        let mut filter = EgValue::new_object();
        filter
            .insert(pkey_field.name(), pkey.clone())
            .expect("Is Object");

        let mut search = IdlClassSearch::new(classname);
        search.set_filter(filter);
        search.flesh = flesh_def;

        let mut list = self.idl_class_search(&search)?;

        match list.len() {
            0 => Ok(None),
            1 => Ok(Some(list.pop().unwrap())),
            _ => {
                return Err(
                    format!("Pkey query for {classname} returned {} results", list.len()).into(),
                )
            }
        }
    }

    /// Fleshes an IDL object in place based on the flesh_fields definitions.
    pub fn flesh_idl_object(&self, object: &mut EgValue, flesh_def: &FleshDef) -> EgResult<()> {
        if flesh_def.depth == 0 {
            log::warn!("Attempt to flesh beyond flesh depth");
            return Ok(());
        }

        let idl_class = self.get_idl_class_from_object(object)?.clone();
        let classname = idl_class.classname();

        // Clone these out since flesh_def is mutable.
        let fieldnames;

        if let Some(list) = flesh_def.fields.get(classname) {
            fieldnames = list.clone();
        } else {
            // Nothing to flesh on this object.  Probably shouldnt
            // ever get here, but treating like a non-error for now.
            return Ok(());
        }

        // What fields are we fleshing on this class?
        for fieldname in fieldnames.iter() {
            self.flesh_idl_object_field(object, flesh_def, fieldname, &idl_class)?;
        }

        Ok(())
    }

    /// Flesh a single field on an object.
    fn flesh_idl_object_field(
        &self,
        object: &mut EgValue,
        flesh_def: &FleshDef,
        fieldname: &str,
        idl_class: &idl::Class,
    ) -> EgResult<()> {
        let classname = idl_class.classname();

        // Def has to be cloned so it can be locally modified and
        // given to another search.
        let mut flesh_def = flesh_def.clone();

        let idl_link = idl_class
            .links()
            .get(fieldname)
            .ok_or_else(|| format!("Field {fieldname} on class {classname} cannot be fleshed"))?;

        let search_value;
        let reltype = idl_link.reltype();

        if reltype == idl::RelType::HasMany || reltype == idl::RelType::MightHave {
            // When the foreign key relationship points from the
            // fleshed object back to us, the search value will be
            // this object's primary key.

            search_value = object
                .pkey_value()
                .ok_or_else(|| format!("Object has no pkey value: {}", object.dump()))?;
        } else {
            //search_value = object[fieldname].clone();
            search_value = &object[fieldname];
        }

        if !search_value.is_string() && !search_value.is_number() {
            return Err(format!(
                "Class {classname} cannot flesh field {fieldname} on value: {}",
                search_value.dump()
            )
            .into());
        }

        // TODO verify the linked class may be accessed by this
        // controller, e.g. pcrud
        // Set the value to an array if needed for reltype.

        if let Some(map_field) = idl_link.map() {
            // When an intermediate mapping object is defined,
            // add it to our pile of fleshed fields.
            let cname = idl_link.class();
            let fname = map_field.to_string();

            if let Some(list) = flesh_def.fields.get_mut(cname) {
                list.push(fname);
            } else {
                flesh_def.fields.insert(cname.to_string(), vec![fname]);
            }
        } else {
            // When adding an implicit mapped field, avoid decrementing
            // the flesh depth so the caller is not penalized.
            flesh_def.depth -= 1;
        }

        log::debug!(
            "Fleshing {}.{}; Link field: {}, remote class: {} , fkey: {}, reltype: {}",
            classname,
            fieldname,
            idl_link.field(),
            idl_link.class(),
            idl_link.key(),
            idl_link.reltype(),
        );

        let mut class_search = IdlClassSearch::new(idl_link.class());
        class_search.flesh = Some(flesh_def);

        let mut filter = eg::hash! {};
        filter[idl_link.key()] = search_value.clone();
        class_search.set_filter(filter);

        let mut children = self.idl_class_search(&class_search)?;

        log::debug!("Fleshed search returned {} results", children.len());

        if children.len() > 0 {
            // Get the values of the mapped fields on the found children
            if let Some(map_field) = idl_link.map() {
                let mut mapped_values = Vec::new();
                for mut child in children.drain(..) {
                    mapped_values.push(child[map_field].take());
                }
                children = mapped_values;
            }
        }

        // Attach the child data to the fleshed object.

        if children.len() > 0
            && (reltype == idl::RelType::HasA || reltype == idl::RelType::MightHave)
        {
            object[fieldname] = children.remove(0); // len() above
        }

        if reltype == idl::RelType::HasMany {
            object[fieldname] = EgValue::from(children);
        }

        Ok(())
    }

    /// Get the IDL Class representing to the provided object.
    pub fn get_idl_class_from_object<'a>(&self, obj: &'a EgValue) -> EgResult<&'a Arc<idl::Class>> {
        obj.idl_class()
            .ok_or_else(|| format!("Not an IDL object: {}", obj.dump()).into())
    }

    /// Create an IDL object in the database
    ///
    /// Returns the created value
    pub fn create_idl_object(&self, obj: &EgValue) -> EgResult<EgValue> {
        let idl_class = self.get_idl_class_from_object(obj)?;

        let mut create = IdlClassCreate::new(idl_class.classname());
        let values = &mut create.values[0]; // list of lists

        for name in idl_class.real_field_names_sorted() {
            values.push((name.to_string(), obj[name].clone()));
        }

        let mut values = self.idl_class_create(&create)?;

        if let Some(v) = values.pop() {
            Ok(v)
        } else {
            // Should encounter an error before we get here, but just
            // to cover our bases.
            Err(format!(
                "Could not create new value for class: {}",
                idl_class.classname()
            )
            .into())
        }
    }

    /// Create one or more IDL objects in the database.
    ///
    /// Returns the created rows.
    pub fn idl_class_create(&self, create: &IdlClassCreate) -> EgResult<Vec<EgValue>> {
        if create.values.len() == 0 {
            Err(format!("No values to create in idl_class_create()"))?;
        }

        if !self.db.borrow().in_transaction() {
            Err(format!("idl_class_create() requires a transaction"))?;
        }

        let classname = &create.classname;

        let idl_class = idl::get_class(classname)?;

        let tablename = idl_class.tablename().ok_or_else(|| {
            format!("Cannot query an IDL class that has no tablename: {classname}")
        })?;

        let pkey_field = idl_class
            .pkey()
            .ok_or_else(|| format!("Cannot create rows that have no primary key"))?;

        let mut query = format!("INSERT INTO {tablename} (");

        // Add the column names
        query += &idl_class.real_field_names_sorted().join(", ");

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

        let query_res = self.db.borrow_mut().client().query(&query, &params);

        if let Err(ref e) = query_res {
            log::error!("DB Error: {e} query={query} param={params:?}");
            Err(format!("DB query failed. See error logs"))?;
        }

        // Use the primary key values reported by PG to find the
        // newly created rows.
        let mut results: Vec<EgValue> = Vec::new();

        for row in query_res.unwrap() {
            let pkey_value = Translator::col_value_to_json_value(&row, 0)?;

            match self.get_idl_object_by_pkey(idl_class.classname(), &pkey_value, None)? {
                Some(pkv) => results.push(pkv),
                None => Err(format!("Could not recover newly created value from the DB"))?,
            };
        }

        Ok(results)
    }

    /// Update one IDL object in the database.
    pub fn update_idl_object(&self, obj: &EgValue) -> EgResult<u64> {
        let idl_class = self.get_idl_class_from_object(obj)?;

        let mut update = IdlClassUpdate::new(idl_class.classname());
        for name in idl_class.real_field_names_sorted() {
            update.add_value(name, &obj[name]);
        }

        let (pkey_field, pkey_value) = obj
            .pkey_info()
            .ok_or_else(|| format!("Object has no primary key field"))?;

        let mut filter = EgValue::new_object();
        filter
            .insert(pkey_field.name(), pkey_value.clone())
            .unwrap();

        update.set_filter(filter);

        self.idl_class_update(&update)
    }

    /// Update one or more IDL objects in the database.
    ///
    /// Returns Result of the number of rows modified.
    pub fn idl_class_update(&self, update: &IdlClassUpdate) -> EgResult<u64> {
        if update.values.len() == 0 {
            Err(format!("No values to update in idl_class_update()"))?;
        }

        if !self.db.borrow().in_transaction() {
            Err(format!("idl_class_update() requires a transaction"))?;
        }

        let classname = &update.classname;

        let class = idl::get_class(classname)?;

        let tablename = class.tablename().ok_or_else(|| {
            format!("Cannot query an IDL class that has no tablename: {classname}")
        })?;

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

        self.execute_one(&query, &params)
    }

    /// Execute a single db command and return the number of rows affected.
    fn execute_one(&self, query: &str, params: &[&(dyn ToSql + Sync)]) -> EgResult<u64> {
        log::debug!("update() executing query: {query}; params=[{params:?}]");

        let query_res = self.db.borrow_mut().client().execute(query, params);

        match query_res {
            Ok(v) => {
                log::debug!("Update modified {v} rows");
                Ok(v)
            }
            Err(e) => {
                log::error!("DB Error: {e} query={query} param={params:?}");
                Err(format!("DB query failed. See error logs").into())
            }
        }
    }

    /// Delete one IDL object via its primary key.
    ///
    /// Returns a Result of the number of rows affected.
    pub fn delete_idl_object_by_pkey(&self, classname: &str, pkey: &EgValue) -> EgResult<u64> {
        if !self.db.borrow().in_transaction() {
            Err(format!("delete_idl_object_by_pkey requires a transaction"))?;
        }

        let class = idl::get_class(classname)?;

        let tablename = class.tablename().ok_or_else(|| {
            format!("Cannot query an IDL class that has no tablename: {classname}")
        })?;

        let pkey_field = class
            .pkey_field()
            .ok_or_else(|| format!("IDL class {classname} has no primary key field"))?;

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

        self.execute_one(&query, &params)
    }

    /// Search for IDL objects in the database.
    ///
    /// Returns a Vec of the found IDL objects.
    pub fn idl_class_search(&self, search: &IdlClassSearch) -> EgResult<Vec<EgValue>> {
        let mut results: Vec<EgValue> = Vec::new();
        let classname = &search.classname;

        log::debug!("idl_class_search() {search:?}");

        let class = idl::get_class(classname)?;

        let tablename = class.tablename().ok_or_else(|| {
            format!("Cannot query an IDL class that has no tablename: {classname}")
        })?;

        let columns = class.real_field_names_sorted().join(", ");

        let mut query = format!("SELECT {columns} FROM {tablename}");

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

        let query_res = self.db.borrow_mut().client().query(&query, &params);

        if let Err(ref e) = query_res {
            log::error!("DB Error: {e} query={query} param={params:?}");
            Err(format!("DB query failed. See error logs"))?;
        }

        for row in query_res.unwrap() {
            let mut obj = self.row_to_idl(&class, &row)?;
            if let Some(flesh_def) = search.flesh.as_ref() {
                self.flesh_idl_object(&mut obj, &flesh_def)?;
            }
            results.push(obj);
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

    /// Translate numeric IDL field values from Strings into Numbers.
    ///
    /// Sometimes numbers are passed as strings in the wild west of JSON,
    /// but the database doesn't want strings for, say, numeric primary key
    /// matches.  Numerify if we should and can.
    ///
    /// JSON Null values are ignored.
    fn try_translate_numeric(
        &self,
        idl_field: &idl::Field,
        value: &EgValue,
    ) -> EgResult<Option<EgValue>> {
        if !value.is_string() {
            return Ok(None);
        }

        if !idl_field.datatype().is_numeric() {
            return Ok(None);
        }

        if let Some(n) = value.as_int() {
            Ok(Some(EgValue::from(n)))
        } else if let Some(n) = value.as_float() {
            Ok(Some(EgValue::from(n)))
        } else {
            Err(format!("Value cannot be coerced int a number: {value}").into())
        }
    }

    /// Create the values lists of an SQL create command.
    fn compile_class_create(
        &self,
        class: &idl::Class,
        values: &Vec<(String, EgValue)>,
        param_index: &mut usize,
        param_list: &mut Vec<String>,
    ) -> EgResult<String> {
        let mut sql = String::from("(");
        let mut strings = Vec::new();

        for kvp in values {
            let field = &kvp.0;
            let value = &kvp.1;

            let idl_field = class.get_real_field(field).ok_or_else(|| {
                format!(
                    "No such real field '{field}' on class '{}'",
                    class.classname()
                )
            })?;

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
        values: &Vec<(String, EgValue)>,
        param_index: &mut usize,
        param_list: &mut Vec<String>,
    ) -> EgResult<String> {
        let mut parts = Vec::new();

        for kvp in values {
            let field = &kvp.0;
            let value = &kvp.1;

            let idl_field = class.get_real_field(field).ok_or_else(|| {
                format!(
                    "No such real field '{field}' on class '{}'",
                    class.classname()
                )
            })?;

            parts.push(format!(
                "{field} {}",
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

        Ok(format!("SET {}", parts.join(", ")))
    }

    /// Create the limit/offset part of the query string.
    fn compile_pager(&self, pager: &Pager) -> String {
        format!(" LIMIT {} OFFSET {}", pager.limit(), pager.offset())
    }

    /// Generate a WHERE clause from a JSON query object.
    fn compile_class_filter(
        &self,
        class: &idl::Class,
        filter: &EgValue,
        param_index: &mut usize,
        param_list: &mut Vec<String>,
    ) -> EgResult<String> {
        if !filter.is_object() {
            return Err(format!(
                "Translator class filter must be an object: {}",
                filter.dump()
            )
            .into());
        }

        let mut filters = Vec::new();
        for (field, subq) in filter.entries() {
            log::trace!("compile_class_filter adding filter on field: {field}");

            let idl_field = class.get_real_field(field).ok_or_else(|| {
                format!(
                    "No such real field '{field}' on class '{}'",
                    class.classname()
                )
            })?;

            let filter = match subq {
                EgValue::Array(_) => self.compile_class_filter_array(
                    param_index,
                    param_list,
                    idl_field,
                    &subq,
                    "IN",
                )?,
                EgValue::Hash(_) => {
                    self.compile_class_filter_object(param_index, param_list, idl_field, &subq)?
                }
                EgValue::Number(_) | EgValue::String(_) => self.append_json_literal(
                    param_index,
                    param_list,
                    idl_field,
                    subq,
                    Some("="),
                    false,
                )?,
                EgValue::Boolean(_) | EgValue::Null => self.append_json_literal(
                    param_index,
                    param_list,
                    idl_field,
                    subq,
                    Some("IS"),
                    false,
                )?,
                EgValue::Blessed(_) => {
                    return Err(format!("Cannot create JSON filter from a blessed value").into())
                }
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
        obj: &EgValue,
        operand: Option<&str>,
        use_default: bool,
    ) -> EgResult<String> {
        if obj.is_object() || obj.is_array() {
            return Err(format!("Cannot format array/object as a literal: {obj:?}").into());
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
        // from EgValue, have a known shape and size (number/bool/null),
        // so query binding is less critical from a sql-injection
        // perspective.
        if obj.is_string() {
            let s = format!("{opstr}${param_index}");
            param_list.push(obj.to_string().expect("Is String"));
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
        obj: &EgValue,
    ) -> EgResult<String> {
        // A filter object may only contain a single operand => value combo
        let (key, val) = obj
            .entries()
            .next()
            .ok_or_else(|| format!("Invalid query object; {obj:?}"))?;

        let operand = key.to_uppercase();

        if !db::is_supported_operator(&operand) {
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
        arr: &EgValue,
        operand: &str,
    ) -> EgResult<String> {
        let operand = operand.to_uppercase();
        if !db::is_supported_operator(&operand) {
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

    /// Maps a PG row into an IDL-based EgValue;
    fn row_to_idl(&self, class: &idl::Class, row: &pg::Row) -> EgResult<EgValue> {
        let mut obj = EgValue::new_object();
        obj.bless(class.classname())?;

        let mut index = 0;

        for name in class.real_field_names_sorted() {
            obj[name] = Translator::col_value_to_json_value(row, index)?;
            index += 1;
        }

        Ok(obj)
    }

    /// Translate a PG-typed row value into a EgValue
    pub fn col_value_to_json_value(row: &pg::Row, index: usize) -> EgResult<EgValue> {
        let col_type = row.columns().get(index).map(|c| c.type_().name()).unwrap();

        match col_type {
            // EgValue has From<Option<T>>
            "bool" => {
                let v: Option<bool> = row.get(index);
                Ok(EgValue::from(v))
            }
            "interval" => {
                let v: Option<pg_interval::Interval> = row.get(index);
                let s = match v {
                    Some(val) => val.to_postgres(),
                    None => return Ok(EgValue::Null),
                };
                Ok(EgValue::from(s))
            }
            "varchar" | "char(n)" | "text" | "name" => {
                let v: Option<String> = row.get(index);
                Ok(EgValue::from(v))
            }
            "date" => {
                let v: Option<chrono::NaiveDate> = row.get(index);
                let s = match v {
                    Some(val) => val.format("%F").to_string(),
                    None => return Ok(EgValue::Null),
                };
                Ok(EgValue::from(s))
            }
            "timestamp" | "timestamptz" => {
                let v: Option<chrono::DateTime<Utc>> = row.get(index);
                let s = match v {
                    Some(val) => val.format("%FT%T%z").to_string(),
                    None => return Ok(EgValue::Null),
                };
                Ok(EgValue::from(s))
            }
            "int2" | "smallserial" | "smallint" => {
                let v: Option<i16> = row.get(index);
                Ok(EgValue::from(v))
            }
            "int" | "int4" | "serial" => {
                let v: Option<i32> = row.get(index);
                Ok(EgValue::from(v))
            }
            "int8" | "bigserial" | "bigint" => {
                let v: Option<i64> = row.get(index);
                Ok(EgValue::from(v))
            }
            "float4" | "real" => {
                let v: Option<f32> = row.get(index);
                Ok(EgValue::from(v))
            }
            "float8" | "double precision" => {
                let v: Option<f64> = row.get(index);
                Ok(EgValue::from(v))
            }
            "numeric" => {
                let decimal: Option<Decimal> = row.get(index);
                match decimal {
                    Some(d) => Ok(EgValue::from(d.to_string())),
                    None => Ok(EgValue::Null),
                }
            }
            "tsvector" => Ok(EgValue::Null),
            _ => Err(format!("Unsupported column type: {col_type}").into()),
        }
    }
}
