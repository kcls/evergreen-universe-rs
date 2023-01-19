///! Tools for translating between IDL objects and Database rows.
use super::db;
use super::idl;
use chrono::prelude::*;
use json::JsonValue;
use log::{debug, trace};
use pg::types::ToSql;
use postgres as pg;
use rust_decimal::Decimal;
use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

const SUPPORTED_OPERANDS: &[&'static str] = &[
    "IS", "IS NOT", "LIKE", "ILIKE", "<", "<=", ">", ">=", "<>", "!=", "~", "=", "!~", "!~*", "~*",
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

#[derive(Debug, Clone, PartialEq)]
pub struct Pager {
    limit: usize,
    offset: usize,
}

impl Pager {
    pub fn new(limit: usize, offset: usize) -> Self {
        Pager { limit, offset }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn reset(&mut self) {
        self.limit = 0;
        self.offset = 0
    }
}

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

    pub fn is_supported_operand(op: &str) -> bool {
        SUPPORTED_OPERANDS.contains(&op.to_uppercase().as_str())
    }

    /// Retrieve an IDL object via pkey lookup.
    ///
    /// Numeric pkey values should be passed as strings.  They will be
    /// numerified withih before the query is issued.
    ///
    /// TODO: create a pkey type to handle strings, numbers, other?
    pub fn idl_class_by_pkey(
        &self,
        classname: &str,
        pkey: &str,
    ) -> Result<Option<JsonValue>, String> {
        let idl_class = match self.idl().classes().get(classname) {
            Some(c) => c,
            None => return Err(format!("No such IDL class: {classname}")),
        };

        let pkey_field = match idl_class.pkey() {
            Some(f) => f,
            None => {
                return Err(format!(
                    "IDL class {} has no pkey value and cannot be queried",
                    idl_class.classname()
                ));
            }
        };

        let idl_field = match idl_class.fields().get(pkey_field) {
            Some(f) => f,
            None => {
                return Err(format!(
                    "Field {pkey_field} is listed as pkey, but is not listed as a field"
                ))
            }
        };

        let mut filter = JsonValue::new_object();

        if idl_field.datatype().is_numeric() {
            let num = match pkey.parse::<f64>() {
                Ok(n) => n,
                Err(_) => {
                    return Err(format!(
                        "Pkey is numeric, but filter value provided is not: {pkey:?}"
                    ))
                }
            };

            filter.insert(&pkey_field, json::from(num)).unwrap();
        } else {
            filter.insert(&pkey_field, json::from(pkey)).unwrap();
        }

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

    pub fn idl_class_search(&self, search: &IdlClassSearch) -> Result<Vec<JsonValue>, String> {
        let mut results: Vec<JsonValue> = Vec::new();
        let classname = &search.classname;

        let class = match self.idl().classes().get(classname) {
            Some(c) => c,
            None => {
                return Err(format!("No such IDL class: {classname}"));
            }
        };

        let tablename = match class.tablename() {
            Some(t) => t,
            None => {
                return Err(format!(
                    "Cannot query an IDL class that has no tablename: {classname}"
                ));
            }
        };

        let select = self.compile_class_select(&class);

        let mut query = format!("{select} FROM {tablename}");

        // Track String parameters so we can use query binding on the
        // them in the final query.  All other types, being derived
        // from JsonValue, have a known shape and size (number, bool,
        // etc.), so query binding is less critical from a sql-injection
        // perspective.
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

        debug!("search() executing query: {query}");

        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for p in param_list.iter() {
            params.push(p);
        }

        let query_res = self
            .db
            .borrow_mut()
            .client()
            .query(&query[..], params.as_slice());

        if let Err(e) = query_res {
            return Err(format!("DB query failed: {e}"));
        }

        for row in query_res.unwrap() {
            results.push(self.row_to_idl(&class, &row)?);
        }

        Ok(results)
    }

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

    fn compile_class_select(&self, class: &idl::Class) -> String {
        let mut sql = String::from("SELECT");

        for (name, field) in class.fields() {
            if !field.is_virtual() {
                sql += &format!(" {name},");
            }
        }

        String::from(&sql[0..sql.len() - 1]) // Trim final ","
    }

    fn compile_pager(&self, pager: &Pager) -> String {
        format!(" LIMIT {} OFFSET {}", pager.limit(), pager.offset())
    }

    /// Generate a WHERE clause from a JSON query object for an IDL class.
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

        let mut sql = String::from(" WHERE");

        let mut first = true;
        for (field, subq) in filter.entries() {
            trace!("compile_class_filter adding filter on field: {field}");

            if class
                .fields()
                .iter()
                .filter(|(n, _)| n.eq(&field))
                .next()
                .is_none()
            {
                return Err(format!(
                    "Cannot query field '{field}' on class '{}'",
                    class.classname()
                ));
            }

            if first {
                first = false;
            } else {
                sql += " AND";
            }

            sql += &format!(" {field}");

            match subq {
                JsonValue::Array(_) => {
                    sql += &self.compile_class_filter_array(param_index, param_list, &subq)?;
                }
                JsonValue::Object(_) => {
                    sql += &self.compile_class_filter_object(param_index, param_list, &subq)?;
                }
                JsonValue::Number(_) | JsonValue::String(_) | JsonValue::Short(_) => {
                    sql += &format!(
                        " {}",
                        self.append_json_literal(param_index, param_list, subq, Some("="))?
                    );
                }
                JsonValue::Boolean(_) | JsonValue::Null => {
                    sql += &format!(
                        " {}",
                        self.append_json_literal(param_index, param_list, subq, Some("IS"))?
                    );
                }
            }
        }

        Ok(sql)
    }

    fn append_json_literal(
        &self,
        param_index: &mut usize,
        param_list: &mut Vec<String>,
        obj: &JsonValue,
        operand: Option<&str>,
    ) -> Result<String, String> {
        if obj.is_object() || obj.is_array() {
            return Err(format!("Cannot format non-literl as a literal: {obj:?}"));
        }

        let opstr = match operand {
            Some(op) => format!("{op} "),
            None => String::new(),
        };

        if obj.is_string() {
            let s = format!("{opstr}${param_index}");
            param_list.push(obj.to_string());
            *param_index += 1;
            Ok(s)
        } else {
            Ok(format!("{opstr}{}", obj.to_string()))
        }
    }

    /// Turn an object-based subquery into part of the WHERE AND.
    fn compile_class_filter_object(
        &self,
        param_index: &mut usize,
        param_list: &mut Vec<String>,
        obj: &JsonValue,
    ) -> Result<String, String> {
        let mut sql = String::new();

        for (key, val) in obj.entries() {
            let operand = key.to_uppercase();

            if !Translator::is_supported_operand(&operand) {
                Err(format!("Unsupported operand: {operand}"))?;
            }

            sql += &format!(
                " {}",
                self.append_json_literal(param_index, param_list, val, Some(&operand))?
            );

            // A filter object may only contain a single operand => value combo
            break;
        }

        Ok(sql)
    }

    /// Turn an array-based subquery into part of the WHERE AND.
    fn compile_class_filter_array(
        &self,
        param_index: &mut usize,
        param_list: &mut Vec<String>,
        arr: &JsonValue,
    ) -> Result<String, String> {
        let mut sql = String::from(" IN (");
        let mut strings: Vec<String> = Vec::new();

        for val in arr.members() {
            strings.push(self.append_json_literal(param_index, param_list, val, None)?);
        }

        sql += &strings.join(",");

        sql += ")";

        Ok(sql)
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
