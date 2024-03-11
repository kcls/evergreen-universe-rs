///! EgValue
use crate::{EgResult, EgError};
use crate::idl;
use crate::util;
use opensrf::params::ApiParams;
use json::JsonValue;
use std::ops::{Index, IndexMut};
use std::fmt;
use std::sync::Arc;
use std::collections::HashMap;

const EG_NULL: EgValue = EgValue::Null;

/// An JSON-ish object whose structure is defined in the IDL.
#[derive(Debug, PartialEq)]
pub struct ClassedObject {
    idl_class: Arc<idl::Class>,
    values: HashMap<String, EgValue>,
}

impl ClassedObject {
    pub fn idl_class(&self) -> &Arc<idl::Class> {
        &self.idl_class
    }
    pub fn values(&self) -> &HashMap<String, EgValue> {
        &self.values
    }
}

/// Wrapper class which stores JSON-style values with one special
/// value type which maps to IDL objects.
#[derive(Debug, PartialEq)]
pub enum EgValue {
    Null,
    Number(json::number::Number),
    Boolean(bool),
    String(String),
    Array(Vec<EgValue>),
    Object(HashMap<String, EgValue>),
    Classed(ClassedObject),
}

impl EgValue {

    /// Transform a JSON value into an EgValue.
    ///
    /// Panics if the value is shaped like and IDL object but contains
    /// an unrecognized class name.
    pub fn from_json_value(mut v: JsonValue) -> EgValue {
        match v {
            JsonValue::Null => return EgValue::Null,
            JsonValue::Boolean(b) => return EgValue::Boolean(b),
            JsonValue::Short(_) | JsonValue::String(_) =>
                return EgValue::String(v.take_string().unwrap()),
            JsonValue::Number(n) => return EgValue::Number(n),
            JsonValue::Array(mut list) => {
                let mut val_list = Vec::new();
                for v in list.drain(..) {
                    val_list.push(EgValue::from_json_value(v));
                }
                return EgValue::Array(val_list)
            },
            _ => {}
        };

        // JSON object
        let mut map = HashMap::new();
        let mut keys: Vec<String> = v.entries().map(|(k, _)| k.to_string()).collect();

        if let Some(cls) = v[idl::CLASSNAME_KEY].as_str() {
            if let Some(idl_class) = idl::get_class(cls) {

                while let Some(k) = keys.pop() {
                    if k == idl::CLASSNAME_KEY {
                        // No need to store the class name since we
                        // store a ref to the idl::Class
                        continue;
                    }

                    if !k.starts_with("_") && !idl_class.fields().contains_key(&k) {
                        let err = format!("IDL class '{}' has no field named '{k}'", idl_class.classname());
                        log::error!("{}", err);
                        panic!("{}", err)
                    }

                    let val = EgValue::from_json_value(v.remove(&k));
                    map.insert(k, val);
                }

                EgValue::Classed(ClassedObject {
                    idl_class: idl_class.clone(),
                    values: map
                })

            } else {
                let err = format!("Not and IDL class: '{cls}'");
                log::error!("{}", err);
                panic!("{}", err)
            }

        } else {

            while let Some(k) = keys.pop() {
                let val = EgValue::from_json_value(v.remove(&k));
                map.insert(k, val);
            }

            EgValue::Object(map)
        }
    }

    /// Turn an EgValue into a vanilla JsonValue consuming the EgValue.
    pub fn into_json_value(self) -> JsonValue {
        match self {
            EgValue::Null => JsonValue::Null,
            EgValue::Boolean(v) => JsonValue::Boolean(v),
            EgValue::String(v) => JsonValue::String(v),
            EgValue::Number(v) => json::from(v),
            EgValue::Array(mut list) => {
                let mut list2 = Vec::new();
                for v in list.drain(..) {
                    list2.push(v.into_json_value());
                }
                json::from(list2)
            }
            EgValue::Object(mut o) => {
                let mut obj = json::object! {};
                for (k, v) in o.drain() {
                    obj[k] = v.into_json_value();
                }
                obj
            }
            EgValue::Classed(mut o) => {
                let mut obj = json::object! {};
                obj[idl::CLASSNAME_KEY] = json::from(o.idl_class().classname());
                for (k, v) in o.values.drain() {
                    obj[k] = v.into_json_value();
                }
                obj
            }
        }
    }

    pub fn is_null(&self) -> bool {
        self == &EgValue::Null
    }

    pub fn is_number(&self) -> bool {
        match self {
            EgValue::Number(_) => true,
            _ => false,
        }
    }

    pub fn is_string(&self) -> bool {
        match self {
            EgValue::String(_) => true,
            _ => false
        }
    }

    pub fn is_bool(&self) -> bool {
        match self {
            EgValue::Boolean(_) => true,
            _ => false,
        }
    }

    pub fn is_array(&self) -> bool {
        match self {
            EgValue::Array(_) => true,
            _ => false,
        }
    }

    /// True if this is a vanilla object or a classed object.
    pub fn is_object(&self) -> bool {
        match self {
            EgValue::Object(_) | EgValue::Classed(_) => true,
            _ => false,
        }
    }

    /// True if this is an IDL-classed object
    pub fn is_classed(&self) -> bool {
        match self {
            &Self::Classed(_) => true,
            _ => false
        }
    }

    /// Returns our idl::Class or panics if we are unclassed.
    fn idl_class_unchecked(&self) -> &Arc<idl::Class> {
        if let Self::Classed(ref o) = self {
            o.idl_class()
        } else {
            let s = format!("EgValue is not an IDL object: {self}");
            log::error!("{s}");
            panic!("{s}");
        }
    }

    /// Our IDL class name.
    pub fn classname(&self) -> &str {
        self.idl_class_unchecked().classname()
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Self::Number(n) => (*n).try_into().ok(),
            // It's not uncommon to receive numeric strings over the wire.
            Self::String(ref s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Self::Number(n) => Some((*n).into()),
            Self::String(ref s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// True if this EgValue is a non-scalar or its scalar value is true-ish.
    ///
    /// Zeros and strings that start with "f" are false since that's how
    /// false values are conveyed by the DB layer.
    pub fn as_boolish(&self) -> bool {
        match self {
            Self::Boolean(b) => *b,
            Self::Number(n) => *n != 0,
            Self::String(ref s) => s.len() > 0 && !s.starts_with("f"),
            _ => true,
        }
    }

    /// Returns the numeric ID of this EgValue.
    ///
    /// Handy shortcut.
    ///
    /// Must be an IDL object with an "id" field and a numeric value.
    /// or the IDL class has no field called "id".
    pub fn id(&self) -> Option<i64> {
        if let Self::Classed(ref o) = self {
            if o.idl_class().has_field("id") {
                self["id"].as_int()
            } else {
                panic!("Class {} has no 'id' field", self.classname());
            }
        } else {
            panic!("Not an IDL object: {}", self);
        }
    }

    /// Returns the idl::Field for the primary key if present.
    pub fn pkey_field(&self) -> Option<&idl::Field> {
        self.idl_class_unchecked().pkey_field()
    }

    /// Returns the value from the primary key field.
    ///
    /// Returns EgNull if the pkey value is NULL or the
    /// IDL class in question has no primary key field.
    pub fn pkey_value(&self) -> &EgValue {
        if let Some(pkey_field) = self.pkey_field() {
            &self[pkey_field.name()]
        } else {
            &EG_NULL
        }
    }

    /// Value stored in the reporter:selector field if set.
    pub fn selector_value(&self) -> Option<&EgValue> {
        if let Some(selector) = self.idl_class_unchecked().selector() {
            Some(&self[selector])
        } else {
            None
        }
    }

    /// Iterator over values in an EgValue::Array.
    ///
    /// Returns an empty iterator if this is not an EgValue::Array type.
    pub fn members(&self) -> EgValueMembers {
        match *self {
            EgValue::Array(ref list) => list.iter(),
            _ => [].iter()
        }
    }

    /// Mutable Iterator over values in an EgValue::Array.
    ///
    /// Returns an empty iterator if this is not an EgValue::Array type.
    pub fn members_mut(&mut self) -> EgValueMembersMut {
        match *self {
            EgValue::Array(ref mut list) => list.iter_mut(),
            _ => [].iter_mut()
        }
    }

    /// Iterator over key-value pairs of an EgValue::{Object, Classed}
    ///
    /// Returns an empty iterator if this is not an Object or Classed type.
    pub fn entries(&self) -> EgValueEntries {
        EgValueEntries {
            map_iter: match self {
                EgValue::Object(ref o) => Some(o.iter()),
                EgValue::Classed(ref o) => Some(o.values.iter()),
                _ => None,
            }
        }
    }

    /// Mutable Iterator over key-value pairs of an EgValue::{Object, Classed}
    ///
    /// Returns an empty iterator if this is not an Object or Classed type.
    pub fn entries_mut(&mut self) -> EgValueEntriesMut {
        EgValueEntriesMut {
            map_iter: match self {
                EgValue::Object(ref mut o) => Some(o.iter_mut()),
                EgValue::Classed(ref mut o) => Some(o.values.iter_mut()),
                _ => None,
            }
        }
    }

    /// Iterator over keys of an EgValue::{Object, Classed} type.
    ///
    /// Returns an empty iterator if this is not an Object or Classed type.
    pub fn keys(&self) -> EgValueKeys {
        EgValueKeys {
            map_iter: match self {
                EgValue::Object(ref o) => Some(o.keys()),
                EgValue::Classed(ref o) => Some(o.values.keys()),
                _ => None,
            }
        }
    }
}

// EgValue Iterators ------------------------------------------------------

// List iterators are simply standard slices.
pub type EgValueMembers<'a> = std::slice::Iter<'a, EgValue>;
pub type EgValueMembersMut<'a> = std::slice::IterMut<'a, EgValue>;

// HashMap iterators are a little more complicated and required
// tracking the hashmap iterator within a custom iterator type.

pub struct EgValueEntriesMut<'a> {
    map_iter: Option<std::collections::hash_map::IterMut<'a, String, EgValue>>
}

impl<'a> Iterator for EgValueEntriesMut<'a> {
    type Item = (&'a String, &'a mut EgValue);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = self.map_iter.as_mut() {
            iter.next()
        } else {
            None
        }
    }
}

pub struct EgValueEntries<'a> {
    map_iter: Option<std::collections::hash_map::Iter<'a, String, EgValue>>
}

impl<'a> Iterator for EgValueEntries<'a> {
    type Item = (&'a String, &'a EgValue);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = self.map_iter.as_mut() {
            iter.next()
        } else {
            None
        }
    }
}

pub struct EgValueKeys<'a> {
    map_iter: Option<std::collections::hash_map::Keys<'a, String, EgValue>>
}

impl<'a> Iterator for EgValueKeys<'a> {
    type Item = &'a String;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = self.map_iter.as_mut() {
            iter.next()
        } else {
            None
        }
    }
}


impl fmt::Display for EgValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EgValue::Null => write!(f, "null"),
            EgValue::Boolean(b) => write!(f, "{b}"),
            EgValue::String(ref s) => write!(f, "{s}"),
            EgValue::Number(n) => write!(f, "{n}"),
            EgValue::Array(_) => write!(f, "<array>"),
            EgValue::Object(_) => write!(f, "<object>"),
            EgValue::Classed(ref o) => {
                let mut s = o.idl_class.classname().to_string();
                if let Some(pkey) = self.pkey_field() {
                    let pval = self.pkey_value();
                    s += &format!(" {}={pval}", pkey.name());
                }
                if let Some(selector) = self.selector_value() {
                    s += &format!(" label={selector}");
                }
                write!(f, "{s}")
            }
        }
    }
}

impl From<JsonValue> for EgValue {
    fn from(v: JsonValue) -> EgValue {
        EgValue::from_json_value(v)
    }
}

impl From<&str> for EgValue {
    fn from (s: &str) -> EgValue {
        EgValue::String(s.to_string())
    }
}

impl From<String> for EgValue {
    fn from (s: String) -> EgValue {
        EgValue::String(s)
    }
}

impl From<EgValue> for ApiParams {
    fn from(v: EgValue) -> ApiParams {
        ApiParams::from(v.into_json_value())
    }
}

impl From<EgValue> for JsonValue {
    fn from(v: EgValue) -> JsonValue {
        v.into_json_value()
    }
}


/// Allows index-based access to EgValue's
///
/// Follows the pattern of JsonValue where undefined values are all null's
impl Index<&str> for EgValue {
    type Output = EgValue;

    /// Returns the JsonValue stored in this EgValue at the
    /// specified index (field name).
    ///
    /// Panics if the IDL Class for this EgValue does not
    /// contain the named field.
    fn index(&self, key: &str) -> &Self::Output {
        match self {
            Self::Classed(ref o) => {
                if key.starts_with("_") || self.idl_class_unchecked().has_field(key) {
                    o.values.get(key).unwrap_or(&EG_NULL)
                } else {
                    let err = format!("IDL class {} has no field {key}", self.classname());
                    log::error!("{err}");
                    panic!("{}", err);
                }
            },
            Self::Object(ref hash) => hash.get(key).unwrap_or(&EG_NULL),
            // Only Object-y things can be indexed
            _ => &EG_NULL
        }
    }
}

/// DOCS
///
/// ```
/// use evergreen::value::EgValue;
/// let mut v = EgValue::String("hello".to_string());
/// v["blarg"] = EgValue::String("b".to_string());
/// assert_eq!(v["blarg"], EgValue::String("b".to_string()));
/// ```
impl IndexMut<&str> for EgValue {
    fn index_mut(&mut self, key: &str) -> &mut Self::Output {
        let classed = match self {
            Self::Classed(_) => true,
             _ => false
        };

        if classed {
            let has_field = key.starts_with("_") || self.idl_class_unchecked().has_field(key);

            if !has_field {
                let err = format!("IDL class {} has no field {key}", self.classname());
                log::error!("{err}");
                panic!("{}", err);
            }

            if let Self::Classed(ref mut o) = self {
                if o.values.get(key).is_none() {
                    o.values.insert(key.to_string(), EG_NULL);
                }

                return o.values.get_mut(key).unwrap();
            } else {
                panic!("Cannot get here");
            }

        } else {

            if let Self::Object(ref mut hash) = self {
                if hash.get(key).is_none() {
                    hash.insert(key.to_string(), EG_NULL);
                }
                return hash.get_mut(key).unwrap();
            }

            // Indexing into a non-object turns it into an object.
            let mut map = HashMap::new();
            map.insert(key.to_string(), EG_NULL);
            *self = EgValue::Object(map);
            &mut self[key]
        }
    }
}


