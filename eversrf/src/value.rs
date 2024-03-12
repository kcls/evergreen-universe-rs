///! EgValue
/// Wrapper class for JsonValue's which may also contain IDL-blessed values,
/// i.e. those that have an IDL class and a well-defined set of fields.
use crate::idl;
use crate::EgResult;
use crate::classified::ClassifiedJson;
use json::JsonValue;
use std::ops::{Index, IndexMut};
use std::fmt;
use std::sync::Arc;
use std::collections::HashMap;

const EG_NULL: EgValue = EgValue::Null;

/// An JSON-ish object whose structure is defined in the IDL.
#[derive(Debug, PartialEq, Clone)]
pub struct BlessedValue {
    idl_class: Arc<idl::Class>,
    values: HashMap<String, EgValue>,
}

impl BlessedValue {
    pub fn idl_class(&self) -> &Arc<idl::Class> {
        &self.idl_class
    }
    pub fn values(&self) -> &HashMap<String, EgValue> {
        &self.values
    }
}

/// Wrapper class which stores JSON-style values with one special
/// value type which maps to IDL objects.
#[derive(Debug, PartialEq, Clone)]
pub enum EgValue {
    Null,
    Number(json::number::Number),
    Boolean(bool),
    String(String),
    Array(Vec<EgValue>),
    Hash(HashMap<String, EgValue>),
    Blessed(BlessedValue),
}

impl EgValue {

    pub fn new_object() -> EgValue {
        EgValue::Hash(HashMap::new())
    }

    pub fn new_array() -> EgValue {
        EgValue::Array(Vec::new())
    }

    pub fn take(&mut self) -> EgValue {
        std::mem::replace(self, EgValue::Null)
    }

    /// Insert a new value into an object-typed value.  Returns Err
    /// if this is not an object-typed value.
    pub fn insert(&mut self, key: &str, value: EgValue) -> EgResult<()>{
        match self {
            EgValue::Hash(ref mut o) => o.insert(key.to_string(), value),
            EgValue::Blessed(ref mut o) => o.values.insert(key.to_string(), value),
            _ => return Err(format!("{self} Cannot call insert() on a non-object type").into()),
        };

        Ok(())
    }

    pub fn has_key(&self, key: &str) -> bool {
        match self {
            EgValue::Hash(ref o) => o.contains_key(key),
            EgValue::Blessed(ref o) => o.values.contains_key(key),
            _ => false,
        }
    }

    /// Transform a JSON value into an EgValue.
    ///
    /// Returns an Err if the value is shaped like and IDL object
    /// but contains an unrecognized class name.
    pub fn from_json_value(mut v: JsonValue) -> EgResult<EgValue> {
        match v {
            JsonValue::Null => return Ok(EgValue::Null),
            JsonValue::Boolean(b) => return Ok(EgValue::Boolean(b)),
            JsonValue::Short(_) | JsonValue::String(_) =>
                return Ok(EgValue::String(v.take_string().unwrap())),
            JsonValue::Number(n) => return Ok(EgValue::Number(n)),
            JsonValue::Array(mut list) => {
                let mut val_list = Vec::new();
                for v in list.drain(..) {
                    val_list.push(EgValue::from_json_value(v)?);
                }
                return Ok(EgValue::Array(val_list))
            },
            _ => {}
        };

        // JSON object
        let mut map = HashMap::new();
        let mut keys: Vec<String> = v.entries().map(|(k, _)| k.to_string()).collect();

        let classname = match ClassifiedJson::classname(&v) {
            Some(c) => c,
            None => {
                // Vanilla JSON object
                while let Some(k) = keys.pop() {
                    let val = EgValue::from_json_value(v.remove(&k))?;
                    map.insert(k, val);
                }

                return Ok(EgValue::Hash(map));
            }
        };

        let idl_class = match idl::get_class(classname) {
            Some(c) => c,
            None => return Err(format!("Not and IDL class: '{classname}'").into()),
        };

        let mut map = HashMap::new();
        for field in idl_class.fields().values() {
            map.insert(
                field.name().to_string(),
                EgValue::from_json_value(v[field.array_pos()].take())?
            );
        }

        Ok(
            EgValue::Blessed(
                BlessedValue {
                    idl_class: idl_class.clone(),
                    values: map
                }
            )
        )
    }

    /// Turn an EgValue into a vanilla JsonValue consuming the EgValue.
    ///
    /// Blessed objects are serialized into IDL-classed Arrays
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
            EgValue::Hash(mut o) => {
                let mut obj = json::object! {};
                for (k, v) in o.drain() {
                    obj[k] = v.into_json_value();
                }
                obj
            }
            EgValue::Blessed(mut o) => {
                let fields = o.idl_class.fields();

                // Translate the fields hash into a sorted array
                let mut sorted = fields.values().collect::<Vec<&idl::Field>>();
                sorted.sort_by_key(|f| f.array_pos());

                let mut array = JsonValue::new_array();

                for field in sorted {
                    let v = match o.values.remove(field.name()) {
                        Some(v) => v,
                        None => EG_NULL,
                    };

                    array.push(v.into_json_value()).expect("Is Array");
                }

                ClassifiedJson::classify(array, o.idl_class.classname())
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
            EgValue::Hash(_) | EgValue::Blessed(_) => true,
            _ => false,
        }
    }

    /// True if this is an IDL-classed object
    pub fn is_classed(&self) -> bool {
        match self {
            &EgValue::Blessed(_) => true,
            _ => false
        }
    }

    /// Returns our idl::Class or panics if we are unclassed.
    fn idl_class_unchecked(&self) -> &Arc<idl::Class> {
        if let EgValue::Blessed(ref o) = self {
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

    pub fn as_str(&self) -> Option<&str> {
        if let EgValue::String(s) = self {
            Some(s.as_str())
        } else {
            None
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            EgValue::Number(n) => (*n).try_into().ok(),
            // It's not uncommon to receive numeric strings over the wire.
            EgValue::String(ref s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            EgValue::Number(n) => Some((*n).into()),
            EgValue::String(ref s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            EgValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// True if this EgValue is a non-scalar or its scalar value is true-ish.
    ///
    /// Zeros and strings that start with "f" are false since that's how
    /// false values are conveyed by the DB layer.
    pub fn as_boolish(&self) -> bool {
        match self {
            EgValue::Boolean(b) => *b,
            EgValue::Number(n) => *n != 0,
            EgValue::String(ref s) => s.len() > 0 && !s.starts_with("f"),
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
        if let EgValue::Blessed(ref o) = self {
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

    /// Iterator over key-value pairs of an EgValue::{Object, Blessed}
    ///
    /// Returns an empty iterator if this is not an Object or Blessed type.
    pub fn entries(&self) -> EgValueEntries {
        EgValueEntries {
            map_iter: match self {
                EgValue::Hash(ref o) => Some(o.iter()),
                EgValue::Blessed(ref o) => Some(o.values.iter()),
                _ => None,
            }
        }
    }

    /// Mutable Iterator over key-value pairs of an EgValue::{Object, Blessed}
    ///
    /// Returns an empty iterator if this is not an Object or Blessed type.
    pub fn entries_mut(&mut self) -> EgValueEntriesMut {
        EgValueEntriesMut {
            map_iter: match self {
                EgValue::Hash(ref mut o) => Some(o.iter_mut()),
                EgValue::Blessed(ref mut o) => Some(o.values.iter_mut()),
                _ => None,
            }
        }
    }

    /// Iterator over keys of an EgValue::{Object, Blessed} type.
    ///
    /// Returns an empty iterator if this is not an Object or Blessed type.
    pub fn keys(&self) -> EgValueKeys {
        EgValueKeys {
            map_iter: match self {
                EgValue::Hash(ref o) => Some(o.keys()),
                EgValue::Blessed(ref o) => Some(o.values.keys()),
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
            EgValue::Hash(_) => write!(f, "<object>"),
            EgValue::Blessed(ref o) => {
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

impl From<Vec<EgValue>> for EgValue {
    fn from (v: Vec<EgValue>) -> EgValue {
        EgValue::Array(v)
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

impl From<i32> for EgValue {
    fn from (s: i32) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<i8> for EgValue {
    fn from (s: i8) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<i64> for EgValue {
    fn from (s: i64) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<f64> for EgValue {
    fn from (s: f64) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<f32> for EgValue {
    fn from (s: f32) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<u32> for EgValue {
    fn from (s: u32) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<u64> for EgValue {
    fn from (s: u64) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<u8> for EgValue {
    fn from (s: u8) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<usize> for EgValue {
    fn from (s: usize) -> EgValue {
        EgValue::Number(s.into())
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
            Self::Blessed(ref o) => {
                if key.starts_with("_") || self.idl_class_unchecked().has_field(key) {
                    o.values.get(key).unwrap_or(&EG_NULL)
                } else {
                    let err = format!("IDL class {} has no field {key}", self.classname());
                    log::error!("{err}");
                    panic!("{}", err);
                }
            },
            EgValue::Hash(ref hash) => hash.get(key).unwrap_or(&EG_NULL),
            // Only Object-y things can be indexed
            _ => &EG_NULL
        }
    }
}

/// DOCS
///
/// ```
/// use eversrf::value::EgValue;
/// let mut v = EgValue::String("hello".to_string());
/// v["blarg"] = EgValue::String("b".to_string());
/// assert_eq!(v["blarg"], EgValue::String("b".to_string()));
/// ```
impl IndexMut<&str> for EgValue {
    fn index_mut(&mut self, key: &str) -> &mut Self::Output {
        let classed = match self {
            Self::Blessed(_) => true,
             _ => false
        };

        if classed {
            let has_field = key.starts_with("_") || self.idl_class_unchecked().has_field(key);

            if !has_field {
                let err = format!("IDL class {} has no field {key}", self.classname());
                log::error!("{err}");
                panic!("{}", err);
            }

            if let Self::Blessed(ref mut o) = self {
                if o.values.get(key).is_none() {
                    o.values.insert(key.to_string(), EG_NULL);
                }

                return o.values.get_mut(key).unwrap();
            } else {
                panic!("Cannot get here");
            }

        } else {

            if let EgValue::Hash(ref mut hash) = self {
                if hash.get(key).is_none() {
                    hash.insert(key.to_string(), EG_NULL);
                }
                return hash.get_mut(key).unwrap();
            }

            // Indexing into a non-object turns it into an object.
            let mut map = HashMap::new();
            map.insert(key.to_string(), EG_NULL);
            *self = EgValue::Hash(map);
            &mut self[key]
        }
    }
}


