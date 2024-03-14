///! EgValue
/// Wrapper class for JsonValue's which may also contain IDL-blessed values,
/// i.e. those that have an IDL class and a well-defined set of fields.
use crate as eg;
use eg::idl;
use eg::{EgError, EgResult};
use json::JsonValue;
use std::collections::HashMap;
use std::fmt;
use std::ops::{Index, IndexMut};
use std::sync::Arc;

const EG_NULL: EgValue = EgValue::Null;
const JSON_CLASS_KEY: &str = "__c";
const JSON_PAYLOAD_KEY: &str = "__p";

// ---
// Create some wrapper macros for JSON value building so that we can
// build EgValue's without having to invoke json directly.
#[macro_export]
macro_rules! hash {
    ($($tts:tt)*) => {
        eg::value::EgValue::from_json_value_plain(json::object!($($tts)*))
    }
}

#[macro_export]
macro_rules! array {
    ($($tts:tt)*) => {
        eg::value::EgValue::from_json_value_plain(json::array!($($tts)*))
    }
}
// ---

#[test]
fn macros() {
    let v = eg::hash! {
        "hello": "stuff",
        "gbye": ["floogle", EgValue::new_object()]
    };

    assert_eq!(v["hello"].as_str(), Some("stuff"));
}

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
    /// Translate an EgValue::Hash into an EGValue::Blessed using
    /// the provided class name.
    ///
    /// Returns Err of the classname is unknown or the object
    /// contains a field which is not present in the IDL class.
    ///
    /// The provided EgValue::Hash does not have to have all IDL fields.
    pub fn bless(v: EgValue, classname: &str) -> EgResult<EgValue> {
        let idl_class =
            idl::get_class(classname).ok_or_else(|| format!("No such IDL class: {classname}"))?;

        if let Self::Hash(mut h) = v {
            // Take the hashmap away from the ::Hash variant
            let myhash = std::mem::replace(&mut h, HashMap::new());

            for k in myhash.keys() {
                if !idl_class.has_field(k) {
                    let msg = format!("IDL class '{classname}' has no field named '{k}'");
                    log::error!("{msg}");
                    return Err(msg.into());
                }
            }

            Ok(EgValue::Blessed(BlessedValue {
                idl_class: idl_class.clone(),
                values: myhash,
            }))
        } else {
            Err(format!("Cannot bless a non-HASH object").into())
        }
    }

    /// Remove NULL values from EgValue::Hash's contained within
    /// EgValue::Hash's or EgValue::Array's
    ///
    /// Does not remove NULL Array values, since that would change value
    /// positions, but may modify a hash/object which is a member of an
    /// array.
    pub fn scrub_hash_nulls(&mut self) {
        if let EgValue::Hash(ref mut m) = self {
            // Build a new map containg the scrubbed values and no
            // NULLs then turn that into the map used by this EGValue.
            let mut newmap = HashMap::new();

            for (key, mut val) in m.drain() {
                if val.is_array() || val.is_object() {
                    val.scrub_hash_nulls();
                }
                if !val.is_null() {
                    newmap.insert(key, val);
                }
            }

            let _ = std::mem::replace(m, newmap);
        } else if let EgValue::Array(ref mut list) = self {
            for v in list.iter_mut() {
                v.scrub_hash_nulls();
            }
        }
    }

    pub fn contains(&self, item: impl PartialEq<EgValue>) -> bool {
        match *self {
            EgValue::Array(ref vec) => vec.iter().any(|member| item == *member),
            _ => false,
        }
    }

    /// Wrap a JSON object (obj) in {"__c":"classname", "__p": obj}
    ///
    /// ```
    /// use eversrf::EgValue;
    ///
    /// let v = json::array! ["one", "two", "three"];
    /// let v = EgValue::add_class_wrapper(v, "foo");
    /// let v = EgValue::from_json_value_plain(v);
    /// assert!(v.is_object());
    /// assert_eq!(v["__c"].as_str(), Some("foo"));
    /// assert_eq!(v["__p"][0].as_str(), Some("one"));
    /// assert_eq!(EgValue::wrapped_classname(&v.into_json_value()), Some("foo"));
    pub fn add_class_wrapper(val: JsonValue, class: &str) -> json::JsonValue {
        let mut hash = json::JsonValue::new_object();

        hash.insert(JSON_CLASS_KEY, class).expect("Is Object");
        hash.insert(JSON_PAYLOAD_KEY, val).expect("Is Object");
        hash
    }

    pub fn remove_class_wrapper(mut obj: JsonValue) -> Option<(String, JsonValue)> {
        if let Some(cname) = EgValue::wrapped_classname(&obj) {
            Some((cname.to_string(), obj[JSON_PAYLOAD_KEY].take()))
        } else {
            None
        }
    }

    pub fn wrapped_classname(obj: &JsonValue) -> Option<&str> {
        if obj.is_object()
            && obj.has_key(JSON_CLASS_KEY)
            && obj.has_key(JSON_PAYLOAD_KEY)
            && obj[JSON_CLASS_KEY].is_string()
        {
            obj[JSON_CLASS_KEY].as_str()
        } else {
            None
        }
    }

    /// Returns the number of elements/entries contained in an EgValue
    /// Array, Hash, or BlessedValue.
    ///
    ///
    /// use eversrf::EgValue;
    ///
    /// let v = eversrf::array! ["one", "two", "three"];
    /// assert_eq!(v.len(), 3);
    ///
    /// let v = eversrf::object! {"just":"some","stuff",["fooozle", "fazzle", "frizzle"]};
    /// assert_eq(v.len(), 2);
    pub fn len(&self) -> usize {
        match self {
            EgValue::Array(ref l) => l.len(),
            EgValue::Hash(ref h) => h.len(),
            EgValue::Blessed(ref b) => b.values.len(),
            _ => 0,
        }
    }

    pub fn new_object() -> EgValue {
        EgValue::Hash(HashMap::new())
    }

    pub fn new_array() -> EgValue {
        EgValue::Array(Vec::new())
    }

    /// Replace self with EgValue::Null and return what was previously
    /// stored at self.
    pub fn take(&mut self) -> EgValue {
        std::mem::replace(self, EgValue::Null)
    }

    pub fn dump(&self) -> String {
        self.clone().into_json_value().dump()
    }

    pub fn push(&mut self, v: impl Into<EgValue>) -> EgResult<()> {
        if let EgValue::Array(ref mut list) = self {
            list.push(v.into());
            Ok(())
        } else {
            Err(format!("push() requires an EgValue::Array").into())
        }
    }

    /// Insert a new value into an object-typed value.  Returns Err
    /// if this is not an object-typed value.
    pub fn insert(&mut self, key: &str, value: impl Into<EgValue>) -> EgResult<()> {
        match self {
            EgValue::Hash(ref mut o) => o.insert(key.to_string(), value.into()),
            EgValue::Blessed(ref mut o) => o.values.insert(key.to_string(), value.into()),
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

    /// Translates a JsonValue into an EgValue treating values which
    /// appear to be IDL-classed values as vanilla JsonValue::Object's
    ///
    /// Useful if you know the data you are working with does
    /// not contain any IDL-classed content.
    pub fn from_json_value_plain(mut v: JsonValue) -> EgValue {
        match v {
            JsonValue::Null => return EgValue::Null,
            JsonValue::Boolean(b) => return EgValue::Boolean(b),
            JsonValue::Short(_) | JsonValue::String(_) => {
                return EgValue::String(v.take_string().unwrap())
            }
            JsonValue::Number(n) => return EgValue::Number(n),
            JsonValue::Array(mut list) => {
                let mut val_list = Vec::new();
                for v in list.drain(..) {
                    val_list.push(EgValue::from_json_value_plain(v));
                }
                return EgValue::Array(val_list);
            }
            JsonValue::Object(_) => {
                let mut map = HashMap::new();
                let mut keys: Vec<String> = v.entries().map(|(k, _)| k.to_string()).collect();

                while let Some(k) = keys.pop() {
                    let val = EgValue::from_json_value_plain(v.remove(&k));
                    map.insert(k, val);
                }
                return EgValue::Hash(map);
            }
        };
    }

    /// Transform a JSON value into an EgValue.
    ///
    /// Returns an Err if the value is shaped like and IDL object
    /// but contains an unrecognized class name.
    pub fn from_json_value(mut v: JsonValue) -> EgResult<EgValue> {
        if v.is_number() || v.is_null() || v.is_boolean() || v.is_string() {
            return Ok(EgValue::from_json_value_plain(v));
        }

        if let JsonValue::Array(mut list) = v {
            let mut val_list = Vec::new();
            for v in list.drain(..) {
                val_list.push(EgValue::from_json_value(v)?);
            }
            return Ok(EgValue::Array(val_list));
        }

        // JSON object
        let mut map = HashMap::new();
        let mut keys: Vec<String> = v.entries().map(|(k, _)| k.to_string()).collect();

        if EgValue::wrapped_classname(&v).is_none() {
            // Vanilla JSON object
            while let Some(k) = keys.pop() {
                let val = EgValue::from_json_value(v.remove(&k))?;
                map.insert(k, val);
            }

            return Ok(EgValue::Hash(map));
        }

        let (classname, mut list) = EgValue::remove_class_wrapper(v).unwrap();

        let idl_class = idl::get_class(&classname)
            .ok_or_else(|| format!("Not and IDL class: '{classname}'"))?;

        let mut map = HashMap::new();
        for field in idl_class.fields().values() {
            if list.len() > field.array_pos() {
                map.insert(
                    field.name().to_string(),
                    EgValue::from_json_value(list[field.array_pos()].take())?,
                );
            }
        }

        Ok(EgValue::Blessed(BlessedValue {
            idl_class: idl_class.clone(),
            values: map,
        }))
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

                Self::add_class_wrapper(array, o.idl_class.classname())
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
            _ => false,
        }
    }

    pub fn is_boolean(&self) -> bool {
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
    pub fn is_blessed(&self) -> bool {
        match self {
            &EgValue::Blessed(_) => true,
            _ => false,
        }
    }

    /// Returns the IDL class if this is a blessed object.
    pub fn classname(&self) -> Option<&str> {
        if let EgValue::Blessed(b) = self {
            Some(b.idl_class.classname())
        } else {
            None
        }
    }

    pub fn idl_class(&self) -> Option<&Arc<idl::Class>> {
        if let Self::Blessed(b) = self {
            Some(b.idl_class())
        } else {
            None
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        if let EgValue::String(s) = self {
            Some(s.as_str())
        } else {
            None
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        self.as_i64()
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            EgValue::Number(n) => (*n).try_into().ok(),
            // It's not uncommon to receive numeric strings over the wire.
            EgValue::String(ref s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    pub fn as_usize(&self) -> Option<usize> {
        match self {
            EgValue::Number(n) => (*n).try_into().ok(),
            // It's not uncommon to receive numeric strings over the wire.
            EgValue::String(ref s) => s.parse::<usize>().ok(),
            _ => None,
        }
    }

    pub fn as_isize(&self) -> Option<isize> {
        match self {
            EgValue::Number(n) => (*n).try_into().ok(),
            // It's not uncommon to receive numeric strings over the wire.
            EgValue::String(ref s) => s.parse::<isize>().ok(),
            _ => None,
        }
    }

    pub fn as_u16(&self) -> Option<u16> {
        match self {
            EgValue::Number(n) => (*n).try_into().ok(),
            // It's not uncommon to receive numeric strings over the wire.
            EgValue::String(ref s) => s.parse::<u16>().ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            EgValue::Number(n) => Some((*n).into()),
            EgValue::String(ref s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        self.as_f64()
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            EgValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// True if this EgValue is a non-scalar or its scalar value is true-ish.
    ///
    /// Zeros, empty strings, and strings that start with "f" are false
    /// since that's how false values are conveyed by the DB layer.
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
                panic!("Class {} has no 'id' field", self.classname().unwrap());
            }
        } else {
            panic!("Not an IDL object: {}", self);
        }
    }

    /// Returns the idl::Field for the primary key if present.
    pub fn pkey_field(&self) -> Option<&idl::Field> {
        if let EgValue::Blessed(b) = self {
            b.idl_class.pkey_field()
        } else {
            None
        }
    }

    /// Returns the value from the primary key field.
    ///
    /// Returns None if the value has no primary key field.
    pub fn pkey_value(&self) -> Option<&EgValue> {
        if let Some(pkey_field) = self.pkey_field() {
            Some(&self[pkey_field.name()])
        } else {
            None
        }
    }

    /// Value stored in the reporter:selector field if set.
    pub fn selector_value(&self) -> Option<&EgValue> {
        if let EgValue::Blessed(b) = self {
            if let Some(selector) = b.idl_class.selector() {
                return Some(&self[selector]);
            }
        }

        None
    }

    /// Iterator over values in an EgValue::Array.
    ///
    /// Returns an empty iterator if this is not an EgValue::Array type.
    pub fn members(&self) -> EgValueMembers {
        match *self {
            EgValue::Array(ref list) => list.iter(),
            _ => [].iter(),
        }
    }

    /// Mutable Iterator over values in an EgValue::Array.
    ///
    /// Returns an empty iterator if this is not an EgValue::Array type.
    pub fn members_mut(&mut self) -> EgValueMembersMut {
        match *self {
            EgValue::Array(ref mut list) => list.iter_mut(),
            _ => [].iter_mut(),
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
            },
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
            },
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
            },
        }
    }

    /// De-Flesh a blessed object.
    ///
    /// Replace Object values with the primary key value for each fleshed field.
    /// Replace Array values with empty arrays.
    /// Ignore everything else.
    pub fn de_flesh(&mut self) -> EgResult<()> {
        let inner = match self {
            EgValue::Blessed(ref mut i) => i,
            _ => return Ok(()),
        };

        // This alternate idl_class access allows us to modify ourselves
        // in the loop below w/o a parallel borrow
        let idl_class = idl::get_class(inner.idl_class.classname()).expect("Blessed Has a Class");

        for (name, field) in idl_class.fields().iter() {
            if self[name].is_array() {
                self[name] = EgValue::new_array();
                continue;
            }

            if !self[name].is_blessed() {
                continue;
            }

            if field.is_virtual() {
                // Virtual fields can be fully cleared.
                self[name] = EG_NULL;
            } else {
                if let Some(pval) = self[name].pkey_value() {
                    self[name] = pval.clone();
                } else {
                    self[name] = EG_NULL;
                }
            }
        }

        Ok(())
    }

    /// Iterator over the real IDL fields for this blessed value.
    ///
    /// Empty iterator if this is not a blessed value.
    pub fn real_fields(&self) -> Vec<&idl::Field> {
        if let EgValue::Blessed(b) = self {
            b.idl_class().real_fields()
        } else {
            Vec::new()
        }
    }

    /// List of real field sorted by field name.
    pub fn real_fields_sorted(&self) -> Vec<&idl::Field> {
        if let EgValue::Blessed(b) = self {
            b.idl_class().real_fields_sorted()
        } else {
            Vec::new()
        }
    }

    pub fn has_real_field(&self, field: &str) -> bool {
        if let EgValue::Blessed(b) = self {
            b.idl_class().has_real_field(field)
        } else {
            false
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
    map_iter: Option<std::collections::hash_map::IterMut<'a, String, EgValue>>,
}

impl<'a> Iterator for EgValueEntriesMut<'a> {
    type Item = (&'a str, &'a mut EgValue);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = self.map_iter.as_mut() {
            iter.next().map(|(k, v)| (k.as_str(), v))
        } else {
            None
        }
    }
}

pub struct EgValueEntries<'a> {
    map_iter: Option<std::collections::hash_map::Iter<'a, String, EgValue>>,
}

impl<'a> Iterator for EgValueEntries<'a> {
    type Item = (&'a str, &'a EgValue);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = self.map_iter.as_mut() {
            iter.next().map(|(k, v)| (k.as_str(), v))
        } else {
            None
        }
    }
}

pub struct EgValueKeys<'a> {
    map_iter: Option<std::collections::hash_map::Keys<'a, String, EgValue>>,
}

impl<'a> Iterator for EgValueKeys<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = self.map_iter.as_mut() {
            iter.next().map(|k| k.as_str())
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
                    if let Some(pval) = self.pkey_value() {
                        s += &format!(" {}={pval}", pkey.name());
                    }
                }
                if let Some(selector) = self.selector_value() {
                    s += &format!(" label={selector}");
                }
                write!(f, "{s}")
            }
        }
    }
}

impl PartialEq<EgValue> for &str {
    fn eq(&self, val: &EgValue) -> bool {
        if let Some(s) = val.as_str() {
            s == *self
        } else {
            false
        }
    }
}

impl PartialEq<EgValue> for &String {
    fn eq(&self, val: &EgValue) -> bool {
        if let Some(s) = val.as_str() {
            s == self.as_str()
        } else {
            false
        }
    }
}

impl PartialEq<EgValue> for String {
    fn eq(&self, val: &EgValue) -> bool {
        if let Some(s) = val.as_str() {
            s == self.as_str()
        } else {
            false
        }
    }
}

impl PartialEq<EgValue> for i64 {
    fn eq(&self, val: &EgValue) -> bool {
        if let Some(v) = val.as_i64() {
            v == *self
        } else {
            false
        }
    }
}

impl PartialEq<EgValue> for f64 {
    fn eq(&self, val: &EgValue) -> bool {
        if let Some(v) = val.as_f64() {
            v == *self
        } else {
            false
        }
    }
}

impl PartialEq<EgValue> for bool {
    fn eq(&self, val: &EgValue) -> bool {
        if let Some(v) = val.as_bool() {
            v == *self
        } else {
            false
        }
    }
}

impl From<EgValue> for JsonValue {
    fn from(v: EgValue) -> JsonValue {
        v.into_json_value()
    }
}

impl TryFrom<JsonValue> for EgValue {
    type Error = EgError;
    fn try_from(v: JsonValue) -> EgResult<EgValue> {
        EgValue::from_json_value(v)
    }
}

impl From<bool> for EgValue {
    fn from(v: bool) -> EgValue {
        EgValue::Boolean(v)
    }
}

impl From<Vec<EgValue>> for EgValue {
    fn from(v: Vec<EgValue>) -> EgValue {
        EgValue::Array(v)
    }
}

impl From<&str> for EgValue {
    fn from(s: &str) -> EgValue {
        EgValue::String(s.to_string())
    }
}

impl From<String> for EgValue {
    fn from(s: String) -> EgValue {
        EgValue::String(s)
    }
}

impl From<i32> for EgValue {
    fn from(s: i32) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<i8> for EgValue {
    fn from(s: i8) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<i64> for EgValue {
    fn from(s: i64) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<f64> for EgValue {
    fn from(s: f64) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<f32> for EgValue {
    fn from(s: f32) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<u32> for EgValue {
    fn from(s: u32) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<u64> for EgValue {
    fn from(s: u64) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<u8> for EgValue {
    fn from(s: u8) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<usize> for EgValue {
    fn from(s: usize) -> EgValue {
        EgValue::Number(s.into())
    }
}

/// Allows numeric index access to EgValue::Array's
impl Index<usize> for EgValue {
    type Output = EgValue;

    /// Returns the JsonValue stored in the provided index or EgValue::Null;
    fn index(&self, index: usize) -> &Self::Output {
        match self {
            Self::Array(ref o) => {
                if let Some(v) = o.get(index) {
                    v
                } else {
                    &EG_NULL
                }
            }
            _ => &EG_NULL,
        }
    }
}

/// Mutably acessing an index that is beyond the size of the list will
/// cause NULL values to be appended to the list until the list reaches
/// the needed size to allow editing the specified index.
impl IndexMut<usize> for EgValue {
    fn index_mut(&mut self, index: usize) -> &mut EgValue {
        if !self.is_array() {
            *self = EgValue::new_array();
        }
        if let EgValue::Array(ref mut list) = self {
            while list.len() < index + 1 {
                list.push(EG_NULL)
            }
            &mut list[index]
        } else {
            panic!("Cannot get here")
        }
    }
}

/// Allows index-based access to EgValue Hash and Blessed values.
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
                if key.starts_with("_") || o.idl_class.has_field(key) {
                    o.values.get(key).unwrap_or(&EG_NULL)
                } else {
                    let err = format!("IDL class {} has no field {key}", self.classname().unwrap());
                    log::error!("{err}");
                    panic!("{}", err);
                }
            }
            EgValue::Hash(ref hash) => hash.get(key).unwrap_or(&EG_NULL),
            // Only Object-y things can be indexed
            _ => &EG_NULL,
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
        let (is_classed, has_field) = match self {
            Self::Blessed(o) => (true, o.idl_class.has_field(key)),
            _ => (false, false),
        };

        if is_classed {
            if has_field || key.starts_with("_") {
                let err = format!("IDL class {} has no field {key}", self.classname().unwrap());
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

impl Index<&String> for EgValue {
    type Output = EgValue;
    fn index(&self, key: &String) -> &Self::Output {
        &self[key.as_str()]
    }
}

impl IndexMut<&String> for EgValue {
    fn index_mut(&mut self, key: &String) -> &mut Self::Output {
        &mut self[key.as_str()]
    }
}
