//! Wrapper class for JSON values which may also contain IDL-blessed values,
//! i.e. those that have an IDL class and a well-defined set of fields.
use crate as eg;
use eg::idl;
use eg::{EgError, EgResult};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::mem;
use std::ops::{Index, IndexMut};
use std::sync::Arc;

/// Classname and payload fields for wire-protocol JSON values.
const JSON_CLASS_KEY: &str = "__c";
const JSON_PAYLOAD_KEY: &str = "__p";

/// Key to store the classname when translating blessed EgValue's
/// into flat hashes.
const HASH_CLASSNAME_KEY: &str = "_classname";

/// Macro for building EgValue::Hash's via serde_json::json!.
///
/// Keys must be quoted string literals (e.g., `"key": value`).
///
/// Panics if an attempt is made to build an EgValue::Blessed with
/// an unknown class name or invalid field, which can only happen
/// in practice if the caller defines the hash using the wire-level
/// JSON_CLASS_KEY ("__c") and JSON_PAYLOAD_KEY ("__p") structure.
///
/// let h = eg::hash! {"hello": "errbody"};
#[macro_export]
macro_rules! hash {
    ($($tts:tt)*) => {
        match $crate::EgValue::from_json_value(serde_json::json!({$($tts)*})) {
            Ok(v) => v,
            Err(e) => {
                // Unlikely to get here, but not impossible.
                let msg = format!("eg::hash! {e}");
                log::error!("{msg}");
                panic!("{}", msg);
            }
        }
    }
}

/// Macro for buildling EgValue::Blessed values by encoding the
/// classname directly in the hash via the HASH_CLASSNAME_KEY key
/// ("_classname").
///
/// Returns `Result<EgValue>` to accommodate invalid classnames or fields.
/// Becuase of this, the macro only works within functions that return
/// EgResult.
///
/// let v = eg::blessed! {
///     "_classname": "aou",
///     "id": 123,
///     "name": "TEST",
///     "shortname": "FOO",
/// }?;
#[macro_export]
macro_rules! blessed {
    ($($tts:tt)*) => {{
        match $crate::EgValue::from_json_value(serde_json::json!({$($tts)*})) {
            Ok(mut v) => {
                v.from_classed_hash()?;
                Ok(v)
            },
            Err(e) => {
                log::error!("eg::hash! {e}");
                Err(e)
            }
        }
    }}
}

/// Macro for building EgValue arrays via serde_json::json!.
///
/// let a = eg::array! ["hello", "errbody"];
#[macro_export]
macro_rules! array {
    ($($tts:tt)*) => {
        match $crate::EgValue::from_json_value(serde_json::json!([$($tts)*])) {
            Ok(v) => v,
            Err(e) => {
                // Unlikely to get here, but not impossible.
                let msg = format!("eg::array! {e}");
                log::error!("{msg}");
                panic!("{}", msg);
            }
        }
    }
}

#[test]
fn macros() {
    let v = eg::hash! {
        "hello": "stuff",
        "gbye": ["floogle", EgValue::new_object()]
    };

    assert_eq!(v["hello"].as_str(), Some("stuff"));
    assert_eq!((eg::array![1, 2, 3]).len(), 3);
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
    Number(serde_json::Number),
    Boolean(bool),
    String(String),
    Array(Vec<EgValue>),
    Hash(HashMap<String, EgValue>),
    Blessed(BlessedValue),
}

impl EgValue {
    /// Parse a JSON string and turn it into an EgValue
    ///
    /// ```
    /// use evergreen::EgValue;
    /// let v = EgValue::parse("{\"id\":123}").expect("Parse OK");
    /// assert!(v.id().is_ok());
    /// if let EgValue::Hash(h) = v {
    ///     assert!(h.get("id").is_some());
    ///     assert!(h.get("id").unwrap().is_number());
    /// } else {
    ///     panic!("Should Be Object");
    /// }
    /// ```
    pub fn parse(s: &str) -> EgResult<EgValue> {
        serde_json::from_str(s).map_err(|e| format!("JSON Parse Error: {e} : {s}").into())
    }

    /// Create a new empty blessed value using the provided class name.
    pub fn stub(classname: &str) -> EgResult<EgValue> {
        let idl_class = idl::get_class(classname)?.clone();
        Ok(EgValue::Blessed(BlessedValue {
            idl_class: idl_class.clone(),
            values: HashMap::new(),
        }))
    }

    /// Create a new blessed value from an existing Hash value using
    /// the provided class name.
    pub fn create(classname: &str, mut v: EgValue) -> EgResult<EgValue> {
        v.bless(classname)?;
        Ok(v)
    }

    /// Translate an EgValue::Hash into an EGValue::Blessed, non-recursively,
    /// using the provided class name.
    ///
    /// Returns Err if the classname is unknown or the object
    /// contains fields which are not in the IDL.
    ///
    /// Having all IDL fields is not required.
    pub fn bless(&mut self, classname: &str) -> EgResult<()> {
        let idl_class = idl::get_class(classname)?;

        // Pull the map out of the EgValue::Hash so we can inspect
        // it and eventually consume it.
        let map = match self {
            Self::Hash(h) => std::mem::take(h),
            _ => return Err("Only EgValue::Hash's can be blessed".into()),
        };

        // Verify the existing data contains only fields that are
        // represented in the IDL for the provided class.
        for k in map.keys() {
            if !idl_class.has_field(k) {
                let msg = format!("IDL class '{classname}' has no field named '{k}'");
                log::error!("{msg}");
                return Err(msg.into());
            }
        }

        // Transmute ourselves into a Blessed value and absorb the
        // existing hashmap.
        *self = EgValue::Blessed(BlessedValue {
            idl_class: idl_class.clone(),
            values: map,
        });

        Ok(())
    }

    /// Translates a Blessed value into a generic Hash value, non-recursively.
    ///
    /// Fields which are not represented in the Blessed value, but do
    /// exist in the class definition for the value, are included in the
    /// generated Hash as Null values.
    ///
    /// NO-OP for non-Blessed values.
    pub fn unbless(&mut self) {
        let (idl_class, mut map) = match self {
            Self::Blessed(o) => (&o.idl_class, std::mem::take(&mut o.values)),
            _ => return,
        };

        // Null's are not stored in Blessed values by default, but we do
        // want all of the real fields to be present in the plain Hash that's
        // generated from this method call, including NULL values.
        for field in idl_class.real_fields() {
            if !map.contains_key(field.name()) {
                map.insert(field.name().to_string(), Self::Null);
            }
        }

        // Add the _classname entry
        map.insert(
            HASH_CLASSNAME_KEY.to_string(),
            EgValue::from(idl_class.classname()),
        );

        *self = EgValue::Hash(map);
    }

    /// Translates Blessed values into generic Hash values, recursively,
    /// retaining the original classname in the HASH_CLASSNAME_KEY key.
    ///
    /// Fields which are not represented in the Blessed value, but do
    /// exist in the class definition for the value, are included in the
    /// generated Hash as Null values.
    pub fn to_classed_hash(&mut self) {
        let (idl_class, mut map) = match self {
            Self::Array(list) => {
                list.iter_mut().for_each(|v| v.to_classed_hash());
                return;
            }
            Self::Hash(h) => {
                h.values_mut().for_each(|v| v.to_classed_hash());
                return;
            }
            Self::Blessed(o) => (&o.idl_class, std::mem::take(&mut o.values)),
            _ => return,
        };

        map.values_mut().for_each(|v| v.to_classed_hash());

        // Null's are not stored in Blessed values by default, but we do
        // want all of the real fields to be present in the plain Hash that's
        // generated from this method call, including NULL values.
        for field in idl_class.real_fields() {
            if !map.contains_key(field.name()) {
                map.insert(field.name().to_string(), Self::Null);
            }
        }

        // Add the _classname entry
        map.insert(
            HASH_CLASSNAME_KEY.to_string(),
            EgValue::from(idl_class.classname()),
        );

        *self = EgValue::Hash(map);
    }

    /// Translate a raw Value, which may contain class name keys
    /// in the HASH_CLASSNAME_KEY field, into an EgValue.
    pub fn from_classed_json_hash(v: Value) -> EgResult<EgValue> {
        let mut value = EgValue::from_json_value(v)?;
        value.from_classed_hash()?;
        Ok(value)
    }

    /// Translate Hash values containing class names in the HASH_CLASSNAME_KEY
    /// into Blessed values, recursively.
    pub fn from_classed_hash(&mut self) -> EgResult<()> {
        if self.is_scalar() || self.is_blessed() {
            return Ok(());
        }

        if let Self::Array(list) = self {
            for val in list.iter_mut() {
                val.from_classed_hash()?;
            }
            return Ok(());
        }

        // Only option left is Self::Hash

        let classname = match self[HASH_CLASSNAME_KEY].as_str() {
            Some(c) => c,
            None => {
                // Vanilla, un-classed hash
                if let Self::Hash(m) = self {
                    for v in m.values_mut() {
                        v.from_classed_hash()?;
                    }
                }
                return Ok(());
            }
        };

        // This hash has class
        let idl_class = idl::get_class(classname)?.clone();

        let mut map = match self {
            Self::Hash(m) => std::mem::take(m),
            _ => return Ok(()), // can't get here
        };

        for (key, value) in map.iter_mut() {
            if key == HASH_CLASSNAME_KEY {
                // Skip _classname field.
                continue;
            }
            if !idl_class.has_field(key) {
                return Err(format!(
                    "Class '{}' has no field named '{key}'",
                    idl_class.classname()
                )
                .into());
            }

            value.from_classed_hash()?;
        }

        *self = EgValue::Blessed(BlessedValue {
            idl_class,
            values: map,
        });

        Ok(())
    }

    /// Remove NULL values from EgValue::Hash's contained within
    /// EgValue::Hash's or EgValue::Array's
    ///
    /// Does not remove NULL Array values, since that would change value
    /// positions, but may modify a hash/object which is a member of an
    /// array.
    ///
    /// ```
    /// use evergreen::EgValue;
    ///
    /// let mut h = EgValue::new_object();
    /// h["hello"] = EgValue::Null;
    /// h["hello2"] = 1.into();
    /// h["hello3"] = vec![EgValue::from(2), EgValue::Null].into();
    ///
    /// h.scrub_hash_nulls();
    ///
    /// assert!(!h.has_key("hello"));
    /// assert!(h.has_key("hello2"));
    ///
    /// // Array NULLs are retained
    /// assert_eq!(h["hello3"].len(), 2);
    /// ```
    pub fn scrub_hash_nulls(&mut self) {
        if let EgValue::Hash(m) = self {
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
        } else if let EgValue::Array(list) = self {
            for v in list.iter_mut() {
                v.scrub_hash_nulls();
            }
        }
    }

    /// True if this value is an Array and it contains the provided item.
    /// ```
    /// use evergreen::EgValue;
    /// let v = EgValue::from(vec!["yes".to_string(), "no".to_string()]);
    /// assert!(v.contains("no"));
    /// assert!(!v.contains("nope"));
    /// ```
    ///
    pub fn contains(&self, item: impl PartialEq<EgValue>) -> bool {
        match *self {
            EgValue::Array(ref vec) => vec.iter().any(|member| item == *member),
            _ => false,
        }
    }

    /// Wrap a JSON object (obj) in {"__c":"classname", "__p": obj}
    ///
    /// ```
    /// use evergreen::EgValue;
    ///
    /// let v = serde_json::json!(["one", "two", "three"]);
    /// let v = EgValue::add_class_wrapper(v, "foo");
    /// let v = EgValue::from_json_value_plain(v);
    /// assert!(v.is_object());
    /// assert_eq!(v["__c"].as_str(), Some("foo"));
    /// assert_eq!(v["__p"][0].as_str(), Some("one"));
    /// assert_eq!(EgValue::wrapped_classname(&v.into_json_value()), Some("foo"));
    /// ```
    pub fn add_class_wrapper(val: Value, class: &str) -> Value {
        serde_json::json!({
            JSON_CLASS_KEY: class,
            JSON_PAYLOAD_KEY: val,
        })
    }

    /// Un-package a value wrapped in class+payload object and return
    /// the class name and wrapped object.
    pub fn remove_class_wrapper(mut obj: Value) -> Option<(String, Value)> {
        EgValue::wrapped_classname(&obj)
            .map(|cname| cname.to_string())
            .map(|cname| (cname, obj[JSON_PAYLOAD_KEY].take()))
    }

    /// Return the classname of the wrapped object if one exists.
    ///
    /// ```
    /// use evergreen::EgValue;
    ///
    /// let h = serde_json::json!({
    ///   "__c": "yup",
    ///   "__p": [1, 2, 3]
    /// });
    ///
    /// assert_eq!(EgValue::wrapped_classname(&h), Some("yup"));
    /// ```
    pub fn wrapped_classname(obj: &Value) -> Option<&str> {
        let map = obj.as_object()?;
        if map.contains_key(JSON_CLASS_KEY) && map.contains_key(JSON_PAYLOAD_KEY) {
            map[JSON_CLASS_KEY].as_str()
        } else {
            None
        }
    }

    /// Returns the number of elements/entries contained in an EgValue
    /// Array, Hash, or BlessedValue.
    ///
    ///
    /// ```
    /// use evergreen as eg;
    /// use eg::EgValue;
    ///
    /// let v = evergreen::array! ["one", "two", "three"];
    /// assert_eq!(v.len(), 3);
    ///
    /// let v = evergreen::hash! {"just":"some","stuff":["fooozle", "fazzle", "frizzle"]};
    /// assert_eq!(v.len(), 2);
    /// ```
    pub fn len(&self) -> usize {
        match self {
            EgValue::Array(l) => l.len(),
            EgValue::Hash(h) => h.len(),
            EgValue::Blessed(b) => b.values.len(),
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

    /// Returns an owned String if this value is a String or a Number.
    ///
    /// Implementation directly mimics
    /// <https://docs.rs/json/latest/src/json/value/mod.rs.html#367-381>
    ///
    /// ```
    /// use evergreen as eg;
    /// use eg::EgValue;
    ///
    /// let mut v = EgValue::from("howdy");
    /// let s = v.take_string().expect("Has String");
    /// assert_eq!(s, "howdy");
    /// assert!(v.is_null());
    ///
    /// let mut v = EgValue::from(17.88);
    /// let s = v.take_string().expect("Has Stringifiable Number");
    /// assert_eq!(s, "17.88");
    /// assert!(v.is_null());
    ///
    /// let mut v = eg::array! [null, false];
    /// let s = v.take_string();
    /// assert!(s.is_none());
    /// ```
    pub fn take_string(&mut self) -> Option<String> {
        let mut placeholder = Self::Null;

        mem::swap(self, &mut placeholder);

        if let Self::String(s) = placeholder {
            return Some(s);
        }

        if let Self::Number(n) = placeholder {
            return Some(format!("{n}"));
        }

        // Not a Self::String value.
        mem::swap(self, &mut placeholder);

        None
    }

    /// Returns the inner Vec of this value if it's an Array, None otherwise.
    ///
    /// Inner value is replaced with an empty Vec.
    ///
    /// # Examples
    ///
    ///```
    /// use evergreen as eg;
    /// use eg::EgValue;
    ///
    /// let mut v = EgValue::from(["hello", "everyone"].as_slice());
    /// let l = v.take_vec().expect("Is Array");
    ///
    /// assert_eq!(l.len(), 2);
    /// if let EgValue::Array(newl) = v {
    ///     assert!(newl.is_empty());
    /// } else {
    ///     panic!("Something went wrong");
    /// }
    /// ```
    pub fn take_vec(&mut self) -> Option<Vec<EgValue>> {
        let mut placeholder = Self::Null;

        mem::swap(self, &mut placeholder);

        if let Self::Array(list) = placeholder {
            *self = Self::Array(Vec::new());
            Some(list)
        } else {
            // This is not an array.
            // Put the original value back
            mem::swap(self, &mut placeholder);
            None
        }
    }

    /// Turn a value into a JSON string.
    ///
    /// serde_json::to_string() can produce an Err which should be reported.
    /// TODO deprecate me and use to_json_string.
    ///
    pub fn dump(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|e| {
            log::error!("Error creating JSON string: {e}");
            "null".to_string()
        })
    }

    /// Stringify to JSON.
    ///
    /// ```
    /// use evergreen::EgValue;
    ///
    /// let v = EgValue::from("hello");
    /// assert_eq!(v.to_json_string().unwrap(), r#""hello""#);
    ///
    /// let v = EgValue::from(42);
    /// assert_eq!(v.to_json_string().unwrap(), "42");
    ///
    /// assert_eq!(EgValue::Null.to_json_string().unwrap(), "null");
    /// ```
    pub fn to_json_string(&self) -> EgResult<String> {
        serde_json::to_string(self).map_err(|e| e.to_string().into())
    }

    /// Turn a value into a pretty-printed JSON string.
    ///
    /// Note: serde_json always uses 2-space indentation; the indent
    /// parameter is accepted for API compatibility but is not used.
    pub fn pretty(&self, _indent: u16) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|e| e.to_string())
    }

    /// Push a value onto the end of an Array.
    ///
    /// Err if self is not an Array.
    pub fn push(&mut self, v: impl Into<EgValue>) -> EgResult<()> {
        if let EgValue::Array(list) = self {
            list.push(v.into());
            Ok(())
        } else {
            Err("push() requires an EgValue::Array".into())
        }
    }

    /// Insert a new value into an object-typed value.  Returns Err
    /// if this is not an object-typed value.
    pub fn insert(&mut self, key: &str, value: impl Into<EgValue>) -> EgResult<()> {
        match self {
            EgValue::Hash(o) => o.insert(key.to_string(), value.into()),
            EgValue::Blessed(o) => o.values.insert(key.to_string(), value.into()),
            _ => return Err(format!("{self} Cannot call insert() on a non-object type").into()),
        };

        Ok(())
    }

    /// True if this is a Hash or Blessed value which contains the
    /// provided key.
    /// ```
    /// use evergreen::EgValue;
    /// let v = EgValue::parse("{\"id\":123}").expect("Parses");
    /// assert!(v.has_key("id"));
    /// assert!(!v.has_key("foo"));
    /// ```
    pub fn has_key(&self, key: &str) -> bool {
        match self {
            EgValue::Hash(o) => o.contains_key(key),
            EgValue::Blessed(o) => o.values.contains_key(key),
            _ => false,
        }
    }

    /// Translates a Value into an EgValue treating values which
    /// appear to be IDL-classed values as vanilla objects.
    ///
    /// Useful if you know the data you are working with does
    /// not contain any IDL-classed content or you're interested
    /// in the parts of the message that may be Blessed.
    pub fn from_json_value_plain(v: Value) -> EgValue {
        match v {
            Value::Null => EgValue::Null,
            Value::Bool(b) => EgValue::Boolean(b),
            Value::String(s) => EgValue::String(s),
            Value::Number(n) => EgValue::Number(n),
            Value::Array(list) => EgValue::Array(
                list.into_iter()
                    .map(EgValue::from_json_value_plain)
                    .collect(),
            ),
            Value::Object(map) => {
                let mut hash = HashMap::new();
                for (k, v) in map {
                    hash.insert(k, EgValue::from_json_value_plain(v));
                }
                EgValue::Hash(hash)
            }
        }
    }

    /// Transform a Value into an EgValue.
    ///
    /// Returns an Err if the value is shaped like an IDL object
    /// but contains an unrecognized class name.
    pub fn from_json_value(v: Value) -> EgResult<EgValue> {
        match v {
            Value::Null
            | Value::Bool(_)
            | Value::Number(_)
            | Value::String(_) => Ok(EgValue::from_json_value_plain(v)),

            Value::Array(list) => {
                let mut val_list = Vec::new();
                for v in list {
                    val_list.push(EgValue::from_json_value(v)?);
                }
                Ok(EgValue::Array(val_list))
            }

            Value::Object(obj_map) => {
                // Check for IDL class wrapper
                if let (Some(classname), Some(_)) = (
                    obj_map.get(JSON_CLASS_KEY).and_then(|v| v.as_str()),
                    obj_map.get(JSON_PAYLOAD_KEY),
                ) {
                    let classname = classname.to_string();
                    let idl_class = idl::get_class(&classname)?;

                    // The payload is an array of field values for blessed objects.
                    let payload = obj_map
                        .get(JSON_PAYLOAD_KEY)
                        .cloned()
                        .unwrap_or(Value::Null);

                    let mut map = HashMap::new();
                    if let Value::Array(mut arr) = payload {
                        for field in idl_class.fields().values() {
                            if arr.len() > field.array_pos() {
                                // No point in storing NULL entries since blessed values
                                // have a known set of fields.
                                let val = mem::replace(
                                    &mut arr[field.array_pos()],
                                    Value::Null,
                                );

                                if !val.is_null() {
                                    map.insert(
                                        field.name().to_string(),
                                        EgValue::from_json_value(val)?,
                                    );
                                }
                            }
                        }
                    }

                    Ok(EgValue::Blessed(BlessedValue {
                        idl_class: idl_class.clone(),
                        values: map,
                    }))
                } else {
                    // Vanilla JSON object
                    let mut map = HashMap::new();
                    for (k, v) in obj_map {
                        map.insert(k, EgValue::from_json_value(v)?);
                    }
                    Ok(EgValue::Hash(map))
                }
            }
        }
    }

    /// Turn an EgValue into a Value consuming the EgValue.
    ///
    /// Blessed objects are serialized into IDL-classed Arrays
    pub fn into_json_value(self) -> Value {
        match self {
            EgValue::Null => Value::Null,
            EgValue::Boolean(v) => Value::Bool(v),
            EgValue::String(v) => Value::String(v),
            EgValue::Number(v) => Value::Number(v),
            EgValue::Array(list) => {
                Value::Array(list.into_iter().map(|v| v.into_json_value()).collect())
            }
            EgValue::Hash(o) => {
                let mut map = serde_json::Map::new();
                for (k, v) in o {
                    map.insert(k, v.into_json_value());
                }
                Value::Object(map)
            }
            EgValue::Blessed(mut o) => {
                let fields = o.idl_class.fields();

                // Translate the fields hash into a sorted array
                let mut sorted = fields.values().collect::<Vec<&idl::Field>>();
                sorted.sort_by_key(|f| f.array_pos());

                let mut array = Vec::new();
                for field in sorted {
                    let v = match o.values.remove(field.name()) {
                        Some(v) => v,
                        None => eg::NULL,
                    };
                    array.push(v.into_json_value());
                }

                Self::add_class_wrapper(Value::Array(array), o.idl_class.classname())
            }
        }
    }

    /// True if self is not a Hash, Blessed, or Array.
    pub fn is_scalar(&self) -> bool {
        self.is_number() || self.is_null() || self.is_boolean() || self.is_string()
    }

    pub fn is_null(&self) -> bool {
        self == &EgValue::Null
    }

    pub fn is_number(&self) -> bool {
        matches!(self, EgValue::Number(_))
    }

    /// True if this is a number or a string that is numeric.
    ///
    /// # Examples
    ///
    /// ```
    /// use evergreen::value::EgValue;
    /// assert!(EgValue::from(1).is_numeric());
    /// assert!(EgValue::from("-12.99999").is_numeric());
    /// assert!(!EgValue::from(true).is_numeric());
    /// assert!(!EgValue::from(vec![1]).is_numeric());
    /// ```
    pub fn is_numeric(&self) -> bool {
        self.as_i64().is_some() || self.as_f64().is_some()
    }

    pub fn is_string(&self) -> bool {
        matches!(self, EgValue::String(_))
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, EgValue::Boolean(_))
    }

    pub fn is_array(&self) -> bool {
        matches!(self, EgValue::Array(_))
    }

    /// True if this is a vanilla object or a classed object.
    pub fn is_object(&self) -> bool {
        matches!(self, EgValue::Hash(_) | EgValue::Blessed(_))
    }

    pub fn is_hash(&self) -> bool {
        matches!(self, &EgValue::Hash(_))
    }

    /// True if this is an IDL-classed object
    pub fn is_blessed(&self) -> bool {
        matches!(self, &EgValue::Blessed(_))
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

    /// True if the value is considered "empty".
    ///
    /// # Examples
    ///
    /// ```
    /// use evergreen::value::EgValue;
    /// assert!(EgValue::from("").is_empty());
    /// assert!(!EgValue::from(" ").is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Number(n) => n.as_f64() == Some(0.0),
            Self::String(s) => s.is_empty(),
            Self::Boolean(b) => !b,
            Self::Null => true,
            Self::Array(l) => l.is_empty(),
            Self::Hash(h) => h.is_empty(),
            Self::Blessed(h) => h.values.is_empty(),
        }
    }

    /// Variant of as_str() that produces an error if this value
    /// is not a string.
    ///
    /// NOTE if the value may exist as a Number, consider .to_string()
    /// instead, which will coerce numbers into strings.
    pub fn str(&self) -> EgResult<&str> {
        self.as_str()
            .ok_or_else(|| format!("{self} is not a string").into())
    }

    pub fn as_str(&self) -> Option<&str> {
        if let EgValue::String(s) = self {
            Some(s.as_str())
        } else {
            None
        }
    }

    /// Translates String and Number values into allocated strings.
    ///
    /// None if the value is neither a Number or String.
    ///
    /// TODO: rename this function for clarity
    ///
    /// # Examples
    ///
    /// ```
    /// use evergreen::value::EgValue;
    /// assert_eq!(EgValue::from("abc").to_string().as_deref(), Some("abc"));
    /// assert_eq!(EgValue::from(true).to_string(), None);
    /// ```
    pub fn to_string(&self) -> Option<String> {
        match self {
            EgValue::String(s) => Some(s.to_string()),
            EgValue::Number(n) => Some(format!("{n}")),
            _ => None,
        }
    }

    /// Translates String and Number values into allocated strings.
    ///
    /// Err if self cannot be stringified.
    pub fn string(&self) -> EgResult<String> {
        self.to_string()
            .ok_or_else(|| format!("{self} cannot be stringified").into())
    }

    pub fn as_int(&self) -> Option<i64> {
        self.as_i64()
    }

    /// Variant of EgValue::as_int() that produces an Err self cannot be
    /// turned into an int
    pub fn int(&self) -> EgResult<i64> {
        self.as_int()
            .ok_or_else(|| format!("{self} is not an integer").into())
    }

    /// Useful for panicing if a value cannot be coerced into an int,
    /// particularly within iterator filters, etc.
    pub fn int_required(&self) -> i64 {
        self.int().expect("No int found")
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            EgValue::Number(n) => n.as_i64(),
            // It's not uncommon to receive numeric strings over the wire.
            EgValue::String(s) => s.parse::<i64>().ok(),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            EgValue::Number(n) => n.as_u64(),
            // It's not uncommon to receive numeric strings over the wire.
            EgValue::String(s) => s.parse::<u64>().ok(),
            _ => None,
        }
    }

    pub fn as_usize(&self) -> Option<usize> {
        match self {
            EgValue::Number(n) => n.as_u64().and_then(|v| usize::try_from(v).ok()),
            // It's not uncommon to receive numeric strings over the wire.
            EgValue::String(s) => s.parse::<usize>().ok(),
            _ => None,
        }
    }

    pub fn as_isize(&self) -> Option<isize> {
        match self {
            EgValue::Number(n) => n.as_i64().and_then(|v| isize::try_from(v).ok()),
            // It's not uncommon to receive numeric strings over the wire.
            EgValue::String(s) => s.parse::<isize>().ok(),
            _ => None,
        }
    }

    pub fn as_u16(&self) -> Option<u16> {
        match self {
            EgValue::Number(n) => n.as_u64().and_then(|v| u16::try_from(v).ok()),
            // It's not uncommon to receive numeric strings over the wire.
            EgValue::String(s) => s.parse::<u16>().ok(),
            _ => None,
        }
    }

    pub fn as_i16(&self) -> Option<i16> {
        match self {
            EgValue::Number(n) => n.as_i64().and_then(|v| i16::try_from(v).ok()),
            // It's not uncommon to receive numeric strings over the wire.
            EgValue::String(s) => s.parse::<i16>().ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            EgValue::Number(n) => n.as_f64(),
            EgValue::String(s) => s.parse::<f64>().ok(),
            _ => None,
        }
    }

    /// Variant of EgValue::as_float() that produces an Err if no float
    /// value is found.
    pub fn float(&self) -> EgResult<f64> {
        self.as_float()
            .ok_or_else(|| format!("{self} is not a float").into())
    }

    /// Returns a float if we can be coerced into one.
    pub fn as_float(&self) -> Option<f64> {
        self.as_f64()
    }

    /// Returns a bool if we are a boolean value.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            EgValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// True if this EgValue is scalar and its value is true-ish.
    ///
    /// Zeros, empty strings, and strings that start with "f" are false
    /// since that's how false values are conveyed by the DB layer.
    pub fn boolish(&self) -> bool {
        match self {
            EgValue::Boolean(b) => *b,
            EgValue::Number(n) => n.as_f64() != Some(0.0),
            EgValue::String(s) => !s.is_empty() && !s.starts_with('f'),
            _ => false,
        }
    }

    /// Returns the numeric ID of this EgValue.
    ///
    /// Must be a Hash or Blessed with an "id" field and a numeric value.
    pub fn id(&self) -> EgResult<i64> {
        // If it's Blessed, verify "id" is a valid field so
        // the index lookup doesn't panic.
        if let EgValue::Blessed(o) = self
            && o.idl_class().has_field("id")
        {
            self["id"]
                .as_i64()
                .ok_or_else(|| format!("{self} has no valid ID"))?;
        }
        self["id"]
            .as_i64()
            .ok_or_else(|| format!("{self} has no valid ID").into())
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

    pub fn pkey_info(&self) -> Option<(&idl::Field, &EgValue)> {
        if let Some(f) = self.pkey_field()
            && let Some(v) = self.pkey_value()
        {
            return Some((f, v));
        }
        None
    }

    /// Value stored in the reporter:selector field if set.
    pub fn selector_value(&self) -> Option<&EgValue> {
        if let EgValue::Blessed(b) = self
            && let Some(selector) = b.idl_class.selector()
        {
            return Some(&self[selector]);
        }

        None
    }

    pub fn pop(&mut self) -> EgValue {
        if let Self::Array(list) = self {
            list.pop().unwrap_or(eg::NULL)
        } else {
            eg::NULL
        }
    }

    /// Remove and return the value found at the specified index.
    ///
    /// If the index is not present or self is not an Array,
    /// returns EgValue::Null.
    pub fn array_remove(&mut self, index: usize) -> EgValue {
        if let Self::Array(list) = self
            && list.len() > index
        {
            return list.remove(index);
        }
        eg::NULL
    }

    /// Remove a value from an object-like thing and, if found, return
    /// the value to the caller.
    pub fn remove(&mut self, key: &str) -> Option<EgValue> {
        if let Self::Hash(map) = self {
            map.remove(key)
        } else if let Self::Blessed(o) = self {
            o.values.remove(key)
        } else {
            None
        }
    }

    /// Iterator over values in an EgValue::Array.
    ///
    /// Returns an empty iterator if this is not an EgValue::Array type.
    pub fn members(&self) -> impl Iterator<Item = &EgValue> {
        match *self {
            EgValue::Array(ref list) => list.iter(),
            _ => [].iter(),
        }
    }

    /// Mutable Iterator over values in an EgValue::Array.
    ///
    /// Returns an empty iterator if this is not an EgValue::Array type.
    pub fn members_mut(&mut self) -> impl Iterator<Item = &mut EgValue> {
        match *self {
            EgValue::Array(ref mut list) => list.iter_mut(),
            _ => [].iter_mut(),
        }
    }

    /// Iterator over key-value pairs of an EgValue::{Object, Blessed}
    ///
    /// Returns an empty iterator if this is not an Object or Blessed type.
    pub fn entries(&self) -> EgValueEntries<'_> {
        EgValueEntries {
            map_iter: match self {
                EgValue::Hash(o) => Some(o.iter()),
                EgValue::Blessed(o) => Some(o.values.iter()),
                _ => None,
            },
        }
    }

    /// Mutable Iterator over key-value pairs of an EgValue::{Object, Blessed}
    ///
    /// Returns an empty iterator if this is not an Object or Blessed type.
    pub fn entries_mut(&mut self) -> EgValueEntriesMut<'_> {
        EgValueEntriesMut {
            map_iter: match self {
                EgValue::Hash(o) => Some(o.iter_mut()),
                EgValue::Blessed(o) => Some(o.values.iter_mut()),
                _ => None,
            },
        }
    }

    /// Iterator over keys of an EgValue::{Object, Blessed} type.
    ///
    /// Returns an empty iterator if this is not an Object or Blessed type.
    pub fn keys(&self) -> EgValueKeys<'_> {
        EgValueKeys {
            map_iter: match self {
                EgValue::Hash(o) => Some(o.keys()),
                EgValue::Blessed(o) => Some(o.values.keys()),
                _ => None,
            },
        }
    }

    /// De-Flesh a blessed object.
    ///
    /// Replace Object values with the primary key value for each fleshed field.
    /// Replace Array values with empty arrays.
    /// Ignore everything else.
    pub fn deflesh(&mut self) -> EgResult<()> {
        let inner = match self {
            EgValue::Blessed(i) => i,
            _ => return Ok(()),
        };

        // This alternate idl_class access allows us to modify ourselves
        // in the loop below w/o a parallel borrow
        let idl_class = inner.idl_class.clone();

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
                self[name] = eg::NULL;
            } else if let Some(pval) = self[name].pkey_value() {
                self[name] = pval.clone();
            } else {
                self[name] = eg::NULL;
            }
        }

        Ok(())
    }

    /// List of real IDL fields for this blessed value.
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

    /// True if self is a Blessed value and has the provided field
    /// by field name.
    pub fn has_real_field(&self, field: &str) -> bool {
        if let EgValue::Blessed(b) = self {
            b.idl_class().has_real_field(field)
        } else {
            false
        }
    }
}

// EgValue Iterators ------------------------------------------------------

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

// --- Serde Serialize / Deserialize for EgValue ---

impl serde::Serialize for EgValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            EgValue::Null => serializer.serialize_unit(),
            EgValue::Boolean(b) => serializer.serialize_bool(*b),
            EgValue::String(s) => serializer.serialize_str(s),
            EgValue::Number(n) => n.serialize(serializer),
            EgValue::Array(list) => {
                use serde::ser::SerializeSeq;
                let mut seq = serializer.serialize_seq(Some(list.len()))?;
                for v in list {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            EgValue::Hash(map) => {
                use serde::ser::SerializeMap;
                let mut m = serializer.serialize_map(Some(map.len()))?;
                for (k, v) in map {
                    m.serialize_entry(k, v)?;
                }
                m.end()
            }
            EgValue::Blessed(b) => {
                // Wire format: {"__c": classname, "__p": [field_values...]}
                use serde::ser::SerializeMap;
                let mut m = serializer.serialize_map(Some(2))?;
                m.serialize_entry(JSON_CLASS_KEY, b.idl_class.classname())?;

                let fields = b.idl_class.fields();
                let mut sorted: Vec<&idl::Field> = fields.values().collect();
                sorted.sort_by_key(|f| f.array_pos());

                let null = EgValue::Null;
                let values: Vec<&EgValue> = sorted
                    .iter()
                    .map(|field| b.values.get(field.name()).unwrap_or(&null))
                    .collect();

                m.serialize_entry(JSON_PAYLOAD_KEY, &values)?;
                m.end()
            }
        }
    }
}

/// Visitor for deserializing JSON into EgValue.
///
/// IDL-aware: objects matching the `{"__c": ..., "__p": ...}` class
/// wrapper pattern are deserialized as `EgValue::Blessed` if the IDL
/// class is known. Returns a deserialization error for unknown classes.
/// Use `EgValue::from_json_value_plain()` when IDL awareness is not needed.
struct EgValueVisitor;

impl<'de> serde::de::Visitor<'de> for EgValueVisitor {
    type Value = EgValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("any valid JSON value")
    }

    fn visit_bool<E>(self, v: bool) -> Result<EgValue, E> {
        Ok(EgValue::Boolean(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<EgValue, E> {
        Ok(EgValue::Number(v.into()))
    }

    fn visit_u64<E>(self, v: u64) -> Result<EgValue, E> {
        Ok(EgValue::Number(v.into()))
    }

    fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<EgValue, E> {
        match serde_json::Number::from_f64(v) {
            Some(n) => Ok(EgValue::Number(n)),
            None => Ok(EgValue::Null),
        }
    }

    fn visit_str<E>(self, v: &str) -> Result<EgValue, E> {
        Ok(EgValue::String(v.to_string()))
    }

    fn visit_string<E>(self, v: String) -> Result<EgValue, E> {
        Ok(EgValue::String(v))
    }

    fn visit_none<E>(self) -> Result<EgValue, E> {
        Ok(EgValue::Null)
    }

    fn visit_unit<E>(self) -> Result<EgValue, E> {
        Ok(EgValue::Null)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<EgValue, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut list = Vec::new();
        while let Some(v) = seq.next_element()? {
            list.push(v);
        }
        Ok(EgValue::Array(list))
    }

    fn visit_map<A>(self, mut map: A) -> Result<EgValue, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut hash: HashMap<String, EgValue> = HashMap::new();
        while let Some((key, value)) = map.next_entry::<String, EgValue>()? {
            hash.insert(key, value);
        }

        // Check for IDL class wrapper: {"__c": "classname", "__p": [...]}
        let classname = match hash.get(JSON_CLASS_KEY) {
            Some(EgValue::String(s)) => Some(s.clone()),
            _ => None,
        };

        if let Some(classname) = classname {
            if hash.contains_key(JSON_PAYLOAD_KEY) {
                let idl_class = idl::get_class(&classname).map_err(|_| {
                    serde::de::Error::custom(format!("Unknown IDL class: {classname}"))
                })?;

                let mut values = HashMap::new();
                if let Some(EgValue::Array(mut arr)) = hash.remove(JSON_PAYLOAD_KEY) {
                    for field in idl_class.fields().values() {
                        if arr.len() > field.array_pos() {
                            let val = mem::replace(&mut arr[field.array_pos()], EgValue::Null);
                            if !val.is_null() {
                                values.insert(field.name().to_string(), val);
                            }
                        }
                    }
                }

                return Ok(EgValue::Blessed(BlessedValue {
                    idl_class: idl_class.clone(),
                    values,
                }));
            }
        }

        Ok(EgValue::Hash(hash))
    }
}

impl<'de> serde::Deserialize<'de> for EgValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(EgValueVisitor)
    }
}

impl fmt::Display for EgValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EgValue::Null => write!(f, "null"),
            EgValue::Boolean(b) => write!(f, "{b}"),
            EgValue::String(s) => write!(f, "{s}"),
            EgValue::Number(n) => write!(f, "{n}"),
            EgValue::Array(_) => write!(f, "<array>"),
            EgValue::Hash(_) => write!(f, "<hash>"),
            EgValue::Blessed(o) => {
                let mut s = o.idl_class.classname().to_string();
                if let Some(pkey) = self.pkey_field()
                    && let Some(pval) = self.pkey_value()
                {
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

impl From<EgValue> for Value {
    fn from(v: EgValue) -> Value {
        v.into_json_value()
    }
}

impl TryFrom<Value> for EgValue {
    type Error = EgError;
    fn try_from(v: Value) -> EgResult<EgValue> {
        EgValue::from_json_value(v)
    }
}

impl From<Option<&str>> for EgValue {
    fn from(v: Option<&str>) -> EgValue {
        match v {
            Some(v) => EgValue::from(v),
            None => EgValue::Null,
        }
    }
}

impl From<Vec<i64>> for EgValue {
    fn from(mut v: Vec<i64>) -> EgValue {
        EgValue::Array(v.drain(..).map(EgValue::from).collect())
    }
}

impl From<&[&str]> for EgValue {
    fn from(l: &[&str]) -> EgValue {
        EgValue::Array(l.iter().map(|v| EgValue::from(*v)).collect())
    }
}

impl From<Vec<String>> for EgValue {
    fn from(mut v: Vec<String>) -> EgValue {
        EgValue::Array(v.drain(..).map(EgValue::from).collect())
    }
}

impl From<u16> for EgValue {
    fn from(v: u16) -> EgValue {
        EgValue::Number(v.into())
    }
}

impl From<bool> for EgValue {
    fn from(v: bool) -> EgValue {
        EgValue::Boolean(v)
    }
}

impl From<Option<bool>> for EgValue {
    fn from(o: Option<bool>) -> EgValue {
        if let Some(b) = o {
            EgValue::from(b)
        } else {
            eg::NULL
        }
    }
}

impl From<HashMap<std::string::String, Vec<i64>>> for EgValue {
    fn from(mut m: HashMap<std::string::String, Vec<i64>>) -> EgValue {
        let mut map: HashMap<String, EgValue> = HashMap::new();
        for (k, v) in m.drain() {
            map.insert(k, v.into());
        }
        EgValue::Hash(map)
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

impl From<Option<String>> for EgValue {
    fn from(o: Option<String>) -> EgValue {
        if let Some(s) = o {
            EgValue::from(s)
        } else {
            eg::NULL
        }
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

impl From<Option<i32>> for EgValue {
    fn from(v: Option<i32>) -> EgValue {
        if let Some(n) = v {
            EgValue::from(n)
        } else {
            eg::NULL
        }
    }
}

impl From<i8> for EgValue {
    fn from(s: i8) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<i16> for EgValue {
    fn from(s: i16) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<Option<i16>> for EgValue {
    fn from(v: Option<i16>) -> EgValue {
        if let Some(n) = v {
            EgValue::from(n)
        } else {
            eg::NULL
        }
    }
}

impl From<Option<i8>> for EgValue {
    fn from(v: Option<i8>) -> EgValue {
        if let Some(n) = v {
            EgValue::from(n)
        } else {
            eg::NULL
        }
    }
}

impl From<i64> for EgValue {
    fn from(s: i64) -> EgValue {
        EgValue::Number(s.into())
    }
}

impl From<&i64> for EgValue {
    fn from(s: &i64) -> EgValue {
        EgValue::Number((*s).into())
    }
}

impl From<Option<i64>> for EgValue {
    fn from(v: Option<i64>) -> EgValue {
        if let Some(n) = v {
            EgValue::from(n)
        } else {
            eg::NULL
        }
    }
}

impl From<f64> for EgValue {
    fn from(s: f64) -> EgValue {
        match serde_json::Number::from_f64(s) {
            Some(n) => EgValue::Number(n),
            None => EgValue::Null, // NaN/Infinity -> Null
        }
    }
}

impl From<Option<f64>> for EgValue {
    fn from(v: Option<f64>) -> EgValue {
        if let Some(n) = v {
            EgValue::from(n)
        } else {
            eg::NULL
        }
    }
}

impl From<f32> for EgValue {
    fn from(s: f32) -> EgValue {
        match serde_json::Number::from_f64(s as f64) {
            Some(n) => EgValue::Number(n),
            None => EgValue::Null, // NaN/Infinity -> Null
        }
    }
}

impl From<Option<f32>> for EgValue {
    fn from(v: Option<f32>) -> EgValue {
        if let Some(n) = v {
            EgValue::from(n)
        } else {
            eg::NULL
        }
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

impl TryFrom<(&str, EgValue)> for EgValue {
    type Error = EgError;
    fn try_from(parts: (&str, EgValue)) -> EgResult<EgValue> {
        EgValue::create(parts.0, parts.1)
    }
}

/// Allows numeric index access to EgValue::Array's
impl Index<usize> for EgValue {
    type Output = EgValue;

    /// Returns the EgValue stored in the provided index or EgValue::Null;
    fn index(&self, index: usize) -> &Self::Output {
        match self {
            Self::Array(o) => {
                if let Some(v) = o.get(index) {
                    v
                } else {
                    &eg::NULL
                }
            }
            _ => &eg::NULL,
        }
    }
}

/// Allows mutable numeric access to EgValue::Array's.
///
/// Mutably acessing an index that is beyond the size of the list will
/// cause NULL values to be appended to the list until the list reaches
/// the needed size to allow editing the specified index.
impl IndexMut<usize> for EgValue {
    fn index_mut(&mut self, index: usize) -> &mut EgValue {
        if !self.is_array() {
            *self = EgValue::new_array();
        }
        if let EgValue::Array(list) = self {
            while list.len() < index + 1 {
                list.push(eg::NULL)
            }
            &mut list[index]
        } else {
            panic!("Cannot get here")
        }
    }
}

/// Allows index-based access to EgValue Hash and Blessed values.
///
/// Allows index-based access to EgValue Hash and Blessed values.
/// Undefined values return EgValue::Null.
impl Index<&str> for EgValue {
    type Output = EgValue;

    /// Returns the EgValue stored in this EgValue at the
    /// specified index (field name).
    ///
    /// Panics if the IDL Class for this EgValue does not
    /// contain the named field.
    fn index(&self, key: &str) -> &Self::Output {
        match self {
            Self::Blessed(o) => {
                if key.starts_with('_') || o.idl_class.has_field(key) {
                    o.values.get(key).unwrap_or(&eg::NULL)
                } else {
                    let err = format!(
                        "Indexing IDL class '{}': No field named '{key}'",
                        self.classname().unwrap()
                    );
                    log::error!("{err}");
                    panic!("{}", err);
                }
            }
            EgValue::Hash(hash) => hash.get(key).unwrap_or(&eg::NULL),
            // Only Object-y things can be indexed
            _ => &eg::NULL,
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
        let (is_classed, has_field) = match self {
            Self::Blessed(o) => (true, o.idl_class.has_field(key)),
            _ => (false, false),
        };

        if is_classed {
            if !has_field || key.starts_with('_') {
                let err = format!(
                    "Indexing IDL class '{}': No field named '{key}'",
                    self.classname().unwrap()
                );
                log::error!("{err}");
                panic!("{}", err);
            }

            if let Self::Blessed(o) = self {
                if !o.values.contains_key(key) {
                    o.values.insert(key.to_string(), eg::NULL);
                }

                o.values.get_mut(key).unwrap()
            } else {
                panic!("Cannot get here");
            }
        } else {
            if let EgValue::Hash(hash) = self {
                if hash.get(key).is_none() {
                    hash.insert(key.to_string(), eg::NULL);
                }
                return hash.get_mut(key).unwrap();
            }

            // Indexing into a non-object turns it into an object.
            let mut map = HashMap::new();
            map.insert(key.to_string(), eg::NULL);
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
