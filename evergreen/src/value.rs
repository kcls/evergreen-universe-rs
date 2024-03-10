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

#[derive(Debug, PartialEq)]
pub struct ClassedObject {
    idl_class: Arc<idl::Class>,
    values: HashMap<String, EgValue>,
}

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
            JsonValue::Null => EgValue::Null,
            JsonValue::Boolean(b) => EgValue::Boolean(b),
            JsonValue::Short(_) | JsonValue::String(_) => EgValue::String(v.take_string().unwrap()),
            JsonValue::Number(n) => EgValue::Number(n),
            JsonValue::Array(mut list) => {
                let mut val_list = Vec::new();
                for v in list.drain(..) {
                    val_list.push(EgValue::from_json_value(v));
                }
                EgValue::Array(val_list)
            },
            JsonValue::Object(_) => {
                let mut map = HashMap::new();
                let mut keys: Vec<String> = v.entries().map(|(k, _)| k.to_string()).collect();

                if let Some(cls) = v[idl::CLASSNAME_KEY].as_str() {
                    if let Some(idl_class) = idl::get_class(cls) {

                        while let Some(k) = keys.pop() {
                            if k == idl::CLASSNAME_KEY {
                                // No need to store the class name since we
                                // store a ref to the class itself.
                                continue;
                            }

                            if !idl_class.fields().contains_key(&k) {
                                let err = format!("IDL class '{}' has no field named '{k}'", idl_class.classname());
                                log::error!("{}", err);
                                panic!("{}", err)
                            }

                            let val = EgValue::from_json_value(v.remove(&k));
                            map.insert(k, val);
                        }

                        EgValue::Classed(ClassedObject {
                            idl_class: idl_class.clone(), // Arc
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
                obj[idl::CLASSNAME_KEY] = json::from(o.idl_class.classname());
                for (k, v) in o.values.drain() {
                    obj[k] = v.into_json_value();
                }
                obj
            }
        }
    }

    /// True if this is an IDL-classed object
    pub fn has_class(&self) -> bool {
        match self {
            &Self::Classed(_) => true,
            _ => false
        }
    }

    /// Returns our idl::Class or panics if we are unclassed.
    fn idl_class_unchecked(&self) -> &Arc<idl::Class> {
        if let Self::Classed(ref o) = self {
            &o.idl_class
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


    /*
    /// Returns the numeric ID of this EgValue.
    ///
    /// Handy shortcut.
    ///
    /// Must be an IDL object with an "id" field and a numeric value.
    /// or the IDL class has no field called "id".
    pub fn id(&self) -> Option<i64> {
        if let ClassedObject(ref o) = self {
            if let Ok(v) = json::
        }

	    if let Ok(v) = util::json_int(&
        if self.idl_class_unchecked().has_field("id") {
            // The ID may be numeric but arrived over the network
            // as a JSON string.
            util::json_int(&self.value["id"]).expect("Cannot
        } else {
            Err(format!("Class {} has no 'id' field", self.classname()).into())
        }
    }
    */

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
}

impl fmt::Display for EgValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FOO")
        /*
        if let Some(c) = self.idl_class() {
            let mut s = self.classname().to_string();
            if let Some(pkey) = self.pkey_field() {
                let pval = self.pkey_value();
                s += &format!(" {pkey}={pval}");
            }
            if let Some(selector) = self.selector_value() {
                s += &format!(" label={selector}");
            }
            write!(f, "{s}")
        } else {
            if self.value.is_array() {
                write!(f, "<array>")
            } else if self.value.is_object() {
                write!(f, "<object>")
            } else {
                write!(f, "{}", self.value)
            }
        }
        */
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

            let mut map = HashMap::new();
            map.insert(key.to_string(), EG_NULL);
            *self = EgValue::Object(map);
            &mut self[key]
        }
    }
}


