///! EgValue
use crate::EgResult;
use crate::idl;
use opensrf::params::ApiParams;
use json::JsonValue;
use std::ops::{Index, IndexMut};
use std::fmt;
use std::sync::Arc;

// EXPERIMENT
// May not make things easier considering each fleshed object
// would need to be a mixed tree of of EgValue's and JsonValues.
// Could contain both, but then accessing leaf nodes gets more
// complicated.  We'll see.


/// Wrapper for a JsonValue which ensures that read/write access
/// my only occur on valid IDL fields.
#[derive(Debug, Clone, PartialEq)]
pub struct EgValue {
    idl_class: Arc<idl::Class>,
    value: JsonValue,
}

impl EgValue {
    /// The raw JsonValue we contain.
    ///
    /// The value will be a JSON Object with a "_classname" value.
    pub fn inner(&self) -> &JsonValue {
        &self.value
    }

    /// Gives ownership of the JSON value to the caller while replacing
    /// the local value with JSON Null.  (See JsonValue::take())
    pub fn take_inner(&mut self) -> JsonValue {
        self.value.take()
    }

    /// Our IDL class name.
    pub fn classname(&self) -> &str {
        self.idl_class.classname()
    }

    /// Returns the numeric ID of this EgValue.
    ///
    /// Handy shortcut.
    ///
    /// Returns Err if the EgValue in question has no field 
    /// called "id" or the value for the "id" field is non-numeric.
    pub fn id(&self) -> EgResult<i64> {
        if !self.idl_class.has_field("id") {
            return Err(format!("{self} has no 'id' field"))?;
        }
        
        self.value["id"].as_i64()
            .ok_or_else(|| format!("{self} 'id' value is non-numeric").into())
    }

    /// Returns the value from the primary key field if set.
    pub fn pkey_value(&self) -> Option<&JsonValue> {
        if let Some(pkey_field) = self.pkey_field() {
            Some(&self.value[pkey_field.name()])
        } else {
            None
        }
    }

    /// Value stored in the reporter:selector field if set.
    pub fn selector_value(&self) -> Option<&JsonValue> {
        if let Some(selector) = self.idl_class.selector() {
            Some(&self.value[selector])
        } else {
            None
        }
    }

    /// Returns the idl::Field for the primary key if present.
    pub fn pkey_field(&self) -> Option<&idl::Field> {
        self.idl_class.pkey_field()
    }
}

impl From<EgValue> for ApiParams {
    fn from(mut v: EgValue) -> ApiParams {
        ApiParams::from(v.take_inner())
    }
}

impl From<&EgValue> for ApiParams {
    fn from(v: &EgValue) -> ApiParams {
        ApiParams::from(v.inner())
    }
}

impl fmt::Display for EgValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = self.classname().to_string();
        if let Some(pkey) = self.pkey_value() {
            s += &format!(" pkey={pkey}");
        }
        if let Some(selector) = self.selector_value() {
            s += &format!(" label={selector}");
        }
        write!(f, "{s}")
    }
}

/// Ensures field access fails on unknown IDL class fields.
impl Index<&str> for EgValue {
    type Output = JsonValue;

    /// Returns the JsonValue stored in this EgValue at the
    /// specified index (field name).
    ///
    /// Panics if the IDL Class for this EgValue does not
    /// contain the named field.
    fn index(&self, key: &str) -> &Self::Output {
        if key.starts_with("_") || self.idl_class.has_field(key) {
            &self.value[key]
        } else {
            log::error!("IDL class {} has no field {key}", self.classname());
            panic!("IDL class {} has no field {key}", self.classname());
        }
    }
}

/// Ensures setting field values fails on unknown IDL class fields.
impl IndexMut<&str> for EgValue {
    /// Returns the mutabled JsonValue stored in this EgValue at the
    /// specified index (field name).
    ///
    /// Panics if the IDL Class for this EgValue does not
    /// contain the named field.
    fn index_mut(&mut self, key: &str) -> &mut Self::Output {
        if key.starts_with("_") || self.idl_class.has_field(key) {
            &mut self.value[key]
        } else {
            log::error!("IDL class {} has no field {key}", self.classname());
            panic!("IDL class {} has no field {key}", self.classname());
        }
    }
}


/*
    /// Translates a JsonValue into an EgValue.
    pub fn to_eg_value(&self, v: JsonValue) -> EgResult<EgValue> {
        let classname = v[CLASSNAME_KEY].as_str()
            .ok_or_else(|| format!("Invalid IDL Object: {}", v.dump()))?;

        let idl_class = self.classes().get(classname)
            .ok_or_else(|| format!("Invalid IDL class: {classname}"))?;

        Ok(EgValue {
            value: v,
            idl_class: idl_class.clone(), // Arc clone
        })
    }

*/
