/// Encode / Decode JSON values with class names
use serde_json as json;                                                       

const JSON_CLASS_KEY: &str = "__c";
const JSON_PAYLOAD_KEY: &str = "__p";

pub struct ClassifiedJson {
    json: json::Value,
    class: String,
}

impl ClassifiedJson {
    pub fn json(&self) -> &json::Value {
        &self.json
    }

    /// Returns the JSON value stored in the ClassifiedJson struct,
    /// replacing the value with json::Value::Null;
    pub fn take_json(&mut self) -> json::Value {
        std::mem::replace(&mut self.json, json::Value::Null)
    }

    /// Returns the class name / hint value for the classified object.
    pub fn class(&self) -> &str {
        &self.class
    }

    /// Wraps a json value in class and payload keys.
    ///
    /// Non-recursive.
    ///
    /// ```
    /// let obj = json::array![1,2,3];
    /// let obj = opensrf::classified::ClassifiedJson::classify(obj, "abc");
    /// assert_eq!(obj["__c"].as_str().unwrap(), "abc");
    /// assert_eq!(obj["__p"][1].as_u8().unwrap(), 2u8);
    /// ```
    ///
    pub fn classify(json: json::Value, class: &str) -> json::Value {
        let mut hash = json::json!({});
        hash[JSON_CLASS_KEY] = json::Value::String(class.to_string());
        hash[JSON_PAYLOAD_KEY] = json;

        hash
    }

    pub fn can_declassify(obj: &json::Value) -> bool {
        obj.is_object()
            && obj.get(JSON_CLASS_KEY).is_some()
            && obj.get(JSON_PAYLOAD_KEY).is_some()
            && obj[JSON_CLASS_KEY].is_string()
    }

    /// Turns a json value into a ClassifiedJson if it's a hash
    /// with the needed class and payload keys.
    ///
    /// Non-recursive.
    ///
    /// ```
    /// let obj = json::json!({__c: "abc", __p: [1,2,3]});
    /// let value_op = opensrf::classified::ClassifiedJson::declassify(obj);
    /// assert!(value_op.is_some());
    /// let value = value_op.unwrap();
    /// assert_eq!(value.class(), "abc");
    /// assert_eq!(value.json()[1].as_u8().unwrap(), 2u8);
    /// ```
    pub fn declassify(mut obj: json::Value) -> Option<ClassifiedJson> {
        if ClassifiedJson::can_declassify(&obj) {
            Some(ClassifiedJson {
                class: obj[JSON_CLASS_KEY].as_str().unwrap().to_owned(),
                json: obj[JSON_PAYLOAD_KEY].take(),
            })
        } else {
            None
        }
    }
}
