/// Encode / Decode JSON values with class names

const JSON_CLASS_KEY: &str = "__c";
const JSON_PAYLOAD_KEY: &str = "__p";

pub struct ClassifiedJson {
    json: json::JsonValue,
    class: String,
}

impl ClassifiedJson {
    pub fn json(&self) -> &json::JsonValue {
        &self.json
    }

    pub fn take_json(&mut self) -> json::JsonValue {
        std::mem::replace(&mut self.json, json::JsonValue::Null)
    }

    pub fn class(&self) -> &str {
        &self.class
    }

    /// Wraps a json value in class and payload keys.
    ///
    /// Non-recursive.
    pub fn classify(json: json::JsonValue, class: &str) -> json::JsonValue {
        let mut hash = json::JsonValue::new_object();
        hash.insert(JSON_CLASS_KEY, class).ok();
        hash.insert(JSON_PAYLOAD_KEY, json).ok();

        hash
    }

    pub fn can_declassify(obj: &json::JsonValue) -> bool {
        obj.is_object()
            && obj.has_key(JSON_CLASS_KEY)
            && obj.has_key(JSON_PAYLOAD_KEY)
            && obj[JSON_CLASS_KEY].is_string()
    }

    /// Turns a json value into a ClassifiedJson if it's a hash
    /// with the needed class and payload keys.
    ///
    /// Non-recursive.
    pub fn declassify(mut obj: json::JsonValue) -> Option<ClassifiedJson> {
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
