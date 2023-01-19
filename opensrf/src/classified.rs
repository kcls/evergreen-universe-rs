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

    pub fn class(&self) -> &str {
        &self.class
    }

    /// Wraps a json value in class and payload keys.
    ///
    /// Non-recursive.
    pub fn classify(json: &json::JsonValue, class: &str) -> json::JsonValue {
        let mut hash = json::JsonValue::new_object();
        hash.insert(JSON_CLASS_KEY, class).ok();
        hash.insert(JSON_PAYLOAD_KEY, json.clone()).ok();

        hash
    }

    /// Turns a json value into a ClassifiedJson if it's a hash
    /// with the needed class and payload keys.
    ///
    /// Non-recursive.
    pub fn declassify(json: &json::JsonValue) -> Option<ClassifiedJson> {
        if json.is_object()
            && json.has_key(JSON_CLASS_KEY)
            && json.has_key(JSON_PAYLOAD_KEY)
            && json[JSON_CLASS_KEY].is_string()
        {
            Some(ClassifiedJson {
                class: json[JSON_CLASS_KEY].as_str().unwrap().to_string(),
                json: json[JSON_PAYLOAD_KEY].clone(),
            })
        } else {
            None
        }
    }
}
