use json::JsonValue;
use crate::EgValue;

/// Generic container for translating various data types into a Vec<JsonValue>.
///
/// NOTE: Into<ApiParams> values that are Vec/&Vec's are treated as a
/// list of individual API call parameters.  To pass a single parameter
/// that is itself a list, pass the value as either a JsonValue::Array
/// or as a (e.g.) vec![vec![1,2,3]].
pub struct ApiParams {
    params: Vec<EgValue>,
}

impl ApiParams {

    /*
    /// Consumes the stored parameters
    pub fn serialize(&mut self, client: &Client) -> Vec<JsonValue> {
        if let Some(s) = client.singleton().borrow().serializer() {
            let mut arr: Vec<JsonValue> = Vec::new();

            while self.params.len() > 0 {
                arr.push(s.pack(self.params.remove(0)));
            }
            arr
        } else {
            std::mem::replace(&mut self.params, Vec::new())
        }
    }

    /// Consumes the stored parameters
    pub fn deserialize(&mut self, client: &Client) -> Vec<JsonValue> {
        if let Some(s) = client.singleton().borrow().serializer() {
            let mut arr: Vec<JsonValue> = Vec::new();

            while self.params.len() > 0 {
                arr.push(s.unpack(self.params.remove(0)));
            }
            arr
        } else {
            // Replace our params with an empty array and return the
            // original params to the caller.
            std::mem::replace(&mut self.params, Vec::new())
        }
    }
    */

    pub fn params(&self) -> &Vec<EgValue> {
        &self.params
    }

    /// Add a json value to the list of params
    pub fn add(&mut self, v: EgValue) {
        self.params.push(v)
    }
}


impl From<Vec<EgValue>> for ApiParams {
    fn from(v: Vec<EgValue>) -> ApiParams {
        ApiParams { params: v }
    }
}

impl From<EgValue> for ApiParams {
    fn from(v: EgValue) -> ApiParams {
        ApiParams::from(vec![v])
    }
}

impl From<Option<EgValue>> for ApiParams {
    fn from(v: Option<EgValue>) -> ApiParams {
        ApiParams {
            params: match v {
                Some(v) => vec![v],
                None => Vec::new(),
            },
        }
    }
}

impl From<&str> for ApiParams {
    fn from(v: &str) -> ApiParams {
        ApiParams::from(EgValue::from(v))
    }
}

impl From<String> for ApiParams {
    fn from(v: String) -> ApiParams {
        ApiParams::from(EgValue::from(v))
    }
}

impl From<i32> for ApiParams {
    fn from(v: i32) -> ApiParams {
        ApiParams::from(EgValue::from(v))
    }
}


impl From<i64> for ApiParams {
    fn from(v: i64) -> ApiParams {
        ApiParams::from(EgValue::from(v))
    }
}

impl From<u32> for ApiParams {
    fn from(v: u32) -> ApiParams {
        ApiParams::from(EgValue::from(v))
    }
}


impl From<u64> for ApiParams {
    fn from(v: u64) -> ApiParams {
        ApiParams::from(EgValue::from(v))
    }
}

impl From<u8> for ApiParams {
    fn from(v: u8) -> ApiParams {
        ApiParams::from(EgValue::from(v))
    }
}


impl From<i8> for ApiParams {
    fn from(v: i8) -> ApiParams {
        ApiParams::from(EgValue::from(v))
    }
}

impl From<usize> for ApiParams {
    fn from(v: usize) -> ApiParams {
        ApiParams::from(EgValue::from(v))
    }
}

impl From<&Vec<&str>> for ApiParams {
    fn from(v: &Vec<&str>) -> ApiParams {
        ApiParams {
            params: v.iter().map(|j| EgValue::from(*j)).collect(),
        }
    }
}


impl From<Vec<&str>> for ApiParams {
    fn from(v: Vec<&str>) -> ApiParams {
        ApiParams::from(&v)
    }
}

impl From<&Vec<u8>> for ApiParams {
    fn from(v: &Vec<u8>) -> ApiParams {
        ApiParams {
            params: v.iter().map(|j| EgValue::from(*j)).collect(),
        }
    }
}

impl From<Vec<u8>> for ApiParams {
    fn from(v: Vec<u8>) -> ApiParams {
        ApiParams::from(&v)
    }
}


impl From<&Vec<i64>> for ApiParams {
    fn from(v: &Vec<i64>) -> ApiParams {
        ApiParams {
            params: v.iter().map(|j| EgValue::from(*j)).collect(),
        }
    }
}

impl From<Vec<i64>> for ApiParams {
    fn from(v: Vec<i64>) -> ApiParams {
        ApiParams::from(&v)
    }
}

impl From<&Vec<u64>> for ApiParams {
    fn from(v: &Vec<u64>) -> ApiParams {
        ApiParams {
            params: v.iter().map(|j| EgValue::from(*j)).collect(),
        }
    }
}

impl From<Vec<u64>> for ApiParams {
    fn from(v: Vec<u64>) -> ApiParams {
        ApiParams::from(&v)
    }
}

impl From<&Vec<String>> for ApiParams {
    fn from(v: &Vec<String>) -> ApiParams {
        ApiParams {
            params: v.iter().map(|s| EgValue::from(s.as_str())).collect(),
        }
    }
}

impl From<Vec<String>> for ApiParams {
    fn from(v: Vec<String>) -> ApiParams {
        ApiParams::from(&v)
    }
}

impl From<JsonValue> for ApiParams {
    fn from(v: JsonValue) -> ApiParams {
        ApiParams::from(EgValue::from_json_value(v))
    }
}

impl From<&JsonValue> for ApiParams {
    fn from(v: &JsonValue) -> ApiParams {
        ApiParams::from(v.clone())
    }
}

impl From<Option<JsonValue>> for ApiParams {
    fn from(v: Option<JsonValue>) -> ApiParams {
        ApiParams {
            params: match v {
                Some(v) => vec![EgValue::from(v)],
                None => Vec::new(),
            },
        }
    }
}
