use super::client::Client;
use json::JsonValue;

/// Generic container for translating various data types into a Vec<JsonValue>.
///
/// Add more <From> implementations as needed.
///
/// NOTE: Into<ApiParams> values that are Vec/&Vec's are treated as a
/// list of individual API call parameters.  To pass a single parameter
/// that is itself a list, pass the value as either a JsonValue::Array
/// or as a (e.g.) vec![vec![1,2,3]].
pub struct ApiParams {
    params: Vec<JsonValue>,
}

impl ApiParams {
    pub fn serialize(&self, client: &Client) -> Option<Vec<JsonValue>> {
        if let Some(s) = client.singleton().borrow().serializer() {
            Some(self.params.iter().map(|v| s.pack(&v)).collect())
        } else {
            None
        }
    }

    pub fn deserialize(&self, client: &Client) -> Option<Vec<JsonValue>> {
        if let Some(s) = client.singleton().borrow().serializer() {
            Some(self.params.iter().map(|v| s.unpack(&v)).collect())
        } else {
            None
        }
    }

    pub fn params(&self) -> &Vec<JsonValue> {
        &self.params
    }
}

impl From<&Vec<JsonValue>> for ApiParams {
    fn from(v: &Vec<JsonValue>) -> ApiParams {
        ApiParams {
            params: v.iter().map(|j| j.clone()).collect(),
        }
    }
}

impl From<Vec<JsonValue>> for ApiParams {
    fn from(v: Vec<JsonValue>) -> ApiParams {
        ApiParams { params: v }
    }
}

impl From<&Vec<&str>> for ApiParams {
    fn from(v: &Vec<&str>) -> ApiParams {
        ApiParams {
            params: v.iter().map(|j| json::from(*j)).collect(),
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
            params: v.iter().map(|j| json::from(*j)).collect(),
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
            params: v.iter().map(|j| json::from(*j)).collect(),
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
            params: v.iter().map(|j| json::from(*j)).collect(),
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
            params: v.iter().map(|s| json::from(s.to_string())).collect(),
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
        ApiParams { params: vec![v] }
    }
}

impl From<&JsonValue> for ApiParams {
    fn from(v: &JsonValue) -> ApiParams {
        ApiParams {
            params: vec![v.clone()],
        }
    }
}

impl From<&str> for ApiParams {
    fn from(v: &str) -> ApiParams {
        ApiParams::from(json::from(v))
    }
}

impl From<String> for ApiParams {
    fn from(v: String) -> ApiParams {
        ApiParams::from(json::from(v))
    }
}

impl From<i32> for ApiParams {
    fn from(v: i32) -> ApiParams {
        ApiParams::from(json::from(v))
    }
}

impl From<i64> for ApiParams {
    fn from(v: i64) -> ApiParams {
        ApiParams::from(json::from(v))
    }
}

impl From<u32> for ApiParams {
    fn from(v: u32) -> ApiParams {
        ApiParams::from(json::from(v))
    }
}

impl From<u64> for ApiParams {
    fn from(v: u64) -> ApiParams {
        ApiParams::from(json::from(v))
    }
}

impl From<u8> for ApiParams {
    fn from(v: u8) -> ApiParams {
        ApiParams::from(json::from(v))
    }
}

impl From<i8> for ApiParams {
    fn from(v: i8) -> ApiParams {
        ApiParams::from(json::from(v))
    }
}

impl From<usize> for ApiParams {
    fn from(v: usize) -> ApiParams {
        ApiParams::from(json::from(v))
    }
}

impl From<Option<JsonValue>> for ApiParams {
    fn from(v: Option<JsonValue>) -> ApiParams {
        ApiParams {
            params: match v {
                Some(v) => vec![v],
                None => Vec::new(),
            },
        }
    }
}

impl From<Option<&JsonValue>> for ApiParams {
    fn from(v: Option<&JsonValue>) -> ApiParams {
        ApiParams {
            params: match v {
                Some(v) => vec![v.clone()],
                None => Vec::new(),
            },
        }
    }
}
