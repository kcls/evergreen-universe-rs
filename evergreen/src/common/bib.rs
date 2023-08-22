use crate::editor::Editor;
use crate::result::EgResult;
use crate::util;
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub enum DisplayAttrValue {
    Value(String),
    List(Vec<String>),
}

impl DisplayAttrValue {
    /// Get the first value for this attibute.
    ///
    /// If this is a Self::Value, return the value, otherwise return the
    /// first value in our Self::List, otherwise empty str.
    pub fn first(&self) -> &str {
        match self {
            Self::Value(s) => s.as_str(),
            Self::List(v) => v.get(0).map(|v| v.as_str()).unwrap_or(""),
        }
    }
}

/// Returns a HashMap mapping bib record IDs to sub-HashMaps mapping
/// display attribute field names to display attribute values, modeled
/// by DisplayAttrValue.
///
/// Conceptually:
/// {
///   123: {
///     "title": DisplayAttrValue::Value("Gone With the Wind"),
///     "subject" DisplayAttrValue::List(["War", "Radishes"]),
///   },
///   456: ...
/// }
pub fn get_display_attrs(
    editor: &mut Editor,
    bib_ids: &[i64],
) -> EgResult<HashMap<i64, HashMap<String, DisplayAttrValue>>> {
    let mut map = HashMap::new();
    let attrs = editor.search("mfde", json::object! {"source": bib_ids})?;

    for attr in attrs {
        let bib_id = util::json_int(&attr["source"])?;

        // First time seeing this bib record?
        if !map.contains_key(&bib_id) {
            map.insert(bib_id, HashMap::new());
        }

        let attr_name = util::json_string(&attr["name"])?;
        let attr_value = util::json_string(&attr["value"])?;

        let bib_map = map.get_mut(&bib_id).unwrap(); // checked above.
        let mut bib_value = bib_map.get_mut(&attr_name);

        // New entry for this attribute + bib combo
        if bib_value.is_none() {
            bib_map.insert(attr_name, DisplayAttrValue::Value(attr_value));
            continue;
        }

        let bib_value = bib_value.as_mut().unwrap();

        // Was a scalar value, but now we need to store multiples.
        if let DisplayAttrValue::Value(v) = bib_value {
            let values = vec![v.to_owned(), attr_value];
            bib_map.insert(attr_name, DisplayAttrValue::List(values));
            continue;
        }

        if let DisplayAttrValue::List(ref mut v) = bib_value {
            v.push(attr_value);
        }
    }

    Ok(map)
}
