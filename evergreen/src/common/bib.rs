use crate::editor::Editor;
use crate::result::EgResult;
use crate::util;
use std::collections::HashMap;

// Bib record display attributes are used widely. May as well flesh them
// out and provide a bit of structure.

/// Value for a single display attr which may contain one or
/// multiple values.
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

pub struct DisplayAttr {
    name: String,
    label: String,
    value: DisplayAttrValue,
}

impl DisplayAttr {
    pub fn add_value(&mut self, value: String) {
        match self.value {
            DisplayAttrValue::Value(ref s) => {
                // NOTE if we create an Unset variant of DisplayAttrValue
                // we can mem::replace the old value into the new list
                // sans clone.
                self.value = DisplayAttrValue::List(vec![s.clone(), value]);
            }
            DisplayAttrValue::List(ref mut l) => {
                l.push(value);
            }
        }
    }
    pub fn label(&self) -> &str {
        &self.label
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn value(&self) -> &DisplayAttrValue {
        &self.value
    }
}

/// Collection of metabib.flat_display_entry data for a given record.
pub struct DisplayAttrSet {
    attrs: Vec<DisplayAttr>,
}

impl DisplayAttrSet {
    pub fn attrs(&self) -> &Vec<DisplayAttr> {
        &self.attrs
    }

    pub fn attr(&self, name: &str) -> Option<&DisplayAttr> {
        self.attrs.iter().filter(|a| a.name.as_str() == name).next()
    }

    pub fn attr_mut(&mut self, name: &str) -> Option<&mut DisplayAttr> {
        self.attrs
            .iter_mut()
            .filter(|a| a.name.as_str() == name)
            .next()
    }

    /// Returns the first value for an attribute by name.
    ///
    /// For simplicity, returns an empty string if no attribute is found.
    pub fn first_value(&self, name: &str) -> &str {
        if let Some(attr) = self.attr(name) {
            attr.value.first()
        } else {
            ""
        }
    }
}

/// Returns a HashMap mapping bib record IDs to a DisplayAttrSet.
pub fn get_display_attrs(
    editor: &mut Editor,
    bib_ids: &[i64],
) -> EgResult<HashMap<i64, DisplayAttrSet>> {
    let mut map = HashMap::new();
    let attrs = editor.search("mfde", json::object! {"source": bib_ids})?;

    for attr in attrs {
        let bib_id = util::json_int(&attr["source"])?;

        // First time seeing this bib record?
        if !map.contains_key(&bib_id) {
            map.insert(bib_id, DisplayAttrSet { attrs: Vec::new() });
        }

        let attr_set = map.get_mut(&bib_id).unwrap();

        let attr_name = util::json_string(&attr["name"])?;
        let attr_label = util::json_string(&attr["label"])?;
        let attr_value = util::json_string(&attr["value"])?;

        if let Some(attr) = attr_set.attr_mut(&attr_name) {
            attr.add_value(attr_value);
        } else {
            let attr = DisplayAttr {
                name: attr_name,
                label: attr_label,
                value: DisplayAttrValue::Value(attr_value),
            };
            attr_set.attrs.push(attr);
        }
    }

    Ok(map)
}
