use crate as eg;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;
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

/// Build a virtual mvr from a bib record's display attributes
pub fn map_to_mvr(editor: &mut Editor, bib_id: i64) -> EgResult<EgValue> {
    let maps = get_display_attrs(editor, &[bib_id])?;

    let attr_set = match maps.get(&bib_id) {
        Some(m) => m,
        None => return Err(format!("Bib {bib_id} has no display attributes").into()),
    };

    let mut mvr = eg::hash! {"doc_id": bib_id};

    let idl_class = editor
        .idl()
        .classes()
        .get("mvr")
        .ok_or_else(|| format!("IDL missing class 'mvr'"))?;

    // Dynamically copy values from the display attribute set
    // into an mvr JSON object.
    let field_names = idl_class.field_names();

    for attr in attr_set.attrs.iter() {
        if field_names.contains(&attr.name.as_str()) {
            mvr[attr.name.as_str()] = match attr.value() {
                DisplayAttrValue::Value(v) => EgValue::from(v.as_str()),
                DisplayAttrValue::List(l) => EgValue::from(l.clone()),
            }
        }
    }

    EgValue::create("mvr", mvr)
}

/// Returns a HashMap mapping bib record IDs to a DisplayAttrSet.
pub fn get_display_attrs(
    editor: &mut Editor,
    bib_ids: &[i64],
) -> EgResult<HashMap<i64, DisplayAttrSet>> {
    let mut map = HashMap::new();
    let attrs = editor.search("mfde", eg::hash! {"source": bib_ids})?;

    for attr in attrs {
        let bib_id = attr["source"].as_int_unchecked();

        // First time seeing this bib record?
        if !map.contains_key(&bib_id) {
            map.insert(bib_id, DisplayAttrSet { attrs: Vec::new() });
        }

        let attr_set = map.get_mut(&bib_id).unwrap();

        let attr_name = attr["name"].as_string()?;
        let attr_label = attr["label"].as_string()?;
        let attr_value = attr["value"].as_string()?;

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
