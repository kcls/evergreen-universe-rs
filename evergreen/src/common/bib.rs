use crate as eg;
use eg::idl;
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
    Value(Option<String>),
    List(Vec<String>),
}

impl DisplayAttrValue {
    /// Get the first value for this attibute.
    ///
    /// If this is a Self::Value, return the value, otherwise return the
    /// first value in our Self::List, otherwise empty str.
    pub fn first(&self) -> &str {
        match self {
            Self::Value(op) => {
                match op {
                    Some(s) => s.as_str(),
                    None => "",
                }
            },
            Self::List(v) => v.get(0).map(|v| v.as_str()).unwrap_or(""),
        }
    }

    pub fn into_value(mut self) -> EgValue {
        match self {
            Self::Value(ref mut op) => {
                match op.take() {
                    Some(s) => EgValue::from(s),
                    None => EgValue::Null,
                }
            }
            Self::List(ref mut l) => {
                EgValue::from(l.drain(..).collect::<Vec<String>>())
            }
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
            DisplayAttrValue::Value(ref mut op) => {
                if let Some(s) = op.take() {
                    self.value = DisplayAttrValue::List(vec![s, value]);
                } else {
                    self.value = DisplayAttrValue::Value(Some(value));
                }
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
    pub fn into_value(mut self) -> EgValue {
        let mut hash = eg::hash! {};
        for attr in self.attrs.drain(..) {
            let name = attr.name().to_string(); // moved below
            hash[&name] = attr.value.into_value();
        }
        hash
    }
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
    let mut maps = get_display_attrs(editor, &[bib_id])?;

    let mut attr_set = match maps.remove(&bib_id) {
        Some(m) => m,
        None => return Err(format!("Bib {bib_id} has no display attributes").into()),
    };

    let mut mvr = eg::hash! {"doc_id": bib_id};

    let idl_class = idl::get_class("mvr")?;

    // Dynamically copy values from the display attribute set
    // into an mvr JSON object.
    let field_names = idl_class.field_names();

    for attr in attr_set.attrs.iter_mut() {
        if field_names.contains(&attr.name.as_str()) {
            let value = std::mem::replace(&mut attr.value, DisplayAttrValue::Value(None));
            mvr[&attr.name] = value.into_value();
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
        let bib_id = attr["source"].int()?;

        // First time seeing this bib record?
        if !map.contains_key(&bib_id) {
            map.insert(bib_id, DisplayAttrSet { attrs: Vec::new() });
        }

        let attr_set = map.get_mut(&bib_id).unwrap();

        let attr_name = attr["name"].to_string().expect("Required");
        let attr_label = attr["label"].to_string().expect("Required");
        let attr_value = attr["value"].to_string().expect("Required");

        if let Some(attr) = attr_set.attr_mut(&attr_name) {
            attr.add_value(attr_value);
        } else {
            let attr = DisplayAttr {
                name: attr_name,
                label: attr_label,
                value: DisplayAttrValue::Value(Some(attr_value)),
            };
            attr_set.attrs.push(attr);
        }
    }

    Ok(map)
}

pub struct RecordSummary {
    id: i64,
    record: EgValue,
    display: DisplayAttrSet,
    //attributes
    //urls
    record_note_count: usize
}

impl RecordSummary {
    pub fn into_value(mut self) -> EgValue {
        let hash = eg::hash! {
            id: self.id,
            record: self.record.take(),
            display: self.display.into_value(),
            record_note_count: self.record_note_count,
        };

        hash
    }
}

pub fn catalog_record_summary(
    editor: &mut Editor,
    bib_id: i64,
) -> EgResult<RecordSummary> {

    let flesh = eg::hash! {
        "flesh": 1,
        "flesh_fields": {
            "bre": ["mattrs", "creator", "editor", "notes"]
        }
    };

    let mut record = editor.retrieve_with_ops("bre", bib_id, flesh)?
        .ok_or_else(|| editor.die_event())?;

    let mut display_map = get_display_attrs(editor, &[bib_id])?;

    let display = display_map.remove(&bib_id)
        .ok_or_else(|| format!("Cannot load attrs for bib {bib_id}"))?;

    // TODO attrs
    // TODO urls

    let note_count = record["notes"].len();

    // Avoid including the actual notes, which may not all be public.
    record["notes"] = EgValue::new_array();

    Ok(RecordSummary {
        id: bib_id,
        record,
        display,
        record_note_count: note_count,
    })
}
