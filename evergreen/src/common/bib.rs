use crate as eg;
use eg::common::holds;
use eg::idl;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;
use marc;
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
            Self::Value(op) => match op {
                Some(s) => s.as_str(),
                None => "",
            },
            Self::List(v) => v.first().map(|v| v.as_str()).unwrap_or(""),
        }
    }

    pub fn into_value(mut self) -> EgValue {
        match self {
            Self::Value(ref mut op) => op.take().map(EgValue::from).unwrap_or(EgValue::Null),
            Self::List(ref mut l) => EgValue::from(std::mem::take(l)),
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
        self.attrs.iter().find(|a| a.name.as_str() == name)
    }

    pub fn attr_mut(&mut self, name: &str) -> Option<&mut DisplayAttr> {
        self.attrs.iter_mut().find(|a| a.name.as_str() == name)
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
        let attr_set = map
            .entry(bib_id)
            .or_insert_with(|| DisplayAttrSet { attrs: Vec::new() });

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
    attributes: EgValue,
    urls: Option<Vec<RecordUrl>>,
    record_note_count: usize,
    copy_counts: Vec<EgValue>,
    hold_count: i64,
    has_holdable_copy: bool,
}

impl RecordSummary {
    pub fn into_value(mut self) -> EgValue {
        let mut urls = EgValue::new_array();

        if let Some(mut list) = self.urls.take() {
            for v in list.drain(..) {
                urls.push(v.into_value()).expect("Is Array");
            }
        }

        let copy_counts = std::mem::take(&mut self.copy_counts);

        eg::hash! {
            id: self.id,
            record: self.record.take(),
            display: self.display.into_value(),
            record_note_count: self.record_note_count,
            attributes: self.attributes.take(),
            copy_counts: EgValue::from(copy_counts),
            hold_count: self.hold_count,
            urls: urls,
            has_holdable_copy: self.has_holdable_copy,
            // TODO
            staff_view_metabib_attributes: eg::hash!{},
            // TODO
            staff_view_metabib_records: eg::array! [],
        }
    }
}

pub fn catalog_record_summary(
    editor: &mut Editor,
    org_id: i64,
    rec_id: i64,
    is_staff: bool,
    is_meta: bool,
) -> EgResult<RecordSummary> {
    let flesh = eg::hash! {
        "flesh": 1,
        "flesh_fields": {
            "bre": ["mattrs", "creator", "editor", "notes"]
        }
    };

    let mut record = editor
        .retrieve_with_ops("bre", rec_id, flesh)?
        .ok_or_else(|| editor.die_event())?;

    let mut display_map = get_display_attrs(editor, &[rec_id])?;

    let display = display_map
        .remove(&rec_id)
        .ok_or_else(|| format!("Cannot load attrs for bib {rec_id}"))?;

    // Create an object of 'mraf' attributes.
    // Any attribute can be multi so dedupe and array-ify all of them.

    let mut attrs = EgValue::new_object();
    for attr in record["mattrs"].members_mut() {
        let name = attr["attr"].take();
        let val = attr["value"].take();

        if let EgValue::Array(ref mut list) = attrs[name.str()?] {
            list.push(val);
        } else {
            attrs[name.str()?] = vec![val].into();
        }
    }

    let urls = record_urls(editor, None, Some(record["marc"].str()?))?;

    let note_count = record["notes"].len();
    let copy_counts = record_copy_counts(editor, org_id, rec_id, is_staff, is_meta)?;
    let hold_count = holds::record_hold_counts(editor, rec_id, None)?;
    let has_holdable_copy = holds::record_has_holdable_copy(editor, rec_id, is_meta)?;

    // Avoid including the actual notes, which may not all be public.
    record["notes"].take();

    // De-bulk-ify
    record["marc"].take();
    record["mattrs"].take();

    Ok(RecordSummary {
        id: rec_id,
        record,
        display,
        urls,
        copy_counts,
        hold_count,
        attributes: attrs,
        has_holdable_copy,
        record_note_count: note_count,
    })
}

pub struct RecordUrl {
    href: String,
    label: Option<String>,
    notes: Option<String>,
    ind2: String,
}

impl RecordUrl {
    pub fn into_value(self) -> EgValue {
        eg::hash! {
            href: self.href,
            label: self.label,
            notes: self.notes,
            ind2: self.ind2
        }
    }
}

/// Extract/compile 856 URL values from a MARC record.
pub fn record_urls(
    editor: &mut Editor,
    bib_id: Option<i64>,
    xml: Option<&str>,
) -> EgResult<Option<Vec<RecordUrl>>> {
    let rec_binding;

    let xml = match xml.as_ref() {
        Some(x) => x,
        None => {
            if let Some(id) = bib_id {
                rec_binding = editor
                    .retrieve("bre", id)?
                    .ok_or_else(|| editor.die_event())?;
                rec_binding["marc"].str()?
            } else {
                return Err("bib::record_urls requires params".into());
            }
        }
    };

    let record = match marc::Record::from_xml(xml).next() {
        Some(result) => result?,
        None => return Err("MARC XML parsing returned no result".into()),
    };

    let mut urls_maybe = None;

    for field in record.get_fields("856").iter() {
        if field.ind1() != "4" {
            continue;
        }

        // asset.uri's
        if field.has_subfield("9") || field.has_subfield("w") || field.has_subfield("n") {
            continue;
        }

        let label_sf = field.first_subfield("y");
        let notes_sf = field.first_subfield("z").or(field.first_subfield("3"));

        for href in field.get_subfields("u").iter() {
            if href.content().trim().is_empty() {
                continue;
            }

            // It's possible for multiple $u's to exist within 1 856 tag.
            // in that case, honor the label/notes data for the first $u, but
            // leave any subsequent $u's as unadorned href's.
            // use href/link/note keys to be consistent with args.uri's

            let label = label_sf.map(|l| l.content().to_string());
            let notes = notes_sf.map(|v| v.content().to_string());

            let url = RecordUrl {
                label,
                notes,
                href: href.content().to_string(),
                ind2: field.ind2().to_string(),
            };

            let urls = match urls_maybe.as_mut() {
                Some(u) => u,
                None => {
                    urls_maybe = Some(Vec::new());
                    urls_maybe.as_mut().unwrap()
                }
            };

            urls.push(url);
        }
    }

    Ok(urls_maybe)
}

pub fn record_copy_counts(
    editor: &mut Editor,
    org_id: i64,
    rec_id: i64,
    is_staff: bool,
    is_meta: bool,
) -> EgResult<Vec<EgValue>> {
    let key = if is_meta { "metarecord" } else { "record" };
    let func = format!("asset.{key}_copy_count");
    let query = eg::hash! {"from": [func, org_id, rec_id, is_staff]};
    let mut data = editor.json_query(query)?;

    for count in data.iter_mut() {
        // Fix up the key name change; required by stored-proc version.
        count["count"] = count["visible"].take();
        count.remove("visible");
    }

    data.sort_by(|a, b| {
        let da = a["depth"].int_required();
        let db = b["depth"].int_required();
        da.cmp(&db)
    });

    Ok(data)
}
