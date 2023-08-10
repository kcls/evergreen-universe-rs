//! Shared, circ-focused utility functions
use crate::editor::Editor;
use crate::result::EgResult;
use json::JsonValue;

pub fn summarize_circ_chain(e: &mut Editor, circ_id: i64) -> EgResult<JsonValue> {
    let query = json::object! {
        from: ["action.summarize_all_circ_chain", circ_id]
    };

    let circ_list = e.json_query(query)?;

    if circ_list.len() == 0 {
        Err("No such circulation: {circ_id}")?;
    }

    let circ = circ_list[0].to_owned();

    let summary = e.idl().create_from("accs", circ)?;

    Ok(summary)
}

pub fn circ_chain(e: &mut Editor, circ_id: i64) -> EgResult<Vec<JsonValue>> {
    let query = json::object! {
        from: ["action.all_circ_chain", circ_id]
    };

    let mut circ_list = e.json_query(query)?;

    if circ_list.len() == 0 {
        Err("No such circulation: {circ_id}")?;
    }

    let mut idx = 0;
    while idx < circ_list.len() {
        circ_list[idx] = e.idl().create_from("aacs", circ_list[idx].to_owned())?;
        idx += 1;
    }

    Ok(circ_list)
}
