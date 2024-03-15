//! Shared, circ-focused utility functions
use crate::editor::Editor;
use crate::result::EgResult;
use EgValue;

pub fn summarize_circ_chain(e: &mut Editor, circ_id: i64) -> EgResult<EgValue> {
    let query = eg::hash! {
        from: ["action.summarize_all_circ_chain", circ_id]
    };

    if let Some(circ) = e.json_query(query)?.pop() {
        Ok(e.idl().create_from("accs", circ)?)
    } else {
        Err(format!("No such circulation: {circ_id}").into())
    }
}

pub fn circ_chain(e: &mut Editor, circ_id: i64) -> EgResult<Vec<EgValue>> {
    let query = eg::hash! {
        from: ["action.all_circ_chain", circ_id]
    };

    let mut circ_list = e.json_query(query)?;

    if circ_list.len() == 0 {
        Err("No such circulation: {circ_id}")?;
    }

    let mut chains = Vec::new();
    for circ in circ_list.drain(..) {
        chains.push(e.idl().create_from("aacs", circ)?);
    }

    Ok(chains)
}
