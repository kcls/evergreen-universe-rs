//! Shared, circ-focused utility functions
use crate as eg;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;

pub fn summarize_circ_chain(e: &mut Editor, circ_id: i64) -> EgResult<EgValue> {
    let query = eg::hash! {
        from: ["action.summarize_all_circ_chain", circ_id]
    };

    if let Some(circ) = e.json_query(query)?.pop() {
        Ok(EgValue::create("accs", circ)?)
    } else {
        Err(format!("No such circulation: {circ_id}").into())
    }
}

pub fn circ_chain(e: &mut Editor, circ_id: i64) -> EgResult<Vec<EgValue>> {
    let query = eg::hash! {
        from: ["action.all_circ_chain", circ_id]
    };

    let mut circ_list = e.json_query(query)?;

    if circ_list.is_empty() {
        Err("No such circulation: {circ_id}")?;
    }

    let mut chains = Vec::new();
    for circ in circ_list.drain(..) {
        chains.push(EgValue::create("aacs", circ)?);
    }

    Ok(chains)
}
