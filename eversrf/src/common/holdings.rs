use crate as eg;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;

/// Extract the copy status from either a potentially-fleshed copy object
/// of from the in-database copy by ID.
pub fn copy_status(
    editor: &mut Editor,
    copy_id: Option<i64>,
    copy: Option<&EgValue>,
) -> EgResult<i64> {
    if let Some(copy) = copy {
        if let Some(id) = copy["status"].id() {
            Ok(id)
        } else {
            let stat = copy["status"]
                .as_int()
                .ok_or_else(|| format!("Cannot get stopy status ID"))?;
            Ok(stat)
        }
    } else if let Some(id) = copy_id {
        let copy = editor
            .retrieve("acp", id)?
            .ok_or_else(|| editor.die_event())?;

        let stat = copy["status"]
            .as_int()
            .ok_or_else(|| format!("Cannot get stopy status ID"))?;

        Ok(stat)
    } else {
        Err(format!("copy_status() requires a useful parameter").into())
    }
}
