use crate::editor::Editor;
use crate::result::EgResult;
use crate::util;
use EgValue;

/// Extract the copy status from either a potentially-fleshed copy object
/// of from the in-database copy by ID.
pub fn copy_status(
    editor: &mut Editor,
    copy_id: Option<i64>,
    copy: Option<&EgValue>,
) -> EgResult<i64> {
    if let Some(copy) = copy {
        if copy["status"].is_object() {
            util::json_int(&copy["status"]["id"])
        } else {
            util::json_int(&copy["status"])
        }
    } else if let Some(id) = copy_id {
        let copy = editor
            .retrieve("acp", id)?
            .ok_or_else(|| editor.die_event())?;
        util::json_int(&copy["status"])
    } else {
        Err(format!("copy_status() requires a useful parameter").into())
    }
}
