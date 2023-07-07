use crate::editor::Editor;
use crate::util;
use json::JsonValue;

/// Apply a variety of DB transforms to an org unit and return
/// the calculated org unit IDs.
fn org_relations_query(
    editor: &mut Editor,
    org_id: i64,
    transform: &str,
    depth: Option<i64>
) -> Result<Vec<i64>, String> {
    let mut query = json::object! {
        select: {
            aou: [{
                transform: transform,
                column: "id",
                result_field: "id",
                params: []
            }],
            from: "aou",
            where: {id: org_id}
        }
    };

    if let Some(d) = depth {
        query["select"][0]["params"] = json::from(vec![d]);
    }

    let list = editor.json_query(query)?;

    let mut ids = Vec::new();
    for h in list {
        ids.push(util::json_int(&h["id"])?);
    }
    Ok(ids)
}

pub fn ancestors(editor: &mut Editor, org_id: i64) -> Result<Vec<i64>, String> {
    org_relations_query(editor, org_id, "actor.org_unit_ancestors", None)
}

pub fn descendants(editor: &mut Editor, org_id: i64) -> Result<Vec<i64>, String> {
    org_relations_query(editor, org_id, "actor.org_unit_descendants", None)
}

pub fn full_path(
    editor: &mut Editor,
    org_id: i64,
    depth: Option<i64>
) -> Result<Vec<i64>, String> {
    org_relations_query(editor, org_id, "actor.org_unit_full_path", depth)
}
