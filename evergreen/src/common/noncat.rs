use crate as eg;
use eg::common::org;
use eg::common::settings::Settings;
use eg::date;
use eg::editor::Editor;
use eg::result::EgResult;
use eg::EgValue;
use std::time::Duration;

/// Create X number of non-cat checkouts.
///
/// Returns a list of checkouts with the duedate calculated.
pub fn checkout(
    editor: &mut Editor,
    patron_id: i64,
    noncat_type: i64,
    circ_lib: i64,
    count: i64,
    circ_time: Option<&str>,
) -> EgResult<Vec<EgValue>> {
    let mut circs = Vec::new();

    for _ in 0..count {
        let noncat = eg::hash! {
            "patron": patron_id,
            "staff": editor.requestor_id()?,
            "circ_lib": circ_lib,
            "item_type": noncat_type,
            "circ_time": circ_time,
        };

        let noncat = EgValue::create("ancc", noncat)?;
        let mut noncat = editor.create(noncat)?;

        noncat["duedate"] = EgValue::from(noncat_due_date(editor, &noncat)?);

        circs.push(noncat);
    }

    Ok(circs)
}

/// Calculate the due date of a noncat circulation, which is a function
/// of the checkout time, the duration of the noncat type, plus org
/// open time checks.
pub fn noncat_due_date(editor: &mut Editor, noncat: &EgValue) -> EgResult<String> {
    let duration = if noncat["item_type"].is_object() {
        noncat["item_type"]["circ_duration"].string()?
    } else {
        let nct = editor
            .retrieve("cnct", noncat["item_type"].int()?)?
            .ok_or(format!("Invalid noncat_type: {}", noncat["item_type"]))?;

        nct["circ_duration"].string()?
    };

    let circ_lib = noncat["circ_lib"].int()?;
    let mut settings = Settings::new(editor);

    let timezone = settings.get_value_at_org("lib.timezone", circ_lib)?;
    let timezone = timezone.as_str().unwrap_or("local");

    let checkout_time = noncat["circ_time"]
        .as_str()
        .ok_or(format!("Invalid noncat circ_time: {}", noncat["circ_time"]))?;

    let duedate = date::parse_datetime(checkout_time)?;
    let duedate = date::set_timezone(duedate, timezone)?;

    let seconds = date::interval_to_seconds(&duration)?;
    let mut duedate = duedate + Duration::from_secs(seconds as u64);

    let org_open_data = org::next_open_date(editor, circ_lib, &duedate)?;

    if let org::OrgOpenState::OpensOnDate(future_date) = org_open_data {
        duedate = future_date;
    }

    Ok(date::to_iso(&duedate))
}
