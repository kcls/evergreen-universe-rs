//! action_trigger bits
use json::Value;
use opensrf::client::Client;

pub fn create_events_for_hook(
    client: &mut Client,
    hook: &str,
    obj: &json::Value,
    org_id: i64,
    granularity: Option<&str>,
    user_data: Option<&json::Value>,
    wait: bool,
) -> Result<(), String> {
    let mut ses = client.session("open-ils.trigger");

    let params = json::array![
        hook,
        obj.clone(),
        org_id,
        granularity,
        match user_data {
            Some(d) => d.clone(),
            None => json::Value::Null,
        },
    ];

    let mut req = ses.request("open-ils.trigger.event.autocreate", params)?;

    if !wait {
        return Ok(());
    }

    // Block until the request is complete.  The API in question
    // does not return a meaningful value, so discard it.
    let _ = req.first();
    Ok(())
}
