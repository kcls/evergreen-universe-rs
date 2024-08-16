use eg::common::auth;
use eg::init;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use std::fs;

const DEFAULT_STAFF_ACCOUNT: i64 = 4953211; // utiladmin

fn main() -> EgResult<()> {
    let client = init::init()?;
    let mut editor = eg::Editor::new(&client);

    let mut opts = getopts::Options::new();
    opts.optopt("", "ids-file", "", "");
    opts.optopt("", "staff-account", "", "");
    opts.optflag("", "trim-labels", "");
    opts.optflag("", "dry-run", "");

    let args: Vec<String> = std::env::args().collect();
    let params = opts
        .parse(&args[1..])
        .map_err(|e| format!("Error parsing options: {}", e))?;

    let sa = DEFAULT_STAFF_ACCOUNT.to_string();
    let staff_account = params.opt_get_default("staff-account", sa).unwrap();
    let staff_account = staff_account
        .parse::<i64>()
        .map_err(|e| format!("Error parsing staff-account value: {e}"))?;

    let ses = auth::Session::internal_session_api(
        editor.client_mut(),
        &auth::InternalLoginArgs::new(staff_account, auth::LoginType::Staff),
    )?;

    if let Some(s) = ses {
        editor.apply_authtoken(s.token())?;
    } else {
        return Err("Could not retrieve auth session".into());
    }

    let dry_run = params.opt_present("dry-run");

    let mut ids: Vec<i64> = Vec::new();

    if let Some(file_name) = params.opt_str("ids-file") {
        read_ids(&file_name, &mut ids)?;
    }

    if params.opt_present("trim-labels") {
        trim_labels(&mut editor, &ids, dry_run)?;
    }

    client.clear().ok();

    Ok(())
}

/// Read the list of IDs to work on from a file
fn read_ids(file_name: &str, id_list: &mut Vec<i64>) -> EgResult<()> {
    for id_line in fs::read_to_string(file_name)
        .map_err(|e| format!("Cannot read --ids-file: {file_name} : {e}"))?
        .lines()
    {
        let line = id_line.trim();
        if !line.is_empty() {
            let id = line
                .parse::<i64>()
                .map_err(|_| format!("Invalid ID: {line}"))?;
            if id > 0 {
                id_list.push(id);
            }
        }
    }

    Ok(())
}

/// Fetch the requested call numbers, trim the labels (preceding and
/// trailing spaces) where necessary, then update the call numbers in
/// the database, with auto-merge enabled.
fn trim_labels(editor: &mut Editor, ids: &[i64], dry_run: bool) -> EgResult<()> {
    //println!("Trimming {} call numbers", ids.len());

    for id in ids {
        let vol = editor
            .retrieve("acn", *id)?
            .ok_or_else(|| format!("No such call number: {id}"))?;

        trim_one_label(editor, vol, dry_run)?;
    }

    Ok(())
}

fn trim_one_label(editor: &mut Editor, mut vol: EgValue, dry_run: bool) -> EgResult<()> {
    let vol_id = vol.id()?;
    let bib_id = vol["record"].int()?;
    let label = vol["label"].str()?;

    println!("Processing call number rec={bib_id} id={vol_id} [label={label}]");

    let trimmed = label.trim();

    if label == trimmed {
        // Nothing to do.
        return Ok(());
    }

    vol["label"] = trimmed.into();
    vol["ischanged"] = true.into(); // required by API

    println!("Will update to id={vol_id} [label={}]", vol["label"].str()?);

    if dry_run {
        return Ok(());
    }

    let params: Vec<EgValue> = vec![
        editor.authtoken().unwrap().into(),
        EgValue::from(vec![vol]),
        EgValue::Null,
        eg::hash! {"auto_merge_vols": 1},
    ];

    let response = editor
        .client_mut()
        .send_recv_one(
            "open-ils.cat",
            "open-ils.cat.asset.volume.fleshed.batch.update",
            params,
        )?
        .ok_or("No response received when updating call number")?;

    if response.as_int() == Some(1) {
        println!("Successfully updated call number {vol_id}");
    } else {
        println!("Update failed for {vol_id} :\n{}", response.dump());
    }

    Ok(())
}
