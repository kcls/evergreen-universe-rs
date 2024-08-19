use eg::script::ScriptUtil;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use std::fs;

// const DEFAULT_STAFF_ACCOUNT: i64 = 4953211; // utiladmin

fn main() -> EgResult<()> {
    let mut ops = getopts::Options::new();

    ops.optopt("", "ids-file", "", "");
    ops.optflag("", "trim-labels", "");
    ops.optflag("", "dry-run", "");

    let mut scripter = match ScriptUtil::init(&mut ops, false, None)? {
        Some(s) => s,
        None => return Ok(()), // e.g. --help
    };

    let dry_run = scripter.params().opt_present("dry-run");

    let mut ids: Vec<i64> = Vec::new();

    if let Some(file_name) = scripter.params().opt_str("ids-file") {
        read_ids(&file_name, &mut ids)?;
    }

    if scripter.params().opt_present("trim-labels") {
        trim_labels(&mut scripter, &ids, dry_run)?;
    }

    scripter.editor_mut().client_mut().clear().ok();

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
fn trim_labels(scripter: &mut ScriptUtil, ids: &[i64], dry_run: bool) -> EgResult<()> {
    for id in ids {
        let vol = scripter
            .editor_mut()
            .retrieve("acn", *id)?
            .ok_or_else(|| format!("No such call number: {id}"))?;

        trim_one_label(scripter, vol, dry_run)?;
    }

    Ok(())
}

fn trim_one_label(scripter: &mut ScriptUtil, mut vol: EgValue, dry_run: bool) -> EgResult<()> {
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
        scripter.authtoken().into(),
        EgValue::from(vec![vol]),
        EgValue::Null,
        eg::hash! {"auto_merge_vols": 1},
    ];

    let response = scripter
        .editor_mut()
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
