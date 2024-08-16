use eg::init;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use std::fs;

const DEFAULT_STAFF_ACCOUNT: u32 = 4953211; // utiladmin

fn main() -> EgResult<()> {
    let client = init::init()?;
    let mut editor = eg::Editor::new(&client);

    let mut opts = getopts::Options::new();
    opts.optopt("", "ids-file", "", "");
    opts.optflag("", "trim-labels", "");

    let args: Vec<String> = std::env::args().collect();
    let params = opts.parse(&args[1..])
        .map_err(|e| format!("Error parsing options: {}", e))?;

    let mut ids: Vec<i64> = Vec::new();

    if let Some(file_name) = params.opt_str("ids-file") {
        read_ids(&file_name, &mut ids)?;
    }

    if params.opt_present("trim-labels") {
        trim_labels(&mut editor, &ids)?;
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
            let id = line.parse::<i64>().map_err(|_| format!("Invalid ID: {line}"))?;
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
fn trim_labels(editor: &mut Editor, ids: &[i64]) -> EgResult<()> {
    println!("Trimming {} call numbers", ids.len());

    for id in ids {
        let vol = editor.retrieve("acn", *id)? 
            .ok_or_else(|| format!("No such call number: {id}"))?;

        trim_one_label(editor, vol)?;
    }

    Ok(())
}

fn trim_one_label(editor: &mut Editor, vol: EgValue) -> EgResult<()> {
    println!("Processing call number id={} [label={}]", vol.id()?, vol["label"].str()?);

    Ok(())
}


