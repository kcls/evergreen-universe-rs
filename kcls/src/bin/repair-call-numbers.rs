use eg::db::DatabaseConnection;
use eg::init;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use std::fs;

const DEFAULT_STAFF_ACCOUNT: u32 = 4953211; // utiladmin

fn main() -> EgResult<()> {
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
        trim_labels(&ids)?;
    }

    Ok(())
}

fn read_ids(file_name: &str, id_list: &mut Vec<i64>) -> EgResult<()> {
    for id_line in fs::read_to_string(file_name)
        .map_err(|e| format!("Cannot read --ids-file: {file_name} : {e}"))?
        .lines()
    {
        if !id_line.is_empty() {
            let id = id_line.parse::<i64>().map_err(|e| format!("Invalid ID: {id_line}"))?;
            id_list.push(id);
        }
    }

    Ok(())
}


fn trim_labels(ids: &[i64]) -> EgResult<()> {

    println!("Trimming: {ids:?}");

    Ok(())
}


