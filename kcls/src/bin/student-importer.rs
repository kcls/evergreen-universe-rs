use evergreen as eg;
use eg::EgResult;
use csv;
use std::fs::File;
use std::io::BufReader;
use std::collections::HashMap;

// TODO
// Importer struct; Display w/ filename


fn process_file(file_name: &str) -> EgResult<()> {
    let file = File::open(file_name).map_err(|e| e.to_string())?;

    let buf_reader = BufReader::new(file);
    let mut reader = csv::Reader::from_reader(buf_reader);

    let headers = reader.headers().map_err(|e| format!("Error parsing CSV file: {e}"))?;

    let slice = headers.as_slice();
    if  slice.contains("student_id") &&
        slice.contains("family_name") &&
        slice.contains("first_given_name") &&
        slice.contains("dob") 
    {
        log::debug!("File contains required headers");
    } else {
        return Err("Some columns are missing/mis-labeled".into());
    }

    println!("Headers: {headers:?}");

    for row_result in reader.deserialize() {
        let row: HashMap<String, String> = row_result
            .map_err(|e| format!("Error parsing CSV file: {e}"))?;
        println!("row: {row:?}");
    }

    //println!("{reader:?}");

    Ok(())
}

fn main() {
    let file_name = "/home/berick/holy-fam.csv";

    if let Err(e) = process_file(file_name) {
        eprintln!("Error processing file: {file_name} => {e}");
        std::process::exit(1);
    }
}


