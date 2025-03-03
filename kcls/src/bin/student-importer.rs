use evergreen as eg;
use eg::EgResult;
use eg::script;
use std::fmt;
use std::path::Path;
use std::fs::File;
use std::io::BufReader;
use std::collections::HashMap;

struct Importer {
    file_name: String,
    runner: script::Runner,
    district_code: String,
    is_teacher: bool,
    is_classroom: bool,
    is_college: bool,
}

impl fmt::Display for Importer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Importer [{}]", self.file_name)
    }
}

impl Importer {

    fn process(&mut self, file_path: &Path) -> EgResult<()> {
        let file = File::open(file_path).map_err(|e| e.to_string())?;

        let buf_reader = BufReader::new(file);
        let mut reader = csv::Reader::from_reader(buf_reader);

        /*
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
        */

        for row_result in reader.deserialize() {
            let row: HashMap<String, String> = row_result
                .map_err(|e| format!("Error parsing CSV file: {e}"))?;
            self.runner.announce(&format!("row: {row:?}"));
        }

        //println!("{reader:?}");

        Ok(())
    }
}

fn main() {
    let mut ops = getopts::Options::new();

    ops.optopt("", "district-code", "", "");
    ops.optopt("", "teacher", "", "");
    ops.optopt("", "college", "", "");
    ops.optopt("", "classroom", "", "");

    let options = script::Options {
        with_evergreen: true,
        with_database: false,
        //help_text: Some(HELP_TEXT.to_string()),
        help_text: None, // TODO
        extra_params: None,
        options: Some(ops),
    };

    let mut runner = match script::Runner::init(options) {
        Ok(op) => match op {
            Some(s) => s,
            None => return, // --help
        },
        Err(e) => {
            log::error!("SI cannot start: {e}");
            eprintln!("SI cannot start: {e}");
            std::process::exit(1);
        }
    };

    // Student Importer => SI
    runner.set_log_prefix("SI");

    let file_path = Path::new("/home/berick/holy-fam.csv");

    let Some(Some(file_name)) = file_path.file_name().map(|f| f.to_str()) else {
        return runner.exit(1, &format!("Valid file name required: {file_path:?}"));
    };

    let Some(district_code) = runner.params().opt_str("district-code") else {
        return runner.exit(1, "--district-code required");
    };

    let is_teacher = runner.params().opt_present("teacher");
    let is_classroom = runner.params().opt_present("classroom");
    let is_college = runner.params().opt_present("college");

    let mut importer = Importer { 
        runner,
        is_teacher,
        is_classroom,
        is_college,
        file_name: file_name.to_string(),
        district_code: district_code.to_string(),
    };

    if let Err(e) = importer.process(file_path) {
        importer.runner.exit(1, &format!("Error processing file: {file_name} => {e}"));
    }
}


