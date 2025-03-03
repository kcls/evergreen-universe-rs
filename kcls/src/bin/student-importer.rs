use eg::script;
use eg::EgResult;
use evergreen as eg;
use regex::Regex;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

const LOG_PREFIX: &str = "SI";
const STUDENT_PROFILE: u32 = 901; // "Student Ecard"
const TEACHER_PROFILE: u32 = 903; // "Teacher Ecard"
const CLASSROOM_PROFILE: u32 = 902; // "Classroom Databases"
const STUDENT_NET_ACCESS: u32 = 101; // No Access
const STUDENT_IDENT_TYPE: u32 = 101; // "Sch-district file" ident type
const CLASSROOM_IDENT_TYPE: u32 = 3; // ident type "Other"
const CLASSROOM_IDENT_VALUE: &str = "KCLS generated";

const STUDENT_ALERT_MSG: &str =
    "Student Ecard: No physical checkouts. No computer/printing. No laptops.";
const TEACHER_ALERT_MSG: &str =
    "Teacher Ecard: No physical checkouts. No computer/printing. No laptops.";
const CLASSROOM_ALERT_MSG: &str =
    "Classroom use only: No physical checkouts. No computer/printing. No laptops.";

const ALERT2_MSG: &str = "DO NOT MERGE OR EDIT. RECORD MANAGED CENTRALLY.";
const ALERT_TYPE: u32 = 20; // "Alerting note, no Blocks" standing penalty

// KCLS org unit for penalty application
const ROOT_ORG: u32 = 1;

// Age at which k12 accounts are set to expire
const EXPIRE_AGE: u32 = 21;

// If more than this ration of students in a file are new accounts,
// block the import for manual review.
const MAX_NEW_RATIO: f32 = 0.8;

// If all of the students in a file are new, block if the file has more
// than this many students.
const MAX_FORCE_NEW: u32 = 100;

// Map of district code to home org unit id.
const HOME_OU_MAP: &[(&str, u32)] = &[
    ("210", 1509), // Federal Way
    ("216", 119),  // Enumclaw
    ("400", 1525), // Mercer Island
    ("401", 1495), // Highline
    ("402", 1545), // Vashon
    ("403", 1556), // Renton
    ("404", 1536), // Skykomish
    ("405", 1492), // Bellevue
    ("406", 154),  // Tukwila
    ("407", 1503), // Riverview (Duvall)
    ("408", 1490), // Auburn
    ("409", 1527), // Tahoma
    ("410", 1537), // Snoqualmie
    ("411", 1513), // Issaquah
    ("412", 1535), // Shoreline
    ("414", 1533), // Lake Washington (Redmond)
    ("415", 1520), // Kent
    ("417", 1493), // Northshore (Bothell)
    ("lwt", 1533), // Lake Washington (Redmond) Institute of Technology
    ("grc", 1490), // Green River College / Auburn
    ("tos", 1533), // Overlake (Redmond)
    ("bcs", 1533), // Bear Creek (Redmond)
    ("bvc", 1492), // Bellevue Community College (Bellevue)
    ("sbs", 1493), // St Brendan (Bothell)
    ("svs", 1509), // St Vincent de Paul (Federal Way)
    ("rtc", 1557), // Renton Tech (Renton Highlands)
    ("bcl", 1492), // Bellevue Christian School (Bellevue)
    ("bca", 1492), // Bellevue Children's Academy (Bellevue)
    ("hlc", 1495), // Highline College (Burien)
    ("ecs", 1534), // Eastside Catholic (Sammamish)
    ("sts", 1492), // St. Thomas (Bellevue)
    ("scc", 1535), // Shoreline Community College
    ("ttm", 1495), // Three Tree Montessori (Burien)
    ("cas", 1493), // Cascadia College (Bothell)
    ("rps", 1547), // Rainer Prep (White Center)
    ("wms", 1493), // Woodinville Montessori (Bothell)
    ("dit", 1533), // DigiPen Institute of Technology (Redmond)
    ("hfs", 1490), // Holy Family School (Auburn)
    ("frs", 1529), // Forest Ridge School of the Sacred Heart (Newport Way)
];

struct Importer {
    file_name: String,
    runner: script::Runner,
    district_code: String,
    home_ou: u32,
    is_dry_run: bool,
    is_teacher: bool,
    is_classroom: bool,
    is_college: bool,
    is_purge: bool,
    is_force_new: bool,
    out_directory: String,
    new_barcodes: Vec<String>,
    seen_barcodes: HashSet<String>,
}

impl fmt::Display for Importer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Importer [{}]", self.file_name)
    }
}

impl Importer {
    fn process_file(&mut self, file_path: &Path) -> EgResult<()> {
        let file =
            File::open(file_path).map_err(|e| format!("Cannot open file: {file_path:?} {e}"))?;

        let buf_reader = BufReader::new(file);
        let mut reader = csv::Reader::from_reader(buf_reader);

        let mut all_accounts: Vec<HashMap<String, String>> = Vec::new();

        for row_result in reader.deserialize() {
            let mut hash: HashMap<String, String> =
                row_result.map_err(|e| format!("Error parsing CSV file: {e}"))?;

            self.apply_barcode(&mut hash)?;

            all_accounts.push(hash);
        }


        //self.process_account(row)?;

        Ok(())
    }

    /*
    fn process_account(&mut self, mut hash: HashMap<String, String>) -> EgResult<()> {
        let barcode = self._barcode(&hash)?;

        if self.seen_barcodes.contains(&barcode) {
            // Avoid dupes
            return Ok(());
        }

        self.seen_barcodes.insert(barcode.to_string());

        self.runner
            .announce(&format!("Extracted barcode: {barcode}"));

        Ok(())
    }
    */

    /// Normalize the student_id value and use it with the district
    /// code to generate the patron barcode.
    fn apply_barcode(&self, hash: &mut HashMap<String, String>) -> EgResult<()> {
        let mut student_id = hash
            .get("student_id")
            .map(|s| s.to_string())
            .ok_or("student_id column/value required")?;

        // If an ID contains an @, presumably denoting a full email address,
        // remove it and everything after it.
        if let Some(idx) = student_id.find('@') {
            student_id = student_id[..idx].to_string();
        }

        if self.is_college {
            // A limited set of characters are permitted for student_id values.
            // TODO Precompile.
            let reg = Regex::new(r#"[^a-zA-Z0-9_\-\.]"#).unwrap();

            student_id = reg.replace_all(&student_id, "").into_owned();

            // College accounts are forced into lowercase.
            student_id = student_id.to_ascii_lowercase();
        } else {
            // K12 and teacher accounts are further limited in
            // what characters are permitted.

            // TODO Precompile
            let reg = Regex::new(r#"[^a-zA-Z0-9]"#).unwrap();

            student_id = reg.replace_all(&student_id, "").into_owned();
        }

        if self.is_teacher {
            // Left-pad teacher barcodes with 0s to they are at least 4 chars long
            if student_id.len() < 4 {
                student_id = format!("{student_id:0>4}");
            }

            // Teacher accounts are uppercased.
            student_id = student_id.to_ascii_uppercase();
        }

        // Make sure we still have something to work with after cleanup.
        if student_id.is_empty() {
            return Err(format!("student_id column/value required: {hash:?}").into());
        }

        let mut barcode = self.district_code.to_string();

        if self.is_college {
            barcode = barcode.to_uppercase();
        }

        if self.is_teacher {
            if self.is_college {
                barcode = format!("E{barcode}");
            } else {
                barcode += "t";
            }
        }

        hash.insert("barcode".to_string(), barcode + &student_id);

        Ok(())
    }
}

fn main() {
    let mut ops = getopts::Options::new();

    ops.optopt("", "district-code", "", "");
    ops.optopt("", "out-directory", "", "");
    ops.optflag("", "teacher", "");
    ops.optflag("", "college", "");
    ops.optflag("", "classroom", "");
    ops.optflag("", "dry-run", "");
    ops.optflag("", "purge", "");
    ops.optflag("", "force-new", "");

    let options = script::Options {
        with_evergreen: true,
        with_database: false,
        help_text: None, // TODO
        extra_params: None,
        options: Some(ops),
    };

    let mut runner = match script::Runner::init(options) {
        Ok(op) => match op {
            Some(r) => r,
            // --help exits early
            None => return,
        },
        Err(e) => {
            eprintln!("Cannot connect: {e}");
            log::error!("Cannot connect: {e}");
            std::process::exit(1);
        }
    };

    runner.set_log_prefix(LOG_PREFIX);

    // First and only free parameter is the path to the CSV file
    let Some(file_path) = runner.params().free.first().map(|s| s.to_string()) else {
        return runner.exit(1, "CSV file required");
    };

    let file_path = Path::new(&file_path);

    let Some(Some(file_name)) = file_path.file_name().map(|f| f.to_str()) else {
        return runner.exit(1, &format!("Valid file name required: {file_path:?}"));
    };

    let Some(district_code) = runner.params().opt_str("district-code") else {
        return runner.exit(1, "--district-code required");
    };

    let Some(out_directory) = runner
        .params()
        .opt_str("out-directory")
        .map(|s| s.to_string())
    else {
        return runner.exit(1, "--out-directory required");
    };

    let is_teacher = runner.params().opt_present("teacher");
    let is_college = runner.params().opt_present("college");
    let is_classroom = runner.params().opt_present("classroom");
    let is_force_new = runner.params().opt_present("force-new");
    let is_dry_run = runner.params().opt_present("dry-run");
    let is_purge = runner.params().opt_present("purge");

    let Some(home_ou) = HOME_OU_MAP
        .iter()
        .find(|(code, _)| *code == district_code)
        .map(|(_, ou)| *ou)
    else {
        return runner.exit(1, &format!("Unknown district: {district_code}"));
    };

    let mut importer = Importer {
        runner,
        home_ou,
        is_dry_run,
        is_purge,
        is_force_new,
        is_teacher,
        is_college,
        is_classroom,
        out_directory,
        new_barcodes: Vec::new(),
        seen_barcodes: HashSet::new(),
        file_name: file_name.to_string(),
        district_code: district_code.to_string(),
    };

    if let Err(e) = importer.process_file(file_path) {
        importer.runner.exit(
            1,
            &format!("Error processing file {}: {e}", importer.file_name),
        );
    }
}
