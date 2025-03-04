use eg::date;
use eg::script;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
// hrm, for date.year()
use chrono::Datelike;
use regex::Captures;
use regex::Regex;
use std::collections::HashMap;
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

// If more than this ration of students in a file are new accounts,
// block the import for manual review.
const MAX_NEW_RATIO: f32 = 0.8;

/// If the number of new accounts is less than this value, avoid enforcing
/// the new-accounts ratio.
///
/// This is useful when files contain only new accounts, which is
/// atypical, but can happen.
const MAX_ALLOW_NEW_UNCHECKED: usize = 100;

const STUDENT_ID_REGEX: &str = r#"[^a-zA-Z0-9_\-\.]"#;
const COLLEGE_ID_REGEX: &str = r#"[^a-zA-Z0-9]"#;
const DOB_REGEX: &str = r#"^\d{4}-\d{2}-\d{2}$"#;
/// We allow schools to send DoB values in US-style mm/dd/yyyy
const DOB_US_REGEX: &str = r#"^(\d{1,2})/(\d{1,2})/(\d{4})$"#;

const TEACHER_EXPIRE_INTERVAL: &str = "10 years";
const COLLEGE_EXPIRE_INTERVAL: &str = "4 years";
const STUDENT_EXPIRE_AGE: u16 = 21;

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
    dob_regex: Regex,
    dob_us_regex: Regex,
    student_id_regex: Regex,
    college_id_regex: Regex,
}

impl fmt::Display for Importer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Importer [{}]", self.file_name)
    }
}

impl Importer {
    /// Parse a CSV file and generate accounts for new
    /// students, teachers, or classroom cards;
    fn process_file(&mut self, file_path: &Path) -> EgResult<()> {
        let file =
            File::open(file_path).map_err(|e| format!("Cannot open file: {file_path:?} {e}"))?;

        let buf_reader = BufReader::new(file);
        let mut reader = csv::Reader::from_reader(buf_reader);

        let mut all_accounts: Vec<HashMap<String, String>> = Vec::new();
        let mut all_barcodes: Vec<String> = Vec::new();

        // Read all of the accounts from file up front so we can
        // get a count of how many new users we're creating.
        // Derive the barcode for each along the way.
        for row_result in reader.deserialize() {
            let mut hash: HashMap<String, String> =
                row_result.map_err(|e| format!("Error parsing CSV file: {e}"))?;

            let barcode = self.apply_barcode(&mut hash)?;

            // Avoid duplicate accounts
            if !all_barcodes.contains(&barcode) {
                all_accounts.push(hash);
                all_barcodes.push(barcode);
            }
        }

        let new_barcodes = self.get_new_barcodes(&all_barcodes)?;
        let all_count = all_barcodes.len();
        let new_count = new_barcodes.len();

        self.runner
            .announce(&format!("Found {new_count} new barcodes"));

        self.check_new_accounts_ratio(all_count, new_count);

        for hash in all_accounts {
            let barcode = hash.get("barcode").unwrap(); // invariant

            if !new_barcodes.contains(barcode) {
                // This account already exists.
                continue;
            }

            self.add_account(hash)?;
        }

        Ok(())
    }

    /// Verify the number of new accounts does not exceed the configured
    /// MAX_NEW_RATIO.  
    ///
    /// Exits the program if the ratio is exceeded.
    fn check_new_accounts_ratio(&self, all_count: usize, new_count: usize) {
        if self.is_force_new {
            self.runner.announce("Running in --force-new mode");
            return;
        }

        if new_count < MAX_ALLOW_NEW_UNCHECKED {
            // No checks required.
            return;
        }

        let ratio = new_count as f32 / all_count as f32;

        if ratio < MAX_NEW_RATIO {
            // Below the ratio, all good.
            return;
        }

        // Ratio exceeded.  Log and exit.
        self.runner.exit(
            1,
            &format!(
                r#"
                Ratio ({ratio}) of new accounts ({new_count}) to 
                existing accounts ({all_count}) exceeds the MAX_NEW_RATIO 
                ({MAX_NEW_RATIO}) value.  Use --force-new to override"#
            ),
        );
    }

    fn populate_defaults(&mut self, hash: &mut HashMap<String, String>) -> EgResult<()> {
        Ok(())
    }

    /// Returns the subset of all barcodes which do not already exist
    /// in the database.
    fn get_new_barcodes(&mut self, all_barcodes: &[String]) -> EgResult<Vec<String>> {
        // Search for the ones we do have, then return the remainders.
        let mut new_barcodes = Vec::new();

        // This has the potential to be a large number of barcodes.
        // Chunk the lookups into manageable groups.
        for batch in all_barcodes.chunks(250) {
            let query = eg::hash! {
                "select": {"ac": ["barcode"]},
                "from": "ac",
                "where": {"+ac": {"barcode": {"in": batch}}}
            };

            let existing = self.runner.editor_mut().json_query(query)?;

            // Use .string() here since it coerces numeric barcodes
            // into strings.  Panics if a barcode value (from the database)
            // is not stringifiable.
            let existing: Vec<String> = existing.iter().map(|e| e.string().unwrap()).collect();

            for barcode in batch.iter() {
                if !existing.contains(barcode) {
                    new_barcodes.push(barcode.to_string());
                }
            }
        }

        Ok(new_barcodes)
    }

    /// Create the new user account and add it to the database along with
    /// its addresses, alerts, etc.
    fn add_account(&mut self, hash: HashMap<String, String>) -> EgResult<()> {
        // Translate our hash to an EgValue to prep for cleanup and insert.

        let mut patron = eg::blessed! {
            // '_barcode' because it's not a field on the 'au' class.
            // This allows us to skip field name enforcement.
            "_barcode": hash.get("barcode").unwrap().as_str(),
            "_classname": "au"
        }?;

        // Required for everyone.
        // "student_id" has already been verified and "dob" is sometimes optional.
        for field in ["family_name", "first_given_name"] {
            patron[field] = hash
                .get(field)
                .ok_or_else(|| format!("field '{field}' is required: {hash:?}"))?
                .to_uppercase()
                .into();
        }

        self.apply_fields(&hash, &mut patron)?;

        if self.is_dry_run {
            self.runner
                .announce(&format!("Adding account: {}", patron.dump()));
        }

        Ok(())
    }

    /// Extract values from the source hash and translate them into
    /// our patron object, normalizing and applying defaults on the way.
    fn apply_fields(&self, hash: &HashMap<String, String>, patron: &mut EgValue) -> EgResult<()> {
        if self.is_teacher || self.is_classroom {
            // We never care about date of birth for adults/generic cards.
            patron["dob"] = "1900-01-01".into();
        } else {
            let dob = hash
                .get("dob")
                .ok_or_else(|| format!("'dob' value is required: {hash:?}"))?
                .trim();

            // Translate mm/dd/yyyy into ISO.
            let dob = self.dob_us_regex.replace(dob, |caps: &Captures| {
                // caps[0] is the full source string
                format!("{}-{:0>1}-{:0>1}", &caps[3], &caps[1], &caps[2])
            });

            if !self.dob_regex.is_match(&dob) {
                return Err(format!("DOB format is invalid: {dob}").into());
            }

            patron["dob"] = dob.into_owned().into();
        }

        // Password is initially set to last 4 characters of the barcode.
        let barcode = patron["_barcode"].str()?;

        // barcodes have 3-char district codes plus a non-empty student_id.
        patron["_password"] = barcode[(barcode.len() - 4)..].into();

        self.set_expire_date(patron)?;

        Ok(())
    }

    /// Calculate and apply the patron expire_date value.
    fn set_expire_date(&self, patron: &mut EgValue) -> EgResult<()> {
        let now_date = date::now(); // local timezone
        let now_year = now_date.year() as u32;

        if self.is_teacher || self.is_classroom {
            patron["expire_date"] =
                date::to_iso(&date::add_interval(now_date, TEACHER_EXPIRE_INTERVAL)?).into();

            return Ok(());
        }

        if self.is_college {
            patron["expire_date"] =
                date::to_iso(&date::add_interval(now_date, COLLEGE_EXPIRE_INTERVAL)?).into();

            return Ok(());
        }

        // Student accounts expire based on student age.
        let birth_year = &patron["dob"].str()?[..4]; // ISO YYYY-MM-DDDD

        let birth_year: u16 = birth_year
            .parse()
            .map_err(|e| format!("Invalid date of birth year: {birth_year}"))?;

        // Can underflow
        let mut age_years: i32 = now_year as i32 - birth_year as i32;

        // The DoB for any account whose birth date is older than the
        // expire age or less than 2 years old is coerced into that
        // range so we can ensure a reasonable expire date.
        if age_years > (STUDENT_EXPIRE_AGE - 1).into() {
            age_years = (STUDENT_EXPIRE_AGE - 1).into();
        } else if age_years < 2 {
            age_years = 2;
        }

        let expire_year = now_year + (STUDENT_EXPIRE_AGE - age_years as u16) as u32;

        // This can fail if the calculated date is invalid.  If so,
        // running the importer again on the same data the next day or
        // so will likely fix it.
        // Alternatively. we could loop on with_year() for x number of times
        // until a valid date is generated.
        let expire_date = now_date.with_year(expire_year as i32).ok_or_else(|| {
            format!(
                "Error setting expire date: {now_date} + {expire_year} years : {}",
                patron.dump()
            )
        })?;

        patron["expire_date"] = date::to_iso(&expire_date).into();

        Ok(())
    }

    /// Normalize the student_id value and use it with the district
    /// code to generate the patron barcode.
    ///
    /// Returns a copy of the generated barcode.
    ///
    /// The normalization rules may seem arbitrary.  It's all grandfathered in.
    fn apply_barcode(&self, hash: &mut HashMap<String, String>) -> EgResult<String> {
        let mut student_id = hash
            .get("student_id")
            .map(|s| s.trim())
            .map(|s| s.to_string())
            .ok_or("student_id column/value required")?;

        // If an ID contains an @, presumably denoting a full email address,
        // remove it and everything after it.
        if let Some(idx) = student_id.find('@') {
            student_id = student_id[..idx].to_string();
        }

        if self.is_college {
            student_id = self
                .college_id_regex
                .replace_all(&student_id, "")
                .into_owned();

            // College accounts are forced into lowercase.
            student_id = student_id.to_ascii_lowercase();
        } else {
            // K12 teachers and students
            student_id = self
                .student_id_regex
                .replace_all(&student_id, "")
                .into_owned();

            if self.is_teacher {
                // Left-pad teacher barcodes with 0s to they are at least 4 chars long
                if student_id.len() < 4 {
                    student_id = format!("{student_id:0>4}");
                }

                // K12 teachers are uppercased.
                student_id = student_id.to_ascii_uppercase();
            }
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

        let barcode = barcode + &student_id;

        hash.insert("barcode".to_string(), barcode.clone());

        Ok(barcode)
    }
}

fn main() {
    let mut ops = getopts::Options::new();

    ops.optopt("", "district-code", "", "");
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

    // Avoid requiring the caller to pass --announce
    runner.set_announce(true);

    let Some(district_code) = runner.params().opt_str("district-code") else {
        return runner.exit(1, "--district-code required");
    };

    runner.set_log_prefix(&format!("{LOG_PREFIX} [{district_code}]"));

    // First and only free parameter is the path to the CSV file
    let Some(file_path) = runner.params().free.first().map(|s| s.to_string()) else {
        return runner.exit(1, "CSV file required");
    };

    runner.announce("Processing file {file_path}");

    let file_path = Path::new(&file_path);

    let Some(Some(file_name)) = file_path.file_name().map(|f| f.to_str()) else {
        return runner.exit(1, &format!("Valid file name required: {file_path:?}"));
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

    let student_id_regex = Regex::new(STUDENT_ID_REGEX).unwrap();
    let college_id_regex = Regex::new(COLLEGE_ID_REGEX).unwrap();
    let dob_regex = Regex::new(DOB_REGEX).unwrap();
    let dob_us_regex = Regex::new(DOB_US_REGEX).unwrap();

    let mut importer = Importer {
        runner,
        home_ou,
        is_dry_run,
        is_purge,
        is_force_new,
        is_teacher,
        is_college,
        is_classroom,
        dob_regex,
        dob_us_regex,
        student_id_regex,
        college_id_regex,
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
