use chrono::Datelike; // date.year()
use eg::date;
use eg::script;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use regex::Captures;
use regex::Regex;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

const HELP_TEXT: &str = r#"
Import students, teachers, and classroom accounts from a CSV file.

Example:

kcls-student-importer --staff-account 4953211 --district-code 405 --teacher /tmp/403.teacher.2025-03-06.csv

Options:

    --district-code
        3-character school district code.

    --teacher
        Process CSV file entries as teacher accounts.

    --classroom
        Process CSV file entries as classroom accounts.

    --purge
        Purge (permanently delete) every account in the CSV file.

    --force-new
        Force the creation of new accounts when the number of new accounts
        exceeds the "too many new accounts" ratio.

    --test-file
        Print the steps that would be taken without modifying any data.
"#;

const LOG_PREFIX: &str = "KSI";
const STUDENT_PROFILE: u32 = 901; // "Student Ecard"
const TEACHER_PROFILE: u32 = 903; // "Teacher Ecard"
const CLASSROOM_PROFILE: u32 = 902; // "Classroom Databases"
const STUDENT_NET_ACCESS: u32 = 101; // No Access
const STUDENT_IDENT_TYPE: u32 = 101; // "Sch-district file" ident type
const CLASSROOM_IDENT_TYPE: u32 = 3; // ident type "Other"
const CLASSROOM_IDENT_VALUE: &str = "KCLS generated";
const DEFAULT_POST_CODE: u32 = 98010;

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
const STUDENT_EXPIRE_AGE: u16 = 21; // years

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

/// Collection of precompiled regexes.
///
/// Keeping them separate for ease of testing.
struct LocalRegexes {
    student_id_regex: Regex,
    college_id_regex: Regex,
    dob_regex: Regex,
    dob_us_regex: Regex,
}

impl LocalRegexes {
    fn compile() -> Self {
        Self {
            dob_regex: Regex::new(DOB_REGEX).unwrap(),
            dob_us_regex: Regex::new(DOB_US_REGEX).unwrap(),
            student_id_regex: Regex::new(STUDENT_ID_REGEX).unwrap(),
            college_id_regex: Regex::new(COLLEGE_ID_REGEX).unwrap(),
        }
    }
}

struct Importer {
    file_name: String,
    runner: script::Runner,
    district_code: String,
    home_ou: u32,
    test_file: bool,
    is_teacher: bool,
    is_classroom: bool,
    is_college: bool,
    is_purge: bool,
    is_force_new: bool,
    regexes: LocalRegexes,
}

impl fmt::Display for Importer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Importer [{}]", self.file_name)
    }
}

impl Importer {
    /// Process a CSV file
    fn process_file(&mut self, file_path: &Path) -> EgResult<()> {
        let file =
            File::open(file_path).map_err(|e| format!("Cannot open file: {file_path:?} {e}"))?;

        self.process_csv(BufReader::new(file))
    }

    #[cfg(test)]
    /// Process a CSV string
    fn process_string(&mut self, csv: &str) -> EgResult<()> {
        self.process_csv(BufReader::new(csv.as_bytes()))
    }

    fn process_csv(&mut self, reader: impl Read) -> EgResult<()> {
        let accounts = self.load_csv_data(reader)?;

        let (new_barcodes, existing_barcodes) = self.group_barcodes(&accounts)?;

        let new_count = new_barcodes.len();
        let all_count = new_count + existing_barcodes.len();

        self.runner.announce(&format!(
            "{self} contains {new_count} new accounts out of {all_count} total"
        ));

        if self.is_purge {
            return self.purge_accounts(existing_barcodes);
        }

        self.runner
            .announce(&format!("Found {new_count} new barcodes"));

        self.check_new_accounts_ratio(all_count, new_count);

        for hash in accounts {
            let barcode = hash.get("barcode").unwrap(); // set above

            if new_barcodes.contains(barcode) {
                let patron = self.create_account(hash)?;
                if self.test_file {
                    self.log_account(&patron);
                } else {
                    self.insert_account(patron)?;
                }
            }
        }

        Ok(())
    }

    /// Returns the loaded account data as a list of hashes.
    fn load_csv_data(&self, reader: impl Read) -> EgResult<Vec<HashMap<String, String>>> {
        let mut reader = csv::Reader::from_reader(reader);

        let mut accounts: Vec<HashMap<String, String>> = Vec::new();

        // Read all of the accounts from file up front so we can
        // get a count of how many new users we're creating.
        // Derive the barcode for each along the way.
        for row_result in reader.deserialize() {
            let mut hash: HashMap<String, String> =
                row_result.map_err(|e| format!("Error parsing CSV file: {e}"))?;

            let barcode = self.apply_barcode(&mut hash)?;

            if !accounts
                .iter()
                .map(|a| a.get("barcode").unwrap())
                .any(|b| b == &barcode)
            {
                accounts.push(hash);
            }
        }

        Ok(accounts)
    }

    fn log_account(&self, patron: &EgValue) {
        let mut phash = patron.clone();
        phash.to_classed_hash();
        phash.scrub_hash_nulls();
        self.runner.announce(&phash.dump());
    }

    /// Permanently delete all accounts related to the provided barcodes.
    fn purge_accounts(&mut self, mut barcodes: Vec<String>) -> EgResult<()> {
        self.runner
            .announce(&format!("Purging {} accounts", barcodes.len()));

        for barcode in barcodes.drain(..) {
            self.runner.announce(&format!("Purging account {barcode}"));

            if self.test_file {
                continue;
            }

            let card_op = self
                .runner
                .editor_mut()
                .search("ac", eg::hash! {"barcode": barcode})?
                .pop();

            if let Some(card) = card_op {
                self.runner.editor_mut().xact_begin()?;

                self.runner.editor_mut().json_query(eg::hash! {
                    "from": [
                        "actor.usr_delete",
                        card["usr"].int()?,
                        EgValue::Null,
                    ]
                })?;

                self.runner.editor_mut().commit()?;
            }
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

    /// Split the set of barcodes into new and existing, where existing
    /// barcodes are present in actor.card.
    fn group_barcodes(
        &mut self,
        accounts: &[HashMap<String, String>],
    ) -> EgResult<(Vec<String>, Vec<String>)> {
        // Search for the ones we do have, then return the remainders.
        let mut existing_barcodes = Vec::new();

        let all_barcodes: Vec<&String> =
            accounts.iter().map(|a| a.get("barcode").unwrap()).collect();

        // This has the potential to be a large number of barcodes.
        // Chunk the lookups into manageable groups.
        for batch in all_barcodes.chunks(250) {
            // eg::hash! (thus JsonValue) doesn't support From<&std::string::String>
            let batch = batch.iter().map(|s| s.as_str()).collect::<Vec<&str>>();

            let query = eg::hash! {
                "select": {"ac": ["barcode"]},
                "from": "ac",
                "where": {"+ac": {"barcode": {"in": batch}}}
            };

            let existing = self.runner.editor_mut().json_query(query)?;

            // Use .string() here since it coerces numeric barcodes
            // into strings.  Panics if a barcode value (from the database)
            // is not stringifiable.
            let mut existing: Vec<String> = existing
                .iter()
                .map(|e| e["barcode"].string().expect("barcodes stringify"))
                .collect();

            existing_barcodes.append(&mut existing);
        }

        let mut new_barcodes = Vec::new();

        for barcode in all_barcodes {
            if !existing_barcodes.contains(barcode) {
                new_barcodes.push(barcode.to_string());
            }
        }

        Ok((new_barcodes, existing_barcodes))
    }

    /// Create the new user account and add it to the database along with
    /// its addresses, alerts, etc.
    fn create_account(&mut self, hash: HashMap<String, String>) -> EgResult<EgValue> {
        // Translate our hash to an EgValue to prep for cleanup and insert.

        let mut patron = eg::blessed! {
            // '_barcode' because it's not a field on the 'au' class.
            // This allows us to skip field name enforcement.
            "_barcode": hash.get("barcode").unwrap().as_str(),
            "_student_id": hash.get("student_id").unwrap().as_str(),
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

        // Optional middle name
        if let Some(mname) = hash.get("second_given_name") {
            patron["second_given_name"] = mname.as_str().into();
        }

        self.apply_field_values(&hash, &mut patron)?;

        Ok(patron)
    }

    /// Send the account data off to the APIs for database insertion.
    fn insert_account(&mut self, mut patron: EgValue) -> EgResult<()> {
        // Start with actor.usr

        // Password will likely be numeric.
        let password = patron["passwd"].string()?;
        let barcode = patron.remove("_barcode").unwrap();

        self.runner.editor_mut().xact_begin()?;

        let mut new_patron = self.runner.editor_mut().create(patron)?;
        let patron_id = new_patron.id()?;

        self.runner.announce(&format!(
            "Created account for {barcode} with id {patron_id}"
        ));

        let addr = eg::blessed! {
            "_classname": "aua",
            "usr": patron_id,
            "street1": "NONE",
            "city": "NONE",
            "post_code": DEFAULT_POST_CODE,
            "state": "WA",
            "country": "USA",
            "within_city_limits": "f",
        }?;

        let new_addr = self.runner.editor_mut().create(addr)?;

        let card = eg::blessed! {
            "_classname": "ac",
            "barcode": barcode,
            "usr": patron_id,
        }?;

        let new_card = self.runner.editor_mut().create(card)?;

        let alert1_msg = if self.is_classroom {
            CLASSROOM_ALERT_MSG
        } else if self.is_teacher {
            TEACHER_ALERT_MSG
        } else {
            STUDENT_ALERT_MSG
        };

        let message = eg::blessed! {
            "_classname": "aum",
            "usr": patron_id,
            "title": alert1_msg,
            "message": alert1_msg,
            "sending_lib": ROOT_ORG,
            "editor": self.runner.staff_account(),
        }?;

        let new_message_1 = self.runner.editor_mut().create(message)?;

        let alert_1 = eg::blessed! {
            "_classname": "ausp",
            "usr": patron_id,
            "org_unit": ROOT_ORG,
            "standing_penalty": ALERT_TYPE,
            "usr_message": new_message_1.id()?,
        }?;

        let _ = self.runner.editor_mut().create(alert_1)?;

        let message = eg::blessed! {
            "_classname": "aum",
            "usr": patron_id,
            "title": ALERT2_MSG,
            "message": ALERT2_MSG,
            "sending_lib": ROOT_ORG,
            "editor": self.runner.staff_account(),
        }?;

        let new_message_2 = self.runner.editor_mut().create(message)?;

        let alert_2 = eg::blessed! {
            "_classname": "ausp",
            "usr": patron_id,
            "org_unit": ROOT_ORG,
            "standing_penalty": ALERT_TYPE,
            "usr_message": new_message_2.id()?,
        }?;

        let _ = self.runner.editor_mut().create(alert_2)?;

        new_patron["card"] = new_card.id()?.into();
        new_patron["billing_address"] = new_addr.id()?.into();
        new_patron["mailing_address"] = new_addr.id()?.into();

        self.runner.editor_mut().update(new_patron)?;

        eg::common::user::modify_main_password(self.runner.editor_mut(), patron_id, &password)?;

        self.runner.editor_mut().commit()?;

        Ok(())
    }

    /// Extract values from the source hash and translate them into
    /// our patron object, normalizing and applying defaults on the way.
    fn apply_field_values(
        &self,
        hash: &HashMap<String, String>,
        patron: &mut EgValue,
    ) -> EgResult<()> {
        // Password is initially set to last 4 characters of the barcode.
        // string() to avoid mut borrow conflicts
        let barcode = patron["_barcode"].string()?;

        // barcodes have 3-char district codes plus a non-empty student_id.
        // "passwd" field is still required on actor.usr even though
        // the password ultimately ends up in a different table.
        patron["passwd"] = barcode[(barcode.len() - 4)..].into();

        patron["usrname"] = barcode.into();
        patron["home_ou"] = self.home_ou.into();
        patron["net_access_level"] = STUDENT_NET_ACCESS.into();

        if self.is_teacher {
            patron["juvenile"] = "f".into();
            patron["profile"] = TEACHER_PROFILE.into();
            patron["ident_type"] = STUDENT_IDENT_TYPE.into();
            patron["ident_value"] = patron.remove("_student_id").unwrap();
        } else if self.is_classroom {
            patron["juvenile"] = "f".into();
            patron["profile"] = CLASSROOM_PROFILE.into();
            patron["ident_type"] = CLASSROOM_IDENT_TYPE.into();
            patron["ident_value"] = CLASSROOM_IDENT_VALUE.into();
        } else {
            patron["juvenile"] = "t".into();
            patron["profile"] = STUDENT_PROFILE.into();
            patron["ident_type"] = STUDENT_IDENT_TYPE.into();
            patron["ident_value"] = patron.remove("_student_id").unwrap();
        }

        self.set_dob(hash, patron)?;
        self.set_expire_date(patron)?;

        Ok(())
    }

    /// Extract + normalize the dob or apply a default.
    fn set_dob(&self, hash: &HashMap<String, String>, patron: &mut EgValue) -> EgResult<()> {
        // We don't care about dates of birth for adults/generic cards.
        if self.is_teacher || self.is_classroom {
            patron["dob"] = "1900-01-01".into();
            return Ok(());
        }

        let dob = hash
            .get("dob")
            .ok_or_else(|| format!("'dob' value is required: {hash:?}"))?
            .trim();

        // Translate mm/dd/yyyy into ISO.
        let dob = self.regexes.dob_us_regex.replace(dob, |caps: &Captures| {
            // caps[0] is the full source string
            format!("{}-{:0>1}-{:0>1}", &caps[3], &caps[1], &caps[2])
        });

        if !self.regexes.dob_regex.is_match(&dob) {
            return Err(format!("DOB format is invalid: {dob}").into());
        }

        patron["dob"] = dob.into_owned().into();

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
            .map_err(|_| format!("Invalid date of birth year: {birth_year}"))?;

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

        let mut now_date = now_date;
        let mut expire_date = None;

        // date.with_year() can fail if the calculated date is invalid.
        // Give it a few tries.
        for _ in 0..5 {
            expire_date = now_date.with_year(expire_year as i32);
            if expire_date.is_some() {
                break;
            }
            // Add 1 day.  from_days() is not yet finalized.
            now_date += Duration::from_secs(86400);
        }

        let expire_date =
            expire_date.ok_or_else(|| format!("Cannot generate expire date from {now_date}"))?;

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
                .regexes
                .college_id_regex
                .replace_all(&student_id, "")
                .into_owned();

            // College accounts are forced into lowercase.
            student_id = student_id.to_ascii_lowercase();
        } else {
            // K12 teachers and students
            student_id = self
                .regexes
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
    ops.optflag("", "test-file", "");
    ops.optflag("", "purge", "");
    ops.optflag("", "force-new", "");

    let options = script::Options {
        with_evergreen: true,
        with_database: false,
        help_text: Some(HELP_TEXT.to_string()),
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

    runner.announce(&format!("Processing file {file_path}"));

    let file_path = Path::new(&file_path);

    let Some(Some(file_name)) = file_path.file_name().map(|f| f.to_str()) else {
        return runner.exit(1, &format!("Valid file name required: {file_path:?}"));
    };

    let is_teacher = runner.params().opt_present("teacher");
    let is_college = runner.params().opt_present("college");
    let is_classroom = runner.params().opt_present("classroom");
    let is_force_new = runner.params().opt_present("force-new");
    let test_file = runner.params().opt_present("test-file");
    let is_purge = runner.params().opt_present("purge");

    let Some(home_ou) = HOME_OU_MAP
        .iter()
        .find(|(code, _)| *code == district_code)
        .map(|(_, ou)| *ou)
    else {
        return runner.exit(1, &format!("Unknown district: {district_code}"));
    };

    if is_purge
        && !runner
            .prompt("Purging all accounts.  This cannot be undone.")
            .unwrap()
    {
        return;
    }

    let mut importer = Importer {
        runner,
        home_ou,
        test_file,
        is_purge,
        is_force_new,
        is_teacher,
        is_college,
        is_classroom,
        regexes: LocalRegexes::compile(),
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

#[cfg(test)]
const TEST_OK_CSV: &str = r#"
student_id,first_given_name,family_name,dob
0000,first-name1,family-name1,2020-01-01,
0001,first-name2,family-name2,2030-01-01,
0002,first-name3,family-name3,2010-01-01,
0003,first-name4,family-name4,1900-01-01,
"#;

#[cfg(test)]
fn test_create_k12() {
    let options = script::Options::default();
    let mut runner = script::Runner::init(options).unwrap().unwrap();

    let mut importer = Importer {
        runner,
        home_ou: 1234,
        test_file: true,
        is_purge: false,
        is_force_new: false,
        is_teacher: false,
        is_college: false,
        is_classroom: false,
        regexes: LocalRegexes::compile(),
        file_name: "<STRING>".to_string(),
        district_code: "210".to_string(),
    };

    //importer.

    let reader = BufReader::new(TEST_OK_CSV.as_bytes());
    let accounts = importer.load_csv_data(reader).unwrap();

    assert_eq!(accounts.len(), TEST_OK_CSV.split('\n').count() - 1);
}
