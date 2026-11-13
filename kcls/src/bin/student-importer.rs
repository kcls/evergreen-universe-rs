use chrono::{Datelike, Local, NaiveDate, TimeZone};
use eg::EgResult;
use eg::EgValue;
use eg::date::{self, EgDate};
use eg::script;
use evergreen as eg;
use regex::Regex;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const HELP_TEXT: &str = r#"
Import students, teachers, and classroom accounts from a CSV file.

Rows that cannot be imported are logged and written to <file>.err.csv
in the output directory, then processing continues with the next row.
Summary counts are written to <file>.sum.csv.

Example:

kcls-student-importer --staff-account 4953211 --district-code 405 --teacher /tmp/405.teacher.2025-03-06.csv

Options:

    --district-code
        3-character school district code.

    --teacher
        Process CSV file entries as teacher accounts.

    --classroom
        Process CSV file entries as classroom accounts.
        Cannot be combined with --teacher.

    --college
        Process CSV file entries as college accounts.

    --home-ou
        Org unit ID of the home library used for all accounts in the
        file.  Only use this if the home org unit for a batch does not
        match the default org unit for the district code.

    --out-dir [.]
        Directory where the summary and error files are written.

    --expire-interval
        Expire new accounts of every type this far in the future, e.g.
        "2 years", instead of using the default for the account type.
        Also used as the new expire interval for --extend-within.

    --extend-within
        Extend existing accounts which expire within this interval,
        e.g. "6 months", to now plus the --expire-interval.  Without
        --expire-interval, teacher and classroom accounts are extended
        by 10 years and college accounts by 4 years.  K12 student
        accounts require --expire-interval.  Without this option,
        existing accounts are not modified.

    --purge
        Purge (permanently delete) every account in the CSV file.
        Requires typing "yes" to confirm.

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

/// If more than this ratio of accounts in a file are new, block the
/// import for manual review.
const MAX_NEW_RATIO: f64 = 0.5;

/// The new-accounts ratio is only enforced when the number of new
/// accounts exceeds this value.
///
/// This is useful when files contain only new accounts, which is
/// atypical, but can happen.
const MAX_ALLOW_NEW_UNCHECKED: usize = 100;

/// K12 IDs keep only "word" characters.
const NON_WORD_REGEX: &str = r#"[^\w]"#;
/// College IDs (from email addresses) keep a minimal ASCII set.
const COLLEGE_ID_REGEX: &str = r#"[^a-z0-9_\-\.]"#;
const DOB_REGEX: &str = r#"^\d{4}-\d{2}-\d{2}$"#;
/// We allow schools to send DoB values in US-style mm/dd/yyyy.
///
/// Unanchored so values with a trailing time, as some spreadsheet
/// exports produce, are still accepted.
const DOB_US_REGEX: &str = r#"(\d{1,2})/(\d{1,2})/(\d{4})"#;

const TEACHER_EXPIRE_INTERVAL: &str = "10 years";
const COLLEGE_EXPIRE_INTERVAL: &str = "4 years";
const STUDENT_EXPIRE_AGE: i32 = 21; // years

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
    ("eas", 1513), // Eastside Community School (Issaquah)
    ("wps", 1533), // Willow's Prep (Redmond)
    ("tes", 1535), // The Evergreen School (Shoreline)
    ("frs", 1529), // Forest Ridge School of the Sacred Heart (Newport Way)
];

/// One CSV row, keyed on column name.  Values are trimmed and empty
/// values are omitted.
type Row = HashMap<String, String>;

#[derive(Debug, Clone, Copy, PartialEq)]
enum AccountType {
    Student,
    Teacher,
    Classroom,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum FailureType {
    Trans,
    Create,
    Update,
    Delete,
}

impl FailureType {
    /// Matches the err_type values written by the Perl importer.
    fn as_str(&self) -> &'static str {
        match self {
            Self::Trans => "trans",
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

/// A CSV row that could not be processed.
#[derive(Debug)]
struct Failure {
    line: u64,
    row: Row,
    failure_type: FailureType,
    message: String,
}

/// A CSV row with a usable barcode.
#[derive(Debug)]
struct Account {
    line: u64,
    barcode: String,
    /// Source data plus the normalized student_id and barcode.
    row: Row,
}

impl Account {
    fn failure(&self, failure_type: FailureType, message: String) -> Failure {
        Failure {
            line: self.line,
            row: self.row.clone(),
            failure_type,
            message,
        }
    }
}

/// An existing account matching a barcode in the file.
#[derive(Debug)]
struct ExistingUser {
    id: i64,
    expire_date: String,
}

/// Settings for extending existing account expire dates.
#[derive(Debug, Clone)]
struct Extension {
    /// New expire date is now plus this interval.
    expire_interval: String,
    /// Extend accounts that expire within this interval from now.
    extend_within: String,
}

#[derive(Debug)]
struct ParsedFile {
    accounts: Vec<Account>,
    failures: Vec<Failure>,
}

#[derive(Debug, Default)]
struct Stats {
    total: usize,
    trans: usize,
    create: usize,
    update: usize,
    delete: usize,
    trans_err: usize,
    create_err: usize,
    update_err: usize,
    delete_err: usize,
}

impl Stats {
    /// Summary file contents.
    ///
    /// Columns are sorted by name, matching the layout of the file the
    /// Perl importer wrote.  Updates are expire date extensions.
    fn summary_csv(&self) -> String {
        let cols = [
            ("create", self.create),
            ("create_err", self.create_err),
            ("delete", self.delete),
            ("delete_err", self.delete_err),
            ("total", self.total),
            ("trans", self.trans),
            ("trans_err", self.trans_err),
            ("update", self.update),
            ("update_err", self.update_err),
        ];

        let header: Vec<&str> = cols.iter().map(|(k, _)| *k).collect();
        let values: Vec<String> = cols.iter().map(|(_, v)| v.to_string()).collect();

        format!("{}\n{}\n", header.join(","), values.join(","))
    }
}

/// Translated account data, ready to insert.
#[derive(Debug)]
struct PatronData {
    first_given_name: String,
    second_given_name: Option<String>,
    family_name: String,
    dob: NaiveDate,
    expire_date: EgDate,
    passwd: String,
}

/// Data normalization and translation rules.
///
/// Kept separate from the Importer so they can be tested without an
/// Evergreen connection.
///
/// The normalization rules may seem arbitrary.  It's all grandfathered
/// in, and barcodes must match those already generated for existing
/// accounts.
struct Rules {
    account_type: AccountType,
    is_college: bool,
    district_code: String,
    /// Overrides the account type's default expire date for new accounts.
    expire_interval: Option<String>,
    non_word_regex: Regex,
    college_id_regex: Regex,
    dob_regex: Regex,
    dob_us_regex: Regex,
}

impl Rules {
    fn new(account_type: AccountType, is_college: bool, district_code: &str) -> Self {
        Self {
            account_type,
            is_college,
            district_code: district_code.to_string(),
            expire_interval: None,
            non_word_regex: Regex::new(NON_WORD_REGEX).unwrap(),
            college_id_regex: Regex::new(COLLEGE_ID_REGEX).unwrap(),
            dob_regex: Regex::new(DOB_REGEX).unwrap(),
            dob_us_regex: Regex::new(DOB_US_REGEX).unwrap(),
        }
    }

    /// Parse a CSV file into accounts and rows that failed.
    ///
    /// Returns Err if the file as a whole is unusable.
    fn parse_csv(&self, bytes: &[u8]) -> EgResult<ParsedFile> {
        let text =
            std::str::from_utf8(bytes).map_err(|e| format!("File is not valid UTF-8: {e}"))?;

        let mut reader = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(text.as_bytes());

        let headers = reader
            .headers()
            .map_err(|e| format!("Error parsing CSV header: {e}"))?
            .clone();

        let id_idx = headers
            .iter()
            .position(|h| h == "student_id")
            .ok_or("CSV file has no student_id column")?;

        let mut seen = HashSet::new();
        let mut accounts = Vec::new();
        let mut failures = Vec::new();

        for result in reader.records() {
            let record = result.map_err(|e| format!("Error parsing CSV file: {e}"))?;
            let line = record.position().map(|p| p.line()).unwrap_or(0);

            let mut row = Row::new();
            for (name, value) in headers.iter().zip(record.iter()) {
                let value = value.trim();
                if !value.is_empty() {
                    row.insert(name.to_string(), value.to_string());
                }
            }

            let raw_id = record.get(id_idx).unwrap_or("");

            let Some(student_id) = self.normalize_student_id(raw_id) else {
                failures.push(Failure {
                    line,
                    row,
                    failure_type: FailureType::Trans,
                    message: format!("No usable student_id value: '{raw_id}'"),
                });
                continue;
            };

            let barcode = self.barcode(&student_id);

            if !seen.insert(barcode.clone()) {
                log::info!("Skipping duplicate barcode {barcode} on line {line}");
                continue;
            }

            row.insert("student_id".to_string(), student_id);
            row.insert("barcode".to_string(), barcode.clone());

            accounts.push(Account { line, barcode, row });
        }

        Ok(ParsedFile { accounts, failures })
    }

    /// Clean up a raw student_id value.
    ///
    /// Returns None if nothing usable remains.
    fn normalize_student_id(&self, raw: &str) -> Option<String> {
        if raw.trim().is_empty() {
            return None;
        }

        let mut id = raw.to_string();

        // Teacher IDs are left-padded with zeros to 4 characters.  The
        // padding is applied to the raw value, before cleanup, because
        // that's what the Perl importer did and existing barcodes must
        // be reproduced exactly.
        if self.account_type == AccountType::Teacher && id.chars().count() < 4 {
            id = format!("{id:0>4}");
        }

        if self.is_college {
            // College IDs are email addresses.  Remove the domain,
            // lowercase, and keep a minimal set of ASCII characters.
            if let Some(idx) = id.find('@') {
                id.truncate(idx);
            }

            id = self
                .college_id_regex
                .replace_all(&id.to_ascii_lowercase(), "")
                .into_owned();
        } else {
            id = self.non_word_regex.replace_all(&id, "").into_owned();

            if self.account_type == AccountType::Teacher {
                // K12 teacher IDs, which can be alphanumeric, are upper
                // case.  Student IDs retain the provided case.
                id = id.to_uppercase();
            }
        }

        (!id.is_empty()).then_some(id)
    }

    /// Generate a barcode from the district code and normalized student ID.
    fn barcode(&self, student_id: &str) -> String {
        let mut barcode = if self.is_college {
            self.district_code.to_uppercase()
        } else {
            self.district_code.clone()
        };

        if self.account_type == AccountType::Teacher {
            if self.is_college {
                barcode = format!("E{barcode}");
            } else {
                barcode.push('t');
            }
        }

        barcode + student_id
    }

    /// The last 4 characters of the student ID.
    fn password(&self, student_id: &str) -> String {
        let chars: Vec<char> = student_id.chars().collect();
        chars[chars.len().saturating_sub(4)..].iter().collect()
    }

    /// Parse an ISO or mm/dd/yyyy date of birth.
    fn parse_dob(&self, raw: Option<&str>) -> Result<NaiveDate, String> {
        let raw = raw.ok_or("No data present for required field dob")?;

        let iso = match self.dob_us_regex.captures(raw) {
            Some(caps) => format!("{}-{:0>2}-{:0>2}", &caps[3], &caps[1], &caps[2]),
            None => raw.to_string(),
        };

        if !self.dob_regex.is_match(&iso) {
            return Err(format!("Invalid dob: '{raw}'"));
        }

        NaiveDate::parse_from_str(&iso, "%Y-%m-%d").map_err(|e| format!("Invalid dob '{raw}': {e}"))
    }

    /// Translate a CSV row into account data.
    ///
    /// Returns a description of every problem found on failure.
    fn translate(&self, row: &Row, now: EgDate) -> Result<PatronData, String> {
        let mut errors = Vec::new();

        let mut required = |field: &str| {
            let value = row.get(field).map(|v| v.to_uppercase());
            if value.is_none() {
                errors.push(format!("No data present for required field {field}"));
            }
            value
        };

        let first_given_name = required("first_given_name");
        let family_name = required("family_name");

        let dob = match self.account_type {
            // Dates of birth don't matter for adult/generic cards.
            AccountType::Teacher | AccountType::Classroom => NaiveDate::from_ymd_opt(1900, 1, 1),
            AccountType::Student => match self.parse_dob(row.get("dob").map(|s| s.as_str())) {
                Ok(d) => Some(d),
                Err(e) => {
                    errors.push(e);
                    None
                }
            },
        };

        let (Some(first_given_name), Some(family_name), Some(dob)) =
            (first_given_name, family_name, dob)
        else {
            return Err(errors.join("; "));
        };

        let expire_date = self
            .expire_date(now, dob)
            .map_err(|e| format!("Cannot calculate expire date: {e}"))?;

        // parse_csv guarantees a student_id value.
        let passwd = self.password(&row["student_id"]);

        Ok(PatronData {
            first_given_name,
            second_given_name: row.get("second_given_name").map(|v| v.to_uppercase()),
            family_name,
            dob,
            expire_date,
            passwd,
        })
    }

    fn expire_date(&self, now: EgDate, dob: NaiveDate) -> EgResult<EgDate> {
        if let Some(interval) = &self.expire_interval {
            return date::add_interval(now, interval);
        }

        match default_expire_interval(self.account_type, self.is_college) {
            Some(interval) => date::add_interval(now, interval),
            None => student_expire_date(now, dob),
        }
    }
}

/// Default expire interval for the account type.
///
/// None for K12 students, whose expire date depends on their age.
fn default_expire_interval(account_type: AccountType, is_college: bool) -> Option<&'static str> {
    match account_type {
        AccountType::Teacher | AccountType::Classroom => Some(TEACHER_EXPIRE_INTERVAL),
        AccountType::Student if is_college => Some(COLLEGE_EXPIRE_INTERVAL),
        AccountType::Student => None,
    }
}

/// Run `body` inside a transaction and commit.
///
/// Any error rolls the transaction back immediately, so no caller has
/// to remember to.
fn in_xact<T>(
    editor: &mut eg::Editor,
    body: impl FnOnce(&mut eg::Editor) -> EgResult<T>,
) -> EgResult<T> {
    let result = editor
        .xact_begin()
        .and_then(|_| body(editor))
        .and_then(|value| editor.commit().map(|_| value));

    if result.is_err() {
        let _ = editor.rollback();
    }

    result
}

/// Students expire on the July 1 after they reach STUDENT_EXPIRE_AGE.
///
/// The expire date is capped at STUDENT_EXPIRE_AGE - 2 years out, which
/// guards against bogus (e.g. future) dates of birth.  If the computed
/// date has already passed, use the next July 1.
fn student_expire_date(now: EgDate, dob: NaiveDate) -> EgResult<EgDate> {
    let now_year = now.year();
    let max_years_out = STUDENT_EXPIRE_AGE - 2;

    let mut expire_year = (dob.year() + STUDENT_EXPIRE_AGE).min(now_year + max_years_out);

    // Birthdays after July 1 push the expire date into the following year.
    if dob.month() > 7 || (dob.month() == 7 && dob.day() > 1) {
        expire_year += 1;
    }

    let mut expire_date = july_first(expire_year)?;

    if expire_date < now {
        let year = if now.month() < 7 {
            now_year
        } else {
            now_year + 1
        };
        expire_date = july_first(year)?;
    }

    Ok(expire_date)
}

/// July 1 at 6am local time.
///
/// The hour reduces the chance of DST/time zone conversions in external
/// clients displaying the date as June 30.
fn july_first(year: i32) -> EgResult<EgDate> {
    let date = Local
        .with_ymd_and_hms(year, 7, 1, 6, 0, 0)
        .single()
        .ok_or_else(|| format!("Invalid expire year: {year}"))?;

    Ok(date.fixed_offset())
}

/// Interval string as seconds, rejecting values that amount to nothing.
///
/// date::interval_to_seconds silently ignores unknown units, so e.g.
/// "6 fortnights" would otherwise be 0 seconds.
fn parse_interval(interval: &str) -> Result<i64, String> {
    match date::interval_to_seconds(interval) {
        Ok(secs) if secs > 0 => Ok(secs),
        Ok(_) => Err(format!("Invalid interval: '{interval}'")),
        Err(e) => Err(format!("Invalid interval '{interval}': {e}")),
    }
}

/// True if the account expires within the extend_within interval.
fn needs_extension(expire_date: EgDate, now: EgDate, extend_within: &str) -> EgResult<bool> {
    Ok(expire_date < date::add_interval(now, extend_within)?)
}

/// True if the number of new accounts is suspiciously high.
fn new_ratio_exceeded(all_count: usize, new_count: usize) -> bool {
    all_count > 0
        && new_count > MAX_ALLOW_NEW_UNCHECKED
        && (new_count as f64 / all_count as f64) > MAX_NEW_RATIO
}

/// File name minus any .csv extension, used to name the result files.
fn result_file_stem(file_name: &str) -> &str {
    let len = file_name.len();
    if len > 4
        && file_name.is_char_boundary(len - 4)
        && file_name[len - 4..].eq_ignore_ascii_case(".csv")
    {
        &file_name[..len - 4]
    } else {
        file_name
    }
}

/// Write the failures as CSV: the source columns plus csv_line,
/// err_msg, and err_type, sorted by column name.
fn write_failures_csv(writer: impl Write, failures: &[Failure]) -> EgResult<()> {
    let mut columns: BTreeSet<&str> = failures
        .iter()
        .flat_map(|f| f.row.keys().map(|k| k.as_str()))
        .collect();

    columns.extend(["csv_line", "err_msg", "err_type"]);

    let mut writer = csv::Writer::from_writer(writer);
    let err = |e: csv::Error| format!("Error writing failures file: {e}");

    if !failures.is_empty() {
        writer.write_record(&columns).map_err(err)?;
    }

    for failure in failures {
        let line = failure.line.to_string();
        let values = columns.iter().map(|col| match *col {
            "csv_line" => line.as_str(),
            "err_msg" => failure.message.as_str(),
            "err_type" => failure.failure_type.as_str(),
            _ => failure.row.get(*col).map(|v| v.as_str()).unwrap_or(""),
        });
        writer.write_record(values).map_err(err)?;
    }

    writer
        .flush()
        .map_err(|e| format!("Error writing failures file: {e}"))?;

    Ok(())
}

struct Importer {
    runner: script::Runner,
    rules: Rules,
    file_name: String,
    home_ou: u32,
    out_dir: PathBuf,
    test_file: bool,
    is_purge: bool,
    is_force_new: bool,
    extension: Option<Extension>,
    stats: Stats,
    failures: Vec<Failure>,
}

impl fmt::Display for Importer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Importer [{}]", self.file_name)
    }
}

impl Importer {
    fn announce(&self, msg: &str) {
        self.runner.announce(msg);
    }

    /// Log a problem to log::warn! and STDERR without exiting.
    fn warn(&self, msg: &str) {
        let pfx = format!("{LOG_PREFIX} [{}]", self.rules.district_code);
        eprintln!("{} {pfx} {msg}", date::now().format("%F %T%.3f"));
        log::warn!("{pfx} {msg}");
    }

    /// Log a failed row and add it to the failures file.
    fn record_failure(&mut self, failure: Failure) {
        let barcode = failure
            .row
            .get("barcode")
            .map(|b| b.as_str())
            .unwrap_or("<none>");

        self.warn(&format!(
            "{self} {} failed for line {} barcode {barcode}: {}",
            failure.failure_type.as_str(),
            failure.line,
            failure.message
        ));

        match failure.failure_type {
            FailureType::Trans => self.stats.trans_err += 1,
            FailureType::Create => self.stats.create_err += 1,
            FailureType::Update => self.stats.update_err += 1,
            FailureType::Delete => self.stats.delete_err += 1,
        }

        self.failures.push(failure);
    }

    /// Process a CSV file
    fn process_file(&mut self, file_path: &Path) -> EgResult<()> {
        let bytes =
            std::fs::read(file_path).map_err(|e| format!("Cannot read file: {file_path:?} {e}"))?;

        let parsed = self.rules.parse_csv(&bytes)?;

        self.stats.total = parsed.accounts.len() + parsed.failures.len();

        for failure in parsed.failures {
            self.record_failure(failure);
        }

        if self.is_purge {
            self.purge_accounts(&parsed.accounts);
        } else {
            self.process_accounts(&parsed.accounts)?;
        }

        self.write_result_files()?;

        self.announce(&format!(
            "{self} created {} new accounts and extended {} for {}",
            self.stats.create, self.stats.update, self.rules.district_code
        ));

        Ok(())
    }

    /// Create new accounts and, with --extend-within, extend the
    /// expire dates of existing ones.
    fn process_accounts(&mut self, accounts: &[Account]) -> EgResult<()> {
        let existing = self.find_existing(accounts)?;

        let all_count = accounts.len();
        let new_count = accounts
            .iter()
            .filter(|a| !existing.contains_key(&a.barcode))
            .count();

        self.announce(&format!(
            "{self} contains {new_count} new accounts out of {all_count} total"
        ));

        if new_ratio_exceeded(all_count, new_count) {
            if self.is_force_new {
                self.announce("New account ratio exceeded; continuing in --force-new mode");
            } else {
                self.runner.exit(
                    1,
                    &format!(
                        "{self} number of new accounts ({new_count}) out of total \
                        ({all_count}) exceeds the ratio {MAX_NEW_RATIO}.  \
                        Use --force-new to override"
                    ),
                );
            }
        }

        let now = date::now();
        let extension = self.extension.clone();

        for account in accounts {
            if let Some(user) = existing.get(&account.barcode) {
                if let Some(ext) = &extension {
                    self.maybe_extend(account, user, ext, now);
                }
                continue;
            }

            let data = match self.rules.translate(&account.row, now) {
                Ok(d) => d,
                Err(e) => {
                    self.record_failure(account.failure(FailureType::Trans, e));
                    continue;
                }
            };

            self.stats.trans += 1;

            let result = self
                .patron_value(account, &data)
                .and_then(|patron| self.insert_account(patron, &account.barcode, &data));

            match result {
                Ok(Some(id)) => {
                    self.stats.create += 1;
                    self.announce(&format!(
                        "Created account for {} with id {id}",
                        account.barcode
                    ));
                }
                Ok(None) => {} // --test-file
                Err(e) => {
                    self.record_failure(account.failure(FailureType::Create, e.to_string()));
                }
            }
        }

        Ok(())
    }

    /// Map of barcode to the existing, non-deleted account which uses it
    /// as a card barcode or username, either of which would cause an
    /// insert to fail.  Card matches take precedence.
    ///
    /// Deleted accounts are excluded because actor.usr_delete removes
    /// their cards and renames their usernames, freeing the barcode.
    fn find_existing(&mut self, accounts: &[Account]) -> EgResult<HashMap<String, ExistingUser>> {
        let all_barcodes: Vec<&str> = accounts.iter().map(|a| a.barcode.as_str()).collect();
        let mut existing = HashMap::new();

        let to_user = |v: &EgValue| -> EgResult<ExistingUser> {
            Ok(ExistingUser {
                id: v["id"].int()?,
                expire_date: v["expire_date"].string()?,
            })
        };

        // This has the potential to be a large number of barcodes.
        // Chunk the lookups into manageable groups.
        for batch in all_barcodes.chunks(250) {
            let card_query = eg::hash! {
                "select": {"ac": ["barcode"], "au": ["id", "expire_date"]},
                "from": {"ac": "au"},
                "where": {
                    "+ac": {"barcode": {"in": batch}},
                    "+au": {"deleted": "f"}
                }
            };

            for card in self.runner.editor_mut().json_query(card_query)? {
                // string() coerces numeric values.
                existing.insert(card["barcode"].string()?, to_user(&card)?);
            }

            let user_query = eg::hash! {
                "select": {"au": ["usrname", "id", "expire_date"]},
                "from": "au",
                "where": {"+au": {"usrname": {"in": batch}, "deleted": "f"}}
            };

            for user in self.runner.editor_mut().json_query(user_query)? {
                if let std::collections::hash_map::Entry::Vacant(entry) =
                    existing.entry(user["usrname"].string()?)
                {
                    entry.insert(to_user(&user)?);
                }
            }
        }

        Ok(existing)
    }

    /// Extend the account's expire date if it expires within the
    /// extension window.  Failures are logged and recorded.
    fn maybe_extend(
        &mut self,
        account: &Account,
        user: &ExistingUser,
        ext: &Extension,
        now: EgDate,
    ) {
        let barcode = &account.barcode;

        let result = date::parse_datetime(&user.expire_date).and_then(|expire_date| {
            if !needs_extension(expire_date, now, &ext.extend_within)? {
                return Ok(None);
            }
            let new_expire = date::add_interval(now, &ext.expire_interval)?;
            Ok(Some((expire_date, new_expire)))
        });

        let (old_expire, new_expire) = match result {
            Ok(Some(dates)) => dates,
            Ok(None) => return,
            Err(e) => {
                self.record_failure(account.failure(FailureType::Update, e.to_string()));
                return;
            }
        };

        let old_expire = date::to_iso(&old_expire);
        let new_expire_str = date::to_iso(&new_expire);

        if self.test_file {
            self.announce(&format!(
                "Would extend {barcode} (ID {}) from {old_expire} to {new_expire_str}",
                user.id
            ));
            return;
        }

        match self.extend_account(user.id, &new_expire_str) {
            Ok(()) => {
                self.stats.update += 1;
                self.announce(&format!(
                    "Extended {barcode} (ID {}) from {old_expire} to {new_expire_str}",
                    user.id
                ));
            }
            Err(e) => {
                self.record_failure(account.failure(FailureType::Update, e.to_string()));
            }
        }
    }

    /// Apply the new expire date.
    fn extend_account(&mut self, user_id: i64, expire_date: &str) -> EgResult<()> {
        self.announce(&format!(
            "Extending expire date for user {user_id} to {expire_date}"
        ));

        in_xact(self.runner.editor_mut(), |editor| {
            let mut user = editor
                .retrieve("au", user_id)?
                .ok_or_else(|| editor.die_event_msg(&format!("No user found with ID {user_id}")))?;

            user["expire_date"] = expire_date.into();

            editor.update(user)
        })
    }

    /// Build the actor.usr object.
    fn patron_value(&self, account: &Account, data: &PatronData) -> EgResult<EgValue> {
        let (juvenile, profile, ident_type, ident_value) = match self.rules.account_type {
            AccountType::Student => (
                "t",
                STUDENT_PROFILE,
                STUDENT_IDENT_TYPE,
                account.row["student_id"].as_str(),
            ),
            AccountType::Teacher => (
                "f",
                TEACHER_PROFILE,
                STUDENT_IDENT_TYPE,
                account.row["student_id"].as_str(),
            ),
            AccountType::Classroom => (
                "f",
                CLASSROOM_PROFILE,
                CLASSROOM_IDENT_TYPE,
                CLASSROOM_IDENT_VALUE,
            ),
        };

        eg::blessed! {
            "_classname": "au",
            "usrname": account.barcode.as_str(),
            // Required on actor.usr even though the real password is
            // applied separately via modify_main_password.
            "passwd": data.passwd.as_str(),
            "home_ou": self.home_ou,
            "net_access_level": STUDENT_NET_ACCESS,
            "juvenile": juvenile,
            "profile": profile,
            "ident_type": ident_type,
            "ident_value": ident_value,
            "first_given_name": data.first_given_name.as_str(),
            "second_given_name": data.second_given_name.as_deref(),
            "family_name": data.family_name.as_str(),
            "dob": data.dob.format("%F").to_string(),
            "expire_date": date::to_iso(&data.expire_date),
        }
    }

    /// Insert the account along with its address, card, and alerts.
    ///
    /// Returns the new user ID, or None in --test-file mode.
    fn insert_account(
        &mut self,
        patron: EgValue,
        barcode: &str,
        data: &PatronData,
    ) -> EgResult<Option<i64>> {
        if self.test_file {
            let mut dump = patron.clone();
            dump.to_classed_hash();
            dump.scrub_hash_nulls();
            self.announce(&format!("Would create {barcode}: {}", dump.dump()));
            return Ok(None);
        }

        let staff_account = self.runner.staff_account();
        let alert1_msg = match self.rules.account_type {
            AccountType::Student => STUDENT_ALERT_MSG,
            AccountType::Teacher => TEACHER_ALERT_MSG,
            AccountType::Classroom => CLASSROOM_ALERT_MSG,
        };

        in_xact(self.runner.editor_mut(), |editor| {
            let mut new_patron = editor.create(patron)?;
            let patron_id = new_patron.id()?;

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

            let new_addr = editor.create(addr)?;

            let card = eg::blessed! {
                "_classname": "ac",
                "barcode": barcode,
                "usr": patron_id,
            }?;

            let new_card = editor.create(card)?;

            for msg in [alert1_msg, ALERT2_MSG] {
                let message = eg::blessed! {
                    "_classname": "aum",
                    "usr": patron_id,
                    "title": msg,
                    "message": msg,
                    "sending_lib": ROOT_ORG,
                    "editor": staff_account,
                }?;

                let new_message = editor.create(message)?;

                let alert = eg::blessed! {
                    "_classname": "ausp",
                    "usr": patron_id,
                    "org_unit": ROOT_ORG,
                    "standing_penalty": ALERT_TYPE,
                    "usr_message": new_message.id()?,
                }?;

                editor.create(alert)?;
            }

            new_patron["card"] = new_card.id()?.into();
            new_patron["billing_address"] = new_addr.id()?.into();
            new_patron["mailing_address"] = new_addr.id()?.into();

            editor.update(new_patron)?;

            eg::common::user::modify_main_password(editor, patron_id, &data.passwd)?;

            Ok(Some(patron_id))
        })
    }

    /// Permanently delete all accounts related to the provided barcodes.
    fn purge_accounts(&mut self, accounts: &[Account]) {
        self.announce(&format!("Purging {} accounts", accounts.len()));

        for account in accounts {
            let barcode = &account.barcode;

            if self.test_file {
                self.announce(&format!("Would purge account {barcode}"));
                continue;
            }

            match self.purge_account(barcode) {
                Ok(Some(user_id)) => {
                    self.stats.delete += 1;
                    self.announce(&format!("Purged account {barcode} (ID {user_id})"));
                }
                Ok(None) => self.announce(&format!("No account to purge with barcode {barcode}")),
                Err(e) => {
                    self.record_failure(account.failure(FailureType::Delete, e.to_string()));
                }
            }
        }
    }

    /// Returns the ID of the purged user, None if no user was found.
    fn purge_account(&mut self, barcode: &str) -> EgResult<Option<i64>> {
        let editor = self.runner.editor_mut();

        let Some(card) = editor.search("ac", eg::hash! {"barcode": barcode})?.pop() else {
            return Ok(None);
        };

        let user_id = card["usr"].int()?;

        in_xact(editor, |editor| {
            editor.json_query(eg::hash! {"from": ["actor.usr_delete", user_id, EgValue::Null]})
        })?;

        Ok(Some(user_id))
    }

    fn write_result_files(&self) -> EgResult<()> {
        let stem = result_file_stem(&self.file_name);
        let sum_path = self.out_dir.join(format!("{stem}.sum.csv"));
        let err_path = self.out_dir.join(format!("{stem}.err.csv"));

        std::fs::write(&sum_path, self.stats.summary_csv())
            .map_err(|e| format!("Cannot write summary file {sum_path:?}: {e}"))?;

        let err_file = std::fs::File::create(&err_path)
            .map_err(|e| format!("Cannot create error file {err_path:?}: {e}"))?;

        write_failures_csv(err_file, &self.failures)
    }
}

/// Purging requires typing "yes" in full.
fn confirm_purge() -> bool {
    print!(
        "\nPurge every account in the CSV file?  This is irreversible!\nType 'yes' to continue: "
    );
    let _ = io::stdout().flush();

    let mut input = String::new();
    io::stdin().read_line(&mut input).is_ok() && input.trim() == "yes"
}

fn main() {
    let mut ops = getopts::Options::new();

    ops.optopt("", "district-code", "", "");
    ops.optopt("", "home-ou", "", "");
    ops.optopt("", "out-dir", "", "");
    ops.optopt("", "expire-interval", "", "");
    ops.optopt("", "extend-within", "", "");
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

    let params = runner.params();

    let account_type = match (
        params.opt_present("teacher"),
        params.opt_present("classroom"),
    ) {
        (true, true) => return runner.exit(1, "--teacher and --classroom are mutually exclusive"),
        (true, false) => AccountType::Teacher,
        (false, true) => AccountType::Classroom,
        (false, false) => AccountType::Student,
    };

    let is_college = params.opt_present("college");
    let is_force_new = params.opt_present("force-new");
    let test_file = params.opt_present("test-file");
    let is_purge = params.opt_present("purge");
    let out_dir = PathBuf::from(params.opt_str("out-dir").unwrap_or(".".to_string()));
    let home_ou_param = params.opt_str("home-ou");
    let expire_interval = params.opt_str("expire-interval");
    let extend_within = params.opt_str("extend-within");

    if let Some(interval) = &expire_interval
        && let Err(e) = parse_interval(interval)
    {
        return runner.exit(1, &format!("--expire-interval: {e}"));
    }

    let extension = match extend_within {
        None => None,
        Some(within) => {
            let interval =
                expire_interval
                    .clone()
                    .or(default_expire_interval(account_type, is_college).map(|i| i.to_string()));

            let Some(interval) = interval else {
                return runner.exit(
                    1,
                    "--extend-within requires --expire-interval for K12 student accounts",
                );
            };

            let ext = Extension {
                expire_interval: interval,
                extend_within: within,
            };

            // Validated above when user-supplied; defaults are valid.
            let interval_secs = parse_interval(&ext.expire_interval).unwrap_or(0);

            match parse_interval(&ext.extend_within) {
                Err(e) => return runner.exit(1, &format!("--extend-within: {e}")),
                // Otherwise every account would be extended on every run.
                Ok(secs) if secs >= interval_secs => {
                    return runner.exit(
                        1,
                        &format!(
                            "--extend-within ({}) must be shorter than the expire interval ({})",
                            ext.extend_within, ext.expire_interval
                        ),
                    );
                }
                Ok(_) => Some(ext),
            }
        }
    };

    let home_ou = match home_ou_param {
        Some(ou) => match ou.parse::<u32>() {
            Ok(ou) => ou,
            Err(e) => return runner.exit(1, &format!("Invalid --home-ou value '{ou}': {e}")),
        },
        None => match HOME_OU_MAP.iter().find(|(code, _)| *code == district_code) {
            Some((_, ou)) => *ou,
            None => return runner.exit(1, &format!("Unknown district: {district_code}")),
        },
    };

    if is_purge && !confirm_purge() {
        return runner.exit(1, "Purge cancelled");
    }

    let mut rules = Rules::new(account_type, is_college, &district_code);
    rules.expire_interval = expire_interval;

    let mut importer = Importer {
        runner,
        rules,
        file_name: file_name.to_string(),
        home_ou,
        out_dir,
        test_file,
        is_purge,
        is_force_new,
        extension,
        stats: Stats::default(),
        failures: Vec::new(),
    };

    if let Err(e) = importer.process_file(file_path) {
        importer.runner.exit(
            1,
            &format!("Error processing file {}: {e}", importer.file_name),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(account_type: AccountType, is_college: bool) -> Rules {
        Rules::new(account_type, is_college, "405")
    }

    fn local(y: i32, m: u32, d: u32) -> EgDate {
        Local
            .with_ymd_and_hms(y, m, d, 12, 0, 0)
            .single()
            .unwrap()
            .fixed_offset()
    }

    fn ymd(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    fn row(pairs: &[(&str, &str)]) -> Row {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Normalized student ID and barcode, or None.
    fn barcode_for(rules: &Rules, raw: &str) -> Option<(String, String)> {
        rules.normalize_student_id(raw).map(|id| {
            let bc = rules.barcode(&id);
            (id, bc)
        })
    }

    #[test]
    fn student_barcodes() {
        let r = rules(AccountType::Student, false);
        // Case is preserved; non-word characters, including '.' and
        // '-', are removed.
        assert_eq!(
            barcode_for(&r, " ab-12.3_4 "),
            Some(("ab123_4".into(), "405ab123_4".into()))
        );
        // Short student IDs are not padded.
        assert_eq!(barcode_for(&r, "12"), Some(("12".into(), "40512".into())));
        assert_eq!(barcode_for(&r, ""), None);
        assert_eq!(barcode_for(&r, "   "), None);
        assert_eq!(barcode_for(&r, "--"), None);
    }

    #[test]
    fn teacher_barcodes() {
        let r = rules(AccountType::Teacher, false);
        assert_eq!(
            barcode_for(&r, "ab12"),
            Some(("AB12".into(), "405tAB12".into()))
        );
        assert_eq!(
            barcode_for(&r, "7"),
            Some(("0007".into(), "405t0007".into()))
        );
        // Padding happens before cleanup, matching the Perl importer.
        assert_eq!(
            barcode_for(&r, "1-2"),
            Some(("012".into(), "405t012".into()))
        );
        // An empty ID is not padded into a valid one.
        assert_eq!(barcode_for(&r, " "), None);
    }

    #[test]
    fn classroom_barcodes() {
        let r = rules(AccountType::Classroom, false);
        assert_eq!(
            barcode_for(&r, "ab1"),
            Some(("ab1".into(), "405ab1".into()))
        );
    }

    #[test]
    fn college_barcodes() {
        let r = Rules::new(AccountType::Student, true, "grc");
        assert_eq!(
            barcode_for(&r, "John.Smith-Jr_2@grc.edu"),
            Some(("john.smith-jr_2".into(), "GRCjohn.smith-jr_2".into()))
        );
        // Non-ASCII characters are removed.
        assert_eq!(
            barcode_for(&r, "José"),
            Some(("jos".into(), "GRCjos".into()))
        );
        assert_eq!(barcode_for(&r, "@grc.edu"), None);

        let r = Rules::new(AccountType::Teacher, true, "grc");
        assert_eq!(
            barcode_for(&r, "Prof.X@grc.edu"),
            Some(("prof.x".into(), "EGRCprof.x".into()))
        );
        // College teacher IDs are padded too, before the domain is removed.
        assert_eq!(
            barcode_for(&r, "ab"),
            Some(("00ab".into(), "EGRC00ab".into()))
        );
    }

    #[test]
    fn passwords() {
        let r = rules(AccountType::Student, false);
        assert_eq!(r.password("123456"), "3456");
        assert_eq!(r.password("12"), "12");
    }

    #[test]
    fn dob_parsing() {
        let r = rules(AccountType::Student, false);
        assert_eq!(r.parse_dob(Some("2010-01-05")), Ok(ymd(2010, 1, 5)));
        assert_eq!(r.parse_dob(Some("1/5/2010")), Ok(ymd(2010, 1, 5)));
        assert_eq!(r.parse_dob(Some("12/25/2010")), Ok(ymd(2010, 12, 25)));
        assert_eq!(r.parse_dob(Some("1/5/2010 0:00")), Ok(ymd(2010, 1, 5)));
        assert!(r.parse_dob(Some("2010-1-5")).is_err());
        assert!(r.parse_dob(Some("13/45/2010")).is_err());
        assert!(r.parse_dob(Some("2010-02-30")).is_err());
        assert!(r.parse_dob(Some("yesterday")).is_err());
        assert!(r.parse_dob(None).is_err());
    }

    #[test]
    fn student_expire_dates() {
        let march = local(2026, 3, 15);
        let expire = |now, dob| student_expire_date(now, dob).unwrap();

        // July 1 of the year they turn 21.
        assert_eq!(expire(march, ymd(2010, 3, 1)), july_first(2031).unwrap());
        assert_eq!(expire(march, ymd(2010, 7, 1)), july_first(2031).unwrap());

        // Birthdays after July 1 roll into the next year.
        assert_eq!(expire(march, ymd(2010, 7, 2)), july_first(2032).unwrap());
        assert_eq!(expire(march, ymd(2010, 12, 31)), july_first(2032).unwrap());

        // Already 21 or older: next July 1.
        assert_eq!(expire(march, ymd(2000, 1, 1)), july_first(2026).unwrap());
        assert_eq!(
            expire(local(2026, 8, 1), ymd(2000, 1, 1)),
            july_first(2027).unwrap()
        );

        // Bogus/future DoBs are capped at 19 years out, before the
        // birthday adjustment, matching the Perl importer.
        assert_eq!(expire(march, ymd(2030, 1, 1)), july_first(2045).unwrap());
        assert_eq!(expire(march, ymd(2030, 9, 1)), july_first(2046).unwrap());

        let date = expire(march, ymd(2010, 3, 1));
        assert_eq!(date.format("%m-%d %H:%M").to_string(), "07-01 06:00");
    }

    #[test]
    fn translate_student() {
        let r = rules(AccountType::Student, false);
        let now = local(2026, 3, 15);

        let data = r
            .translate(
                &row(&[
                    ("student_id", "123456"),
                    ("first_given_name", "Ada"),
                    ("second_given_name", "q"),
                    ("family_name", "Lovelace"),
                    ("dob", "3/1/2010"),
                ]),
                now,
            )
            .unwrap();

        assert_eq!(data.first_given_name, "ADA");
        assert_eq!(data.second_given_name.as_deref(), Some("Q"));
        assert_eq!(data.family_name, "LOVELACE");
        assert_eq!(data.dob, ymd(2010, 3, 1));
        assert_eq!(data.passwd, "3456");
        assert_eq!(data.expire_date, july_first(2031).unwrap());
    }

    #[test]
    fn translate_reports_all_errors() {
        let r = rules(AccountType::Student, false);
        let err = r
            .translate(
                &row(&[("student_id", "1"), ("dob", "nope")]),
                local(2026, 3, 15),
            )
            .unwrap_err();

        assert!(err.contains("first_given_name"), "{err}");
        assert!(err.contains("family_name"), "{err}");
        assert!(err.contains("Invalid dob"), "{err}");
    }

    #[test]
    fn translate_teacher_ignores_dob() {
        let r = rules(AccountType::Teacher, false);
        let now = local(2026, 3, 15);

        let data = r
            .translate(
                &row(&[
                    ("student_id", "T001"),
                    ("first_given_name", "a"),
                    ("family_name", "b"),
                    ("dob", "garbage"),
                ]),
                now,
            )
            .unwrap();

        assert_eq!(data.dob, ymd(1900, 1, 1));
        assert_eq!(data.second_given_name, None);
        assert_eq!(data.expire_date.year(), 2036);
    }

    #[test]
    fn parse_csv_rows() {
        let r = rules(AccountType::Student, false);

        // Includes trailing commas, blank values, a duplicate, a row
        // with no ID, and a short row.
        let csv = "\
student_id,first_given_name,second_given_name,family_name,dob
0000, first-name1 ,,family-name1,2020-01-01,
0001,first-name2,,family-name2,2030-01-01,
0000,dupe,,dupe,2020-01-01
,no-id,,no-id,2020-01-01
0002,short-row
";

        let parsed = r.parse_csv(csv.as_bytes()).unwrap();

        assert_eq!(parsed.accounts.len(), 3);
        assert_eq!(parsed.failures.len(), 1);

        let first = &parsed.accounts[0];
        assert_eq!(first.barcode, "4050000");
        assert_eq!(first.line, 2);
        assert_eq!(first.row["first_given_name"], "first-name1");
        assert_eq!(first.row["barcode"], "4050000");
        assert!(!first.row.contains_key("second_given_name"));

        let failure = &parsed.failures[0];
        assert_eq!(failure.line, 5);
        assert_eq!(failure.failure_type, FailureType::Trans);
        assert_eq!(failure.row["first_given_name"], "no-id");

        assert!(!parsed.accounts[2].row.contains_key("family_name"));
    }

    #[test]
    fn parse_csv_bom() {
        let r = rules(AccountType::Student, false);
        let csv = "\u{feff}student_id,first_given_name\n1234,a\n";
        let parsed = r.parse_csv(csv.as_bytes()).unwrap();
        assert_eq!(parsed.accounts.len(), 1);
    }

    #[test]
    fn parse_csv_rejects_bad_files() {
        let r = rules(AccountType::Student, false);

        // Latin-1 encoded "José"
        let latin1 = b"student_id,first_given_name\n1234,Jos\xe9\n";
        let err = r.parse_csv(latin1).unwrap_err().to_string();
        assert!(err.contains("UTF-8"), "{err}");

        assert!(r.parse_csv(b"id,first_given_name\n1234,a\n").is_err());
        assert!(r.parse_csv(b"").is_err());
    }

    #[test]
    fn expire_interval_override() {
        let now = local(2026, 3, 15);
        let expected = date::add_interval(now, "2 years").unwrap();

        for account_type in [
            AccountType::Student,
            AccountType::Teacher,
            AccountType::Classroom,
        ] {
            for is_college in [false, true] {
                let mut r = rules(account_type, is_college);
                r.expire_interval = Some("2 years".into());
                assert_eq!(r.expire_date(now, ymd(2010, 3, 1)).unwrap(), expected);
            }
        }
    }

    #[test]
    fn default_expire_intervals() {
        use AccountType::*;
        assert_eq!(default_expire_interval(Student, false), None);
        assert_eq!(
            default_expire_interval(Student, true),
            Some(COLLEGE_EXPIRE_INTERVAL)
        );
        assert_eq!(
            default_expire_interval(Teacher, true),
            Some(TEACHER_EXPIRE_INTERVAL)
        );
        assert_eq!(
            default_expire_interval(Classroom, false),
            Some(TEACHER_EXPIRE_INTERVAL)
        );
    }

    #[test]
    fn intervals() {
        assert_eq!(parse_interval("1 day"), Ok(86400));
        assert!(parse_interval("2 years").is_ok());
        assert!(parse_interval("").is_err());
        assert!(parse_interval("6 fortnights").is_err());
        assert!(parse_interval("0 days").is_err());
    }

    #[test]
    fn extension_window() {
        let now = local(2026, 3, 15);
        let within = |expire| needs_extension(expire, now, "6 months").unwrap();

        assert!(within(local(2026, 6, 1)));
        assert!(within(local(2025, 1, 1)), "already expired");
        assert!(!within(local(2027, 1, 1)));
    }

    #[test]
    fn new_ratio() {
        // At or below the unchecked limit, anything goes.
        assert!(!new_ratio_exceeded(100, 100));
        assert!(!new_ratio_exceeded(0, 0));
        // Above the limit, the ratio applies.
        assert!(new_ratio_exceeded(150, 101));
        assert!(!new_ratio_exceeded(202, 101));
        assert!(new_ratio_exceeded(201, 101));
    }

    #[test]
    fn result_file_stems() {
        assert_eq!(result_file_stem("405.2026-01-01.csv"), "405.2026-01-01");
        assert_eq!(result_file_stem("405.teacher.CSV"), "405.teacher");
        assert_eq!(result_file_stem("405.txt"), "405.txt");
        assert_eq!(result_file_stem(".csv"), ".csv");
    }

    #[test]
    fn summary_file() {
        let stats = Stats {
            total: 10,
            trans: 7,
            create: 6,
            trans_err: 3,
            create_err: 1,
            update: 4,
            update_err: 2,
            ..Default::default()
        };

        assert_eq!(
            stats.summary_csv(),
            "create,create_err,delete,delete_err,total,trans,trans_err,update,update_err\n\
             6,1,0,0,10,7,3,4,2\n"
        );
    }

    #[test]
    fn failures_file() {
        let failures = vec![
            Failure {
                line: 3,
                row: row(&[("student_id", "1"), ("barcode", "4051")]),
                failure_type: FailureType::Trans,
                message: "bad, data".into(),
            },
            Failure {
                line: 9,
                row: row(&[("first_given_name", "x")]),
                failure_type: FailureType::Create,
                message: "oops".into(),
            },
        ];

        let mut out = Vec::new();
        write_failures_csv(&mut out, &failures).unwrap();

        assert_eq!(
            String::from_utf8(out).unwrap(),
            "barcode,csv_line,err_msg,err_type,first_given_name,student_id\n\
             4051,3,\"bad, data\",trans,,1\n\
             ,9,oops,create,x,\n"
        );

        let mut out = Vec::new();
        write_failures_csv(&mut out, &[]).unwrap();
        assert!(out.is_empty());
    }
}
