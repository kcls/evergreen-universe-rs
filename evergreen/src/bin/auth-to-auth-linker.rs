use evergreen::{self as eg, result::EgResult, script, EgError, EgValue};
use postgres::fallible_iterator::FallibleIterator;
use std::collections::HashMap;

const HELP_TEXT: &str = "
eg-auth-to-auth-linker - Link reference headings in authority records to main entry headings in other authority records

For a given set of records, find authority reference headings that also
appear as main entry headings in any other authority record. In the
specific MARC field of the authority record (source) containing the reference
heading with such a match in another authority record (target), add a subfield
0 (zero) referring to the target record by ID.

    -r <id>, --record=<id>
        Specifies the authority record ID (found in the authority.record_entry.id
        column) of the source record to process. This option may be specified more
        than once to process multiple records in a single run.

    -a, --all
        Specifies that all authority records should be processed. For large
        databases, this may take an extraordinarily long amount of time.

    -s <start-id>, --start_id <start-id>
        Specifies the starting ID of the range of authority records to process.
        This option is ignored unless it is accompanied by the -e or --end_id
        option.

    -e <end-id>, --end_id <end-id>
        Specifies the ending ID of the range of authority records to process.
        This option is ignored unless it is accompanied by the -s or --start_id
        option.
";

#[derive(PartialEq, Debug)]
enum ScriptMode {
    SingleRecord,
    RangeOfRecords,
    AllRecords,
    NoRecords,
}

impl From<&getopts::Matches> for ScriptMode {
    fn from(matches: &getopts::Matches) -> Self {
        if matches.opt_present("all") {
            ScriptMode::AllRecords
        } else if matches.opt_present("record") {
            ScriptMode::SingleRecord
        } else if matches.opt_present("start_id") && matches.opt_present("end_id") {
            ScriptMode::RangeOfRecords
        } else {
            ScriptMode::NoRecords
        }
    }
}

struct AuthorityRecordIds {
    start_id: Option<i64>,
    end_id: Option<i64>,
    single_record_id: Option<i64>,
}

impl From<&getopts::Matches> for AuthorityRecordIds {
    fn from(matches: &getopts::Matches) -> Self {
        if matches.opt_present("record") {
            AuthorityRecordIds {
                single_record_id: matches.opt_get("record").unwrap(),
                start_id: None,
                end_id: None,
            }
        } else if matches.opt_present("start_id") && matches.opt_present("end_id") {
            AuthorityRecordIds {
                single_record_id: None,
                start_id: matches.opt_get("start_id").unwrap(),
                end_id: matches.opt_get("end_id").unwrap(),
            }
        } else {
            AuthorityRecordIds {
                single_record_id: None,
                start_id: None,
                end_id: None,
            }
        }
    }
}

struct AuthLinker {
    scripter: script::Runner,
    acsaf_cache: HashMap<i32, EgValue>,
}

impl AuthLinker {
    pub fn new(scripter: script::Runner) -> Result<AuthLinker, EgError> {
        Ok(AuthLinker {
            scripter,
            acsaf_cache: HashMap::new(),
        })
    }

    pub fn run(&mut self) -> EgResult<()> {
        if ScriptMode::from(self.scripter.params()) == ScriptMode::NoRecords {
            println!("{}", HELP_TEXT);
            return Err(EgError::from(
                "Nothing to do, please check the arguments you've passed",
            ));
        }

        let Ok(potential_match_rows) = self.get_the_potential_matches() else {
            return Err(EgError::from(
                "Could not get a list of potential matches from the databse",
            ));
        };

        for potential_match in potential_match_rows
            .iterator()
            .filter_map(Result::ok)
            .collect::<Vec<postgres::Row>>()
        {
            let (source_record_id, links) = get_potential_match_data_from_row(&potential_match)?;
            let source_record = self.get_authority_record(source_record_id)?;
            let source_marc = get_marc_from_db_record(&source_record)?;

            let (edited_marc_record, changed) = links.fold(
                (source_marc, false),
                |(unchanged_marc, changed), (target_record_id, field_id)| match self
                    .build_new_marc_record_with_link(&unchanged_marc, target_record_id, field_id)
                {
                    Some(changed_marc) => (changed_marc, true),
                    None => (unchanged_marc, changed),
                },
            );
            if changed {
                self.persist_marc_data_to_db(source_record, edited_marc_record)?;
            }
        }
        Ok(())
    }

    // Returns Some(marc::Record) if it found a valid link and added it, None if it could not find a valid link to add from the given params
    //
    // PARAMS
    // source_marc: a marc record with 5XX-type fields that need a subfield to show which other record they point to
    // target_record_id: the database id of an authority.record_entry that source_marc potentially needs to be linked to
    // field_id: the database id of an authority.control_set_authority_field with configuration information of the 5XX
    fn build_new_marc_record_with_link(
        &mut self,
        source_marc: &marc::Record,
        target_record_id: i64,
        field_id: i32,
    ) -> Option<marc::Record> {
        let mut mutable_marc_record = source_marc.clone();
        let Ok(target_marc) =
            get_marc_from_db_record(&self.get_authority_record(target_record_id).ok()?)
        else {
            return None;
        };

        let field_config = self.get_an_authority_field_config(field_id).ok()?;
        let main_entry = &field_config["main_entry"];
        let fields_match = build_field_matcher(main_entry);

        let mut changed = false;
        let Some(source_marc_tag) = &field_config["tag"].to_string() else {
            return None;
        };
        let Some(target_marc_tag) = &main_entry["tag"].to_string() else {
            return None;
        };
        for source_field in &mut mutable_marc_record.get_fields_mut(source_marc_tag) {
            if target_marc
                .get_fields(target_marc_tag)
                .iter()
                .any(|field| fields_match(source_field, field))
            {
                source_field.remove_subfields("0");
                if source_field
                    .add_subfield(
                        "0",
                        &control_number_identifier(&target_marc, target_record_id),
                    )
                    .is_ok()
                {
                    changed = true
                };
            }
        }
        if changed {
            Some(mutable_marc_record)
        } else {
            None
        }
    }

    fn persist_marc_data_to_db(
        &mut self,
        source_record: EgValue,
        source_marc: marc::Record,
    ) -> EgResult<()> {
        let mut updated = source_record.clone();
        updated["marc"] = EgValue::from(source_marc.to_xml()?);
        self.save_a_record(updated)
    }

    fn get_the_potential_matches(&mut self) -> Result<postgres::RowIter<'_>, postgres::Error> {
        let match_query = PotentialMatchesQuery::new(self.scripter.params());
        self.scripter
            .db()
            .client()
            .query_raw(&match_query.query(), match_query.bound_params())
    }

    fn save_a_record(&mut self, record: EgValue) -> EgResult<()> {
        self.scripter.editor_mut().xact_begin()?;
        self.scripter.editor_mut().update(record)?;
        self.scripter.editor_mut().commit()
    }

    fn get_authority_record(&mut self, record_id: i64) -> EgResult<EgValue> {
        match self.scripter.editor_mut().retrieve("are", record_id) {
            Ok(Some(record)) => Ok(record),
            _ => Err(EgError::from(format!(
                "Could not retrieve authority record id {}",
                record_id
            ))),
        }
    }

    fn get_an_authority_field_config(&mut self, field_id: i32) -> EgResult<&EgValue> {
        let ops = eg::hash! {
            "flesh": 1,
            "flesh_fields": {"acsaf": ["main_entry"]},
        };

        if let std::collections::hash_map::Entry::Vacant(e) = self.acsaf_cache.entry(field_id) {
            if let Some(field_config) = self
                .scripter
                .editor_mut()
                .retrieve_with_ops("acsaf", field_id, ops)?
            {
                e.insert(field_config);
            }
        }

        self.acsaf_cache
            .get(&field_id)
            .ok_or_else(|| EgError::from("Unable to retrieve authority field config"))
    }
}

fn build_field_matcher(main_entry: &EgValue) -> impl Fn(&marc::Field, &marc::Field) -> bool + '_ {
    move |my_field: &marc::Field, other_field: &marc::Field| -> bool {
        let subfields_to_compare = main_entry["display_sf_list"]
            .to_string()
            .unwrap_or("aivxyz".to_string());
        let joiner = main_entry["joiner"]
            .to_string()
            .unwrap_or(" -- ".to_string());
        matchable_string(my_field, &subfields_to_compare, &joiner)
            == matchable_string(other_field, &subfields_to_compare, &joiner)
    }
}

fn matchable_string(field: &marc::Field, subfields_to_include: &str, joiner: &str) -> String {
    field
        .subfields()
        .iter()
        .filter(|subfield| subfields_to_include.contains(subfield.code()))
        .map(|subfield| subfield.content())
        .collect::<Vec<&str>>()
        .join(joiner)
}

fn parse_link_target_and_field(concatenated: &str) -> EgResult<(i64, i32)> {
    let parts: Vec<&str> = concatenated.split(',').collect();
    let error_message = format!("invalid link in database: {}", concatenated);

    let Ok(target_record_id) = parts[0].parse::<i64>() else {
        return Err(EgError::from(error_message));
    };
    let Ok(field_id) = parts[1].parse::<i32>() else {
        return Err(EgError::from(error_message));
    };
    Ok((target_record_id, field_id))
}

fn get_marc_from_db_record(db_record: &EgValue) -> EgResult<marc::Record> {
    let linkable_xml = db_record["marc"].str()?;
    match marc::Record::from_xml(linkable_xml).next() {
        Some(marc_record) => Ok(marc_record?),
        None => Err("Could not parse marc record".into()),
    }
}

fn control_number_identifier(main_entry_marc: &marc::Record, target_record_id: i64) -> String {
    let potential_control_number_identifiers = main_entry_marc.get_control_fields("003");
    let control_number_identifier = match potential_control_number_identifiers.first() {
        Some(cni_field) => cni_field.content(),
        None => "CONS",
    };
    format!("({}){}", control_number_identifier, target_record_id)
}

fn get_potential_match_data_from_row(
    row: &postgres::Row,
) -> EgResult<(i64, impl Iterator<Item = (i64, i32)> + '_)> {
    let source_record_id = row.try_get::<&str, i64>("source")?;
    let links = match row.try_get::<&str, &str>("links") {
        Ok(concatenated) => {
            concatenated
                .split(';') // links are ;-delimited
                .map(parse_link_target_and_field)
                .filter_map(Result::ok)
        }
        Err(err) => {
            return Err(EgError::from(err));
        }
    };
    Ok((source_record_id, links))
}

// This struct is responsible for creating an SQL query that finds source
// records and target records that they could potentially link to.
// A record is considered a source record if it has a particular
// string in a non-main entry field like 400 or 550.  A record is considered
// a target if it has that same string in a main entry field, like 100
// or 150.
//
// The target entries are in this format: 7892,20;7903,21
// In this example, 7892 and 7903 are autority.record_entry.ids
// and 20 and 21 are authority.control_set_authority_field.ids representing
// the field configuration that ties together the target record and a main entry
// field that is present on the source.
struct PotentialMatchesQuery {
    mode: ScriptMode,
    ids: AuthorityRecordIds,
}

impl PotentialMatchesQuery {
    pub fn new(matches: &getopts::Matches) -> Self {
        PotentialMatchesQuery {
            mode: ScriptMode::from(matches),
            ids: AuthorityRecordIds::from(matches),
        }
    }

    pub fn query(&self) -> String {
        let where_clause = match self.mode {
            ScriptMode::SingleRecord => "WHERE sh2.record = $1",
            ScriptMode::RangeOfRecords => "WHERE sh2.record BETWEEN $1 AND $2",
            _ => "",
        };

        format!("
            SELECT source,
                ARRAY_TO_STRING(ARRAY_AGG(target || ',' || field), ';') AS links
                FROM (
                    SELECT  sh1.record AS target,
                        sh2.record AS source,
                        sh2.atag AS field
                    FROM  authority.simple_heading sh1
                        JOIN authority.simple_heading sh2 USING (sort_value)
                        JOIN authority.control_set_authority_field af1 ON (sh1.atag = af1.id AND af1.main_entry IS NULL)
                        JOIN authority.control_set_authority_field af2 ON (sh2.atag = af2.id AND af2.main_entry IS NOT NULL AND af2.linking_subfield IS NOT NULL)
                        {where_clause}
                        EXCEPT SELECT target, source, field FROM authority.authority_linking
                    ) x GROUP BY 1")
    }

    pub fn bound_params(&self) -> Vec<i64> {
        match self.mode {
            ScriptMode::SingleRecord => match self.ids.single_record_id {
                Some(id) => vec![id],
                None => vec![],
            },
            ScriptMode::RangeOfRecords => {
                if self.ids.start_id.is_some() && self.ids.end_id.is_some() {
                    vec![self.ids.start_id.unwrap(), self.ids.end_id.unwrap()]
                } else {
                    vec![]
                }
            }
            _ => vec![],
        }
    }
}

fn command_line_opts() -> getopts::Options {
    let mut opts = getopts::Options::new();
    opts.optflag("h", "help", "desc");
    opts.optflag("a", "all", "desc");
    opts.optopt("r", "record", "desc", "hint");
    opts.optopt("s", "start_id", "desc", "hint");
    opts.optopt("e", "end_id", "desc", "hint");
    opts
}

fn main() -> EgResult<()> {
    let options = script::Options {
        with_evergreen: true,
        with_database: true,
        help_text: Some(HELP_TEXT.to_string()),
        extra_params: None,
        options: Some(command_line_opts()),
    };

    let scripter = match script::Runner::init(options)? {
        Some(s) => s,
        None => return Ok(()), // e.g. --help
    };
    let mut linker = AuthLinker::new(scripter)?;
    linker.run()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_potential_match_query_has_no_when_linking_all_records() {
        let query = PotentialMatchesQuery {
            mode: ScriptMode::AllRecords,
            ids: AuthorityRecordIds {
                start_id: None,
                end_id: None,
                single_record_id: None,
            },
        };

        assert!(!query.query().to_lowercase().contains("where sh2.record ="));
        assert!(!query
            .query()
            .to_lowercase()
            .contains("where sh2.record between"));
        assert!(query.bound_params().is_empty());
    }

    #[test]
    fn test_potential_match_query_can_limit_to_single_record() {
        let query = PotentialMatchesQuery {
            mode: ScriptMode::SingleRecord,
            ids: AuthorityRecordIds {
                start_id: None,
                end_id: None,
                single_record_id: Some(100),
            },
        };

        assert!(query
            .query()
            .to_lowercase()
            .contains("where sh2.record = $1"));
        assert_eq!(query.bound_params().len(), 1);
        assert_eq!(query.bound_params()[0], 100);
    }

    #[test]
    fn test_potential_match_query_can_limit_to_record_range() {
        let query = PotentialMatchesQuery {
            mode: ScriptMode::RangeOfRecords,
            ids: AuthorityRecordIds {
                start_id: Some(250),
                end_id: Some(500),
                single_record_id: None,
            },
        };

        assert!(query
            .query()
            .to_lowercase()
            .contains("where sh2.record between $1 and $2"));
        assert_eq!(query.bound_params().len(), 2);
        assert_eq!(query.bound_params()[0], 250);
        assert_eq!(query.bound_params()[1], 500);
    }

    #[test]
    fn test_matchable_string_creates_a_string_from_requested_subfields() {
        let mut field = marc::Field::new("550").unwrap();
        field.add_subfield("a", "Frog").unwrap();
        field.add_subfield("b", "Baboon").unwrap();
        field.add_subfield("c", "Elephant").unwrap();

        let result = matchable_string(&field, "ac", "--");
        assert_eq!(result, "Frog--Elephant");
    }

    #[test]
    fn test_linker_can_identify_all_records_mode() {
        let options = command_line_opts();
        let matches = options.parse(["-a"]).unwrap();
        assert_eq!(ScriptMode::from(&matches), ScriptMode::AllRecords);
    }

    #[test]
    fn test_linker_can_identify_single_record_mode() {
        let options = command_line_opts();
        let matches = options.parse(["-r 1234"]).unwrap();
        assert_eq!(ScriptMode::from(&matches), ScriptMode::SingleRecord);
    }

    #[test]
    fn test_linker_can_identify_range_mode() {
        let options = command_line_opts();
        let matches = options.parse(["-s 1", "-e 10"]).unwrap();
        assert_eq!(ScriptMode::from(&matches), ScriptMode::RangeOfRecords);
    }

    #[test]
    fn test_linker_can_identify_no_records_mode() {
        let options = command_line_opts();
        let matches = options.parse(["--help"]).unwrap();
        assert_eq!(ScriptMode::from(&matches), ScriptMode::NoRecords);
    }

    #[test]
    fn test_can_parse_a_link_target_id_and_field_id() {
        assert_eq!(
            parse_link_target_and_field("45678,12").unwrap(),
            (45678, 12)
        );
    }

    #[test]
    fn test_cannot_parse_an_incorrectly_formatted_link_target_id_and_field_id() {
        assert!(parse_link_target_and_field("45678--12").is_err());
    }
}
