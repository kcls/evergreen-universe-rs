/*
 * This script was built from the KCLS authority_control_fields.pl
 * script.  It varies from stock Evergreen.  It should be possible to
 * sync with stock Evergreen with additional command line options.
 */
use eg::date;
use eg::norm::Normalizer;
use eg::script;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use marctk as marc;
use std::collections::HashSet;
use std::sync::OnceLock;
use std::thread;

const DEFAULT_CONTROL_NUMBER_IDENTIFIER: &str = "DLC";

static CONTROLLED_FIELDS: OnceLock<Vec<ControlledField>> = OnceLock::new();

const HELP_TEXT: &str = "
Link bib records to authority records by applying $0 values to controlled fields.

By default, all non-deleted bib records are processed.

    --record-id <id>
        Update links for a specific bib record.

    --min-id <id>
        Minimum bib record ID to process.

    --max-id <id>
        Maximum bib record ID to process.

    --bibs-modified-since <ISO date>
        Limit to bib records whose edit date is >= the provided date.

    --auths-modified-since <ISO date>
        Limit to bib records that share a browse entry with an authority
        record whose edit date is >= the provided date and is not
        already linked to the authority record.

    --parallel <count>
        Number of parallel worker threads to run.  Defaults to 1.

    -h, --help
        Display this help

";

// mapping of authority leader/11 "Subject heading system/thesaurus"
// to the matching bib record indicator
const AUTH_TO_BIB_IND2: &[(&str, &str)] = &[
    ("a", "0"), // Library of Congress Subject Headings (ADULT)
    ("b", "1"), // Library of Congress Subject Headings (JUVENILE)
    ("c", "2"), // Medical Subject Headings
    ("d", "3"), // National Agricultural Library Subject Authority File
    ("n", "4"), // Source not specified
    ("k", "5"), // Canadian Subject Headings
    ("v", "6"), // Répertoire de vedettes-matière
    ("z", "7"), // Source specified in subfield $2 / Other
];

// Produces a new 6XX ind2 value for values found in subfield $2 when the
// original ind2 value is 7 ("Source specified in subfield $2").
const REMAP_BIB_SF2_TO_IND2: &[(&str, &str)] =
    &[("lcsh", "0"), ("mesh", "2"), ("nal", "3"), ("rvm", "6")];

/// Controlled bib field + subfield along with the authority
/// field that controls it.
#[derive(Debug)]
struct ControlledField {
    bib_tag: String,
    auth_tag: String,
    subfield: String,
}

#[derive(Debug, Clone)]
struct AuthLeader {
    auth_id: i64,
    value: String,
}

struct BibLinker {
    scripter: script::Runner,
    min_id: i64,
    max_id: Option<i64>,
    bibs_mod_since: Option<date::EgDate>,
    auths_mod_since: Option<date::EgDate>,
    record_id: Option<i64>,
    parallel: usize,
}

impl BibLinker {
    /// Create a new linker.
    ///
    /// Exits early with None if the --help option is provided.
    fn new(scripter: script::Runner) -> EgResult<Self> {
        let min_id = match scripter.params().opt_str("min-id") {
            Some(id) => id
                .parse::<i64>()
                .map_err(|e| format!("Error parsing --min-id: {e}"))?,
            None => 1,
        };

        let max_id = match scripter.params().opt_str("max-id") {
            Some(id) => Some(
                id.parse::<i64>()
                    .map_err(|e| format!("Error parsing --max-id: {e}"))?,
            ),
            None => None,
        };

        let bibs_mod_since = match scripter.params().opt_str("bibs-modified-since") {
            // verify the date string before we send it to the database.
            Some(ref date_str) => Some(date::parse_datetime(date_str)?),
            None => None,
        };

        let auths_mod_since = match scripter.params().opt_str("auths-modified-since") {
            // verify the date string before we send it to the database.
            Some(ref date_str) => Some(date::parse_datetime(date_str)?),
            None => None,
        };

        let record_id = match scripter.params().opt_str("record-id") {
            Some(id) => Some(
                id.parse::<i64>()
                    .map_err(|e| format!("Error parsing --record-id: {e}"))?,
            ),
            None => None,
        };

        let parallel = match scripter.params().opt_str("parallel") {
            Some(p) => p
                .parse::<usize>()
                .map_err(|e| format!("error parsing value for --parallel: {e}"))?,
            None => 1,
        };

        Ok(BibLinker {
            min_id,
            max_id,
            record_id,
            bibs_mod_since,
            auths_mod_since,
            scripter,
            parallel,
        })
    }

    /// Returns the list of bib record IDs we plan to process.
    fn get_bib_ids(&mut self) -> EgResult<Vec<i64>> {
        if let Some(id) = self.record_id {
            return Ok(vec![id]);
        }

        let select = "SELECT bre.id";
        let from = "FROM biblio.record_entry bre";

        let mut where_ = format!("WHERE NOT bre.deleted AND bre.id >= {}", self.min_id);

        if let Some(end) = self.max_id {
            where_ += &format!(" AND bre.id <= {end}");
        }

        if let Some(dt) = self.bibs_mod_since.as_ref() {
            where_ += &format!(" AND bre.edit_date >= '{}'", date::to_iso(dt));
        }

        if let Some(dt) = self.auths_mod_since.as_ref() {
            // Bib records that share a browse entry with an authority
            // record which has been modified since the provided date
            // and is not already linked to the authority record.

            where_ += &format!(
                "
                AND bre.id IN (
                    SELECT def.source
                    FROM metabib.browse_entry entry
                    JOIN metabib.browse_entry_simple_heading_map map ON map.entry = entry.id
                    JOIN authority.simple_heading ash ON (ash.id = map.simple_heading)
                    JOIN authority.record_entry are ON (are.id = ash.record)
                    JOIN metabib.browse_entry_def_map def ON (def.entry = entry.id)
                    JOIN biblio.record_entry bre ON bre.id = def.source
                    LEFT JOIN authority.bib_linking link ON (
                        link.bib = def.source AND link.authority = ash.record)
                    WHERE
                        NOT bre.deleted
                        AND link.authority IS NULL -- unlinked records
                        AND are.edit_date >= '{}'
                )",
                date::to_iso(dt)
            );
        }

        let order = "ORDER BY id";

        let sql = format!("{select} {from} {where_} {order}");

        log::info!("Searching for bib records to link: {sql}");

        // println!("{sql}");

        let query_res = self.scripter.db().client().query(&sql[..], &[]);

        let rows = query_res.map_err(|e| format!("Failed getting bib IDs: {e}"))?;

        let mut list: Vec<i64> = Vec::new();
        for row in rows {
            let id: Option<i64> = row.get("id");
            list.push(id.unwrap());
        }

        Ok(list)
    }

    /// Collect the list of controlled fields from the database.
    ///
    /// # Panics
    ///
    /// If called more than once.
    fn load_controlled_fields(&mut self) -> EgResult<()> {
        let search = eg::hash! {"id": {"<>": EgValue::Null}};

        let flesh = eg::hash! {
            "flesh": 1,
            "flesh_fields": eg::hash!{
                "acsbf": vec!["authority_field"]
            }
        };

        let bib_fields = self
            .scripter
            .editor_mut()
            .search_with_ops("acsbf", search, flesh)?;

        let linkable_tag_prefixes = ["1", "6", "7", "8"];

        // Skip these for non-6XX fields
        let scrub_subfields1 = ["v", "x", "y", "z"];

        // Skip these for scrub_tags2 fields
        let scrub_subfields2 = ["m", "o", "r", "s"];
        let scrub_tags2 = ["130", "600", "610", "630", "700", "710", "730", "830"];

        let mut controlled_fields: Vec<ControlledField> = Vec::new();

        for bib_field in bib_fields {
            let bib_tag = bib_field["tag"].str()?;

            if !linkable_tag_prefixes.contains(&&bib_tag[..1]) {
                continue;
            }

            let authority_field = &bib_field["authority_field"];

            let auth_tag = authority_field["tag"].str()?;

            // Ignore authority 18X fields
            if auth_tag[..2].eq("18") {
                continue;
            }

            let sf_string = authority_field["sf_list"].str()?;
            let mut subfields: Vec<String> = Vec::new();

            for sf in sf_string.split("").filter(|s| !s.is_empty()) {
                if bib_tag[..1].ne("6") && scrub_subfields1.contains(&sf) {
                    continue;
                }

                if !scrub_tags2.contains(&bib_tag) && scrub_subfields2.contains(&sf) {
                    continue;
                }

                subfields.push(sf.to_string());
            }

            for sf in subfields {
                controlled_fields.push(ControlledField {
                    bib_tag: bib_tag.to_string(),
                    auth_tag: auth_tag.to_string(),
                    subfield: sf.to_string(),
                });
            }
        }

        CONTROLLED_FIELDS.set(controlled_fields).unwrap();

        Ok(())
    }

    /// Fine bib IDs to link then divy them up among the workers.
    fn link_bibs(&mut self) -> EgResult<()> {
        let bib_ids = self.get_bib_ids()?;
        let mut handles = Vec::new();
        let chunksize = bib_ids.len() / self.parallel + 1;

        for chunk in bib_ids.chunks(chunksize) {
            let chunk = chunk.to_vec();
            let staff_account = self.scripter.staff_account();
            let log_stdout = self.scripter.params().opt_present("log-stdout");

            let handle = thread::spawn(move || {
                // Each thread needs its own opensrf connection / editor.
                let client = eg::Client::connect().expect("should connect to opensrf");
                let editor = eg::Editor::new(&client);

                let mut worker = Worker {
                    editor,
                    staff_account,
                    log_stdout,
                };

                if let Err(e) = worker.link_batch(chunk) {
                    log::error!("Batch failed to complete: {e}");
                }
            });

            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            if let Err(e) = handle.join() {
                log::error!("Worker join failed with: {e:?}");
            }
        }

        Ok(())
    }
}

/// Processes one batch of bib IDs within its own thread.
struct Worker {
    editor: Editor,
    staff_account: i64,
    log_stdout: bool,
}

impl Worker {
    /// Log record modifications to STDOUT and info logging.
    fn announce(&self, s: &str) {
        if self.log_stdout {
            println!("{} {s}", date::now().format("%F %T%.3f"));
        }
        log::info!("B2AL: {s}");
    }

    /// Returns a ref to our collection of controlled fields
    ///
    /// # Panics
    ///
    /// If load_controlled_fields() is not called first.
    fn get_controlled_fields(&self) -> &'static Vec<ControlledField> {
        CONTROLLED_FIELDS.get().unwrap()
    }

    // Fetch leader/008 values for authority records.  Filter out any whose
    // 008 14 or 15 field are not appropriate for the requested bib tag.
    // https://www.loc.gov/marc/authority/ad008.html
    fn authority_leaders_008_14_15(
        &mut self,
        bib_tag: &str,
        auth_ids: Vec<i64>,
    ) -> EgResult<Vec<AuthLeader>> {
        let mut leaders: Vec<AuthLeader> = Vec::new();

        let params = eg::hash! {tag: "008", record: auth_ids.clone()};
        let maybe_leaders = self.editor.search("afr", params)?;

        // Sort the auth_leaders list to match the order of the original
        // list of auth_ids, since they are prioritized by heading
        // matchy-ness
        for auth_id in auth_ids {
            for leader in maybe_leaders.iter() {
                if leader["record"].int()? == auth_id {
                    leaders.push(AuthLeader {
                        auth_id: leader["record"].int()?,
                        value: leader["value"].string()?,
                    });
                    break;
                }
            }
        }

        let index = match bib_tag {
            t if t[..2].eq("17") => 14, // author/name record
            t if t[..1].eq("6") => 15,  // subject record
            _ => return Ok(leaders),    // no additional filtering needed
        };

        let mut keepers: Vec<AuthLeader> = Vec::new();

        for leader in leaders {
            if &leader.value[index..(index + 1)] == "a" {
                keepers.push(leader);
                continue;
            }

            log::info!(
                "Skipping authority record {} on bib {bib_tag} match; 008/#14|#15 not appropriate",
                leader.auth_id
            );
        }

        Ok(keepers)
    }

    // Given a set of authority record leaders and a controlled bib field,
    // returns the ID of the first authority record in the set that
    // matches the thesaurus spec of the bib record.
    fn find_matching_auth_for_thesaurus(
        &self,
        bib_field: &marc::Field,
        auth_leaders: &Vec<AuthLeader>,
    ) -> EgResult<Option<i64>> {
        let mut bib_ind2 = bib_field.ind2();
        let mut is_local = false;

        if bib_ind2 == "7" {
            // subject thesaurus code is embedded in the bib field subfield 2
            is_local = true;

            let thesaurus = match bib_field.get_subfields("2").first() {
                Some(sf) => sf.content(),
                None => "",
            };

            log::debug!("Found local thesaurus value '{thesaurus}'");

            // if we have no special remapping value for the found thesaurus,
            // fall back to ind2 => 7=Other.
            bib_ind2 = match REMAP_BIB_SF2_TO_IND2.iter().find(|(k, _)| k == &thesaurus) {
                Some((_, v)) => v,
                None => "7",
            };

            log::debug!("Local thesaurus '{thesaurus}' remapped to ind2 value '{bib_ind2}'");
        } else if bib_ind2 == "4" {
            is_local = true;
            bib_ind2 = "7";
            log::debug!("Local thesaurus ind2=4 mapped to ind2=7");
        }

        let mut authz_leader: Option<AuthLeader> = None;

        for leader in auth_leaders {
            if leader.value.eq("") || leader.value.len() < 12 {
                continue;
            }

            let thesaurus = &leader.value[11..12];

            if thesaurus == "z" {
                // Note for later that we encountered an authority record
                // whose thesaurus values is z=Other.
                authz_leader = Some(leader.clone());
            }

            if let Some((_, ind)) = AUTH_TO_BIB_IND2.iter().find(|(t, _)| t == &thesaurus) {
                if ind == &bib_ind2 {
                    log::debug!(
                        "Found a match on thesaurus '{thesaurus}' for auth {}",
                        leader.auth_id
                    );

                    return Ok(Some(leader.auth_id));
                }
            }
        }

        if is_local {
            if let Some(ldr) = authz_leader {
                return Ok(Some(ldr.auth_id));
            }
        }

        Ok(None)
    }

    // Returns true if the thesaurus controlling the bib field is "fast".
    fn is_fast_heading(&self, bib_field: &marc::Field) -> bool {
        let tag = bib_field.tag();

        // Looking specifically for bib tags matching 65[015]
        if &tag[..2] != "65" {
            return false;
        }

        match &tag[2..3] {
            "0" | "1" | "5" => {} // keep going
            _ => return false,
        }

        if bib_field.ind2() == "7" {
            // Field controlled by "other"
            if let Some(sf) = bib_field.get_subfields("2").first() {
                return sf.content() == "fast";
            }
        }

        false
    }

    fn update_bib_record(
        &mut self,
        mut bre: EgValue,
        orig_record: &marc::Record,
        record: &marc::Record,
    ) -> EgResult<()> {
        let bre_id = bre["id"].int()?;

        if record == orig_record {
            log::debug!("Skipping update of record {bre_id} -- no changes made");
            return Ok(());
        }

        log::debug!("saving changes to record {bre_id}");

        let xml = record.to_xml_string();

        bre["marc"] = xml.into();
        bre["edit_date"] = "now".into();
        bre["editor"] = self.staff_account.into();

        self.editor.xact_begin()?;
        self.editor.update(bre)?;
        self.editor.commit()?;

        Ok(())
    }

    fn find_potential_auth_matches(
        &mut self,
        controlled_fields: &[ControlledField],
        bib_field: &marc::Field,
    ) -> EgResult<Vec<i64>> {
        let bib_tag = bib_field.tag();
        let auth_ids: Vec<i64> = Vec::new();

        let controlled: Vec<&ControlledField> = controlled_fields
            .iter()
            .filter(|cf| cf.bib_tag == bib_tag)
            .collect();

        if controlled.is_empty() {
            return Ok(auth_ids);
        }

        // Assume each bib field is controlled by exactly one authority field.
        let auth_tag = &controlled[0].auth_tag;

        // [ (subfield, value), ... ]
        let mut searches: Vec<(&str, &str)> = Vec::new();

        for bib_sf in bib_field.subfields() {
            if controlled.iter().any(|cf| cf.subfield == bib_sf.code()) {
                searches.push((bib_sf.code(), bib_sf.content()));
            }
        }

        self.find_potential_auth_matches_kcls(auth_tag, &mut searches)
    }

    // KCLS JBAS-1470
    // Find all authority records whose simple_heading is (essentially)
    // a left-anchored substring match of the normalized bib heading.
    // Sort by longest to shortest match.  Include the shorter matches
    // because a longer match may later be discarded, e.g. because it
    // uses a different thesaurus.

    // We don't exactly want a substring match, more like a sub-tag
    // match.  A straight substring match on the heading is both slow
    // (at the DB level) and could result in partial value matches, like
    // 'smith' vs. 'smithsonian', which we don't want.
    fn find_potential_auth_matches_kcls(
        &mut self,
        auth_tag: &str,
        searches: &mut Vec<(&str, &str)>,
    ) -> EgResult<Vec<i64>> {
        let mut auth_ids: Vec<i64> = Vec::new();

        loop {
            let mut heading = auth_tag.to_string();

            for s in searches.iter() {
                // s.0=subfield; s.1=subfield-value
                heading += &format!(" {} {}", s.0, Normalizer::naco_normalize_once(s.1));
            }

            log::debug!("Sub-heading search for: {heading}");

            let search = eg::hash! {
                "simple_heading": EgValue::from(heading),
                "deleted": EgValue::from("f"),
            };

            // TODO idlist searches
            let recs = match self.editor.search("are", search) {
                Ok(r) => r,
                Err(e) => {
                    // Don't let a cstore query failure kill the whole batch.
                    log::error!("Skipping bib field on query failure: {e}");
                    return Ok(vec![]);
                }
            };

            for rec in recs {
                auth_ids.push(rec["id"].int()?);
            }

            searches.pop();

            if searches.is_empty() {
                break;
            }
        }

        Ok(auth_ids)
    }

    fn link_batch(&mut self, batch: Vec<i64>) -> EgResult<()> {
        let mut counter = 0;
        let bib_count = batch.len();

        for rec_id in batch {
            counter += 1;

            log::info!("Processing record [{}/{}] {rec_id}", counter, bib_count);

            let bre = match self.editor.retrieve("bre", rec_id)? {
                Some(r) => r,
                None => {
                    log::warn!("No such bib record: {rec_id}");
                    continue;
                }
            };

            if bre["deleted"].str()? == "t" {
                continue;
            }

            let xml = bre["marc"].str()?;

            let orig_record = match marc::Record::from_xml(xml).next() {
                Some(result) => match result {
                    Ok(rec) => rec,
                    Err(e) => {
                        log::error!("Error parsing XML for record {rec_id}: {e}");
                        continue;
                    }
                },
                None => {
                    log::error!("MARC parsing returned no usable record for {rec_id}");
                    continue;
                }
            };

            let mut record = orig_record.clone();

            if let Err(e) = self.link_one_bib(rec_id, bre, &orig_record, &mut record) {
                log::error!("Error processing bib record {rec_id}: {e}");
                eprintln!("Error processing bib record {rec_id}: {e}");
                self.editor.rollback()?;
            }
        }

        Ok(())
    }

    fn link_one_bib(
        &mut self,
        rec_id: i64,
        bre: EgValue,
        orig_record: &marc::Record,
        record: &mut marc::Record,
    ) -> EgResult<()> {
        log::info!("Processing record {rec_id}");

        let controlled_fields = self.get_controlled_fields();

        let mut seen_bib_tags: HashSet<&str> = HashSet::new();

        for cfield in controlled_fields.iter() {
            if seen_bib_tags.contains(cfield.bib_tag.as_str()) {
                continue;
            }

            seen_bib_tags.insert(&cfield.bib_tag);

            for bib_field in record.get_fields_mut(&cfield.bib_tag) {
                let bib_tag = bib_field.tag().to_string(); // mut borrow

                let is_fast_heading = self.is_fast_heading(bib_field);

                let mut prev_sf0_val = None;

                if let Some(sf0) = bib_field.get_subfields("0").first() {
                    let sf0_val = sf0.content();

                    prev_sf0_val = Some(sf0_val.to_string());

                    if sf0_val.contains(")fst") && is_fast_heading {
                        // Here we have a bib field controlled by a "fast"
                        // thesaurus and a subfield $0 which points to a
                        // "fast" authority record.  All is well.

                        log::debug!(
                            "Ignoring FAST heading on rec={} and tag={} $0={}",
                            rec_id,
                            bib_tag,
                            sf0_val
                        );

                        continue;
                    }

                    // Remove any existing subfield 0 values -- should
                    // only be one of these at the most.
                    bib_field.remove_subfields("0");

                    if is_fast_heading {
                        // This bib field is controlled by a "fast" thesaurus.
                        // We don't control fast headings. Leave the $0 out
                        // (removed above) and move to the next field.
                        log::debug!(
                            "No linking performed on FAST heading field on rec={} and tag={}",
                            rec_id,
                            bib_tag
                        );
                        continue;
                    }
                } else if is_fast_heading {
                    log::debug!(
                        "Skipping FAST heading on bib field {}",
                        bib_field.to_breaker()
                    );
                    continue;
                }

                let mut auth_matches =
                    self.find_potential_auth_matches(controlled_fields, bib_field)?;

                if auth_matches.is_empty() {
                    continue;
                }

                log::debug!(
                    "Found {} potential authority matches for bib {} tag={}",
                    auth_matches.len(),
                    rec_id,
                    bib_tag
                );

                let mut auth_leaders: Vec<AuthLeader> = Vec::new();

                if bib_tag.starts_with('1') || bib_tag.starts_with('6') || bib_tag.starts_with('7')
                {
                    // For 1XX, 6XX, and 7XX bib fields, only link to
                    // authority records whose leader/008 positions 14
                    // and 15 are coded to allow use as a name/author or
                    // subject record, depending.

                    auth_leaders = self.authority_leaders_008_14_15(&bib_tag, auth_matches)?;

                    auth_matches = auth_leaders.iter().map(|l| l.auth_id).collect::<Vec<i64>>();

                    log::debug!("Auth matches trimmed to {auth_matches:?}");
                }

                let mut auth_id = auth_matches.first().copied();

                if bib_tag.eq("650") || bib_tag.eq("651") || bib_tag.eq("655") {
                    // Using the indicator-2 value from the  controlled bib
                    // field, find the first authority in the list of matches
                    // that uses the same thesaurus.  If no such authority
                    // is found, no matching occurs.
                    auth_id = self.find_matching_auth_for_thesaurus(bib_field, &auth_leaders)?;
                }

                if let Some(id) = auth_id {
                    let new_sf0_val = format!("({}){}", DEFAULT_CONTROL_NUMBER_IDENTIFIER, id);

                    if let Some(prev_sf0) = prev_sf0_val.as_ref() {
                        if prev_sf0 != &new_sf0_val {
                            // Replacing $0

                            self.announce(&format!(
                                "[{rec_id}] replacing $0{} with $0{} for {}",
                                prev_sf0,
                                new_sf0_val,
                                bib_field.to_breaker()
                            ));
                        } else {
                            // Retaining existing $0
                            // No changes to save / log.
                        }
                    } else {
                        // Adding a new $0

                        self.announce(&format!(
                            "[{rec_id}] adding $0{new_sf0_val} to {}",
                            bib_field.to_breaker()
                        ));
                    }

                    bib_field.add_subfield("0", &new_sf0_val)?;
                } else if let Some(prev_sf0) = prev_sf0_val {
                    // Removing the $0

                    self.announce(&format!(
                        "[{rec_id}] removing $0{prev_sf0} from {}",
                        bib_field.to_breaker()
                    ));
                }
            } // Each bib field with selected bib tag
        } // Each controlled bib tag

        self.update_bib_record(bre, orig_record, record)
    }
}

fn main() -> EgResult<()> {
    let mut ops = getopts::Options::new();

    ops.optopt("", "min-id", "", "");
    ops.optopt("", "max-id", "", "");
    ops.optopt("", "record-id", "", "");
    ops.optopt("", "parallel", "", "");
    ops.optopt("", "bibs-modified-since", "", "");
    ops.optopt("", "auths-modified-since", "", "");
    ops.optflag("", "log-stdout", "");

    let options = script::Options {
        with_evergreen: true,
        with_database: true,
        help_text: Some(HELP_TEXT.to_string()),
        extra_params: None,
        options: Some(ops),
    };

    let scripter = match script::Runner::init(options)? {
        Some(s) => s,
        None => return Ok(()), // e.g. --help
    };

    Normalizer::init();

    let mut linker = BibLinker::new(scripter)?;
    linker.load_controlled_fields()?;

    linker.link_bibs()?;

    Ok(())
}
