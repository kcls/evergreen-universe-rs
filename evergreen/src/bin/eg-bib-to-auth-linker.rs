/*
 * This script was built from the KCLS authority_control_fields.pl
 * script.  It varies from stock Evergreen.  It should be possible to
 * sync with stock Evergreen with additional command line options.
 */
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use getopts;
use evergreen as eg;
use eg::init;
use eg::norm::Normalizer;
use eg::db::DatabaseConnection;

const DEFAULT_STAFF_ACCOUNT: u32 = 4953211; // utiladmin
const DEFAULT_CONTROL_NUMBER_IDENTIFIER: &str = "DLC";

// mapping of authority leader/11 "Subject heading system/thesaurus"
// to the matching bib record indicator
const AUTH_TO_BIB_IND2: &[(&str, char)] = &[
    ("a", '0'), // Library of Congress Subject Headings (ADULT)
    ("b", '1'), // Library of Congress Subject Headings (JUVENILE)
    ("c", '2'), // Medical Subject Headings
    ("d", '3'), // National Agricultural Library Subject Authority File
    ("n", '4'), // Source not specified
    ("k", '5'), // Canadian Subject Headings
    ("v", '6'), // Répertoire de vedettes-matière
    ("z", '7'), // Source specified in subfield $2 / Other
];

// Produces a new 6XX ind2 value for values found in subfield $2 when the
// original ind2 value is 7 ("Source specified in subfield $2").
const REMAP_BIB_SF2_TO_IND2: &[(&str, char)] = &[
    ("lcsh", '0'),
    ("mesh", '2'),
    ("nal",  '3'),
    ("rvm",  '6'),
];

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
    db: Rc<RefCell<DatabaseConnection>>,
    editor: eg::Editor,
    staff_account: u32,
    start_id: i64,
    end_id: Option<i64>,
    normalizer: Normalizer,
    verbose: bool,
}

impl BibLinker {
    fn new(opts: &mut getopts::Options) -> Result<Self, String> {

        let ctx = init::init_with_options(opts)?;
        let editor = eg::Editor::new(ctx.client(), ctx.idl());

        let params = ctx.params();

        let mut db = DatabaseConnection::new_from_options(params);
        db.connect()?;

        let db = db.into_shared();

        let sa = DEFAULT_STAFF_ACCOUNT.to_string();
        let staff_account = params.opt_get_default("staff-account", sa).unwrap();
        let staff_account = match staff_account.parse::<u32>() {
            Ok(id) => id,
            Err(e) => Err(format!("Error parsing staff-account value: {e}"))?,
        };

        let start_id = match params.opt_str("start-id") {
            Some(id) => match id.parse::<i64>() {
                Ok(i) => i,
                Err(e) => Err(format!("Error parsing --start-id: {e}"))?,
            },
            None => 1,
        };

        let end_id = match params.opt_str("end-id") {
            Some(id) => match id.parse::<i64>() {
                Ok(i) => Some(i),
                Err(e) => Err(format!("Error parsing --end-id: {e}"))?,
            },
            None => None
        };

        let verbose = params.opt_present("verbose");

        Ok(BibLinker {
            db,
            editor,
            staff_account,
            start_id,
            end_id,
            verbose,
            normalizer: Normalizer::new(),
        })
    }

    fn db(&self) -> &Rc<RefCell<DatabaseConnection>> {
        &self.db
    }

    /// Returns the list of bib record IDs we plan to process.
    fn get_bib_ids(&self) -> Result<Vec<i64>, String> {

        let select = "SELECT id";
        let from = "FROM biblio.record_entry";

        let mut where_ = format!("WHERE NOT deleted AND id >= {}", self.start_id);
        if let Some(end) = self.end_id {
            where_ += &format!(" AND id < {end}");
        }

        let order = "ORDER BY id";

        let sql = format!("{select} {from} {where_} {order}");

        let query_res = self.db().borrow_mut().client().query(&sql[..], &[]);

        let rows = match query_res {
            Ok(rows) => rows,
            Err(e) => Err(format!("Failed getting bib IDs: {e}"))?,
        };

        let mut list: Vec<i64> = Vec::new();
        for row in rows {
            let id: Option<i64> = row.get("id");
            list.push(id.unwrap());
        }

        Ok(list)
    }

    /// Collect the list of controlled fields from the database.
    fn get_controlled_fields(&mut self) -> Result<Vec<ControlledField>, String> {

        let search = json::object! {"id": {"<>": json::JsonValue::Null}};

        let flesh = json::object! {
            flesh: 1,
            flesh_fields: json::object!{
                acsbf: vec!["authority_field"]
            }
        };

        let bib_fields = self.editor.search_with_ops("acsbf", search, flesh)?;

        let linkable_tag_prefixes = vec!["1", "6", "7", "8"];

        // Skip these for non-6XX fields
        let scrub_subfields1 = ["v", "x", "y", "z"];

        // Skip these for scrub_tags2 fields
        let scrub_subfields2 = ["m", "o", "r", "s"];
        let scrub_tags2 = ["130", "600", "610", "630", "700", "710", "730", "830"];

        let mut controlled_fields: Vec<ControlledField> = Vec::new();

        for bib_field in bib_fields {
            let bib_tag = bib_field["tag"].as_str().unwrap();

            if !linkable_tag_prefixes.contains(&&bib_tag[..1]) {
                continue;
            }

            let authority_field = &bib_field["authority_field"];

            let auth_tag = authority_field["tag"].as_str().unwrap();

            // Ignore authority 18X fields
            if auth_tag[..2].eq("18") {
                continue;
            }

            let sf_string = authority_field["sf_list"].as_str().unwrap();
            let mut subfields: Vec<String> = Vec::new();

            for sf in sf_string.split("") {

                if bib_tag[..1].ne("6") && scrub_subfields1.contains(&sf) {
                    continue;
                }

                if scrub_tags2.contains(&bib_tag) && scrub_subfields2.contains(&sf) {
                    continue;
                }

                subfields.push(sf.to_string());
            }

            for sf in subfields {
                controlled_fields.push(
                    ControlledField {
                        bib_tag: bib_tag.to_string(),
                        auth_tag: auth_tag.to_string(),
                        subfield: sf.to_string()
                    }
                );
            }
        }

        Ok(controlled_fields)
    }

    // Fetch leader/008 values for authority records.  Filter out any whose
    // 008 14 or 15 field are not appropriate for the requested bib tag.
    // https://www.loc.gov/marc/authority/ad008.html
    fn authority_leaders_008_14_15(&mut self,
        bib_tag: &str, auth_ids: Vec<i64>) -> Result<Vec<AuthLeader>, String> {

        let mut leaders: Vec<AuthLeader> = Vec::new();

        let params = json::object!{tag: "008", record: auth_ids.clone()};
        let maybe_leaders = self.editor.search("afr", params)?;

        // Sort the auth_leaders list to match the order of the original
        // list of auth_ids, since they are prioritized by heading
        // matchy-ness
        for auth_id in auth_ids {
            for leader in maybe_leaders.iter() {
                if leader["record"].as_i64().unwrap() == auth_id {
                    leaders.push(AuthLeader {
                        auth_id: leader["record"].as_i64().unwrap(),
                        value: leader["value"].as_str().unwrap().to_string(),
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
        bib_field: &marcutil::Field,
        auth_leaders: &Vec<AuthLeader>
    ) -> Result<Option<i64>, String> {

        let mut bib_ind2 = bib_field.ind2;
        let mut is_local = false;

        if bib_ind2 == '7' {
            // subject thesaurus code is embedded in the bib field subfield 2
            is_local = true;

            let thesaurus = match bib_field.get_subfields("2").get(0) {
                Some(sf) => &sf.content,
                None => "",
            };

            log::debug!("Found local thesaurus value '{thesaurus}'");

            // if we have no special remapping value for the found thesaurus,
            // fall back to ind2 => 7=Other.
            bib_ind2 = match REMAP_BIB_SF2_TO_IND2
                .iter().filter(|(k, _)| k == &thesaurus).next() {
                Some((_, v)) => *v,
                None => '7',
            };

            log::debug!(
                "Local thesaurus '{thesaurus}' remapped to ind2 value '{bib_ind2}'");

        } else if bib_ind2 == '4' {

            is_local = true;
            bib_ind2 = '7';
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

            if let Some((_, ind)) = AUTH_TO_BIB_IND2
                .iter().filter(|(t, _)| t == &thesaurus).next() {
                if ind == &bib_ind2 {
                    log::debug!(
                        "Found a match on thesaurus '{thesaurus}' for auth {}",
                        leader.auth_id
                    );

                    return Ok(Some(leader.auth_id))
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
    fn is_fast_heading(&self, bib_field: &marcutil::Field) -> bool {
        let tag = &bib_field.tag;

        // Looking specifically for bib tags matching 65[015]
        if &tag[..2] != "65" {
            return false;
        }

        match &tag[2..3] {
            "0" | "1" | "5" => {}, // keep going
            _ => return false,
        }

        if bib_field.ind2 == '7' { // Field controlled by "other"
            if let Some(sf) = bib_field.get_subfields("2").get(0) {
                return &sf.content == "fast";
            }
        }

        false
    }

    fn update_bib_record(
        &mut self,
        bre: &mut json::JsonValue,
        record: &marcutil::Record
    ) -> Result<(), String> {

        let xml = record.to_xml()?;
        let bre_id = bre["id"].as_i64().unwrap();

        if bre["marc"].as_str().unwrap() == xml {
            log::debug!("Skipping update of record {bre_id} -- no changes made");
            return Ok(())
        }

        log::info!("Applying updates to bib record {bre_id}");

        bre["marc"] = json::from(xml);
        bre["edit_date"] = json::from("now");
        bre["editor"] = json::from(self.staff_account);

        self.editor.xact_begin()?;
        self.editor.update(&bre)?;
        self.editor.xact_commit()?;

        Ok(())
    }

    fn find_potential_auth_matches(
        &mut self,
        controlled_fields: &Vec<ControlledField>,
        bib_field: &marcutil::Field
    ) -> Result<Vec<i64>, String> {

        let bib_tag = &bib_field.tag;
        let auth_ids: Vec<i64> = Vec::new();

        let controlled: Vec<&ControlledField> =
            controlled_fields.iter().filter(|cf| &cf.bib_tag == bib_tag).collect();

        if controlled.len() == 0 {
            return Ok(auth_ids);
        }

        // Assume each bib field is controlled by exactly one authority field.
        let auth_tag = &controlled[0].auth_tag;

        // [ (subfield, value), ... ]
        let mut searches: Vec<(&str, &str)> = Vec::new();

        for bib_sf in &bib_field.subfields {
            if controlled.iter().filter(|cf| &cf.subfield == &bib_sf.code).next().is_some() {
                searches.push((&bib_sf.code, &bib_sf.content));
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
        searches: &mut Vec<(&str, &str)>
    ) -> Result<Vec<i64>, String> {

        let mut auth_ids: Vec<i64> = Vec::new();

        loop {

            let mut heading = auth_tag.to_string();

            for s in searches.iter() { // s.0=subfield; s.1=subfield-value
                heading += &format!(" {} {}", s.0, self.normalizer.naco_normalize(s.1));
            }

            log::debug!("Sub-heading search for: {heading}");

            let search = json::object! {
                "simple_heading": json::from(heading),
                "deleted": json::from("f"),
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
                auth_ids.push(rec["id"].as_i64().unwrap());
            }

            if searches.pop().is_none() {
                break;
            }
        }

        Ok(auth_ids)
    }

    fn link_bibs(&mut self) -> Result<(), String> {

        self.editor.connect()?;

        let control_fields = self.get_controlled_fields()?;

        let mut counter = 0;
        let bib_ids = self.get_bib_ids()?;
        let bib_count = bib_ids.len();

        for rec_id in bib_ids {
            counter += 1;

            log::info!("Processing record [{}/{}] {rec_id}", counter, bib_count);

            if counter % 100 == 0 {
                // Periodically reconnect so we don't keep a single
                // cstore backend pinned for the duration.  Allows for
                // drones to cycle, reclaim memory, etc.
                self.editor.disconnect()?;
                self.editor.connect()?;
            }

            let mut bre = match self.editor.retrieve("bre", rec_id)? {
                Some(r) => r,
                None => {
                    log::warn!("No such bib record: {rec_id}");
                    continue;
                }
            };

            if bre["deleted"].as_str().unwrap() == "t" {
                continue;
            }

            let xml = bre["marc"].as_str().unwrap();

            let mut record = match marcutil::Record::from_xml(xml).next() {
                Some(r) => r,
                None => {
                    log::error!("MARC parsing returned no usable record for {rec_id}");
                    continue;
                }
            };

            if let Err(e) = self.link_one_bib(rec_id, &mut bre, &control_fields, &mut record) {
                log::error!("Error processing bib record {rec_id}: {e}");
                self.editor.disconnect()?;
                self.editor.connect()?;
            }
        }

        Ok(())
    }

    fn link_one_bib(
        &mut self,
        rec_id: i64,
        bre: &mut json::JsonValue,
        control_fields: &Vec<ControlledField>,
        record: &mut marcutil::Record
    ) -> Result<(), String> {

        log::info!("Processing record {rec_id}");

        let mut bib_modified = false;

        if self.verbose { println!("Processing bib record {rec_id}"); }

        let mut seen_bib_tags: HashMap<&str, bool> = HashMap::new();

        for cfield in control_fields.iter() {

            if seen_bib_tags.contains_key(cfield.bib_tag.as_str()) {
                continue;
            }

            seen_bib_tags.insert(&cfield.bib_tag, true);

            for bib_field in record.get_fields_mut(&cfield.bib_tag) {
                let bib_tag = bib_field.tag.to_string();

                let is_fast_heading = self.is_fast_heading(&bib_field);

                if let Some(sf0) = bib_field.get_subfields("0").first() {
                    let sfcode = sf0.code.to_string();

                    if sfcode.contains(")fst") && is_fast_heading {
                        log::debug!(
                            "Ignoring FAST heading on rec={} and tag={} $0={}",
                            rec_id, bib_tag, sfcode
                        );

                        continue;
                    }

                    // Remove any existing subfield 0 values -- should
                    // only be one of these at the most.
                    bib_field.remove_subfields("0");
                    bib_modified = true;
                }

                let mut auth_matches =
                    self.find_potential_auth_matches(&control_fields, &bib_field)?;

                log::info!("Found {} potential authority matches for bib {} tag={}",
                    auth_matches.len(), rec_id, bib_tag);

                if auth_matches.len() == 0 {
                    continue
                }

                if self.verbose {
                    println!(
                        "Found {} potential authority matches for bib {} tag={}",
                        auth_matches.len(), rec_id, bib_tag
                    );
                }

                let mut auth_leaders: Vec<AuthLeader> = Vec::new();

                if bib_tag.starts_with("1") ||
                   bib_tag.starts_with("6") ||
                   bib_tag.starts_with("7") {
                    // For 1XX, 6XX, and 7XX bib fields, only link to
                    // authority records whose leader/008 positions 14
                    // and 15 are coded to allow use as a name/author or
                    // subject record, depending.

                    auth_leaders =
                        self.authority_leaders_008_14_15(&bib_tag, auth_matches)?;

                    auth_matches = auth_leaders.iter().map(|l| l.auth_id).collect::<Vec<i64>>();

                    if self.verbose {
                        println!("Auth matches trimmed to {auth_matches:?}");
                    }
                }

                let mut auth_id = match auth_matches.get(0) {
                    Some(id) => Some(*id),
                    None => None
                };

                if bib_tag.eq("650") || bib_tag.eq("651") || bib_tag.eq("655") {
                    // Using the indicator-2 value from the  controlled bib
                    // field, find the first authority in the list of matches
                    // that uses the same thesaurus.  If no such authority
                    // is found, no matching occurs.
                    auth_id = self.find_matching_auth_for_thesaurus(&bib_field, &auth_leaders)?;
                }

                // Avoid exiting here just because we have no matchable
                // auth records, because the bib record may have changed
                // above when subfields were removed.  We need to capture
                // those changes.

                if let Some(id) = auth_id {
                    let content = format!("({}){}", DEFAULT_CONTROL_NUMBER_IDENTIFIER, id);
                    bib_field.add_subfield("0", Some(&content)).unwrap(); // known OK.
                    bib_modified = true;
                    log::info!("Found a match on bib={} tag={} auth={}", rec_id, bib_tag, id);
                }
            } // Each bib field with selected bib tag
        } // Each controlled bib tag

        if bib_modified {
            self.update_bib_record(bre, record)
        } else {
            Ok(())
        }
    }
}

fn main() -> Result<(), String> {

    let mut opts = getopts::Options::new();

    opts.optopt("", "staff-account", "Staff Account ID", "STAFF_ACCOUNT_ID");
    opts.optopt("", "start-id", "Start ID", "START_ID");
    opts.optopt("", "end-id", "End ID", "END_ID");
    opts.optflag("", "verbose", "Verbose");

    DatabaseConnection::append_options(&mut opts);

    let mut linker = BibLinker::new(&mut opts)?;
    linker.link_bibs()?;

    Ok(())
}



