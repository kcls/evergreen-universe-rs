//! Tool to apply Target Audience codes (008 22) to on order bib records.
use eg::db::DatabaseConnection;
use evergreen as eg;
use getopts::Options;
use marc::Record;

/// actor.usr ID
const RECORD_EDITOR: i32 = 1;

/// Bib records with a NULL cataloging date, containing the specified call 
/// number, and an audience value that does not match a desired value.
const TARGET_RECORDS_SQL: &str = r#"
    SELECT bre.id, bre.marc
    FROM biblio.record_entry bre
    JOIN metabib.record_attr_flat mraf ON (mraf.id = bre.id AND mraf.attr = 'audience')
    JOIN metabib.identifier_field_entry mife ON (
        mife.source = bre.id
        AND mife.field = 25
    )
    WHERE
        NOT bre.deleted
        AND bre.id > 0
        AND bre.cataloging_date IS NULL
        AND mife.value = $1  -- call number
        AND mraf.value != $2 -- audience
"#;

const UPDATE_BIB_SQL: &str = r#"
    UPDATE biblio.record_entry 
    SET marc = $1, editor = $2, edit_date = NOW() 
    WHERE id = $3
"#;

#[derive(Debug)]
struct AudienceMap {
    audience: &'static str,
    call_number: &'static str,
}

/// Map of MARC tag, subfield, value (call number), and desired audience code
const CALL_NUMBER_AUDIENCE_MAP: [AudienceMap; 8] = [
    AudienceMap {
        audience: "a",
        call_number: "E ON ORDER",
    },
    AudienceMap {
        audience: "c",
        call_number: "J ON ORDER",
    },
    AudienceMap {
        audience: "c",
        call_number: "J LP ON ORDER",
    },
    AudienceMap {
        audience: "d",
        call_number: "Y ON ORDER",
    },
    AudienceMap {
        audience: "d",
        call_number: "Y LP ON ORDER",
    },
    AudienceMap {
        audience: "e",
        call_number: "ON ORDER",
    },
    AudienceMap {
        audience: "e",
        call_number: "LP ON ORDER",
    },
    AudienceMap {
        audience: "e",
        call_number: "REF ON ORDER",
    },
];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut opts = Options::new();

    // See DatabaseConnection for command line options
    DatabaseConnection::append_options(&mut opts);

    let params = opts.parse(&args[1..]).expect("Cannot Parse Options");

    let mut connection = DatabaseConnection::new_from_options(&params);

    connection.connect().expect("Cannot Connect to Database");

    for map in CALL_NUMBER_AUDIENCE_MAP.iter() {
        process_one_batch(&mut connection, map);
    }

    connection.disconnect();
}

fn process_one_batch(db: &mut DatabaseConnection, map: &AudienceMap) {
    println!("Processing: {map:?}");

    let rows = db
        .client()
        .query(TARGET_RECORDS_SQL, &[&map.call_number, &map.audience])
        .expect("Query Failed");

    for row in rows {
        let id: i64 = row.get("id");
        let xml: &str = row.get("marc");

        let mut record = match Record::from_xml(&xml).next() {
            Some(result) => match result {
                Ok(rec) => rec,
                Err(err) => {
                    eprintln!("Error parsing MARC XML for record {id}: {err}");
                    continue;
                }
            },
            None => {
                eprintln!("MARC XML parsed no content for record {id}");
                continue;
            }
        };

        // We're not concerned with 006 values for this script.
        let cf008 = match record
            .control_fields_mut()
            .iter_mut()
            .filter(|cf| cf.tag() == "008")
            .next()
        {
            Some(cf) => cf,
            None => {
                eprintln!("Record {id} has no 008 value?");
                continue;
            }
        };

        let mut content = cf008.content().to_string();

        if content.len() < 23 {
            eprintln!("Record {id} has invalid 008 content: '{content}'");
            continue;
        }

        println!(
            "Updating record {id} ({}) with current audience value '{}'",
            map.call_number,
            &content[22..23]
        );

        // Replace 1 character at index 22.
        content.replace_range(22..23, map.audience);

        cf008.set_content(content);

        let new_xml = record
            .to_xml_ops(marc::xml::XmlOptions {
                formatted: false,
                with_xml_declaration: false,
            })
            .expect("Failed to Generate XML");

        db.xact_begin().expect("Begin Failed");

        if let Err(err) = db
            .client()
            .query(UPDATE_BIB_SQL, &[&new_xml, &RECORD_EDITOR, &id])
        {
            eprintln!("Error updating record {id}: {err}");
            db.xact_rollback().expect("Rollback Failed");
            continue;
        }

        // TODO update the record.
        // db.xact_commit().expect("Commit Failed");
        db.xact_rollback().expect("Rollback Failed");
    }
}
