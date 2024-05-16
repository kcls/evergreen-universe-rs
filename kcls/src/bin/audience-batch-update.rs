use eg::db::DatabaseConnection;
use evergreen as eg;
use getopts::Options;
use postgres as pg;


const TARGET_RECORDS_SQL: &str = r#"
    SELECT bre.id, bre.marc
    FROM biblio.record_entry bre
    JOIN metabib.record_attr_flat mraf ON (mraf.id = bre.id AND mraf.attr = 'audience')
    JOIN metabib.real_full_rec mrfc ON (
        mrfc.record = bre.id
        AND mrfc.tag = $1
        AND mrfc.subfield = $2
    )
    WHERE
        NOT bre.deleted
        AND bre.cataloging_date IS NULL
        AND mrfc.value = $3
        AND mraf.value != $4
"#;

struct AudienceMap {
    tag: &'static str,
    subfield: &'static str,
    call_number: &'static str,
    audience: &'static str,
}

/// Map of MARC tag, subfield, value (call number), and desired audience code
const CALL_NUMBER_AUDIENCE_MAP: [AudienceMap; 8] = [
    AudienceMap { tag: "092", subfield: "a", audience: "a", call_number: "E ON ORDER" },
    AudienceMap { tag: "092", subfield: "a", audience: "c", call_number: "J ON ORDER" },
    AudienceMap { tag: "092", subfield: "a", audience: "c", call_number: "J LP ON ORDER" },
    AudienceMap { tag: "092", subfield: "a", audience: "d", call_number: "Y ON ORDER" },
    AudienceMap { tag: "092", subfield: "a", audience: "d", call_number: "Y LP ON ORDER" },
    AudienceMap { tag: "092", subfield: "a", audience: "e", call_number: "ON ORDER" },
    AudienceMap { tag: "092", subfield: "a", audience: "e", call_number: "LP ON ORDER" },
    AudienceMap { tag: "092", subfield: "a", audience: "e", call_number: "REF ON ORDER" },
];



fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut opts = Options::new();

    DatabaseConnection::append_options(&mut opts);

    let params = opts.parse(&args[1..]).expect("DB Connection Failed");

    let mut connection = DatabaseConnection::new_from_options(&params);

    connection.connect().expect("DB Connection Failed");

    for map in CALL_NUMBER_AUDIENCE_MAP.iter() {
        process_one_batch(&mut connection, map);
    }

    connection.disconnect();
}

fn process_one_batch(db: &mut DatabaseConnection, map: &AudienceMap) {

    let mut params: Vec<&(dyn pg::types::ToSql + Sync)> = Vec::new();
    params.push(&map.tag);
    params.push(&map.subfield);
    params.push(&map.call_number);
    params.push(&map.audience);

    for row in db.client().query(TARGET_RECORDS_SQL, &params).expect("Query Failed") {
        let id: i64 = row.get("id");
        println!("Found record: {id}");
    }
}



