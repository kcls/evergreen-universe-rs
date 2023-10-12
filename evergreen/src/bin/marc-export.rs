use evergreen::db::DatabaseConnection;
use getopts;
use marc::Record;
use rust_decimal::Decimal;
use std::io::prelude::*;
use std::{env, fs, io};

const XML_COLLECTION_HEADER: &str = r#"<collection xmlns="http://www.loc.gov/MARC21/slim">"#;
const XML_COLLECTION_FOOTER: &str = "</collection>";
const DEFAULT_BATCH_SIZE: u64 = 1000;
const HOLDINGS_SUBFIELD: &str = "852";

const ITEMS_QUERY: &str = r#"
    SELECT
        olib.shortname as owning_lib,
        clib.shortname as circ_lib,
        acpl.name as acpl_name,
        acnp.label as call_number_prefix,
        acn.label as call_number,
        acns.label as call_number_suffix,
        acp.circ_modifier,
        acp.barcode,
        ccs.name as status,
        acp.copy_number,
        acp.price,
        acp.ref,
        acp.holdable,
        acp.circulate,
        acp.opac_visible
    FROM
        asset.copy acp
        JOIN config.copy_status ccs ON ccs.id = acp.status
        JOIN asset.copy_location acpl ON acpl.id = acp.location
        JOIN asset.call_number acn ON acn.id = acp.call_number
        JOIN asset.call_number_prefix acnp ON acnp.id = acn.prefix
        JOIN asset.call_number_suffix acns ON acns.id = acn.suffix
        JOIN actor.org_unit olib ON olib.id = acn.owning_lib
        JOIN actor.org_unit clib ON clib.id = acp.circ_lib
    WHERE
        NOT acp.deleted
        AND NOT acn.deleted
        AND acn.record = $1
"#;

/// Map MARC subfields to SQL row field names.
/// Some are handled manually but left here for documentation.
const ITEM_SUBFIELD_MAP: &[&(&str, &str)] = &[
    &("b", "owning_lib"),
    &("b", "circ_lib"),
    &("b", "acpl_name"),
    &("k", "call_number_prefix"),
    &("j", "call_number"),
    &("m", "call_number_suffix"),
    &("g", "circ_modifier"),
    &("p", "barcode"),
    &("s", "status"),
    // &("y", "price"),
    &("t", "copy_number"),
    // Handled separately
    // &("x", "ref"),
    // &("x", "holdable"),
    // &("x", "circulate"),
    // &("x", "opac_visible"),
];

// TODO holdings location code
struct ExportOptions {
    min_id: i64,
    max_id: i64,
    to_xml: bool,
    newest_first: bool,
    batch_size: u64,
    export_items: bool,
    money: String,
    location_code: Option<String>,
    destination: ExportDestination,
    query_file: Option<String>,
    verbose: bool,
}

#[derive(PartialEq)]
enum ExportDestination {
    Stdout,
    File(String),
}

fn read_options() -> Option<(ExportOptions, DatabaseConnection)> {
    let args: Vec<String> = env::args().collect();
    let mut opts = getopts::Options::new();

    opts.optopt("", "min-id", "", "");
    opts.optopt("", "max-id", "", "");
    opts.optopt("", "out-file", "", "");
    opts.optopt("", "query-file", "", "");
    opts.optopt("", "batch-size", "", "");
    opts.optopt("", "location-code", "", "");
    opts.optopt("", "money", "", "");

    opts.optflag("", "items", "");
    opts.optflag("", "to-xml", "");
    opts.optflag("", "newest-first", "");
    opts.optflag("h", "help", "");
    opts.optflag("v", "verbose", "");

    DatabaseConnection::append_options(&mut opts);

    let params = opts.parse(&args[1..]).unwrap();

    if params.opt_present("help") {
        print_help();
        return None;
    }

    let destination = match params.opt_get::<String>("out-file").unwrap() {
        Some(filename) => ExportDestination::File(filename),
        None => ExportDestination::Stdout,
    };

    let connection = DatabaseConnection::new_from_options(&params);

    Some((
        ExportOptions {
            destination,
            min_id: params.opt_get_default("min-id", -1).unwrap(),
            max_id: params.opt_get_default("max-id", -1).unwrap(),
            location_code: params.opt_str("location-code"),
            money: params.opt_get_default("money", "$".to_string()).unwrap(),
            batch_size: params
                .opt_get_default("batch-size", DEFAULT_BATCH_SIZE)
                .unwrap(),
            newest_first: params.opt_present("newest-first"),
            export_items: params.opt_present("items"),
            verbose: params.opt_present("verbose"),
            to_xml: params.opt_present("to-xml"),
            query_file: params.opt_get("query-file").unwrap(),
        },
        connection,
    ))
}

fn print_help() {
    println!(
        r#"

Synopsis

    cargo run -- --out-file /tmp/records.mrc

Options

    --min-id <record-id>
        Only export records whose ID is >= this value.

    --max-id <record-id>
        Only export records whose ID is <= this value.

    --batch-size
        Number of records to pull from the database per batch.
        Batching the records means not having to load every record
        into memory at once.

    --out-file
        Write data to this file.
        Otherwise, writes to STDOUT.

    --query-file
        Path to a file containing an SQL query.  The query must
        produce rows that have a column named "marc".

    --newest-first
        Export records newest to oldest by create date.
        Otherwise, export oldests to newest.

    --items
        Includes holdings (copies / items) in the export.  Items are
        added as MARC 852 fields.

    --money <symbol>
        Copy price is preceded by this currency symbol.
        Defaults to $.

    --db-host <host>
    --db-port <port>
    --db-user <user>
    --db-name <database>
        Database connection options.  PG environment vars are used
        as defaults when available.

    --verbose
        Print debug info to STDOUT.  This is not compatible with
        printing record data to STDOUT.

    --help Print help message

    "#
    );
}

fn create_sql(ops: &ExportOptions) -> String {
    if let Some(fname) = &ops.query_file {
        return fs::read_to_string(fname).unwrap();
    }

    let select = "SELECT bre.id, bre.marc";
    let from = "FROM biblio.record_entry bre";
    let mut filter = String::from("WHERE NOT bre.deleted");

    if ops.min_id > -1 {
        filter = format!("{} AND id >= {}", filter, ops.min_id);
    }

    if ops.max_id > -1 {
        filter = format!("{} AND id < {}", filter, ops.max_id);
    }

    let order_by = match ops.newest_first {
        true => "ORDER BY create_date DESC, id DESC",
        false => "ORDER BY create_date ASC, id",
    };

    // OFFSET is set in the main query loop.
    format!(
        "{select} {from} {filter} {order_by} LIMIT {}",
        ops.batch_size
    )
}

fn export(con: &mut DatabaseConnection, ops: &ExportOptions) -> Result<(), String> {
    // Where are we spewing bytes?
    let mut writer: Box<dyn Write> = match &ops.destination {
        ExportDestination::File(fname) => Box::new(fs::File::create(fname).unwrap()),
        _ => Box::new(io::stdout()),
    };

    con.connect()?;

    if ops.to_xml {
        write(&mut writer, &XML_COLLECTION_HEADER.as_bytes())?;
    }

    let mut offset = 0;
    loop {
        let mut query = create_sql(ops);

        query += &format!(" OFFSET {offset}");

        if ops.verbose {
            println!("Record batch SQL: {query}");
        }

        let mut some_found = false;
        for row in con.client().query(&query[..], &[]).unwrap() {
            some_found = true;

            let marc_xml: &str = row.get("marc");

            let mut record = match Record::from_xml(&marc_xml).next() {
                Some(r) => r,
                None => {
                    eprintln!("No record built from XML: \n{marc_xml}");
                    continue;
                }
            };

            if ops.export_items {
                let record_id: i64 = row.get("id");
                add_items(record_id, con, ops, &mut record)?;
            }

            if ops.to_xml {
                let options = marc::xml::XmlOptions {
                    formatted: false,
                    with_xml_declaration: false,
                };

                let xml = match record.to_xml_ops(options) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("Error creating XML from record: {e}");
                        continue;
                    }
                };

                write(&mut writer, xml.as_bytes())?;
            } else {
                let binary = match record.to_binary() {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("Error creating binary from record: {e}");
                        continue;
                    }
                };

                write(&mut writer, &binary)?;
            }
        }

        if !some_found {
            // All batches processed.
            break;
        }

        offset += ops.batch_size;
    }

    if ops.to_xml {
        write(&mut writer, &XML_COLLECTION_FOOTER.as_bytes())?;
    }

    con.disconnect();

    Ok(())
}

/// Append holdings data to this MARC record.
fn add_items(
    record_id: i64,
    con: &mut DatabaseConnection,
    ops: &ExportOptions,
    record: &mut Record,
) -> Result<(), String> {
    record.remove_fields(HOLDINGS_SUBFIELD);

    for row in con.client().query(&ITEMS_QUERY[..], &[&record_id]).unwrap() {
        let mut subfields = Vec::new();

        if let Some(lc) = ops.location_code.as_ref() {
            subfields.push("a");
            subfields.push(lc);
        }

        for (subfield, field) in ITEM_SUBFIELD_MAP {
            if let Ok(value) = row.try_get::<&str, &str>(field) {
                if value != "" {
                    subfields.push(*subfield);
                    subfields.push(&value);
                }
            }
        }

        // PG 'numeric' types require a Decimal destination.
        let price: Option<Decimal> = row.get("price");
        let price_binding;
        if let Some(p) = price {
            price_binding = format!("{}{}", ops.money, p.to_string());
            subfields.push("y");
            subfields.push(price_binding.as_str());
        }

        // These bools are all required fields. try_get() not required.

        if row.get::<&str, bool>("ref") {
            subfields.push("x");
            subfields.push("reference");
        }

        if !row.get::<&str, bool>("holdable") {
            subfields.push("x");
            subfields.push("unholdable");
        }

        if !row.get::<&str, bool>("circulate") {
            subfields.push("x");
            subfields.push("noncirculating");
        }

        if !row.get::<&str, bool>("opac_visible") {
            subfields.push("x");
            subfields.push("hidden");
        }

        record.add_data_field(HOLDINGS_SUBFIELD, "4", " ", subfields)?;
    }

    Ok(())
}

fn write(writer: &mut Box<dyn Write>, bytes: &[u8]) -> Result<(), String> {
    match writer.write(bytes) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Error writing bytes: {e}")),
    }
}

fn check_options(ops: &ExportOptions) -> Result<(), String> {
    if ops.verbose && ops.destination == ExportDestination::Stdout {
        return Err(format!(
            "--verbose is not compatible with exporting to STDOUT"
        ));
    }

    Ok(())
}

fn main() -> Result<(), String> {
    if let Some((options, mut connection)) = read_options() {
        check_options(&options)?;
        export(&mut connection, &options)
    } else {
        Ok(())
    }
}
