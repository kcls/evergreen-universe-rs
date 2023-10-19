use eg::date;
use eg::db::DatabaseConnection;
use evergreen as eg;
use getopts;
use marc::Record;
use rust_decimal::Decimal;
use std::io::prelude::*;
use std::path::Path;
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

struct ExportOptions {
    min_id: i64,
    max_id: i64,

    /// Output to XML.  Default is binary MARC21.
    /// All data is UTF-8.
    to_xml: bool,

    /// How many records to pull from the database within each query batch.
    batch_size: u64,

    /// Export items / copies in addition to records.
    export_items: bool,

    currency_symbol: String,

    /// List of org unit shortnames
    libraries: Vec<String>,

    /// Comma-separated list of org unit IDs
    library_ids: Option<String>,

    /// MARC holdings location code.
    location_code: Option<String>,

    /// Where to write the exported records
    destination: ExportDestination,

    /// Parsed ISO date string
    modified_since: Option<String>,

    pretty_print_xml: bool,

    /// Load bib record IDs via pipe / stdin.
    pipe: bool,

    /// Comma-separated list of bib record IDs
    record_ids: Option<String>,

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
    opts.optopt("", "currency-symbol", "", "");
    opts.optopt("", "modified-since", "", "");

    opts.optmulti("", "library", "", "");

    opts.optflag("", "pretty-print-xml", "");
    opts.optflag("", "pipe", "");
    opts.optflag("", "items", "");
    opts.optflag("", "to-xml", "");
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

    let mut modified_since = None;
    if let Some(mod_since) = params.opt_str("modified-since") {
        match date::parse_datetime(&mod_since) {
            Ok(d) => modified_since = Some(date::to_iso(&d)),
            Err(e) => {
                eprintln!("Invalid modified-since value: {e}");
                return None;
            }
        }
    }

    Some((
        ExportOptions {
            destination,
            modified_since,
            pipe: params.opt_present("pipe"),
            record_ids: None,
            pretty_print_xml: params.opt_present("pretty-print-xml"),
            min_id: params.opt_get_default("min-id", -1).unwrap(),
            max_id: params.opt_get_default("max-id", -1).unwrap(),
            location_code: params.opt_str("location-code"),
            libraries: params.opt_strs("library"),
            library_ids: None,
            currency_symbol: params
                .opt_get_default("currency-symbol", "$".to_string())
                .unwrap(),
            batch_size: params
                .opt_get_default("batch-size", DEFAULT_BATCH_SIZE)
                .unwrap(),
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

    # Export all bib records as XML
    eg-marc-export --to-xml --out-file /tmp/records.xml

    # Export bib records that have holdings at BR1 including holdings.
    eg-marc-export --to-xml --out-file /tmp/records.xml --items --library BR1

Options

    --min-id <record-id>
        Only export records whose ID is >= this value.

    --max-id <record-id>
        Only export records whose ID is <= this value.

    --batch-size
        Number of records to pull from the database per batch.  Batching
        the records means not having to load every record into memory up
        front before output writing can begin.

    --out-file
        Write data to this file.  Otherwise, writes to STDOUT.

    --query-file
        Path to a file containing an SQL query.  The query must produce
        rows that have a columns named "id" and "marc".

    --items
        Includes holdings (copies / items) in the export.  Items are
        added as MARC 852 fields.

    --library <shortname>
        Limit to records that have holdings at the specified library
        by shortname.  Repeatable.

    --modified-since <ISO date>
        Export record modified on or after the provided date(time).
        E.g. --modified-since 2023-10-12
        E.g. --modified-since 2023-10-12T11:29:03-0400

    --currency-symbol <symbol>
        Money values (e.g. copy price) are preceded by this symbol.
        Defaults to $.

    --db-host <host>
    --db-port <port>
    --db-user <user>
    --db-name <database>
        Database connection options.  PG environment vars are used as
        defaults when available.

    --verbose
        Print debug info to STDOUT.  This is not compatible with
        printing record data to STDOUT.

    --help Print help message

    "#
    );
}

fn create_records_sql(ops: &ExportOptions) -> String {
    if let Some(fname) = &ops.query_file {
        return fs::read_to_string(fname).unwrap();
    }

    let select = "SELECT DISTINCT bre.id, bre.marc";
    let mut from = "FROM biblio.record_entry bre".to_string();

    // Also check for presence of at least one copy and/or URI?
    if let Some(ids) = ops.library_ids.as_ref() {
        from += &format!(
            r#"
            JOIN asset.call_number acn ON (
                acn.record = bre.id
                AND acn.owning_lib IN ({ids})
                AND NOT acn.deleted
            )
        "#
        );
    }

    let mut filter = String::from("WHERE NOT bre.deleted");

    if ops.min_id > -1 {
        filter += &format!(" AND bre.id >= {}", ops.min_id);
    }

    if ops.max_id > -1 {
        filter += &format!(" AND bre.id < {}", ops.max_id);
    }

    if let Some(record_ids) = ops.record_ids.as_ref() {
        filter += &format!(" AND bre.id in ({record_ids})");
    }

    if let Some(since) = ops.modified_since.as_ref() {
        // edit_date is set at create time, so there's no
        // need to additionally check create_date.
        filter += &format!(" AND bre.edit_date >= '{since}'");
    }

    // We have to order by something to support paging.
    let order_by = "ORDER BY bre.id";

    // OFFSET is set in the main query loop.
    format!(
        "{select} {from} {filter} {order_by} LIMIT {}",
        ops.batch_size
    )
}

/// Read record IDs, one per line, from STDIN for use in
/// the main record query.
fn set_pipe_ids(ops: &mut ExportOptions) -> Result<(), String> {
    if !ops.pipe {
        return Ok(());
    }

    let mut ids = String::new();
    let mut line = String::new();

    let stdin = io::stdin();

    loop {
        line.clear();
        match stdin.read_line(&mut line) {
            Ok(count) => {
                if count == 0 {
                    break; // EOF
                }

                // Make sure the ID values provided are numeric before
                // we trust them.  Silently ignore any other data.
                if let Ok(id) = line.trim().parse::<i64>() {
                    ids += &format!("{id},");
                }
            }
            Err(e) => return Err(format!("Error reading stdin: {e}")),
        }
    }

    ids.pop(); // remove trailing ","
    if ids.len() > 0 {
        ops.record_ids = Some(ids);
    }

    Ok(())
}

/// Translate library filter shortnames into org unit IDs
fn set_library_ids(con: &mut DatabaseConnection, ops: &mut ExportOptions) -> Result<(), String> {
    if ops.libraries.len() == 0 {
        return Ok(());
    }

    let mut ids = String::new();
    let query = "select id from actor.org_unit where shortname=any($1::text[])";

    for row in con.client().query(&query[..], &[&ops.libraries]).unwrap() {
        ids += &format!("{},", row.get::<&str, i32>("id"));
    }

    ids.pop(); // trailing ","
    ops.library_ids = Some(ids);

    Ok(())
}

fn export(con: &mut DatabaseConnection, ops: &mut ExportOptions) -> Result<(), String> {
    // Where are we spewing bytes?
    let mut writer: Box<dyn Write> = match &ops.destination {
        ExportDestination::File(fname) => {
            if Path::new(fname).exists() {
                return Err(format!("Output file already exists: {fname}"));
            }
            Box::new(fs::File::create(fname).unwrap())
        }
        _ => Box::new(io::stdout()),
    };

    con.connect()?;

    set_library_ids(con, ops)?;
    set_pipe_ids(ops)?;

    if ops.to_xml {
        write(&mut writer, &XML_COLLECTION_HEADER.as_bytes())?;
    }

    let mut offset = 0;
    loop {
        let mut query = create_records_sql(ops);

        if ops.query_file.is_none() {
            // Internally built SQL is paged
            query += &format!(" OFFSET {offset}");
        }

        if ops.verbose {
            println!("Record batch SQL:\n{query}");
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
                    formatted: ops.pretty_print_xml,
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

        if !some_found || ops.query_file.is_some() {
            // All batches processed.
            // Query files are processed in one big batch.
            break;
        }

        offset += ops.batch_size;
    }

    if ops.to_xml {
        if ops.pretty_print_xml {
            write(&mut writer, "\n".as_bytes())?;
        }
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
        let mut field = marc::Field::new(HOLDINGS_SUBFIELD)?;
        field.set_ind1("4")?;

        if let Some(lc) = ops.location_code.as_ref() {
            field.add_subfield("a", lc)?;
        }

        for (subfield, fname) in ITEM_SUBFIELD_MAP {
            if let Ok(value) = row.try_get::<&str, &str>(fname) {
                if value != "" {
                    field.add_subfield(*subfield, value)?;
                }
            }
        }

        // PG 'numeric' types require a Decimal destination.
        let price: Option<Decimal> = row.get("price");
        let price_binding;
        if let Some(p) = price {
            price_binding = format!("{}{}", ops.currency_symbol, p.to_string());
            field.add_subfield("y", price_binding.as_str())?;
        }

        // These bools are all required fields. try_get() not required.

        if row.get::<&str, bool>("ref") {
            field.add_subfield("x", "reference")?;
        }

        if !row.get::<&str, bool>("holdable") {
            field.add_subfield("x", "unholdable")?;
        }

        if !row.get::<&str, bool>("circulate") {
            field.add_subfield("x", "noncirculating")?;
        }

        if !row.get::<&str, bool>("opac_visible") {
            field.add_subfield("x", "hidden")?;
        }

        record.insert_field(field);
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
    if let Some((mut options, mut connection)) = read_options() {
        check_options(&options)?;
        export(&mut connection, &mut options)
    } else {
        Ok(())
    }
}
