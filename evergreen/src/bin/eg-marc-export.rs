use evergreen::db::DatabaseConnection;
use getopts;
use marcutil::Record;
use std::io::prelude::*;
use std::{env, fs, io};

const XML_COLLECTION_HEADER: &str = r#"<collection xmlns="http://www.loc.gov/MARC21/slim">"#;
const XML_COLLECTION_FOOTER: &str = "</collection>";

struct ExportOptions {
    min_id: i64,
    max_id: i64,
    to_xml: bool,
    newest_first: bool,
    destination: ExportDestination,
    query_file: Option<String>,
}

enum ExportDestination {
    Stdout,
    File(String),
}

fn read_options() -> Option<(ExportOptions, DatabaseConnection)> {
    let args: Vec<String> = env::args().collect();
    let mut opts = getopts::Options::new();

    opts.optopt("", "min-id", "Minimum record ID", "MIN_REC_ID");
    opts.optopt("", "max-id", "Maximum record ID", "MAX_REC_ID");
    opts.optopt("", "out-file", "Output File", "OUTPUT_FILE");
    opts.optopt("", "query-file", "SQL Query File", "QUERY_FILE");

    opts.optflag("", "to-xml", "Export to XML");
    opts.optflag("", "newest-first", "Newest First");
    opts.optflag("h", "help", "Help");

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
            newest_first: params.opt_present("newest-first"),
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

    --min-id
        Only export records whose ID is >= this value.

    --max-id
        Only export records whose ID is <= this value.

    --out-file
        Write data to this file.
        Otherwise, writes to STDOUT.

    --query-file
        Path to a file containing an SQL query.  The query must
        produce rows that have a column named "marc".

    --newest-first
        Export records newest to oldest by create date.
        Otherwise, export oldests to newest.

    --db-host
    --db-port
    --db-user
    --db-name
        Database connection options.  PG environment vars are used
        as defaults when available.

    --help Print help message

    "#
    );
}

fn create_sql(ops: &ExportOptions) -> String {
    if let Some(fname) = &ops.query_file {
        return fs::read_to_string(fname).unwrap();
    }

    let select = "SELECT bre.marc";
    let from = "FROM biblio.record_entry bre";
    let mut filter = String::from("WHERE NOT bre.deleted");

    if ops.min_id > -1 {
        filter = format!("{} AND id >= {}", filter, ops.min_id);
    }

    if ops.max_id > -1 {
        filter = format!("{} AND id < {}", filter, ops.max_id);
    }

    let order_by = match ops.newest_first {
        true => "ORDER BY create_date DESC",
        false => "ORDER BY create_date ASC",
    };

    format!("{select} {from} {filter} {order_by}")
}

fn export(con: &mut DatabaseConnection, ops: &ExportOptions) -> Result<(), String> {
    // Where are we spewing bytes?
    let mut writer: Box<dyn Write> = match &ops.destination {
        ExportDestination::File(fname) => Box::new(fs::File::create(fname).unwrap()),
        _ => Box::new(io::stdout()),
    };

    con.connect()?;

    let query = create_sql(ops);

    if ops.to_xml {
        write(&mut writer, &XML_COLLECTION_HEADER.as_bytes())?;
    }

    for row in con.client().query(&query[..], &[]).unwrap() {
        let marc_xml: &str = row.get("marc");

        if ops.to_xml {
            // No need to parse the record if we going XML to XML.
            write(&mut writer, &marc_xml.as_bytes())?;
            continue;
        }

        if let Some(record) = Record::from_xml(&marc_xml).next() {
            let binary = record.to_binary()?;
            write(&mut writer, &binary)?;
        }
    }

    if ops.to_xml {
        write(&mut writer, &XML_COLLECTION_FOOTER.as_bytes())?;
    }

    con.disconnect();

    Ok(())
}

fn write(writer: &mut Box<dyn Write>, bytes: &[u8]) -> Result<(), String> {
    match writer.write(bytes) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Error writing bytes: {e}")),
    }
}

fn main() -> Result<(), String> {
    if let Some((options, mut connection)) = read_options() {
        export(&mut connection, &options)
    } else {
        Ok(())
    }
}
