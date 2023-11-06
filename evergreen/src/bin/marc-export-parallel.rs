use eg::date;
use eg::db::DatabaseConnection;
use evergreen as eg;
use getopts;
use marc::Record;
use postgres_cursor::Cursor;
use rust_decimal::Decimal;
use std::io::prelude::*;
use std::path::Path;
use std::{env, fs, io};
use std::thread;
use std::sync::mpsc;

const XML_COLLECTION_HEADER: &str = r#"<collection xmlns="http://www.loc.gov/MARC21/slim">"#;
const XML_COLLECTION_FOOTER: &str = "</collection>";
const DEFAULT_BATCH_SIZE: u32 = 1000;
const HOLDINGS_SUBFIELD: &str = "852";

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
    // &("x", "ref"),
    // &("x", "holdable"),
    // &("x", "circulate"),
    // &("x", "opac_visible"),
];

struct Writer {
    write_channel_rx: mpsc::Receiver<(i64, Vec<u8>)>, // id, marcxml
    options: ExportOptions,
}

impl Writer {
    fn run(&mut self) {

        // Where are we spewing bytes?
        let mut writer: Box<dyn Write> = match &self.options.destination {
            ExportDestination::File(fname) => {
                if Path::new(fname).exists() {
                    eprintln!("Output file already exists: {fname}");
                    return;
                }
                Box::new(fs::File::create(fname).unwrap())
            }
            _ => Box::new(io::stdout()),
        };

        if self.options.to_xml {
            self.write(&mut writer, &XML_COLLECTION_HEADER.as_bytes()).expect("Writing");
        }


        loop {
            let (record_id, bytes) = match self.write_channel_rx.recv() {
                Ok((r, b)) => (r, b),
                Err(_) => return, // parent thread exited.
            };

            if record_id == 0 {
                if self.options.verbose {
                    println!("Writer ending on record ID 0");
                    break;
                }
            }

            self.write(&mut writer, &bytes).expect("Writing data");

        }

        if self.options.to_xml {
            if self.options.pretty_print_xml {
                self.write(&mut writer, "\n".as_bytes()).expect("Writing");
            }
            self.write(&mut writer, &XML_COLLECTION_FOOTER.as_bytes()).expect("Writing");
        }

    }

    fn write(&self, writer: &mut Box<dyn Write>, bytes: &[u8]) -> Result<(), String> {
        match writer.write(bytes) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Error writing bytes: {e}")),
        }
    }
}

struct Collector {
    db: DatabaseConnection,
    work_channel_rx: mpsc::Receiver<(i64, String)>, // id, marcxml
    write_channel_tx: mpsc::Sender<(i64, Vec<u8>)>, // id, marcxml
    items_query: Option<String>,
    options: ExportOptions,
}

/// Collects MARC records from the database and optionally other data,
/// turns that set of data into MARC Recors which are streamed
/// back to the main thread.
impl Collector {

    fn run(&mut self) {

        if let Err(e) = self.db.connect() {
            eprintln!("{e}");
            return;
        }

        let items_query = self.items_query.clone();

        loop {

            // Pull a record from work queue to process
            let (record_id, marc_xml) = match self.work_channel_rx.recv() {
                Ok((a, b)) => (a, b),
                Err(e) => {
                    eprintln!("Parent thread exited: {e}");
                    return;
                }
            };

            if record_id == 0 {
                if self.options.verbose {
                    println!("Exiting on record_id 0");
                }
                return;
            }

            let mut record = match Record::from_xml(&marc_xml).next() {
                Some(r) => r,
                None => {
                    eprintln!("No record built from XML: record={record_id} \n{marc_xml}");
                    return;
                }
            };

            if let Some(items_sql) = &items_query {
                if let Err(e) = self.add_items(record_id, &mut record, &items_sql) {
                    eprintln!("Error adding items: {e}");
                    continue;
                }
            }

            let bytes = if self.options.to_xml {

                let options = marc::xml::XmlOptions {
                    formatted: self.options.pretty_print_xml,
                    with_xml_declaration: false,
                };

                let xml = match record.to_xml_ops(options) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("Error creating XML from record: record={record_id} {e}");
                        continue;
                    }
                };

                xml.into_bytes()

            } else {

                let binary = match record.to_binary() {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("Error creating binary from record: record={record_id} {e}");
                        continue;
                    }
                };

                binary
            };

            if let Err(e) = self.write_channel_tx.send((record_id, bytes)) {
                eprintln!("Cannot send data for writing: {e}");
                return;
            }
        }
    }

    /// Append holdings data to this MARC record.
    fn add_items(
        &mut self,
        record_id: i64,
        record: &mut Record,
        items_query: &String,
    ) -> Result<(), String> {
        record.remove_fields(HOLDINGS_SUBFIELD);

        for row in self.db.client().query(&items_query[..], &[&record_id]).unwrap() {
            let mut field = marc::Field::new(HOLDINGS_SUBFIELD)?;
            field.set_ind1("4")?;

            if let Some(lc) = self.options.location_code.as_ref() {
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
                price_binding = format!("{}{}", self.options.currency_symbol, p.to_string());
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

            if self.options.force_ordered_holdings_fields {
                record.insert_field(field);
            } else {
                record.fields_mut().push(field);
            }
        }

        Ok(())
    }
}


#[derive(Clone)]
struct ExportOptions {
    min_id: i64,
    max_id: i64,

    /// Output to XML.  Default is binary MARC21.
    /// All data is UTF-8.
    to_xml: bool,

    /// How many records to pull from the database within each query batch.
    batch_size: u32,

    /// Export items / copies in addition to records.
    export_items: bool,

    /// Insert holdings fields in tag order.
    force_ordered_holdings_fields: bool,

    /// Limit exported items to those that are OPAC visible
    limit_to_visible: bool,

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

    order_by_id: bool,

    query_file: Option<String>,
    verbose: bool,
}

#[derive(Clone, PartialEq)]
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

    opts.optflag("", "order-by-id", "");
    opts.optflag("", "pretty-print-xml", "");
    opts.optflag("", "force-ordered-holdings-fields", "");
    opts.optflag("", "pipe", "");
    opts.optflag("", "limit-to-opac-visible", "");
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

    let batch_size = match params.opt_get_default("batch-size", DEFAULT_BATCH_SIZE) {
        Ok(s) => {
            if s == 0 {
                DEFAULT_BATCH_SIZE
            } else {
                s
            }
        }
        Err(e) => {
            eprintln!("Invalid batch size: {e}");
            return None;
        }
    };

    let options = ExportOptions {
        destination,
        modified_since,
        batch_size,
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
        order_by_id: params.opt_present("order-by-id"),
        force_ordered_holdings_fields: params.opt_present("force-ordered-holdings-fields"),
        export_items: params.opt_present("items"),
        limit_to_visible: params.opt_present("limit-to-opac-visible"),
        verbose: params.opt_present("verbose"),
        to_xml: params.opt_present("to-xml"),
        query_file: params.opt_get("query-file").unwrap(),
    };

    Some((options, connection))
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

    --limit-to-opac-visible
        Limits holdings (copies / items) in the export to those that
        are visible in the OPAC. This option does nothing if the
        --items option is not used.

    --library <shortname>
        Limit to records that have holdings at the specified library
        by shortname.  Repeatable.

    --force-ordered-holdings-fields
        Insert holdings/items fields in tag order.  The default is
        to append the fields to the end of the record, which is
        generally faster.

    --order-by-id
        Sort data (records, etc.) by ID.
        This is useful for comparing output data, but increases
        the overhead of any SQL queries.

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

    let mut sql = format!("{select} {from} {filter}");

    if ops.order_by_id {
        sql += " ORDER BY bre.id";
    }

    sql
}

fn create_items_sql(ops: &ExportOptions) -> String {
    let mut items_query: String = String::from(
        r#"
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
"#,
    );

    if ops.limit_to_visible {
        let opac_vis_ops: &str = r#"
        AND acp.opac_visible
        AND acpl.opac_visible
        AND clib.opac_visible
        AND ccs.opac_visible
"#;
        items_query.push_str(opac_vis_ops);
    };

    // Consistent ordering makes comparing outputs easier.
    if ops.order_by_id {
        items_query += "ORDER BY acp.id";
    }

    items_query
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

    con.connect()?;

    set_library_ids(con, ops)?;
    set_pipe_ids(ops)?;

    let items_query = if ops.export_items {
        Some(create_items_sql(&ops))
    } else {
        None
    };

    let query = create_records_sql(ops);

    if ops.verbose {
        println!("Record batch SQL:\n{query}");
    }

    let (write_tx, write_rx): (
        mpsc::Sender<(i64, Vec<u8>)>,
        mpsc::Receiver<(i64, Vec<u8>)>
    ) = mpsc::channel();

    let mut work_channels = Vec::new();
    let mut handles = Vec::new();
    for _ in 0 .. 2 {
        let (work_tx, work_rx): (
            mpsc::Sender<(i64, String)>,
            mpsc::Receiver<(i64, String)>
        ) = mpsc::channel();

        work_channels.push(work_tx);

        let thr_db = con.clone();
        let thr_write_tx = write_tx.clone();
        let thr_items_query = items_query.clone();
        let thr_options = ops.clone();
        let mut collector = Collector {
            db: thr_db,
            work_channel_rx: work_rx,
            write_channel_tx: thr_write_tx,
            items_query: thr_items_query,
            options: thr_options,
        };

        handles.push(thread::spawn(move || collector.run()));
    }

    // Spawn a write queue handler.
    let thr_options = ops.clone();
    let mut writer = Writer {
        options: thr_options,
        write_channel_rx: write_rx
    };

    let write_handle = thread::spawn(move || writer.run());

    // A cursor is by definition a long running mutable thing.  Create a
    // separate connection so we can leave the cursor open and running
    // while doing other DB stuff with the main connection.
    let mut cursor_con = con.clone();
    cursor_con.connect()?;

    let mut cursor = Cursor::build(cursor_con.client())
        .batch_size(ops.batch_size)
        .query(&query)
        .finalize()
        .expect("Create PG Cursor");

    let mut batch_counter = 0;
    let mut row_counter = 0;
    let mut work_channel_idx = 0;

    for result in &mut cursor {
        let rows = match result {
            Ok(r) => r,
            Err(e) => return Err(format!("Cursor response failed: {e}"))?,
        };

        for row in &rows {
            let marc_xml: &str = row.get("marc");
            let record_id: i64 = row.get("id");

            work_channel_idx = if work_channel_idx == work_channels.len()  - 1 {
                0
            } else {
                work_channel_idx + 1
            };

            if let Err(e) = work_channels[work_channel_idx].send((record_id, marc_xml.to_owned())) {
                eprintln!("Cannot send work to Collector: {e}");
                break;
            }

            row_counter += 1;
        }

        batch_counter += 1;
        if ops.verbose {
            println!("Processed: batches={batch_counter} rows={row_counter}");
        }
    }

    for c in &work_channels {
        // Let the workers know we're done.
        if let Err(e) = c.send((0, String::new())) {
            eprintln!("Cannot write to work channel: {e}");
        }
    }

    if ops.verbose {
        println!("Waiting for worker threads to finish...");
    }

    for h in handles.drain(..) {
        h.join().ok();
    }

    write_tx.send((0, Vec::new())).ok();

    write_handle.join().ok();

    Ok(())
}

fn check_options(ops: &ExportOptions) -> Result<(), String> {
    if ops.verbose && ops.destination == ExportDestination::Stdout {
        return Err(format!(
            "--verbose is not compatible with exporting to STDOUT"
        ));
    }

    if ops.limit_to_visible && !ops.export_items {
        eprintln!("--limit-to-opac-visible does nothing without the --items option");
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
