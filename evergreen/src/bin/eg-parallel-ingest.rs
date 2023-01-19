use evergreen::db::DatabaseConnection;
use getopts::Options;
use log::{debug, error, info};
use opensrf;
use postgres as pg;
use std::fs;
use std::thread;
use threadpool::ThreadPool;

#[derive(Debug, Clone)]
struct IngestOptions {
    max_threads: usize,
    do_browse: bool,
    do_attrs: bool,
    do_search: bool,
    do_facets: bool,
    do_display: bool,
    rebuild_rmsr: bool,
    min_id: usize,
    max_id: usize,
    newest_first: bool,
    batch_size: usize,
    attrs: Vec<String>,
    sql_file: Option<String>,
}

/// Read command line options and setup our database connection.
fn init() -> Option<(IngestOptions, DatabaseConnection)> {
    let mut opts = Options::new();

    opts.optopt("", "sql-file", "SQL Query File", "QUERY_FILE");

    opts.optopt("", "max-threads", "Max Worker Threads", "MAX_THREADS");
    opts.optopt(
        "",
        "batch-size",
        "Number of Records to Process per Batch",
        "BATCH_SIZE",
    );
    opts.optopt("", "min-id", "Minimum Record ID", "MIN_REC_ID");
    opts.optopt("", "max-id", "Maximum Record ID", "MAX_REC_ID");
    opts.optmulti(
        "",
        "attr",
        "Reingest Specific Attribute, Repetable",
        "RECORD_ATTR",
    );

    opts.optflag("h", "help", "Show Help Text");
    opts.optflag("", "do-browse", "Update Browse");
    opts.optflag("", "do-attrs", "Update Record Attributes");
    opts.optflag("", "do-search", "Update Search Indexes");
    opts.optflag("", "do-facets", "Update Facets");
    opts.optflag("", "do-display", "Update Display Fields");
    opts.optflag("", "newest-first", "Update Records Newest to Oldest");
    opts.optflag("", "rebuild-rmsr", "Rebuild Reporter Simple Record");

    DatabaseConnection::append_options(&mut opts);

    // We don't need a Client or IDL, so use the OpenSRF init directly.
    let (_, params) = opensrf::init::init_with_options(&mut opts).unwrap();

    if params.opt_present("help") {
        println!("{}", opts.usage("Usage: "));
        return None;
    }

    let ingest_ops = IngestOptions {
        max_threads: params.opt_get_default("max-threads", 5).unwrap(),
        do_browse: params.opt_present("do-browse"),
        do_attrs: params.opt_present("do-attrs"),
        do_search: params.opt_present("do-search"),
        do_facets: params.opt_present("do-facets"),
        do_display: params.opt_present("do-display"),
        min_id: params.opt_get_default("min-id", 0).unwrap(),
        max_id: params.opt_get_default("max-id", 0).unwrap(),
        newest_first: params.opt_present("newest-first"),
        rebuild_rmsr: params.opt_present("rebuild-rmsr"),
        batch_size: params.opt_get_default("batch-size", 100).unwrap(),
        attrs: params.opt_strs("attr"),
        sql_file: params.opt_get("sql-file").unwrap(),
    };

    let connection = DatabaseConnection::new_from_options(&params);

    Some((ingest_ops, connection))
}

fn create_sql(options: &IngestOptions) -> String {
    if let Some(ref fname) = options.sql_file {
        return fs::read_to_string(fname).unwrap();
    }

    let select = "SELECT id FROM biblio.record_entry";
    let mut filter = format!("WHERE NOT deleted AND id > {}", options.min_id);

    if options.max_id > 0 {
        filter += &format!(" AND id < {}", options.max_id);
    }

    let order_by;
    if options.newest_first {
        order_by = "ORDER BY create_date DESC, id DESC";
    } else {
        order_by = "ORDER BY id";
    }

    format!("{select} {filter} {order_by}")
}

fn get_record_ids(connection: &mut DatabaseConnection, sql: &str) -> Vec<i64> {
    let mut ids = Vec::new();

    for row in connection.client().query(&sql[..], &[]).unwrap() {
        let id: i64 = row.get("id");
        ids.push(id);
    }

    info!("Found {} record IDs to process", ids.len());

    ids
}

fn ingest_records(
    options: &IngestOptions,
    connection: &mut DatabaseConnection,
    ids: &mut Vec<i64>,
) {
    if options.do_browse {
        // Cannot be run in parallel
        reingest_browse(options, connection, ids);
    }

    if options.rebuild_rmsr {
        // Cannot be run in parallel
        rebuild_rmsr(options, connection, ids);
    }

    if options.do_search {
        // Cannot currently be run in parallel.
        // https://bugs.launchpad.net/evergreen/+bug/1931737
        do_search(options, connection, ids);
    }

    if !(options.do_attrs || options.do_facets || options.do_display) {
        return;
    }

    // Remaining actions can be run in parallel

    let pool = ThreadPool::new(options.max_threads);

    while !ids.is_empty() {
        let end = match ids.len() {
            n if n >= options.batch_size => options.batch_size,
            _ => ids.len(),
        };

        // Always pull from index 0 since we are draining the Vec each time.
        let batch: Vec<i64> = ids.drain(0..end).collect();

        let ops = options.clone();
        let con = connection.partial_clone();

        pool.execute(move || process_batch(ops, con, batch));

        if pool.queued_count() > options.batch_size * 2 {
            // Wait for each batch of batches to complete before
            // moving on to the next.  With this we avoid queueing up
            // huge numbers of pending threads w/ cloned closure data
            // consuming lots of memory up front, which can be spread
            // over time instead.
            pool.join();
        }
    }

    pool.join();
}

/// Start point for our threads
fn process_batch(options: IngestOptions, mut connection: DatabaseConnection, ids: Vec<i64>) {
    let idlen = ids.len();

    info!(
        "{:?} processing {} records: {}..{}",
        thread::current().id(),
        idlen,
        &ids[0],
        &ids[idlen - 1],
    );

    connection.connect().unwrap();

    if options.do_attrs {
        reingest_attributes(&options, &mut connection, &ids);
    }

    if options.do_facets || options.do_display {
        reingest_field_entries(&options, &mut connection, &ids);
    }

    connection.disconnect();
}

/// Execute the provided SQL on all records, chopped into batches.
fn run_serialized_updates(
    options: &IngestOptions,
    connection: &mut DatabaseConnection,
    ids: &Vec<i64>,
    sql: &str,
) {
    // We can't create the statement until we are connected.
    let mut stmt: Option<pg::Statement> = None;

    let mut counter: usize = 0;
    for id in ids {
        if counter % options.batch_size == 0 {
            connection.reconnect().unwrap();
            stmt = Some(connection.client().prepare(sql).unwrap());
            info!("Browse has processed {counter} records");
        }

        counter += 1;

        if let Err(e) = connection.client().query(stmt.as_ref().unwrap(), &[id]) {
            error!("Error with browse index for record {id}: {e}");
        }
    }
}

/// Reingest browse data for the full record data set.
///
/// This occurs in the main thread without any parallelification.
fn reingest_browse(options: &IngestOptions, connection: &mut DatabaseConnection, ids: &Vec<i64>) {
    debug!("Starting reingest_browse()");

    let sql = r#"
		SELECT metabib.reingest_metabib_field_entries(
		    bib_id := $1,
		    skip_browse  := FALSE,
		    skip_facet   := TRUE,
		    skip_search  := TRUE,
		    skip_display := TRUE
        )
	"#;

    run_serialized_updates(options, connection, ids, sql);
}

fn do_search(options: &IngestOptions, connection: &mut DatabaseConnection, ids: &Vec<i64>) {
    debug!("Starting do_search()");

    let sql = r#"
        SELECT metabib.reingest_metabib_field_entries(
            bib_id := $1,
            skip_facet := TRUE,
            skip_browse := TRUE,
            skip_search := FALSE,
            skip_display := TRUE
        )
    "#;

    run_serialized_updates(options, connection, ids, sql);
}

/// Reingest browse data for the full record data set.
///
/// This occurs in the main thread without any parallelification.
fn rebuild_rmsr(options: &IngestOptions, connection: &mut DatabaseConnection, ids: &Vec<i64>) {
    debug!("Starting rebuild_rmsr()");

    let sql = r#"SELECT reporter.simple_rec_update($1)"#;

    run_serialized_updates(options, connection, ids, sql);
}

fn reingest_field_entries(
    options: &IngestOptions,
    connection: &mut DatabaseConnection,
    ids: &Vec<i64>,
) {
    debug!("Starting reingest_field_entries()");

    let sql = r#"
        SELECT metabib.reingest_metabib_field_entries(
            bib_id := $1,
            skip_facet := $2,
            skip_browse := TRUE,
            skip_search := TRUE,
            skip_display := $4
        )
    "#;

    let stmt = connection.client().prepare(&sql).unwrap();

    for id in ids {
        if let Err(e) = connection
            .client()
            .query(&stmt, &[id, &!options.do_facets, &!options.do_display])
        {
            error!("Error processing record: {id} {e}");
        }
    }
}

fn reingest_attributes(
    options: &IngestOptions,
    connection: &mut DatabaseConnection,
    ids: &Vec<i64>,
) {
    debug!("Batch starting reingest_attributes()");

    let has_attr_filter = !options.attrs.is_empty();

    let mut sql = r#"
        SELECT metabib.reingest_record_attributes($1)
        FROM biblio.record_entry
        WHERE id = $2
    "#;

    if has_attr_filter {
        sql = r#"
            SELECT metabib.reingest_record_attributes($1, $3)
            FROM biblio.record_entry
            WHERE id = $2
        "#;
    }

    let client = connection.client();
    let stmt = client.prepare(sql).unwrap();

    for id in ids {
        let result = match has_attr_filter {
            false => client.query(&stmt, &[id, id]),
            _ => client.query(&stmt, &[id, id, &options.attrs.as_slice()]),
        };

        if let Err(e) = result {
            error!("Error processing record: {id} {e}");
        }
    }
}

fn main() {
    let (options, mut connection) = match init() {
        Some((o, c)) => (o, c),
        None => return,
    };

    connection.connect().unwrap();

    let sql = create_sql(&options);
    let mut ids = get_record_ids(&mut connection, &sql);

    ingest_records(&options, &mut connection, &mut ids);
}
