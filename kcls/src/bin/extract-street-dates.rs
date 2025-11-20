use getopts::Options;
use calamine::{Reader, open_workbook, Xlsx};

use eg::script;
use evergreen as eg;


/// Convert Excel date serial number to YYYY-MM-DD format
/// Excel stores dates as days since 1900-01-01
fn excel_date_to_string(serial: f64) -> String {
    // Excel's date system: 1 = 1900-01-01
    // Excel incorrectly treats 1900 as a leap year (it wasn't)
    // So dates after Feb 29, 1900 need adjustment

    // Use 1899-12-31 as base (day 0), so day 1 = 1900-01-01
    let base_date = chrono::NaiveDate::from_ymd_opt(1899, 12, 31).unwrap();

    // Adjust for Excel's leap year bug (for dates after Feb 29, 1900)
    let days = if serial > 60.0 {
        serial - 1.0  // Subtract 1 to account for the fake Feb 29, 1900
    } else {
        serial
    };

    let date = base_date + chrono::Duration::days(days as i64);

    date.format("%Y-%m-%d").to_string()
}

const HELP_TEXT: &str = r#"
Extract Street Dates from Vendor Excel Files

This tool reads vendor Excel files and extracts Invoice Number, EAN, and Publication Date,
then applies the street dates to matching line items in Evergreen acquisitions.

Options:
    --file <path>
        Path to the Excel file to process (required)

    -h, --help
        Display this help message

Example:
    extract-street-dates --file vendor_data.xlsx
"#;

#[derive(Debug)]
struct StreetDateRecord {
    invoice_number: String,
    ean: String,
    pub_date: String,
}

struct ApplyStats {
    invoices_found: usize,
    invoices_not_found: usize,
    street_dates_created: usize,
    street_dates_updated: usize,
    street_dates_unchanged: usize,
}

impl ApplyStats {
    fn new() -> Self {
        Self {
            invoices_found: 0,
            invoices_not_found: 0,
            street_dates_created: 0,
            street_dates_updated: 0,
            street_dates_unchanged: 0,
        }
    }
}

fn main() {
    let mut opts = Options::new();

    // TODO: add a --edi-account-host command line to filter on ACQ vendors
    opts.optopt("", "file", "Path to Excel file", "FILE");

    let options = script::Options {
        with_evergreen: true,
        with_database: false,
        help_text: None,
        extra_params: None,
        options: Some(opts),
    };

    let mut scripter = match script::Runner::init(options).expect("Init OK") {
        Some(s) => s,
        None => return,
    };

    // Get the input file path (required)
    let file_path = match scripter.params().opt_str("file") {
        Some(path) => path,
        None => {
            eprintln!("Error: --file option is required");
            println!("{}", HELP_TEXT);
            std::process::exit(1);
        }
    };

    println!("Processing file: {}", file_path);

    match process_excel_file(&file_path) {
        Ok(records) => {
            println!("Successfully extracted {} records", records.len());

            apply_street_dates(&mut scripter, &records).expect("OK");
        }
        Err(e) => {
            eprintln!("Error processing file: {}", e);
            std::process::exit(1);
        }
    }
}

/// Apply street date records to Evergreen acquisitions
fn apply_street_dates(scripter: &mut script::Runner, records: &[StreetDateRecord]) -> Result<(), String> {
    println!("\nApplying street dates...");

    scripter.editor_mut().xact_begin()?;

    let mut defs = scripter.editor_mut().search("acqliad", eg::hash! {"code": "street_date"})?;
    let street_date_def = defs.pop().ok_or("street_date definition not found in acq.lineitem_attr_definition")?;
    let street_date_def_id = street_date_def.id()?;

    let mut stats = ApplyStats::new();

    // TODO: group records by invoice number
    for record in records {
        let inv_ident = record.invoice_number.trim();
        let mut invoices = scripter.editor_mut().search("acqinv",
            eg::hash! {"inv_ident": inv_ident})?;

        let Some(invoice) = invoices.pop() else {
            stats.invoices_not_found += 1;
            continue;
        };

        stats.invoices_found += 1;

        let entries = scripter.editor_mut().search("acqie", eg::hash! {"invoice": invoice.id()?})?;

        for entry in entries {
            let attrs = scripter.editor_mut()
                .search("acqlia", eg::hash! {"order_ident": "t", "lineitem": entry["lineitem"].int()?})?;

            let isbn = record.ean.trim();
            let mut matched_lineitem_id: Option<i64> = None;

            for attr in &attrs {
                if attr["attr_name"].str()? == "isbn" && attr["attr_value"].str()? == isbn {
                    matched_lineitem_id = Some(entry["lineitem"].int()?);
                    break;
                }
            }

            if let Some(li_id) = matched_lineitem_id {
                // Check for existing street_date attribute
                let existing_street_dates = scripter.editor_mut().search("acqlia",
                    eg::hash! {
                        "lineitem": li_id,
                        "definition": street_date_def_id
                    })?;

                if let Some(mut existing_attr) = existing_street_dates.into_iter().next() {
                    // Street date already exists - check if it needs updating
                    let current_value = existing_attr["attr_value"].str()?;
                    let new_value = record.pub_date.trim();

                    if current_value != new_value {
                        println!("Updating street date for lineitem {}: {} -> {}",
                            li_id, current_value, new_value);
                        existing_attr["attr_value"] = eg::EgValue::from(new_value);
                        scripter.editor_mut().update(existing_attr)?;
                        stats.street_dates_updated += 1;
                    } else {
                        stats.street_dates_unchanged += 1;
                    }
                } else {
                    println!("Creating street date for lineitem {}: {}", li_id, record.pub_date);
                    let attr = eg::blessed! {
                        "_classname": "acqlia",
                        "lineitem": li_id,
                        "definition": street_date_def_id,
                        "attr_type": "lineitem_attr_definition",
                        "attr_name": "street_date",
                        "attr_value": record.pub_date.clone(),
                    }?;

                    scripter.editor_mut().create(attr)?;
                    stats.street_dates_created += 1;
                }
            }
        }
    }

    scripter.editor_mut().xact_commit()?;

    println!("\n=== Summary ===");
    println!("Invoices found: {}", stats.invoices_found);
    println!("Invoices not found: {}", stats.invoices_not_found);
    println!("Street dates created: {}", stats.street_dates_created);
    println!("Street dates updated: {}", stats.street_dates_updated);
    println!("Street dates unchanged: {}", stats.street_dates_unchanged);

    Ok(())
}

/// Process the Excel file and extract street date records
fn process_excel_file(file_path: &str) -> Result<Vec<StreetDateRecord>, String> {
    // Open the Excel workbook
    let mut workbook: Xlsx<_> = open_workbook(file_path)
        .map_err(|e| format!("Failed to open Excel file: {}", e))?;

    // Get the first worksheet
    let sheet_names = workbook.sheet_names().to_vec();
    if sheet_names.is_empty() {
        return Err("Excel file contains no worksheets".to_string());
    }

    println!("Found {} worksheet(s): {:?}", sheet_names.len(), sheet_names);
    println!("Reading from worksheet: {}", sheet_names[0]);

    let range = workbook
        .worksheet_range(&sheet_names[0])
        .map_err(|e| format!("Failed to read worksheet: {}", e))?;

    let mut records = Vec::new();

    // First, let's inspect the header row to understand the structure
    let mut headers: Vec<String> = Vec::new();

    // Read first row as headers
    if let Some(first_row) = range.rows().next() {
        println!("\nColumn Headers:");
        for (idx, cell) in first_row.iter().enumerate() {
            let header = cell.to_string();
            println!("  Column {}: {}", idx, header);
            headers.push(header);
        }
    }

    // Find the column indices for our target fields
    let invoice_col = find_column_index(&headers, &["invoice number"]);
    let ean_col = find_column_index(&headers, &["ean", "isbn", "barcode"]);
    let pub_date_col = find_column_index(&headers, &["pub date", "publication date", "street date", "release date"]);

    println!("\nColumn mapping:");
    println!("  Invoice Number: {:?}", invoice_col);
    println!("  EAN: {:?}", ean_col);
    println!("  Pub Date: {:?}", pub_date_col);

    // Process data rows (skip header row)
    for (_row_idx, row) in range.rows().enumerate().skip(1) {
        // Extract values based on column indices
        let invoice = invoice_col
            .and_then(|col| row.get(col))
            .map(|cell| cell.to_string())
            .unwrap_or_default();

        let ean = ean_col
            .and_then(|col| row.get(col))
            .map(|cell| cell.to_string())
            .unwrap_or_default();

        let pub_date = pub_date_col
            .and_then(|col| row.get(col))
            .map(|cell| {
                // Try to parse cell string as a number (Excel date serial)
                let cell_str = cell.to_string();
                if let Ok(serial) = cell_str.parse::<f64>() {
                    // If it's a number, convert from Excel serial date
                    excel_date_to_string(serial)
                } else {
                    // Otherwise use the string as-is
                    cell_str
                }
            })
            .unwrap_or_default();

        // Only add records that have at least some data
        if !invoice.is_empty() || !ean.is_empty() || !pub_date.is_empty() {
            records.push(StreetDateRecord {
                invoice_number: invoice,
                ean,
                pub_date,
            });
        }
    }

    Ok(records)
}

/// Find column index by searching for keywords in headers (case-insensitive)
fn find_column_index(headers: &[String], keywords: &[&str]) -> Option<usize> {
    headers.iter().position(|header| {
        let header_lower = header.to_lowercase();
        keywords.iter().any(|keyword| header_lower.contains(keyword))
    })
}
