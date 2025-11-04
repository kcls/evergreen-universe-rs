use std::env;
use std::fs::File;
use getopts::Options;
use calamine::{Reader, open_workbook, Xlsx};
use csv::Writer;

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

This tool reads vendor Excel files and extracts Invoice Number, EAN, and Publication Date.

Options:
    --file <path>
        Path to the Excel file to process (required)

    --output <path>
        Path to save the output CSV file (optional, defaults to STDOUT)

    -h, --help
        Display this help message

Examples:
    # Print extracted data to screen
    extract-street-dates --file vendor_data.xlsx

    # Save extracted data to a CSV file
    extract-street-dates --file vendor_data.xlsx --output results.csv
"#;

/// Represents one row of extracted data from the vendor file
#[derive(Debug)]
struct StreetDateRecord {
    invoice_number: String,
    ean: String,
    pub_date: String,
}

fn main() {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();

    opts.optopt("", "file", "Path to Excel file", "FILE");
    opts.optopt("", "output", "Path to output CSV file", "FILE");
    opts.optflag("h", "help", "Show this help message");

    let params = match opts.parse(&args[1..]) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Error parsing options: {}", e);
            println!("{}", HELP_TEXT);
            std::process::exit(1);
        }
    };

    // Show help if requested
    if params.opt_present("help") {
        println!("{}", HELP_TEXT);
        return;
    }

    // Get the input file path (required)
    let file_path = match params.opt_str("file") {
        Some(path) => path,
        None => {
            eprintln!("Error: --file option is required");
            println!("{}", HELP_TEXT);
            std::process::exit(1);
        }
    };

    // Get the optional output file path
    let output_path = params.opt_str("output");

    println!("Processing file: {}", file_path);

    // TODO: Read and process the Excel file
    // This is where we'll add Excel reading functionality

    match process_excel_file(&file_path) {
        Ok(records) => {
            println!("Successfully extracted {} records", records.len());

            // Output the results
            output_results(&records, output_path.as_deref());
        }
        Err(e) => {
            eprintln!("Error processing file: {}", e);
            std::process::exit(1);
        }
    }
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
    for (row_idx, row) in range.rows().enumerate().skip(1) {
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

        // Show first few records for debugging
        if row_idx <= 5 {
            println!("Row {}: Invoice={}, EAN={}, PubDate={}",
                row_idx,
                records.last().map(|r| r.invoice_number.as_str()).unwrap_or(""),
                records.last().map(|r| r.ean.as_str()).unwrap_or(""),
                records.last().map(|r| r.pub_date.as_str()).unwrap_or("")
            );
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

/// Output the extracted records to STDOUT or a file
fn output_results(records: &[StreetDateRecord], output_path: Option<&str>) {
    if let Some(path) = output_path {
        // Write to CSV file
        match write_csv_file(records, path) {
            Ok(_) => println!("Successfully wrote {} records to {}", records.len(), path),
            Err(e) => eprintln!("Error writing CSV file: {}", e),
        }
    } else {
        // Print to STDOUT
        println!("\nExtracted Records:");
        println!("Invoice Number | EAN | Publication Date");
        println!("{}", "-".repeat(50));

        for record in records {
            println!("{} | {} | {}",
                record.invoice_number,
                record.ean,
                record.pub_date
            );
        }
    }
}

/// Write records to a CSV file
fn write_csv_file(records: &[StreetDateRecord], path: &str) -> Result<(), String> {
    let file = File::create(path)
        .map_err(|e| format!("Failed to create file: {}", e))?;

    let mut writer = Writer::from_writer(file);

    // Write header row
    writer.write_record(&["Invoice Number", "EAN", "Publication Date"])
        .map_err(|e| format!("Failed to write header: {}", e))?;

    // Write data rows
    for record in records {
        writer.write_record(&[
            &record.invoice_number,
            &record.ean,
            &record.pub_date,
        ])
        .map_err(|e| format!("Failed to write record: {}", e))?;
    }

    writer.flush()
        .map_err(|e| format!("Failed to flush writer: {}", e))?;

    Ok(())
}
