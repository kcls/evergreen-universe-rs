//! CLI for translating a JSON query string into an SQL query.
//!
//! Pass a JSON query hash as a JSON string via STDIN or command line
//! parameter and this tool will print the compiled SQL query.
//!
//! % cargo run --package evergreen --bin eg-compile-json-query -- \
//!     '{"select":{"au":["id"]},"from":"au","where":{"id":1}}'
//! SELECT "au".id FROM actor.usr AS "au" WHERE "au".id = 1;
use eg::common::jq::JsonQueryCompiler;
use eg::{EgResult, EgValue};
use evergreen as eg;
use std::env;
use std::io;
use std::io::IsTerminal;

fn main() -> EgResult<()> {
    // Load the IDL from the usual locations without connecting to Evergreen.
    eg::init::load_idl()?;

    // First see if the JSON was sent via command line
    let mut buffer = String::new();

    let lines = match env::args().skip(1).reduce(|words, w| words + &w) {
        Some(l) => l,
        None => {
            // No content was read from the command line, see if we have
            // any piped to us on STDIN.
            let stdin = io::stdin();

            if stdin.is_terminal() {
                // Avoid blocking on STDIN in interactive mode.
                return Ok(());
            }

            let mut lines = String::new();

            // Read the JSON piped to us via STDIN
            while stdin.read_line(&mut buffer).map_err(|e| e.to_string())? > 0 {
                lines += &buffer;
                buffer.clear();
            }

            lines
        }
    };

    let query = EgValue::parse(lines.trim())?;

    let mut jq_compiler = JsonQueryCompiler::new();
    jq_compiler.compile(&query)?;

    println!("{}", jq_compiler.debug_query_kludge());

    Ok(())
}
