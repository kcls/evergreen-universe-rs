use eg::db::DatabaseConnection;
use eg::idl;
use eg::idldb::{IdlClassSearch, OrderBy, OrderByDir, Pager, Translator};
use evergreen as eg;
use getopts;
use opensrf as osrf;
use std::env;

fn main() -> Result<(), String> {
    let mut opts = getopts::Options::new();
    DatabaseConnection::append_options(&mut opts);

    let ctx = eg::init::init_with_options(&mut opts)?;

    let mut db = DatabaseConnection::new_from_options(ctx.params());
    db.connect()?;
    let db = db.into_shared();

    let translator = Translator::new(ctx.idl().clone(), db.clone());

    // Give me all rows
    let mut search = IdlClassSearch::new("aou");

    for org in translator.idl_class_search(&search)? {
        println!("org: {} {}\n", org["id"], org["shortname"]);
    }

    search.set_filter(json::object! {id: 1, name: "CONS", opac_visible: false});

    for org in translator.idl_class_search(&search)? {
        println!("org: {} {}\n", org["id"], org["shortname"]);
    }

    search.set_filter(json::object! {id: json::object! {">": 1}, ou_type: [1, 2, 3]});

    for org in translator.idl_class_search(&search)? {
        println!("org: {} {}\n", org["id"], org["shortname"]);
    }

    search.set_order_by(vec![OrderBy::new("name", OrderByDir::Asc)]);

    for org in translator.idl_class_search(&search)? {
        println!("org: {} {}\n", org["id"], org["shortname"]);
    }

    search.set_pager(Pager::new(10, 0));

    for org in translator.idl_class_search(&search)? {
        println!("org: {} {}\n", org["id"], org["shortname"]);
    }

    Ok(())
}
