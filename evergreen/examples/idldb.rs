use eg::db::DatabaseConnection;
use eg::idldb::{IdlClassSearch, IdlClassUpdate, OrderBy, OrderByDir, Translator};
use eg::util::Pager;
use evergreen as eg;
use getopts;
use std::env;

fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    let mut opts = getopts::Options::new();
    DatabaseConnection::append_options(&mut opts);

    let params = match opts.parse(&args[1..]) {
        Ok(p) => p,
        Err(e) => panic!("Error parsing options: {}", e),
    };

    let ctx = eg::init::init()?;

    let mut db = DatabaseConnection::new_from_options(&params);
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

    let mut update = IdlClassUpdate::new("aou");
    update.set_filter(json::object! {id: 1, shortname: "CONS"});
    update.add_value("shortname", &json::from("CONS-TEST"));
    update.add_value("name", &json::from("; drop table foobar;"));

    translator.idl_class_update(&update)?;

    Ok(())
}
