use eg::db::DatabaseConnection;
use eg::idldb::{FleshDef, IdlClassSearch, IdlClassUpdate, OrderBy, OrderByDir, Translator};
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

    let mut translator = Translator::new(ctx.idl().clone(), db.clone());

    // Give me all rows
    let mut search = IdlClassSearch::new("aou");

    for org in translator.idl_class_search(&search)? {
        println!("org: {} {}", org["id"], org["shortname"]);
    }

    search.set_filter(json::object! {id: 1, name: "CONS", opac_visible: false});

    for org in translator.idl_class_search(&search)? {
        println!("org: {} {}", org["id"], org["shortname"]);
    }

    search.set_filter(json::object! {id: json::object! {">": 1}, ou_type: [1, 2, 3]});

    for org in translator.idl_class_search(&search)? {
        println!("org: {} {}", org["id"], org["shortname"]);
    }

    search.set_filter(json::object! {"id": {"not in": [1, 2]}});

    for org in translator.idl_class_search(&search)? {
        println!("org: ID NOT IN: {} {}", org["id"], org["shortname"]);
    }

    search.set_order_by(vec![OrderBy::new("name", OrderByDir::Asc)]);

    for org in translator.idl_class_search(&search)? {
        println!("org: {} {}", org["id"], org["shortname"]);
    }

    search.set_pager(Pager::new(10, 0));

    for org in translator.idl_class_search(&search)? {
        println!("org: {} {}", org["id"], org["shortname"]);
    }

    // Grab an org unit to update.
    search.set_filter(json::object! {id: json::object! {">": 0}});
    let results = translator.idl_class_search(&search)?;
    let org = results.first().expect("Wanted at least one org unit");

    let shortname = org["shortname"].as_str().unwrap();

    translator.xact_begin()?;
    let mut update = IdlClassUpdate::new("aou");
    update.set_filter(json::object! {id: org["id"].clone()});
    update.add_value("shortname", &json::from(format!("{}-TEST", shortname)));

    translator.idl_class_update(&update)?;
    translator.xact_commit()?;

    search.set_filter(json::object! {id: org["id"].clone()});
    let results = translator.idl_class_search(&search)?;
    let org_updated = results.first().expect("Cannot find org unit");

    println!("org name updated to: {}", org_updated["shortname"]);

    translator.xact_begin()?;
    update.reset(); // clear filters and values
    update.set_filter(json::object! {id: org["id"].clone()});
    update.add_value("shortname", &json::from(shortname));

    translator.idl_class_update(&update)?;

    translator.xact_commit()?;

    search.set_filter(json::object! {id: org["id"].clone()});
    let results = translator.idl_class_search(&search)?;
    let org_updated = results.first().expect("Cannot find org unit");
    println!("org name updated to: {}", org_updated["shortname"]);

    // Now update some objects directly
    let mut org_mod = org_updated.clone();

    translator.xact_begin()?;
    org_mod["shortname"] = json::from("TEST NAME");
    translator.update_idl_object(&org_mod)?;
    translator.xact_commit()?;

    search.set_filter(json::object! {id: org["id"].clone()});
    let results = translator.idl_class_search(&search)?;
    let org_updated = results.first().expect("Cannot find org unit");
    println!("org name updated to: {}", org_updated["shortname"]);

    translator.xact_begin()?;
    org_mod["shortname"] = json::from(shortname);
    translator.update_idl_object(&org_mod)?;
    translator.xact_commit()?;

    search.set_filter(json::object! {id: org["id"].clone()});
    let results = translator.idl_class_search(&search)?;
    let org_updated = results.first().expect("Cannot find org unit");
    println!("org name updated to: {}", org_updated["shortname"]);

    translator.xact_begin()?;
    let mut cbt = ctx.idl().create("cbt").expect("Invalid IDL class");
    cbt["name"] = json::from("A Billing Type");
    cbt["owner"] = json::from(1);
    translator.create_idl_object(&cbt)?;
    translator.xact_rollback()?;

    // Give me all rows
    let mut search = IdlClassSearch::new("au");
    search.set_filter(json::object! {id: [1, 2, 3, 4, 5, 6, 7, 8, 9]});
    let flesh = json::object! {
        "flesh": 2,
        "flesh_fields":{"au": ["addresses", "home_ou", "profile"], "aou": ["ou_type"]}
    };

    search.set_flesh(FleshDef::from_json_value(&flesh)?);

    for user in translator.idl_class_search(&search)? {
        println!(
            "user: {} {} depth={} {}",
            user["usrname"],
            user["home_ou"]["shortname"],
            user["home_ou"]["ou_type"]["depth"],
            user["profile"]["name"],
        );

        for addr in user["addresses"].members() {
            println!("street = {}", addr["street1"]);
        }
    }

    let flesh = ctx.idl().field_paths_to_flesh(
        "acqpo",
        &[
            "lineitems.lineitem_details.owning_lib",
            "lineitems.lineitem_details.fund",
        ],
    )?;

    println!("FLESH is {}", flesh.dump());

    println!("{:?}", eg::idl::get_class("aou").expect("Class Exists"));

    Ok(())
}
