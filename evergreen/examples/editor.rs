use eg::common::auth;
use evergreen as eg;

fn main() -> eg::EgResult<()> {
    let client = eg::init()?;
    let mut editor = eg::Editor::new(&client);

    let orgs = editor.search("aou", eg::hash! {"id": {">": 0}})?;

    for org in orgs {
        println!("Org: {org}");
    }

    let mut new_org = eg::blessed! {
        "_classname": "aou",
        "ou_type": 1,
        "shortname": "TEST",
        "name": "TEST NAME",
    }?;

    // Modify a field on a value.
    new_org["email"] = "home@example.org".into();

    // Start a database transaction so we can modify data.
    editor.xact_begin()?;

    // Editor::create returns the newly created value.
    editor.create(new_org)?;

    // Search for the newly created org unit
    // NOTE: ^-- editor.create() also returns the newly created value.
    let org_list = editor.search("aou", eg::hash! {"shortname": "TEST"})?;

    // If found, log it
    if let Some(org) = org_list.first() {
        println!("Add Org: {org} email={}", org["email"]);
    }

    // Rollback the transaction and disconnect
    editor.rollback()?;

    let args = auth::LoginArgs::new("br1mclark", "montyc1234", auth::LoginType::Temp, None);
    let auth_ses = match auth::Session::login(&client, &args)? {
        Some(s) => s,
        None => panic!("Login failed"),
    };

    let token = auth_ses.token();

    println!("Logged in and got authtoken: {}", token);

    let mut editor = eg::Editor::with_auth(&client, token);

    if editor.checkauth()? {
        println!("Auth Check OK: {}", editor.requestor().unwrap()["usrname"]);
    }

    if editor.allowed("EVERYTHING")? {
        println!("Requestor is allowed");
    } else {
        println!("Requestor is NOT allowed");
    }

    if editor.allowed_at("VIEW_USER", 4)? {
        println!("VIEW_USER is allowed");
    } else {
        println!("VIEW_USER is NOT allowed");
    }

    if let Some(org) = editor.retrieve("aou", 4)? {
        println!("Fetched org unit: {}", org["shortname"]);
    }

    let query = eg::hash! {"id": eg::hash!{"<": 10u8}};
    for perm in editor.search("ppl", query)? {
        println!("Search found permission: {perm}");
        println!("Search found permission: {}", perm["code"]);
    }

    // Testing internal auth
    let args = auth::InternalLoginArgs::new(1, auth::LoginType::Temp);

    let auth_ses = match auth::Session::internal_session_api(&client, &args)? {
        Some(s) => s,
        None => panic!("Internal Login failed"),
    };

    let token = auth_ses.token();

    println!("Logged in with internal and got authtoken: {}", token);

    Ok(())
}
