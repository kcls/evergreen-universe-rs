use evergreen as eg;

fn main() -> eg::EgResult<()> {
    let ctx = eg::init()?;
    let mut editor = eg::Editor::new(ctx.client());

    let orgs = editor.search("aou", eg::hash! {"id": {">": 0}})?;

    for org in orgs {
       println!("Org: {org}");
    }

    let mut new_org = eg::hash! {
        "ou_type": 1,
        "shortname": "TEST",
        "name": "TEST NAME",
    };

	// Turn a bare hash into a blessed org unit ("aou") value.
    new_org.bless("aou")?;

    // Modify a value after instantiation
    new_org["email"] = "home@example.org".into();

    // Start a database transaction so we can modify data.
    editor.xact_begin()?;

    // Editor::create returns the newly created value.
    new_org = editor.create(new_org)?;

    println!("Add Org: {new_org} email={}", new_org["email"]);

    // Rollback the transaction and disconnect
    editor.rollback()?;


    let client = ctx.client();

    let args = eg::auth::AuthLoginArgs::new("br1mclark", "montyc1234", "temp", None);
    let auth_ses = match eg::auth::AuthSession::login(client, &args)? {
        Some(s) => s,
        None => panic!("Login failed"),
    };

    let token = auth_ses.token();

    println!("Logged in and got authtoken: {}", token);

    let mut editor = eg::Editor::with_auth(client, token);

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
    let args = eg::auth::AuthInternalLoginArgs::new(1, "temp");

    let auth_ses = match eg::auth::AuthSession::internal_session(client, &args)? {
        Some(s) => s,
        None => panic!("Internal Login failed"),
    };

    let token = auth_ses.token();

    println!("Logged in with internal and got authtoken: {}", token);

    Ok(())
}
