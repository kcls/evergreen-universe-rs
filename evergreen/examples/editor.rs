use evergreen as eg;

fn main() -> Result<(), String> {
    let ctx = eg::init::init()?;
    let client = ctx.client();

    println!("Logging in...");

    //let args = eg::auth::AuthLoginArgs::new("admin", "demo123", "temp", None);
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
