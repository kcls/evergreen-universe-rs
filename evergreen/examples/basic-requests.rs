use eg::idl;
use evergreen as eg;
use opensrf as osrf;

fn main() -> Result<(), String> {
    let ctx = eg::init::init()?;
    let client = ctx.client();

    println!("parser class count = {}", ctx.idl().classes().len());

    let mut ses = client.session("open-ils.cstore");

    ses.connect()?;

    let mut req = ses.request("opensrf.system.echo", vec!["howdy", "world"])?;

    while let Some(txt) = req.recv(10)? {
        println!("Echo returned: {txt:?}");
    }

    ses.disconnect()?;

    let method = "open-ils.cstore.direct.actor.user.search";

    let params = vec![
        json::object! {
            id: vec![1, 2, 3]
        },
        json::object! {
            flesh: 1,
            flesh_fields: json::object!{
                au: vec!["home_ou"]
            }
        },
    ];

    for _ in 0..9 {
        // Iterator example
        for user in ses.sendrecv(method, &params)? {
            println!(
                "{} {} home_ou={}",
                user["id"], user["usrname"], user["home_ou"]["name"]
            );
        }
    }

    // Manual request management example
    let mut req = ses.request(method, &params)?;

    while let Some(user) = req.recv(10)? {
        println!(
            "{} {} home_ou={}",
            user["id"], user["usrname"], user["home_ou"]["name"]
        );
    }

    let args = eg::auth::AuthLoginArgs::new("admin", "demo123", "temp", None);

    match eg::auth::AuthSession::login(client, &args)? {
        Some(ses) => println!("\nLogged in and got authtoken: {}", ses.token()),
        None => println!("\nLogin failed"),
    }

    Ok(())
}
