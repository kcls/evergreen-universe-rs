use eg::remote::RemoteAccount;
use eg::script;
use eg::EgResult;
use std::path::Path;
use evergreen as eg;

const HELP_TEXT: &str = r#"
    --url <full-url-string>
        E.g. sftp://user@localhost:2020/edi/pick-up/edifact.*

    --proto <sftp|ftp>

    --host <hostname>
        Required

    --remote-path <path>
        Required

    --username <username>
        Required

    --passsword <password>

    --ssh-private-key <key-file>
        SSH private key file.

    --remote-file <remote-file>
        Name of remote file to perform actions on.

    --local-file <local-file>
        Name of local file to perform actions on.

    --timeout
        Timeout in seconds for blocking operations
"#;

pub fn main() -> EgResult<()> {
    let mut ops = getopts::Options::new();

    ops.optopt("", "timeout", "", "");
    ops.optopt("", "url", "", "");
    ops.optopt("", "host", "", "");
    ops.optopt("", "username", "", "");
    ops.optopt("", "password", "", "");
    ops.optopt("", "remote-path", "", "");
    ops.optopt("", "remote-file", "", "");
    ops.optopt("", "local-file", "", "");
    ops.optopt("", "ssh-private-key", "", "");
    ops.optflag("", "ls", "");
    ops.optflag("", "get", "");

    let options = script::Options {
        with_evergreen: true,
        with_database: true,
        help_text: Some(HELP_TEXT.to_string()),
        extra_params: None,
        options: Some(ops),
    };

    let scripter = match script::Runner::init(options)? {
        Some(s) => s,
        None => return Ok(()), // e.g. --help
    };

    let mut account = if let Some(url) = scripter.params().opt_str("url") {
        RemoteAccount::from_url_string(&url)?
    } else if let Some(host) = scripter.params().opt_str("host") {
        RemoteAccount::new(&host)
    } else {
        return Err(format!("--host or --url requried").into());
    };

    if account.remote_path().is_none() {
        let remote_path = scripter
            .params()
            .opt_str("remote-path")
            .expect("--remote-path is required");

        account.set_remote_path(Path::new(&remote_path));
    }

    // proto

    if let Some(username) = scripter.params().opt_str("username") {
        account.set_username(&username);
    }

    if let Some(password) = scripter.params().opt_str("password") {
        account.set_password(&password);
    }

    if let Some(ref ssh_private_key) = scripter.params().opt_str("ssh-private-key") {
        account.set_ssh_private_key(Path::new(ssh_private_key));
    }

    if let Some(timeout) = scripter.params().opt_str("timeout") {
        let t = timeout
            .parse::<u32>()
            .map_err(|e| format!("Invalid timeout: {timeout} : {e}"))?;
        account.set_timeout(t);
    }

    account.connect()?;

    if scripter.params().opt_present("ls") {
        for path in account.ls()?.iter() {
            println!("Found remote file: {}", path.display());
            //println!("Found remote file: {}", path);
        }
    }

    if scripter.params().opt_present("get") {
        let remote_file = scripter
            .params()
            .opt_str("remote-file")
            .expect("Pass --remote-file");
        let local_file = scripter
            .params()
            .opt_str("local-file")
            .expect("Pass --local-file");

        let _file = account.get(Path::new(&remote_file), Path::new(&local_file))?;

        println!("Saved {remote_file} as {local_file}");
    }

    Ok(())
}
