use eg::remote::RemoteAccount;
use eg::script::ScriptUtil;
use eg::EgResult;
use evergreen as eg;

const HELP_TEXT: &str = r#"
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

    --cat <remote-file>

    --remote-file <remote-file>
        Name of remote file to perform actions on.

    --local-file <local-file>
        Name of local file to perform actions on.
"#;

pub fn main() -> EgResult<()> {
    let mut ops = getopts::Options::new();

    ops.optopt("", "host", "", "");
    ops.optopt("", "username", "", "");
    ops.optopt("", "password", "", "");
    ops.optopt("", "remote-path", "", "");
    ops.optopt("", "remote-file", "", "");
    ops.optopt("", "local-file", "", "");
    ops.optopt("", "ssh-private-key", "", "");
    ops.optflag("", "ls", "");
    ops.optflag("", "get", "");

    let scripter = match ScriptUtil::init(&mut ops, true, Some(HELP_TEXT))? {
        Some(s) => s,
        None => return Ok(()), // e.g. --help
    };

    let host = scripter
        .params()
        .opt_str("host")
        .expect("--hostname is required");

    let mut account = RemoteAccount::new(&host);

    let remote_path = scripter
        .params()
        .opt_str("remote-path")
        .expect("--remote-path is required");

    account.set_remote_path(&remote_path);

    // proto

    if let Some(username) = scripter.params().opt_str("username") {
        account.set_username(&username);
    }

    if let Some(password) = scripter.params().opt_str("password") {
        account.set_password(&password);
    }

    if let Some(ssh_private_key) = scripter.params().opt_str("ssh-private-key") {
        account.set_ssh_private_key(&ssh_private_key);
    }

    account.connect()?;

    if scripter.params().opt_present("ls") {
        for file in account.ls()?.iter() {
            println!("Found remote file: {file}");
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

        let _file = account.get(&remote_file, &local_file)?;

        println!("Saved {remote_file} as {local_file}");
    }

    Ok(())
}
