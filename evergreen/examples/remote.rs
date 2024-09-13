use evergreen as eg;
use eg::EgValue;
use eg::remote::RemoteAccount;
use eg::script::ScriptUtil;

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

    --cat-remote-file <remote-file>
"#;


pub fn main() {
    let mut ops = getopts::Options::new();

    ops.optopt("", "host", "", "");
    ops.optopt("", "username", "", "");
    ops.optopt("", "password", "", "");
    ops.optopt("", "remote-path", "", "");
    ops.optopt("", "ssh-private-key", "", "");
    ops.optopt("", "cat-remote-file", "", "");

    let scripter = match ScriptUtil::init(&mut ops, true, Some(HELP_TEXT))
        .expect("ScriptUtil should init OK")
    {
        Some(s) => s,
        None => return, // e.g. --help
    };

    let host = scripter.params().opt_str("host")
        .expect("--hostname is required");

    let mut account = RemoteAccount::new(&host);

    let remote_path = scripter.params().opt_str("remote-path")
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

    // TODO actions

    account.connect().expect("Should Connect");

    for file in account.ls().expect("ls should return files").iter() {
        println!("Found remote file: {file}");
    }

    if let Some(cat_file) = scripter.params().opt_str("cat-remote-file").as_ref() {
        println!("{}", account.get(cat_file).expect("Cat file"));
    }
}



