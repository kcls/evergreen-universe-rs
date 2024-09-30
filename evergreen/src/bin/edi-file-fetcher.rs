use eg::remote::RemoteAccount;
use eg::script::ScriptUtil;
use eg::EgResult;
use evergreen as eg;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

const DEFAULT_TIMEOUT: u32 = 10;

const HELP_TEXT: &str = r#"

Remote Account Selection:

    --edi-account <edi-account-id>
        Perform actions for a single EDI account.

    --active-edi-accounts
        Perform actions across all active EDI accounts

    --url <full-url-string>
        E.g. sftp://user@localhost:2020/edi/pick-up/edifact.*

        For manually testing connectivity.

    --password <password>
        Used in conjunction with --url to manually specify a destination

    --list-edi-accounts
        Print a list of EDI accounts linked to active providers,
        grouped by connection details, so that the final list
        matches the list used to retrieve files from a unique set of
        hosts/logins/directories/etc.

Actions:

    --list-files
        List files at the destination.  If the remote path
        contains a file name glob, only files matching the glob
        will be listed.

    --save-files
        Save a local copy of every matching file to the --output-dir.

    --output-dir <directory>
        Location to store fetched files.

        When storing files from a known remote/edi account, append
        the remote account ID to the directory path to ensure we
        can always map a file back to its remote account.

    --force-save
        Save local copies even if a matching EDI message is found
        in the database (acq.edi_message).  This is useful for
        re-fetching files which have already been processed (e.g. to
        make backups).

        New copies of files will not be saved if existing copies
        exist in the output directory.

General Settings:

    --timeout
        Timeout in seconds for blocking operations
"#;

pub fn main() -> EgResult<()> {
    let mut ops = getopts::Options::new();

    ops.optopt("", "url", "", "");
    ops.optopt("", "timeout", "", "");
    ops.optopt("", "password", "", "");
    ops.optopt("", "output-dir", "", "");
    ops.optopt("", "edi-account", "", "");
    ops.optflag("", "list-files", "");
    ops.optflag("", "save-files", "");
    ops.optflag("", "active-edi-accounts", "");
    ops.optflag("", "list-edi-accounts", "");
    ops.optflag("", "force-save", "");

    let mut scripter = match ScriptUtil::init(&mut ops, true, false, Some(HELP_TEXT))? {
        Some(s) => s,
        None => return Ok(()), // e.g. --help
    };

    if let Some(url) = scripter.params().opt_str("url") {
        let mut account = RemoteAccount::from_url_string(&url)?;
        process_one_account(&mut scripter, &mut account)?;
    }

    if let Some(id_str) = scripter.params().opt_str("edi-account") {
        let id = id_str.parse::<i64>().expect("ID should be a number");
        let mut account = RemoteAccount::from_edi_account(scripter.editor_mut(), id, true)?;
        process_one_account(&mut scripter, &mut account)?;
    }

    if scripter.params().opt_present("list-edi-accounts") {
        list_accounts(&mut scripter)?;
    }

    if scripter.params().opt_present("active-edi-accounts") {
        for mut account in get_active_edi_accounts(&mut scripter)?.drain(..) {
            process_one_account(&mut scripter, &mut account)?;
        }
    }

    Ok(())
}

/// Returns a set of RemoteAccounts mapped from a unique set of EDI
/// accounts, linked to active providers.
///
/// Accounts are uniqued by host/directory/etc.
fn get_active_edi_accounts(scripter: &mut ScriptUtil) -> EgResult<Vec<RemoteAccount>> {
    let mut remote_accounts = Vec::new();

    let providers = scripter
        .editor_mut()
        .search("acqpro", eg::hash! {"active": "t"})?;

    for provider in providers.iter() {
        let account_hashes = scripter.editor_mut().json_query(eg::hash! {
            "select": {"acqedi": ["id"]},
            "from": "acqedi",
            "where": {"provider": provider.id()?}
        })?;

        for account_hash in account_hashes.iter() {
            let id = account_hash.id()?;
            let account = match RemoteAccount::from_edi_account(scripter.editor_mut(), id, true) {
                Ok(a) => a,
                Err(e) => {
                    eprintln!("Skipping account {id} : {e}");
                    continue;
                }
            };

            if !remote_accounts.contains(&account) {
                remote_accounts.push(account);
            }
        }
    }

    Ok(remote_accounts)
}

fn list_accounts(scripter: &mut ScriptUtil) -> EgResult<()> {
    for account in get_active_edi_accounts(scripter)? {
        println!(
            "Account edi-acount-id={} host={} username={} remote-path={}",
            account.remote_account_id().unwrap_or(0),
            account.host(),
            account.username().unwrap_or(""),
            account.remote_path().unwrap_or(""),
        );
    }

    Ok(())
}

/// Returns true if row exists in acq.edi_message with the same file
/// name and remote account connectivity details.
fn edi_message_exists(
    scripter: &mut ScriptUtil,
    account: &RemoteAccount,
    file_name: &str,
) -> EgResult<bool> {
    // acq.edi_account host is scheme-qualified.
    let scheme: &str = account.proto().into();
    let host = format!("{scheme}://{}", account.host());

    let query = eg::hash! {
        "select": {"acqedim": ["id"]},
        "from": {"acqedim": "acqedi"},
        "where": {
            "+acqedim": {
                "remote_file": {
                    "=": {
                        "transform": "evergreen.lowercase",
                        "value": ["evergreen.lowercase", file_name]
                    }
                },
                "status": {"in": ["processed", "proc_error", "trans_error"]}
            },
            "+acqedi": {
                "host": host,
                "username": account.username(), // null-able
                "password": account.password(), // null-able
                "in_dir": account.remote_path(), // null-able
            },
        },
        "limit": 1
    };

    Ok(!scripter.editor_mut().json_query(query)?.is_empty())
}

fn process_one_account(scripter: &mut ScriptUtil, account: &mut RemoteAccount) -> EgResult<()> {
    if let Some(password) = scripter.params().opt_str("password") {
        account.set_password(&password);
    }

    let timeout = scripter
        .params()
        .opt_str("timeout")
        .map(|t| t.parse::<u32>().expect("Timeout should be valid/numeric"))
        .unwrap_or(DEFAULT_TIMEOUT);

    account.set_timeout(timeout);

    account.connect()?;

    if scripter.params().opt_present("list-files") {
        for file in account.ls()?.iter() {
            println!("{file}");
        }
    }

    if scripter.params().opt_present("save-files") {
        let dir = scripter
            .params()
            .opt_str("output-dir")
            .ok_or("--output-dir required to save files")?;

        let mut dir_path = PathBuf::new();
        dir_path.push(&dir);

        if let Some(id) = account.remote_account_id() {
            // Append the account ID to the output directory so we can
            // guarantee a link back from the retrieved file to the
            // EDI account whence it came.
            dir_path.push(format!("edi-account-{id}"));
        }

        // This will error if the directory already exists.  That's fine.
        // We'll fail later if we cannot actually write files to the dir.
        fs::create_dir_all(dir_path.as_path()).ok();

        for file in account.ls()?.iter() {
            let file_path = Path::new(file);

            if let Some(Some(file_name)) = file_path.file_name().map(|s| s.to_str()) {
                if edi_message_exists(scripter, account, file)?
                    && !scripter.params().opt_present("force-save")
                {
                    println!("EDI file already exists: {account} => {file_name}");
                    continue;
                }

                dir_path.push(file_name);
                let out_file = dir_path.as_os_str().to_string_lossy().to_string();

                if Path::new(&out_file).exists() {
                    println!("Skipping existing file: {out_file}");
                } else {
                    println!("Saving file {out_file}");
                    account.get(file, &out_file)?;
                }

                // Remove the file name
                dir_path.pop();
            }
        }
    }

    Ok(())
}
