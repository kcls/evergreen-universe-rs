use eg::remote::RemoteAccount;
use eg::script;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

const DEFAULT_TIMEOUT: u32 = 10;

const ARCHIVE_DIR: &str = "archive";
const ERROR_DIR: &str = "error";

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

    --process-files
        Send locally saved files to the open-ils.acq API for processing.

        This is experimental and requires Evergreen API changes.

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

    let options = script::Options {
        with_evergreen: true,
        with_database: false,
        help_text: Some(HELP_TEXT.to_string()),
        extra_params: None,
        options: Some(ops),
    };

    let mut scripter = match script::Runner::init(options)? {
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
            if let Err(e) = process_one_account(&mut scripter, &mut account) {
                eprintln!("Error process files for account: {account}: {e}");
            }
        }
    }

    Ok(())
}

/// Returns a set of RemoteAccounts mapped from a unique set of EDI
/// accounts, linked to active providers.
///
/// Accounts are uniqued by host/directory/etc.
fn get_active_edi_accounts(scripter: &mut script::Runner) -> EgResult<Vec<RemoteAccount>> {
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

fn list_accounts(scripter: &mut script::Runner) -> EgResult<()> {
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
    scripter: &mut script::Runner,
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

/// List, fetch, process remote EDI files for a single EDI account.
fn process_one_account(scripter: &mut script::Runner, account: &mut RemoteAccount) -> EgResult<()> {
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

    let save_files = scripter.params().opt_present("save-files");
    let process_files = scripter.params().opt_present("process-files");

    if !save_files && !process_files {
        return Ok(());
    }

    let out_dir = scripter
        .params()
        .opt_str("output-dir")
        .ok_or("--output-dir required to save or process files")?;

    let account_id = account.remote_account_id().expect("Account ID should be set");

    let mut local_base_path = PathBuf::new();
    local_base_path.push(&out_dir);

    if save_files {
        // Append the account ID to the output directory so we can
        // guarantee a link back from the retrieved file to the
        // EDI account whence it came.
        local_base_path.push(format!("edi-account-{account_id}"));

        // This will also error if the directory already exists.
        fs::create_dir_all(local_base_path.as_path()).ok();

        if let Ok(true) = local_base_path.try_exists() {
            // Directory existed or we successfully created it.
        } else {
            return Err(format!("Cannot create directory {local_base_path:?}").into());
        }

        for remote_file in account.ls()?.iter() {
            save_one_file(scripter, account, &mut local_base_path, remote_file)?;
        }
    }

    if process_files {
        // Process all files in the local output directory for this EDI account.

        // We need an authtoken to process EDI files.
        if scripter.editor().authtoken().is_none() {
            scripter.login_staff()?;
        };

        let file_list = fs::read_dir(&out_dir).map_err(|e|
            format!("Cannot list files in directory: {out_dir} {e}"))?;

        for local_file_res in file_list {
            let local_file = local_file_res.map_err(|e| format!("Cannot read file: {e}"))?;

            let local_file = local_file.path().as_os_str().to_string_lossy().to_string();

            match process_edi_file(scripter, account_id, &local_file) {
                Ok(()) => {
                    println!("Successfully processed {local_file}");

                    local_base_path.push(ARCHIVE_DIR);
                    if let Err(e) = fs::rename(&local_file, &local_base_path) {
                        eprintln!("Cannot archive EDI file: {e}");
                    }
                    local_base_path.pop();
                }
                Err(e) => {
                    eprintln!("{e}");

                    local_base_path.push(ERROR_DIR);
                    if let Err(e) = fs::rename(&local_file, &local_base_path) {
                        eprintln!("Cannot archive EDI file: {e}");
                    }
                    local_base_path.pop();
                }
            }
        }
    }

    Ok(())
}

/// Retrieve one file from the remote location and save it locally.
fn save_one_file(
    scripter: &mut script::Runner,
    account: &mut RemoteAccount,
    local_base_path: &mut PathBuf,
    remote_file: &str,
) -> EgResult<()> {
    let remote_file_path = Path::new(remote_file);

    let Some(Some(file_name)) = remote_file_path.file_name().map(|s| s.to_str()) else {
        eprintln!("Remote file has no file name: {remote_file}");
        return Ok(()); // skip it.
    };

    println!("Fetching remote EDI file {remote_file}");

    // Local file is the local base path plus the file name
    local_base_path.push(file_name);

    let local_file = local_base_path.as_os_str().to_string_lossy().to_string();

    // Remove the file name so the local base path can be reused.
    local_base_path.pop();

    if edi_message_exists(scripter, account, &local_file)?
        && !scripter.params().opt_present("force-save")
    {
        println!("EDI file already exists: {account} => {file_name}");
        return Ok(());
    }

    println!("Saving file {local_file}");

    account.get(remote_file, &local_file)?;

    Ok(())
}


/// Send a file to the acq API for processing.
fn process_edi_file(
    scripter: &mut script::Runner,
    account_id: i64,
    local_file: &str,
) -> EgResult<()> {
    println!("Processing local EDI file {local_file}");

    let params: Vec<EgValue> = vec![
        scripter.authtoken().into(),
        account_id.into(),
        local_file.into(),
    ];

    let resp = scripter.editor_mut().send_recv_one(
        "open-ils.acq",
        "open-ils.acq.edi.file.process",
        params
    )?;

    if let Some(val) = resp.as_ref() {
        if val.is_number() {
            println!("Successfully processed EDI file {local_file} => ID {val}");
            return Ok(());
        }
    }

    Err(format!("Failed to process EDI file: {resp:?}").into())
}
