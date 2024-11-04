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
        Used in conjunction with --url to manually specify a password

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
    ops.optflag("", "process-files", "");
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
    local_file: &Path,
) -> EgResult<bool> {
    // acq.edi_account host is scheme-qualified.
    let scheme: &str = account.proto().into();
    let host = format!("{scheme}://{}", account.host());

    let Some(Some(file_name)) = local_file.file_name().map(|s| s.to_str()) else {
        return Err(format!("Local EDI file has no name: {local_file:?}").into());
    };

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

/// Fine or create the base and archive directories for an EDI account.
///
/// Returns (base_path, archive_path)
fn create_account_directories(
    scripter: &mut script::Runner,
    account: &mut RemoteAccount,
) -> EgResult<(PathBuf, PathBuf)> {
    let base_dir = scripter
        .params()
        .opt_str("output-dir")
        .ok_or("--output-dir required to save or process files")?;

    // When testing direct URL-bases connections, there will be
    // no account id.  Use ID 0 as the catch-all
    let account_id = account.remote_account_id().unwrap_or(0);

    let mut base_path = PathBuf::new();

    // Base path for all EDI file output
    base_path.push(&base_dir);

    // Append the account ID to the output directory so we can
    // guarantee a link back from the retrieved file to the
    // EDI account whence it came.
    base_path.push(format!("edi-account-{account_id}"));

    let mut archive_path = PathBuf::new();
    archive_path.push(&base_path);
    archive_path.push(ARCHIVE_DIR);

    // This will also error if the directory already exists.
    fs::create_dir_all(base_path.as_path()).ok();

    if !base_path.try_exists().unwrap_or(false) {
        return Err(format!("Cannot create directory {base_path:?}").into());
    }

    if !archive_path.try_exists().unwrap_or(false) {
        fs::create_dir(&archive_path).map_err(|e| format!("Cannot create archive dir: {e}"))?;
    }

    Ok((base_path, archive_path))
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

    let (base_path, archive_path) = create_account_directories(scripter, account)?;

    if save_files {
        for remote_file in account.ls()?.iter() {
            save_one_file(scripter, account, &base_path, &archive_path, remote_file)?;
        }
    }

    if process_files {
        // Process all files in the local output directory for this EDI account.

        // We need an authtoken to process EDI files.
        if scripter.editor().authtoken().is_none() {
            scripter.login_staff()?;
        };

        let file_list = fs::read_dir(&base_path)
            .map_err(|e| format!("Cannot list files in directory: {base_path:?} {e}"))?;

        for local_file_res in file_list {
            let local_file = local_file_res.map_err(|e| format!("Cannot read file: {e}"))?;

            if local_file.path().is_dir() {
                // Avoid processing the archive directory
                continue;
            }

            let local_file_name = local_file.file_name();

            // Note that just because the API successfully created and
            // EDI message, it does not mean it was successfully processed
            // as an EDI file.
            if let Err(e) = process_edi_file(scripter, account, &local_file.path()) {
                eprintln!("Error processing EDI file: {e}");
            }

            let mut path = archive_path.clone();
            path.push(local_file_name);

            fs::rename(local_file.path(), &path)
                .map_err(|e| format!("Cannot archive EDI file: {e}"))?;
        }
    }

    Ok(())
}

/// Retrieve one file from the remote location and save it locally.
fn save_one_file(
    scripter: &mut script::Runner,
    account: &mut RemoteAccount,
    base_path: &Path,
    archive_path: &Path,
    remote_file: &str,
) -> EgResult<()> {
    let remote_file_path = Path::new(remote_file);
    let mut file_path = base_path.to_path_buf();

    let Some(Some(file_name)) = remote_file_path.file_name().map(|s| s.to_str()) else {
        eprintln!("Remote file has no file name: {remote_file}");
        return Ok(()); // skip it.
    };

    println!("Fetching remote EDI file {remote_file}");

    // Verify we don't already have a local copy
    file_path.push(file_name);
    if file_path.try_exists().unwrap_or(false) {
        println!("EDI file already exists locally: {file_path:?}");
        return Ok(());
    }

    // Verify we don't have a copy in the archive directory.
    let mut path = archive_path.to_path_buf();
    path.push(file_name);
    if path.try_exists().unwrap_or(false) {
        println!("EDI file already exists in archive: {path:?}");
        return Ok(());
    }

    // Verify we don't have a bzip2 copy in the archive directory.
    let mut path = archive_path.to_path_buf();
    path.push(file_name);
    path.set_extension("bz2");
    if path.try_exists().unwrap_or(false) {
        println!("EDI file already exists in archive: {path:?}");
        return Ok(());
    }

    if !scripter.params().opt_present("force-save") {
        let exists = edi_message_exists(scripter, account, &file_path)?;

        if exists {
            println!("EDI message already exists with file name: {file_name}");
            return Ok(());
        }
    }

    let local_file = file_path.display().to_string();

    println!("Saving file {local_file}");

    account.get(remote_file, &local_file)?;

    Ok(())
}

/// Send a file to the acq API for processing.
fn process_edi_file(
    scripter: &mut script::Runner,
    account: &RemoteAccount,
    file_path: &Path,
) -> EgResult<()> {
    let local_file = file_path.display();

    println!("Processing local EDI file {local_file}");

    let Some(account_id) = account.remote_account_id() else {
        return Err("Cannot process EDI files without an account ID".into());
    };

    if edi_message_exists(scripter, account, file_path)? {
        println!("Already processed EDI file {local_file}");
        return Ok(());
    }

    let params: Vec<EgValue> = vec![
        scripter.authtoken().into(),
        account_id.into(),
        local_file.to_string().into(),
    ];

    let resp = scripter.editor_mut().send_recv_one(
        "open-ils.acq",
        "open-ils.acq.edi.file.process",
        params,
    )?;

    if let Some(val) = resp.as_ref() {
        if val.is_numeric() {
            println!("Successfully processed EDI file {local_file} => ID {val}");
            return Ok(());
        }
    }

    Err(format!("Failed to process EDI file: {resp:?}").into())
}
