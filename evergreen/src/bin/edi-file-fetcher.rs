use eg::remote::RemoteAccount;
use eg::script::ScriptUtil;
use eg::EgResult;
use evergreen as eg;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

const HELP_TEXT: &str = r#"

    --url <full-url-string>
        E.g. sftp://user@localhost:2020/edi/pick-up/edifact.*

    --password <password>
        Used in conjunction with --url to manually specify a destination

    --edi-account <edi-account-id>
        When set, all connection information will be extracted
        from the EDI account in the database.

    --list
        List files at the destination.  If the remote path
        contains a file name glob, only files matching the glob
        will be listed.

    --save-files
        Save a local copy of every matching file in the --output-dir.

    --output-dir <directory>
        Location to store fetched files

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
    ops.optflag("", "list", "");
    ops.optflag("", "save-files", "");

    let mut scripter = match ScriptUtil::init(&mut ops, false, Some(HELP_TEXT))? {
        Some(s) => s,
        None => return Ok(()), // e.g. --help
    };

    if let Some(url) = scripter.params().opt_str("url") {
        let mut account = RemoteAccount::from_url_string(&url)?;
        process_one_account(&scripter, &mut account)?;
        return Ok(());
    }

    if let Some(id_str) = scripter.params().opt_str("edi-account") {
        let id = id_str.parse::<i64>().expect("ID should be a number");
        // TODO read_mode will be false if we're writing files...
        let mut account = RemoteAccount::from_edi_account(scripter.editor_mut(), id, true)?;
        process_one_account(&scripter, &mut account)?;
        return Ok(());
    }

    Ok(())
}

fn process_one_account(scripter: &ScriptUtil, account: &mut RemoteAccount) -> EgResult<()> {
    if let Some(password) = scripter.params().opt_str("password") {
        account.set_password(&password);
    }

    account.connect()?;

    if scripter.params().opt_present("list") {
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

        // This will error if the directory already exists.  That's fine.
        // We'll fail later if we cannot actually write files to the dir.
        fs::create_dir_all(dir).ok();

        if scripter.params().opt_present("list") {
            for file in account.ls()?.iter() {
                let file_path = Path::new(file);

                if let Some(Some(file_name)) = file_path.file_name().map(|s| s.to_str()) {
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
    }

    Ok(())
}
