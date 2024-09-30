//! Script utilities.
use crate as eg;
use eg::common::auth;
use eg::db::DatabaseConnection;
use eg::init;
use eg::Editor;
use eg::EgResult;

const HELP_TEXT: &str = "
ScriptUtil Additions:

    --help
        Show help text

    --staff-account
        ID of the account to use where a staff account would typically
        be used, e.g. setting the last editor on a bib record.

    --staff-workstation
        Name of the staff login workstation.  See --staff-acount. Optional.

Database Connector Additions:

    Parameters supported when ScriptUtil is started with a database connection.

    --db-host
    --db-port
    --db-user
    --db-name
";

/// Account ID for updates, e.g. applying an 'editor' value to a bib record update.
const DEFAULT_STAFF_ACCOUNT: i64 = 1;

pub struct ScriptUtil {
    staff_account: i64,
    staff_workstation: Option<String>,
    editor: Option<Editor>,
    params: getopts::Matches,
    db: Option<DatabaseConnection>,
}

impl ScriptUtil {
    /// Parse the command line parameters, connect to Evergreen, and
    /// optionally create a direct database connection.
    ///
    /// Return None if a command line option results in early exit, e.g. --help.
    ///
    /// * `ops` - getopts in progress
    /// * `with_evergreen` - if true, connect to the evergreen/opensrf message
    ///    bus and parse the Evergreen IDL.
    /// * `with_database` - if true, connect to the database.
    /// * `help_text` - Optional script-specific help text.  This text will
    ///    be augmented with ScriptUtil help text.
    pub fn init(
        ops: &mut getopts::Options,
        with_evergreen: bool,
        with_database: bool,
        help_text: Option<&str>,
    ) -> EgResult<Option<ScriptUtil>> {
        ops.optflag("h", "help", "");
        ops.optopt("", "staff-account", "", "");
        ops.optopt("", "staff-workstation", "", "");

        if with_database {
            // Append the datbase-specifc command line options.
            DatabaseConnection::append_options(ops);
        }

        let args: Vec<String> = std::env::args().collect();

        let params = ops
            .parse(&args[1..])
            .map_err(|e| format!("Error parsing options: {e}"))?;

        if params.opt_present("help") {
            println!(
                "{}\n{}",
                help_text.unwrap_or("No Application Help Text Provided"),
                HELP_TEXT
            );
            return Ok(None);
        }

        let sa = DEFAULT_STAFF_ACCOUNT.to_string();
        let staff_account = params.opt_get_default("staff-account", sa).unwrap();
        let staff_account = staff_account
            .parse::<i64>()
            .map_err(|e| format!("Error parsing staff-account value: {e}"))?;

        let staff_workstation = params.opt_str("staff-workstation").map(|v| v.to_string());

        let editor = if with_evergreen {
            let client = init::init()?;
            Some(eg::Editor::new(&client))
        } else {
            None
        };

        let db = if with_database {
            let mut db = DatabaseConnection::new_from_options(&params);
            db.connect()?;
            Some(db)
        } else {
            None
        };

        Ok(Some(ScriptUtil {
            db,
            editor,
            params,
            staff_account,
            staff_workstation,
        }))
    }

    pub fn staff_account(&self) -> i64 {
        self.staff_account
    }

    /// * Panics if "with_evergreen" was set to false at init time.
    pub fn editor_mut(&mut self) -> &mut Editor {
        self.editor.as_mut().unwrap()
    }

    /// * Panics if "with_evergreen" was set to false at init time.
    pub fn editor(&self) -> &Editor {
        self.editor.as_ref().unwrap()
    }

    pub fn params(&self) -> &getopts::Matches {
        &self.params
    }

    /// Returns the active authtoken
    ///
    /// Panics if no auth session is present.
    pub fn authtoken(&self) -> &str {
        self.editor().authtoken().unwrap()
    }

    /// Returns a mutable ref to our database connection
    ///
    /// * Panics if the database connection was not initialized.
    ///
    pub fn db(&mut self) -> &mut DatabaseConnection {
        self.db
            .as_mut()
            .expect("database connection should be established")
    }

    /// Create an internal login session using the provided staff_account.
    ///
    /// Auth session is also linked to our Editor instance.
    ///
    /// Returns the auth token.
    pub fn login_staff(&mut self) -> EgResult<String> {
        let mut args = auth::InternalLoginArgs::new(self.staff_account, auth::LoginType::Staff);

        if let Some(ws) = self.staff_workstation.as_ref() {
            args.set_workstation(ws);
        }

        let ses = auth::Session::internal_session_api(self.editor_mut().client_mut(), &args)?;

        if let Some(s) = ses {
            self.editor_mut().apply_authtoken(s.token())?;
            Ok(s.token().to_string())
        } else {
            Err("Could not retrieve auth session".into())
        }
    }
}
