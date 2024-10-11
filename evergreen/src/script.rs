//! Script utilities.
use crate as eg;
use eg::common::auth;
use eg::db::DatabaseConnection;
use eg::init;
use eg::Editor;
use eg::EgResult;

const HELP_TEXT: &str = "
Runner Additions:

    --help
        Show help text

    --staff-account
        ID of the account to use where a staff account would typically
        be used, e.g. setting the last editor on a bib record.

    --staff-workstation
        Name of the staff login workstation.  See --staff-acount. Optional.

Database Connector Additions:

    Parameters supported when Runner is started with a database connection.

    --db-host
    --db-port
    --db-user
    --db-name
";

/// Account ID for updates, e.g. applying an 'editor' value to a bib record update.
const DEFAULT_STAFF_ACCOUNT: i64 = 1;

pub struct Options {
    /// Getops Options prepopulated with script-local parameter definitions.
    pub options: Option<getopts::Options>,

    /// Connect to the Evergreen/OpenSRF message bus and load the
    /// Evergreen IDL.
    pub with_evergreen: bool,

    /// Connect to the Evergreen database and append the database
    /// options to the getops parameters.
    pub with_database: bool,

    /// Tell the world what your script does and how to use it.
    pub help_text: Option<String>,

    /// Pass additional command line options to the getopts parser.
    ///
    /// These are appended to parameters collected from the command line.
    pub extra_params: Option<Vec<String>>,
}

pub struct Runner {
    staff_account: i64,
    staff_workstation: Option<String>,
    editor: Option<Editor>,
    params: getopts::Matches,
    db: Option<DatabaseConnection>,
}

impl Runner {
    /// Parse the command line parameters, connect to Evergreen, and
    /// optionally create a direct database connection.
    ///
    /// Return None if a command line option results in early exit, e.g. --help.
    ///
    /// * `options` - Script options.
    pub fn init(mut options: Options) -> EgResult<Option<Runner>> {
        let mut ops_binding = None;

        let ops = options.options.as_mut().unwrap_or_else(|| {
            ops_binding = Some(getopts::Options::new());
            ops_binding.as_mut().unwrap()
        });

        ops.optflag("h", "help", "");
        ops.optopt("", "staff-account", "", "");
        ops.optopt("", "staff-workstation", "", "");

        if options.with_database {
            // Append the datbase-specifc command line options.
            DatabaseConnection::append_options(ops);
        }

        let mut args: Vec<String> = std::env::args().collect();

        if let Some(extras) = options.extra_params.as_mut() {
            args.append(extras);
        }

        let params = ops
            .parse(&args[1..])
            .map_err(|e| format!("Error parsing options: {e}"))?;

        if params.opt_present("help") {
            println!(
                "{}\n{}",
                options
                    .help_text
                    .unwrap_or("No Application Help Text Provided".to_string()),
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

        let editor = if options.with_evergreen {
            let client = init::init()?;
            Some(eg::Editor::new(&client))
        } else {
            None
        };

        let db = if options.with_database {
            let mut db = DatabaseConnection::new_from_options(&params);
            db.connect()?;
            Some(db)
        } else {
            None
        };

        Ok(Some(Runner {
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
