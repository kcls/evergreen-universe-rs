//! Script utilities.
use crate as eg;
use eg::common::auth;
use eg::date;
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

    --log-stdout
        Log announcements to STDOUT in addition to log::info!().

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

/// Parts of a script runner which are thread-Sendable.
///
/// Useful for cloning the sendable parts of a Runner, passing it to
/// a thread, then reconstituting a Runner on the other side.
#[derive(Debug, Clone)]
pub struct RunnerCore {
    staff_account: i64,
    staff_workstation: Option<String>,
    authtoken: Option<String>,
    params: getopts::Matches,
    log_prefix: Option<String>,
    log_stdout: bool,
}

/// Core runner plus non-sendable components (editor, db).
pub struct Runner {
    core: RunnerCore,
    editor: Option<Editor>,
    db: Option<DatabaseConnection>,
}

impl From<RunnerCore> for Runner {
    fn from(core: RunnerCore) -> Self {
        Runner {
            core,
            editor: None,
            db: None,
        }
    }
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
        ops.optflag("", "log-stdout", "");
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
        let log_stdout = params.opt_present("log-stdout");

        let mut runner = Runner {
            db: None,
            editor: None,
            core: RunnerCore {
                params,
                staff_account,
                staff_workstation,
                log_stdout,
                authtoken: None,
                log_prefix: None,
            },
        };

        if options.with_database {
            runner.connect_db()?;
        }

        if options.with_evergreen {
            // Avoid using self.connect_evergreen() here since that
            // variant simply connects to the bus and does not
            // initialize the IDL, logging, etc.
            let client = init::init()?;
            runner.editor = Some(eg::Editor::new(&client));
        }

        Ok(Some(runner))
    }

    /// Connect to the database.
    pub fn connect_db(&mut self) -> EgResult<()> {
        let mut db = DatabaseConnection::new_from_options(self.params());
        db.connect()?;
        self.db = Some(db);
        Ok(())
    }

    /// Connects to the Evergreen bus.
    ///
    /// Does not parse the IDL, etc., assuming those steps have already
    /// been taken.
    pub fn connect_evergreen(&mut self) -> EgResult<()> {
        let client = eg::Client::connect()?;
        self.editor = Some(eg::Editor::new(&client));
        Ok(())
    }

    /// Our core.
    pub fn core(&self) -> &RunnerCore {
        &self.core
    }

    /// Send messages to log::info! and additoinally log messages to
    /// STDOUT when self.core.log_stdout is true.
    ///
    /// Log prefix is appplied when set.
    pub fn announce(&self, msg: &str) {
        let pfx = self.core.log_prefix.as_deref().unwrap_or("");
        if self.core.log_stdout {
            println!("{} {pfx}{msg}", date::now().format("%F %T%.3f"));
        }
        log::info!("{pfx}{msg}");
    }

    /// Set the announcement log prefix.
    ///
    /// Append a space so we don't have to do that at log time.
    pub fn set_log_prefix(&mut self, p: &str) {
        self.core.log_prefix = Some(p.to_string() + " ");
    }

    /// Apply an Editor.
    ///
    /// This does not propagate the authtoken or force the editor
    /// to fetch/set its requestor value.  If needed, call
    /// editor.apply_authtoken(script.authtoken()).
    pub fn set_editor(&mut self, e: Editor) {
        self.editor = Some(e);
    }

    /// Returns the staff account value.
    pub fn staff_account(&self) -> i64 {
        self.core.staff_account
    }

    /// Mutable ref to our editor.
    ///
    /// # Panics
    ///
    /// Panics if `with_evergreen` was false during init and no calls
    /// to set_editor were made.
    pub fn editor_mut(&mut self) -> &mut Editor {
        self.editor.as_mut().unwrap()
    }

    /// Ref to our Editor.
    ///
    /// # Panics
    ///
    /// Panics if `with_evergreen` was false during init and no calls
    /// to set_editor were made.
    pub fn editor(&self) -> &Editor {
        self.editor.as_ref().unwrap()
    }

    /// Ref to our compiled command line parameters.
    pub fn params(&self) -> &getopts::Matches {
        &self.core.params
    }

    /// Returns the active authtoken
    ///
    /// # Panics
    ///
    /// Panics if no auth session is present.
    pub fn authtoken(&self) -> &str {
        self.core.authtoken.as_deref().unwrap()
    }

    /// Returns a mutable ref to our database connection
    ///
    /// # Panics
    ///
    /// Panics if the database connection was not initialized.
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
        let mut args =
            auth::InternalLoginArgs::new(self.core.staff_account, auth::LoginType::Staff);

        if let Some(ws) = self.core.staff_workstation.as_ref() {
            args.set_workstation(ws);
        }

        let ses = auth::Session::internal_session_api(self.editor_mut().client_mut(), &args)?;

        if let Some(s) = ses {
            self.editor_mut().apply_authtoken(s.token())?;
            self.core.authtoken = Some(s.token().to_string());
            Ok(s.token().to_string())
        } else {
            Err("Could not retrieve auth session".into())
        }
    }
}
