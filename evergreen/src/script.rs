/*
 * Script utilities.
 */
use crate as eg;
use eg::common::auth;
use eg::db::DatabaseConnection;
use eg::init;
use eg::Editor;
use eg::EgResult;

/// Account ID for updates, e.g. applying an 'editor' value to a bib record update.
const DEFAULT_STAFF_ACCOUNT: i64 = 1;

pub struct ScriptUtil {
    staff_account: i64,
    editor: Editor,
    params: getopts::Matches,
    db: Option<DatabaseConnection>,
}

impl ScriptUtil {
    pub fn init(
        ops: &mut getopts::Options,
        with_database: bool,
        help_text: Option<&str>,
    ) -> EgResult<Option<ScriptUtil>> {
        ops.optflag("h", "help", "");
        ops.optopt("", "staff-account", "", "");

        if with_database {
            DatabaseConnection::append_options(ops);
        }

        let args: Vec<String> = std::env::args().collect();

        let params = ops
            .parse(&args[1..])
            .map_err(|e| format!("Error parsing options: {e}"))?;

        if params.opt_present("help") {
            println!("{}", help_text.unwrap_or("No Help Text Provided"));
            return Ok(None);
        }

        let sa = DEFAULT_STAFF_ACCOUNT.to_string();
        let staff_account = params.opt_get_default("staff-account", sa).unwrap();
        let staff_account = staff_account
            .parse::<i64>()
            .map_err(|e| format!("Error parsing staff-account value: {e}"))?;

        let client = init::init()?;
        let editor = eg::Editor::new(&client);

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
        }))
    }

    pub fn staff_account(&self) -> i64 {
        self.staff_account
    }

    pub fn editor_mut(&mut self) -> &mut Editor {
        &mut self.editor
    }

    pub fn params(&self) -> &getopts::Matches {
        &self.params
    }

    /// Returns the active authtoken
    ///
    /// Panics if no auth session is present.
    pub fn authtoken(&self) -> &str {
        self.editor.authtoken().unwrap()
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
        let ses = auth::Session::internal_session_api(
            self.editor.client_mut(),
            &auth::InternalLoginArgs::new(self.staff_account, auth::LoginType::Staff),
        )?;

        if let Some(s) = ses {
            self.editor.apply_authtoken(s.token())?;
            Ok(s.token().to_string())
        } else {
            Err("Could not retrieve auth session".into())
        }
    }
}
