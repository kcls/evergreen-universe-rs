//! Create, connect, and manage database connections.
use crate::result::EgResult;
use getopts;
use log::debug;
use postgres as pg;
use std::cell::RefCell;
use std::env;
use std::rc::Rc;

const DEFAULT_DB_PORT: u16 = 5432;
const DEFAULT_DB_HOST: &str = "localhost";
const DEFAULT_DB_USER: &str = "evergreen";
const DEFAULT_DB_NAME: &str = "evergreen";

const SUPPORTED_OPERATORS: [&str; 20] = [
    "IS",
    "IS NOT",
    "IN",
    "NOT IN",
    "LIKE",
    "ILIKE",
    "<",
    "<=",
    ">",
    ">=",
    "<>",
    "!=",
    "~",
    "=",
    "!~",
    "!~*",
    "~*",
    "SIMILAR TO",
    "IS DISTINCT FROM",
    "IS NOT DISTINCT FROM",
];

/// For compiling a set of connection parameters
///
/// Values are applied like so:
///
/// 1. Manually applying a value via set_* method
/// 2. Values provided via getopts::Matches struct.
/// 3. Values pulled from the environment (e.g. PGHOST) where possible.
/// 4. Default values defined in this module.
#[derive(Default)]
pub struct DatabaseConnectionBuilder {
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    application: Option<String>,
}

impl DatabaseConnectionBuilder {
    /// Create a new database connection with default values
    ///
    /// # Example
    ///
    /// ```
    /// use evergreen::db::DatabaseConnectionBuilder;
    ///
    /// let builder = DatabaseConnectionBuilder::new();
    /// let connection = builder.build();
    /// assert_eq!(connection.dsn(), "host=localhost port=5432 user=evergreen dbname=evergreen");
    /// ```
    pub fn new() -> Self {
        Default::default()
    }

    /// Set connection values via getopts matches.
    ///
    /// Values are only applied where values do not already exist.
    /// This generally means a set_* method has higher precedence
    /// than a set of getopts matches.
    ///
    /// Supported options:
    ///     --db-host
    ///     --db-port
    ///     --db-user
    ///     --db-name
    ///
    /// # Example
    ///
    /// ```
    /// use evergreen::db::{DatabaseConnectionBuilder, DatabaseConnection};
    ///
    /// let mut options = getopts::Options::new();
    /// DatabaseConnection::append_options(&mut options);
    /// let matches = options.parse([
    ///   "--db-host=my-host",
    ///   "--db-port=1234",
    ///   "--db-user=my-user",
    ///   "--db-pass=my-password",
    ///   "--db-name=evergreen-db"
    /// ]).unwrap();
    /// let mut builder = DatabaseConnectionBuilder::new();
    ///
    /// builder.set_opts(&matches);
    ///
    /// let connection = builder.build();
    /// assert_eq!(connection.dsn(), "host=my-host port=1234 user=my-user dbname=evergreen-db password=my-password");
    /// ```
    pub fn set_opts(&mut self, params: &getopts::Matches) {
        if self.host.is_none() && params.opt_defined("db-host") {
            self.host = params.opt_str("db-host");
        }

        if self.user.is_none() && params.opt_defined("db-user") {
            self.user = params.opt_str("db-user");
        }

        if self.password.is_none() && params.opt_defined("db-pass") {
            self.password = params.opt_str("db-pass");
        }

        if self.database.is_none() && params.opt_defined("db-name") {
            self.database = params.opt_str("db-name");
        }

        if self.port.is_none() && params.opt_defined("db-port") {
            if let Some(v) = params.opt_str("db-port") {
                self.port = Some(v.parse::<u16>().unwrap());
            }
        }
    }

    /// # Example
    ///
    /// ```
    /// use evergreen::db::DatabaseConnectionBuilder;
    ///
    /// let mut builder = DatabaseConnectionBuilder::new();
    ///
    /// builder.set_host("/var/run/postgres"); // The host can be a domain socket or a hostname
    ///
    /// let connection = builder.build();
    /// assert_eq!(connection.dsn(), "host=/var/run/postgres port=5432 user=evergreen dbname=evergreen");
    /// ```
    pub fn set_host(&mut self, host: &str) {
        self.host = Some(host.to_string())
    }

    pub fn set_port(&mut self, port: u16) {
        self.port = Some(port);
    }

    pub fn set_user(&mut self, user: &str) {
        self.user = Some(user.to_string());
    }

    pub fn set_password(&mut self, password: &str) {
        self.password = Some(password.to_string());
    }

    pub fn set_database(&mut self, database: &str) {
        self.database = Some(database.to_string());
    }

    /// Set an application for your database connection, so that it's
    /// easy to identify while monitoring and troubleshooting.
    ///
    /// ```
    /// use evergreen::db::DatabaseConnectionBuilder;
    ///
    /// let mut builder = DatabaseConnectionBuilder::new();
    ///
    /// builder.set_application("open-ils.booking@appserver3(thread_123)");
    ///
    /// let connection = builder.build();
    /// assert_eq!(
    ///   connection.dsn(),
    ///   "host=localhost port=5432 user=evergreen dbname=evergreen application_name=open-ils.booking@appserver3(thread_123)"
    /// );
    /// ```
    pub fn set_application(&mut self, application: &str) {
        self.application = Some(application.to_string());
    }

    fn from_env(name: &str) -> Option<String> {
        env::vars()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.to_string())
    }

    /// Create the final database connection object from the collected
    /// parameters.
    pub fn build(self) -> DatabaseConnection {
        let host = match self.host {
            Some(h) => h,
            None => match DatabaseConnectionBuilder::from_env("PGHOST") {
                Some(h) => h,
                None => DEFAULT_DB_HOST.to_string(),
            },
        };

        let mut pass = self.password;

        if pass.is_none() {
            pass = DatabaseConnectionBuilder::from_env("PGPASS");
        };

        let user = match self.user {
            Some(h) => h,
            None => match DatabaseConnectionBuilder::from_env("PGUSER") {
                Some(h) => h,
                None => DEFAULT_DB_USER.to_string(),
            },
        };

        let database = match self.database {
            Some(h) => h,
            None => match DatabaseConnectionBuilder::from_env("PGDATABASE") {
                Some(h) => h,
                None => DEFAULT_DB_NAME.to_string(),
            },
        };

        let port = match self.port {
            Some(h) => h,
            None => match DatabaseConnectionBuilder::from_env("PGPORT") {
                Some(h) => h.parse::<u16>().unwrap(),
                None => DEFAULT_DB_PORT,
            },
        };

        DatabaseConnection {
            host,
            port,
            user,
            database,
            password: pass,
            application: self.application,
            client: None,
            in_transaction: false,
        }
    }
}

/// Wrapper for a postgres::Client with connection metadata.
pub struct DatabaseConnection {
    client: Option<pg::Client>,
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
    database: String,
    application: Option<String>,
    in_transaction: bool,
}

impl Drop for DatabaseConnection {
    fn drop(&mut self) {
        // This is probably unnecessary, since I expect the PG backend
        // will automatically rollback, but let's make it official.
        // The pg::Client will close its own connection once it's dropped.
        if self.in_transaction {
            self.xact_rollback().ok();
        }
    }
}

impl DatabaseConnection {
    /// Add options to an in-progress getopts::Options related to creating
    /// a database connection.
    ///
    /// # Examples
    ///
    /// Without using append_options:
    ///
    /// ```should_panic
    /// let options = getopts::Options::new();
    ///
    /// // The following line will panic because options doesn't know about --db-host
    /// let matches = options.parse(["--db-host=my-host"]).unwrap(); // panics!
    /// ```
    ///
    /// With append_options:
    ///
    /// ```
    /// use evergreen::db::DatabaseConnection;
    ///
    /// let mut options = getopts::Options::new();
    /// DatabaseConnection::append_options(&mut options);
    ///
    /// // The following line is okay, since append_options taught options about --db-host
    /// let matches = options.parse(["--db-host=my-host"]).unwrap();
    /// assert_eq!(matches.opt_str("db-host").unwrap(), "my-host");
    /// ```
    pub fn append_options(options: &mut getopts::Options) {
        options.optopt("", "db-host", "Database Host", "DB_HOST");
        options.optopt("", "db-port", "Database Port", "DB_PORT");
        options.optopt("", "db-user", "Database User", "DB_USER");
        options.optopt("", "db-pass", "Database Password", "DB_PASSWORD");
        options.optopt("", "db-name", "Database Name", "DB_NAME");
    }

    pub fn builder() -> DatabaseConnectionBuilder {
        DatabaseConnectionBuilder::new()
    }

    /// Create a new DB connection from a set of gettops matches.
    ///
    /// # Example
    ///
    /// ```
    /// use evergreen::db::DatabaseConnection;
    ///
    /// let mut options = getopts::Options::new();
    /// DatabaseConnection::append_options(&mut options);
    /// let matches = options.parse([
    ///   "--db-host=my-host",
    ///   "--db-port=1234",
    ///   "--db-user=my-user",
    ///   "--db-pass=my-password",
    ///   "--db-name=evergreen-db"
    /// ]).unwrap();
    ///
    /// let mut connection = DatabaseConnection::new_from_options(&matches);
    ///
    /// assert_eq!(connection.dsn(), "host=my-host port=1234 user=my-user dbname=evergreen-db password=my-password");
    /// assert_eq!(connection.in_transaction(), false);
    /// ```
    pub fn new_from_options(params: &getopts::Matches) -> Self {
        let mut builder = DatabaseConnectionBuilder::new();
        builder.set_opts(params);
        builder.build()
    }

    /// Our database connection string.
    ///
    /// * `redact` - Set the password to "[REDACTED]" in the connection string.
    fn dsn_internal(&self, redact: bool) -> String {
        let mut dsn = format!(
            "host={} port={} user={} dbname={}",
            self.host, self.port, self.user, self.database
        );

        if let Some(ref app) = self.application {
            dsn += &format!(" application_name={}", app);
        }

        if redact {
            dsn += " password=[REDACTED]";
        } else if let Some(ref p) = self.password {
            dsn += &format!(" password={}", p);
        }

        dsn
    }

    /// Our database connection string, including the password if available.
    pub fn dsn(&self) -> String {
        self.dsn_internal(false)
    }

    /// Our database connection string minus the password
    fn redacted_dsn(&self) -> String {
        self.dsn_internal(true)
    }

    /// Mutable client ref
    ///
    /// Panics if the client is not yet connected / created.
    pub fn client(&mut self) -> &mut pg::Client {
        if self.client.is_none() {
            panic!("DatabaseConnection is not connected!");
        }

        self.client.as_mut().unwrap()
    }

    /// Connect to the database
    ///
    /// Non-TLS connections only supported at present.
    pub fn connect(&mut self) -> EgResult<()> {
        let dsn = self.dsn();
        debug!("Connecting to DB {dsn}");

        match pg::Client::connect(&dsn, pg::NoTls) {
            Ok(c) => {
                self.client = Some(c);
                Ok(())
            }
            Err(e) => Err(format!(
                "Error connecting to database with params {}: {e}",
                self.redacted_dsn()
            )
            .into()),
        }
    }

    pub fn disconnect(&mut self) {
        debug!("Disconnecting from DB {}", self.dsn());
        self.client = None;
    }

    /// Disconect + connect to PG.
    ///
    /// Useful for releasing PG resources mid-script.
    pub fn reconnect(&mut self) -> EgResult<()> {
        self.disconnect();
        self.connect()
    }

    /// Clone everything except the actual PG client.
    ///
    /// Let the caller decide when/if/how a new connection to the database
    /// is created.
    pub fn partial_clone(&self) -> DatabaseConnection {
        DatabaseConnection {
            client: None,
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
            password: self.password.clone(),
            database: self.database.clone(),
            application: self.application.clone(),
            in_transaction: false,
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    /// Start a new transaction on this database connection.
    ///
    /// There is a pg::Transaction object we could use instead, but it's
    /// a wrapper around the pg::Client, which we also have a reference
    /// to, so it causes all kinds of hassle with lifetimes and RefCell
    /// borrows.  This means we can only have one open transaction per
    /// DatabaseConnection.
    pub fn xact_begin(&mut self) -> EgResult<()> {
        if self.in_transaction {
            return Err("DatabaseConnection is already in a transaction".into());
        }
        self.in_transaction = true;
        match self.client().execute("BEGIN", &[]) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("BEGIN transaction error: {e}").into()),
        }
    }

    pub fn xact_commit(&mut self) -> EgResult<()> {
        if !self.in_transaction {
            return Err("DatabaseConnection has no transaction to commit".to_string())?;
        }
        self.in_transaction = false;
        match self.client().execute("COMMIT", &[]) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("COMMIT transaction error: {e}").into()),
        }
    }

    pub fn xact_rollback(&mut self) -> EgResult<()> {
        if !self.in_transaction {
            log::warn!("No transaction to roll back");
            return Ok(()); // error as well?
        }
        self.in_transaction = false;
        match self.client().execute("ROLLBACK", &[]) {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("ROLLBACK transaction error: {e}").into()),
        }
    }

    pub fn into_shared(self) -> Rc<RefCell<DatabaseConnection>> {
        Rc::new(RefCell::new(self))
    }
}

/// Determine whether a string is potentially a valid SQL identifier.
pub fn is_identifier(s: &str) -> bool {
    let s = s.trim();
    for c in s.chars() {
        // NOTE: is the 'ascii' check to strict?
        if c.is_ascii_alphanumeric() || c == '$' || c == '_' || c == '.' {
            // OK
        } else {
            return false;
        }
    }
    !s.is_empty()
}

/// Verify a query operator provided by the caller is allowed.
pub fn is_supported_operator(op: &str) -> bool {
    SUPPORTED_OPERATORS.contains(&op.to_uppercase().as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_it_can_redact_dsn() {
        let mut builder = DatabaseConnection::builder();
        builder.set_password("my_secret_password");
        let connection = builder.build();

        assert_eq!(
            connection.redacted_dsn(),
            "host=localhost port=5432 user=evergreen dbname=evergreen password=[REDACTED]"
        );
        assert!(connection.dsn().contains("my_secret_password"));
        assert!(!connection.redacted_dsn().contains("my_secret_password"));
    }
}
