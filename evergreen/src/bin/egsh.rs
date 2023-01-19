use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::time::Instant;

use getopts;
use rustyline;

use eg::auth::AuthSession;
use eg::db::DatabaseConnection;
use eg::idl;
use eg::idldb;
use eg::init;
use eg::event;
use evergreen as eg;

//const PROMPT: &str = "egsh# ";
const PROMPT: &str = "\x1b[1;32megsh# \x1b[0m";
const HISTORY_FILE: &str = ".egsh_history";
const SEPARATOR: &str = "---------------------------------------------------";
const DEFAULT_REQUEST_TIMEOUT: i32 = 120;
const DEFAULT_JSON_PRINT_DEPTH: u16 = 2;

const HELP_TEXT: &str = r#"
Options

    --with-database
        Connect directly to an Evergreen database.

    Standard OpenSRF command line options (e.g. --osrf-config)
    also supported.

Commands

    idl get <classname> <pkey-value>
        Retrieve and IDL-classed object by primary key.

    idl search <classname> <field> <operand> <value>
        Examples:
            idl search aou name ~* "branch"
            idl search aout depth > 1

    idlf ...
        Same as 'idl' commands but values are displayed as formatted
        key / value pairs, minus NULL values.

    db sleep <seconds>
        Runs PG_SLEEP(<seconds>).  Mostly for debugging.

    login <username> <password> [<login_type>, <workstation>]

    router <domain> <command> [<router_class>]
        Sends <command> to the router at <domain> and reports the result.
        Specify "_" as the <domain> to send the request to the router
        on the same node as the primary connection node for egsh.

    req <service> <method> [<param>, <param>, ...]
        Send an API request.

    reqauth <service> <method> [<param>, <param>, ...]
        Same as 'req', but the first parameter sent to the server
        is our previously stored authtoken (see login)

    pref set <name> <value>
        Set a preference value

    pref get <name>
        Get the value of a specific preference.

    pref list
        List all preferences

    setting get <setting>
        Get server setting values.
        Displays the value best suited to the current context.  This can
        be impacted by whether the user is logged in and if the user
        logged in with a workstation.

    help
        Show this message
"#;

fn main() -> Result<(), String> {
    let mut shell = Shell::setup();
    shell.main_loop();
    Ok(())
}

/// Collection of context data, etc. for our shell.
struct Shell {
    ctx: init::Context,
    db: Option<Rc<RefCell<DatabaseConnection>>>,
    db_translator: Option<idldb::Translator>,
    history_file: Option<String>,
    auth_session: Option<AuthSession>,
    result_count: usize,
    json_print_depth: u16,
    command: String,
}

impl Shell {
    /// Handle command line options, OpenSRF init, build the Shell struct.
    fn setup() -> Shell {
        let mut opts = getopts::Options::new();
        opts.optflag("", "with-database", "Open Direct Database Connection");

        // We don't know if the user passed --with-database until after
        // we parse the command line options.  Append the DB options
        // in case we need them.
        DatabaseConnection::append_options(&mut opts);

        let context = match eg::init::init_with_options(&mut opts) {
            Ok(c) => c,
            Err(e) => panic!("Cannot init to OpenSRF: {}", e),
        };

        let mut shell = Shell {
            ctx: context,
            db: None,
            db_translator: None,
            history_file: None,
            auth_session: None,
            result_count: 0,
            command: String::new(),
            json_print_depth: DEFAULT_JSON_PRINT_DEPTH,
        };

        if shell.ctx().params().opt_present("with-database") {
            shell.setup_db();
        }

        shell
    }

    fn ctx(&self) -> &init::Context {
        &self.ctx
    }

    /// Connect directly to the specified database.
    fn setup_db(&mut self) {
        let params = self.ctx().params();
        let mut db = DatabaseConnection::new_from_options(params);

        if let Err(e) = db.connect() {
            panic!("Cannot connect to database: {}", e);
        }

        let db = db.into_shared();
        let translator = idldb::Translator::new(self.ctx().idl().clone(), db.clone());

        self.db = Some(db);
        self.db_translator = Some(translator);
    }

    /// Setup our rustyline instance, used for reading lines (yep)
    /// and managing history.
    fn setup_readline(&mut self) -> rustyline::Editor<()> {
        let config = rustyline::Config::builder()
            .history_ignore_space(true)
            .completion_type(rustyline::CompletionType::List)
            .build();

        let mut readline = rustyline::Editor::with_config(config).unwrap();

        if let Ok(home) = std::env::var("HOME") {
            let histfile = format!("{home}/{HISTORY_FILE}");
            readline.load_history(&histfile).ok(); // err() if not exists
            self.history_file = Some(histfile);
        }

        readline
    }

    fn db_translator_mut(&mut self) -> Result<&mut idldb::Translator, String> {
        self.db_translator.as_mut().ok_or(format!("DB connection required"))
    }

    /// Main entry point.
    fn main_loop(&mut self) {
        if let Err(e) = self.process_script_lines() {
            eprintln!("{e}");
            return;
        }

        let mut readline = self.setup_readline();

        loop {
            if let Err(e) = self.read_one_line(&mut readline) {
                eprintln!("Command failed: {e}");
            }
        }
    }

    fn add_to_history(&self, readline: &mut rustyline::Editor<()>, line: &str) {
        readline.add_history_entry(line);

        if let Some(filename) = self.history_file.as_ref() {
            if let Err(e) = readline.append_history(filename) {
                eprintln!("Cannot append to history file: {e}");
            }
        }
    }

    fn process_script_lines(&mut self) -> Result<(), String> {
        // Avoid mucking with STDIN if we have no piped data to process.
        // Otherwise, it conflict with rustlyine.
        if atty::is(atty::Stream::Stdin) {
            return Ok(());
        }

        let mut buffer = String::new();
        let stdin = io::stdin();

        loop {
            buffer.clear();
            match stdin.read_line(&mut buffer) {
                Ok(count) => {
                    if count == 0 {
                        break; // EOF
                    }

                    let command = buffer.trim();

                    if command.len() == 0 {
                        // Empty line, but maybe still more data to process.
                        continue;
                    }

                    if let Err(e) = self.dispatch_command(&command) {
                        eprintln!("Error processing piped requests: {e}");
                        break;
                    }
                }

                Err(e) => return Err(format!("Error reading stdin: {e}")),
            }
        }

        // If we started on the receiving end of a pipe, exit after
        // all piped data has been processed, even if no usable
        // data was found.
        self.exit();

        Ok(())
    }

    /// Read a single line of user input and execute the command.
    ///
    /// If the command was successfully executed, return the command
    /// as a string so it may be added to our history.
    fn read_one_line(&mut self, readline: &mut rustyline::Editor<()>) -> Result<(), String> {
        let user_input = match readline.readline(PROMPT) {
            Ok(line) => line,
            Err(_) => return Ok(()),
        };

        let now = Instant::now();

        let user_input = user_input.trim();

        if user_input.len() == 0 {
            return Ok(());
        }

        // Add all commands to history -- often useful to repeat
        // commands that failed.
        self.add_to_history(readline, &user_input);

        self.result_count = 0;
        self.dispatch_command(&user_input)?;
        self.print_duration(&now);

        Ok(())
    }

    fn print_duration(&self, now: &Instant) {
        println!("{SEPARATOR}");
        print!("Duration: {:.4}", now.elapsed().as_secs_f32());
        if self.result_count > 0 {
            print!("; Results: {}", self.result_count);
        }
        println!("");
        println!("{SEPARATOR}");
    }

    /// Route a command line to its handler.
    fn dispatch_command(&mut self, line: &str) -> Result<(), String> {
        let full_args: Vec<&str> = line.split(" ").collect();

        if full_args.len() == 0 {
            return Ok(())
        }

        self.command = full_args[0].to_lowercase();

        let args = match full_args.len() {
            0 => &[],
            _ => &full_args[1..],
        };

        match self.command.as_str() {
            "stop" | "quit" | "exit" => {
                self.exit();
                Ok(())
            }
            "login" => self.handle_login(args),
            "idl" => self.idl_query(args),
            "idlf" => self.idl_query(args),
            "db" => self.db_command(args),
            "req" | "request" => self.send_request(args),
            "reqauth" => self.send_reqauth(args),
            "router" => self.send_router_command(args),
            "pref" => self.handle_prefs(args),
            "setting" => self.handle_settings(args),
            "help" => {
                println!("{HELP_TEXT}");
                Ok(())
            }
            _ => Err(format!("Unknown command: {}", self.command)),
        }
    }

    fn handle_settings(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;
        let subcom = args[0];

        match subcom {
            "get" => self.get_setting(args),
            _ => Err(format!("Unknown 'setting' command: {subcom}")),
        }
    }

    fn check_for_event(&mut self, v: &json::JsonValue) -> Result<(), String> {
        if let Some(evt) = event::EgEvent::parse(v) {
            if !evt.success() {
                return Err(format!("Non-SUCCESS event returned: {evt}"));
            }
        }

        Ok(())
    }

    fn get_setting(&mut self, args: &[&str]) -> Result<(), String> {

        let authtoken = match &self.auth_session {
            Some(s) => json::from(s.token()),
            None => json::JsonValue::Null,
        };

        let setting = args[1];
        let setarg = json::from(setting);

        let org_id = if args.len() > 2 {
            let org_str = args[2];
            json::parse(org_str).or_else(|e|
                Err(format!("Cannot parse parameter: {org_str} {e}")))?
        } else {
            json::JsonValue::Null
        };

        let params = vec![json::from(vec![setarg]), authtoken, org_id];

        let mut ses = self.ctx().client().session("open-ils.actor");
        let mut req = ses.request("open-ils.actor.settings.retrieve", &params)?;

        while let Some(resp) = req.recv(DEFAULT_REQUEST_TIMEOUT)? {
            self.check_for_event(&resp)?;
            println!("");
            println!("{setting} => {}", resp["value"]);
        }
        println!("");

        Ok(())
    }

    fn handle_prefs(&mut self,  args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 1)?;
        let subcom = args[0];

        match subcom {
            "get" => self.get_pref(args),
            "set" => self.set_pref(args),
            "list" => self.list_prefs(),
            _ => Err(format!("Unknown pref command: {subcom}")),
        }
    }

    fn list_prefs(&mut self) -> Result<(), String> {
        for pref in ["json_print_depth"] {
            self.get_pref(&["get", pref])?;
        }
        Ok(())
    }

    fn set_pref(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 3)?;
        let pref = args[1];
        let value = args[2];

        match pref {
            "json_print_depth" => {
                let value_num = value
                    .parse::<u16>()
                    .or_else(|e| Err(format!("Invalid value for {pref} {e}")))?;
                self.json_print_depth = value_num;
                self.get_pref(args)
            }
            _ => Err(format!("No such pref: {pref}"))?,
        }
    }

    fn get_pref(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;
        let pref = args[1];

        let value = match pref {
            "json_print_depth" => self.json_print_depth.to_string(),
            _ => return Err(format!("No such pref: {pref}")),
        };

        println!("{pref} = {value}");
        Ok(())
    }

    fn send_reqauth(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;

        let authtoken = match &self.auth_session {
            Some(s) => json::from(s.token()).dump(),
            None => return Err(format!("reqauth requires an auth token")),
        };

        let mut params = args.to_vec();
        params.insert(2, authtoken.as_str());

        self.send_request(params.as_slice())
    }

    fn handle_login(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;

        let username = args[0];
        let password = args[1];
        let login_type = args.get(2).unwrap_or(&"temp");
        let workstation = if args.len() > 3 { Some(args[3]) } else { None };

        let args = eg::auth::AuthLoginArgs::new(username, password, *login_type, workstation);

        match eg::auth::AuthSession::login(self.ctx().client(), &args)? {
            Some(s) => {
                println!("Login succeeded: {}", s.token());
                self.auth_session = Some(s);
            }
            None => {
                println!("Login failed");
            }
        };

        Ok(())
    }

    fn send_router_command(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;

        let mut domain = args[0];
        let command = args[1];

        if domain.eq("_") {
            domain = self.ctx().config().client().domain().name();
        }

        let router_class = match args.len() > 2 {
            true => Some(args[2]),
            false => None,
        };

        // Assumes the caller wants to see the response for any
        // router request.
        if let Some(resp) =
            self.ctx()
                .client()
                .send_router_command(domain, command, router_class, true)?
        {
            self.print_json_record(&resp)?;
        }

        Ok(())
    }

    fn send_request(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;

        let mut params: Vec<json::JsonValue> = Vec::new();

        let mut idx = 2;
        while idx < args.len() {
            let p = match json::parse(args[idx]) {
                Ok(p) => p,
                Err(e) => return Err(format!("Cannot parse parameter: {} {}", args[idx], e)),
            };
            params.push(p);
            idx += 1;
        }

        let mut ses = self.ctx().client().session(args[0]);
        let mut req = ses.request(args[1], &params)?;

        while let Some(resp) = req.recv(DEFAULT_REQUEST_TIMEOUT)? {
            self.print_json_record(&resp)?;
        }

        Ok(())
    }

    fn db_command(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;

        match args[0].to_lowercase().as_str() {
            "sleep" => self.db_sleep(args[1]),
            _ => Err(format!("Unknown 'db' command: {args:?}")),
        }
    }

    fn db_sleep(&mut self, secs: &str) -> Result<(), String> {
        let secs: f64 = match secs.parse::<f64>() {
            Ok(s) => s,
            Err(_) => return Err(format!("Invalid sleep duration: {secs}")),
        };

        let db = match &mut self.db {
            Some(d) => d,
            None => return Err(format!("'db' command requires --with-database")),
        };

        let query = "SELECT PG_SLEEP($1)";

        let query_res = db.borrow_mut().client().query(&query[..], &[&secs]);

        if let Err(e) = query_res {
            return Err(format!("DB query failed: {e}"));
        }

        Ok(())
    }

    /// Returns Err if the str slice does not contain enough entries.
    fn args_min_length(&self, args: &[&str], len: usize) -> Result<(), String> {
        if args.len() < len {
            Err(format!("Command is incomplete: {args:?}"))
        } else {
            Ok(())
        }
    }

    fn exit(&mut self) {
        std::process::exit(0x0);
    }

    /// Launch an IDL query.
    fn idl_query(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 3)?;

        match args[0] {
            "get" => self.idl_get(args),
            "search" => self.idl_search(args),
            _ => return Err(format!("Could not parse idl query command: {args:?}")),
        }
    }

    /// Retrieve a single IDL object by its primary key value
    fn idl_get(&mut self, args: &[&str]) -> Result<(), String> {
        let classname = args[1];
        let pkey = args[2];

        let translator = self.db_translator_mut()?;

        let obj = match translator.idl_class_by_pkey(classname, pkey)? {
            Some(o) => o,
            None => return Ok(()),
        };

        if self.command.eq("idlf") {
            self.print_idl_object(&obj)
        } else {
            self.print_json_record(&obj)
        }
    }

    /// Retrieve a single IDL object by its primary key value
    fn idl_search(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 5)?;

        let classname = args[1];
        let fieldname = args[2];
        let operand = args[3];
        let value = args[4];

        let idl_class = self.ctx().idl().classes().get(classname)
            .ok_or(format!("No such IDL class: {classname}"))?;

        if idl_class.fields().get(fieldname).is_none() {
            Err(format!("No such IDL field: {fieldname}"))?;
        }

        if !idldb::Translator::is_supported_operand(&operand) {
            Err(format!("Invalid query operand: {operand}"))?;
        }

        let value = json::parse(value)
            .or_else(|e| Err(format!("Cannot parse query value: {value} : {e}")))?;

        let mut search = idldb::IdlClassSearch::new(classname);

        // Apply some kind of limit here to prevent
        // excessive queries.  TODO: configurable?
        // TODO: support paging in the UI?
        search.set_pager(idldb::Pager::new(100, 0));

        let mut filter = json::JsonValue::new_object();
        let mut subfilter = json::JsonValue::new_object();
        subfilter[operand] = value;
        filter[fieldname] = subfilter;

        search.set_filter(filter);

        let translator = self.db_translator_mut()?;

        for obj in translator.idl_class_search(&search)? {
            if self.command.eq("idlf") {
                self.print_idl_object(&obj)?;
            } else {
                self.print_json_record(&obj)?;
            }
        }

        Ok(())
    }

    fn print_json_record(&mut self, obj: &json::JsonValue) -> Result<(), String> {
        self.result_count += 1;

        println!("{SEPARATOR}");
        if self.json_print_depth == 0 {
            println!("{}", obj.dump());
        } else {
            println!("{}", obj.pretty(self.json_print_depth));
        }
        Ok(())
    }

    fn print_idl_object(&mut self, obj: &json::JsonValue) -> Result<(), String> {
        self.result_count += 1;

        let classname = obj[idl::CLASSNAME_KEY].as_str()
            .ok_or(format!("Not a valid IDL object value: {}", obj.dump()))?;

        let idl_class = self.ctx().idl().classes().get(classname)
            .ok_or(format!("Object has an invalid class name {classname}"))?;

        // Get the max field name length for improved formatting.
        let mut maxlen = 0;
        let mut fields = Vec::new();
        for field in idl_class.real_fields_sorted() {
            let fname = field.name();

            if obj[fname].is_null() { continue; }

            fields.push(fname);

            if fname.len() > maxlen {
                maxlen = fname.len();
            }
        }

        maxlen += 3;

        println!("{SEPARATOR}");

        for name in fields {
            let value = &obj[name];
            if !value.is_null() {
                println!("{name:.<width$} {value}", width = maxlen);
            }
        }


        Ok(())
    }
}
