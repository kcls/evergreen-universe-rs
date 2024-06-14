use eg::common::auth;
use eg::Client;
use eg::EgValue;
use evergreen as eg;
use std::cell::RefCell;
use std::io;
use std::rc::Rc;
use std::time::Instant;


use eg::common::settings;
use eg::db;
use eg::db::DatabaseConnection;
use eg::editor;
use eg::event;
use eg::idl;
use eg::idldb;

use eg::osrf::logging::Logger;
use eg::util;

// const PROMPT: &str = "egsh# "; // without color
const PROMPT: &str = "\x1b[1;32megsh# \x1b[0m";
const HISTORY_FILE: &str = ".egsh_history";
const SEPARATOR: &str = "- - - - - - - - - - - - - - - - - - - - - - - - - -";
const DEFAULT_JSON_PRINT_DEPTH: u16 = 2;
const DEFAULT_LOGIN_TYPE: &str = "temp";

const HELP_TEXT: &str = r#"
Options

    --with-database
        Connect directly to an Evergreen database.
        Commands that start with "db" require this.

    Standard OpenSRF environment variables (e.g. OSRF_CONFIG) are
    also supported.

Commands

    db idl get <classname> <pkey-value>
        Retrieve and IDL-classed object by primary key directly
        from the database.

    db idl search <classname> <field> <operator> <value>
        Examples:
            db idl search aou name ~* "branch"
            db idl search aout depth > 1

    db idlf ...
        Same as 'db idl' commands but values are displayed as formatted
        label / value pairs, minus NULL values.

    db sleep <seconds>
        Runs PG_SLEEP(<seconds>).  Mostly for debugging.

    login <username> <password> [<login_type> <workstation>]

    req <service> <method> [<param> <param> ...]
        Send an API request.

    reqauth <service> <method> [<param> <param> ...]
        Same as 'req', but the first parameter sent to the server
        is our previously stored authtoken (see login)

    cstore <action> <hint_or_fielmapper> <param> [<param> ...]
        Shortcut for open-ils.cstore queries.

        Examples:
            cstore retrieve au 1
            cstore search au {"id":{"<":5}}
            cstore search actor.user {"id":{"<":5}}
            cstore search actor::user {"id":{"<":5}}

        'hint_or_fielmapper' may be either an IDL class hint ("ahr") or
        the class fieldmapper name ("action::hold_request"), or the fieldmapper
        name with "::" replaced by "." ("action.hold_request", "actor.user")

        NOTE: this command only works when connecting to a domain
        that has access to cstore, e.g. private.localhost.

    introspect <service> [<prefix>]
        List methods published by <service>, optionally limiting to
        those which start with the string <prefix>.

    introspect-names <service> [<prefix>]
        Same as introspect, but only lists method names instead of
        the full method definition.

    introspect-summary <service> [<prefix>]
        Same as introspect, but only lists method names and params.

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

    sip <host> <command> [<arg>, ...]
        Example:
            sip localhost:6001 login sip-user sip-pass

        Supported Commands:
            login <sip-user> <sip-pass>
            sc-status
            item-information <item-barcode>
            patron-information <patron-barcode>
            patron-status <patron-barcode>

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
    client: Client,
    db: Option<Rc<RefCell<DatabaseConnection>>>,
    db_translator: Option<idldb::Translator>,
    history_file: Option<String>,
    auth_session: Option<auth::Session>,
    result_count: usize,
    /// Pretty-printed JSON uses this many spaces for formatting.
    json_print_depth: u16,
    /// Print IDL objects as they travel on the wire, as classed arrays,
    /// instead of using our internal structure.
    json_as_wire_protocal: bool,
    json_hash_slim: bool,
    sip_client: Option<sip2::Client>,
    command: String,
}

impl Shell {
    /// Handle command line options, OpenSRF init, build the Shell struct.
    fn setup() -> Shell {
        let args: Vec<String> = std::env::args().collect();
        let mut opts = getopts::Options::new();
        opts.optflag("", "with-database", "Open Direct Database Connection");

        // We don't know if the user passed --with-database until after
        // we parse the command line options.  Append the DB options
        // in case we need them.
        DatabaseConnection::append_options(&mut opts);

        let params = match opts.parse(&args[1..]) {
            Ok(p) => p,
            Err(e) => panic!("Error parsing options: {}", e),
        };

        let client = match eg::init() {
            Ok(c) => c,
            Err(e) => panic!("Cannot init to OpenSRF: {}", e),
        };

        let mut shell = Shell {
            client,
            db: None,
            db_translator: None,
            history_file: None,
            auth_session: None,
            result_count: 0,
            command: String::new(),
            json_print_depth: DEFAULT_JSON_PRINT_DEPTH,
            json_as_wire_protocal: false,
            json_hash_slim: false,
            sip_client: None,
        };

        if params.opt_present("with-database") {
            shell.setup_db(&params);
        }

        shell
    }

    fn client(&self) -> &Client {
        &self.client
    }

    /// Connect directly to the specified database.
    fn setup_db(&mut self, params: &getopts::Matches) {
        let mut db = DatabaseConnection::new_from_options(params);

        if let Err(e) = db.connect() {
            panic!("Cannot connect to database: {}", e);
        }

        let db = db.into_shared();
        let translator = idldb::Translator::new(db.clone());

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
        self.db_translator
            .as_mut()
            .ok_or_else(|| "DB connection required".to_string())
    }

    /// Main entry point.
    fn main_loop(&mut self) {
        if let Err(e) = self.process_script_lines() {
            eprintln!("{e}");
            return;
        }

        let mut readline = self.setup_readline();

        println!("\nNOTE: Request parameters should be separated by spaces, not commas.\n");

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

        if self.db.is_some() {
            eprintln!("Cannot process piped content while --with-database is on");
            self.exit();
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

                    if command.is_empty() {
                        // Empty line, but maybe still more data to process.
                        continue;
                    }

                    if let Err(e) = self.dispatch_command(command) {
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

        if user_input.is_empty() {
            return Ok(());
        }

        // Add all commands to history -- often useful to repeat
        // commands that failed.
        self.add_to_history(readline, user_input);

        self.result_count = 0;
        self.dispatch_command(user_input)?;
        self.print_duration(&now);

        Ok(())
    }

    fn print_duration(&self, now: &Instant) {
        println!("{SEPARATOR}");
        print!("Duration: {:.4}", now.elapsed().as_secs_f32());
        if self.result_count > 0 {
            print!("; Results: {}", self.result_count);
        }
        println!();
        println!("{SEPARATOR}");
    }

    /// Route a command line to its handler.
    fn dispatch_command(&mut self, line: &str) -> Result<(), String> {
        let full_args: Vec<&str> = line.split(' ').collect();

        if full_args.is_empty() {
            return Ok(());
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
            "db" => self.db_command(args),
            "req" | "request" => self.send_request(args),
            "reqauth" => self.send_reqauth(args),
            //"introspect" | "introspect-names" | "introspect-summary" => self.introspect(args),
            x if x.starts_with("introspect") => self.introspect(args),
            "pref" => self.handle_prefs(args),
            "setting" => self.handle_settings(args),
            "cstore" => self.handle_cstore(args),
            "sip" => {
                let res = self.handle_sip(args);
                if res.is_err() {
                    if let Some(c) = self.sip_client.as_mut() {
                        c.disconnect().ok();
                    };
                    self.sip_client = None;
                }
                res
            }
            "help" => {
                println!("{HELP_TEXT}");
                Ok(())
            }
            _ => Err(format!("Unknown command: {}", self.command)),
        }
    }

    fn handle_sip(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;
        let hostport = args[0];
        let command = args[1]; // e.g. login, status

        if self.sip_client.is_none() {
            if command != "login" {
                println!(
                    "\nNOTE: SIP server may require login before other message types are allowed\n"
                );
            }
            let c = sip2::Client::new(hostport).map_err(|e| e.to_string())?;
            self.sip_client = Some(c);
        }

        let mut sip_params = sip2::ParamSet::new();

        let response = if command == "login" {
            self.args_min_length(args, 4)?; // SIP username + password
            sip_params.set_sip_user(args[2]).set_sip_pass(args[3]);

            self.sip_client
                .as_mut()
                .unwrap()
                .login(&sip_params)
                .map_err(|e| e.to_string())?
        } else if command == "sc-status" {
            self.sip_client
                .as_mut()
                .unwrap()
                .sc_status()
                .map_err(|e| e.to_string())?
        } else if command == "item-information" {
            self.args_min_length(args, 3)?; //  item barcode
            sip_params.set_item_id(args[2]);

            self.sip_client
                .as_mut()
                .unwrap()
                .item_info(&sip_params)
                .map_err(|e| e.to_string())?
        } else if command == "patron-information" {
            self.args_min_length(args, 3)?; //  patron barcode
            sip_params.set_patron_id(args[2]);

            self.sip_client
                .as_mut()
                .unwrap()
                .patron_info(&sip_params)
                .map_err(|e| e.to_string())?
        } else if command == "patron-status" {
            self.args_min_length(args, 3)?; //  patron barcode
            sip_params.set_patron_id(args[2]);

            self.sip_client
                .as_mut()
                .unwrap()
                .patron_status(&sip_params)
                .map_err(|e| e.to_string())?
        } else {
            return Err(format!("Unsupported SIP command {command}"));
        };

        println!("\n{}", response.msg());

        Ok(())
    }

    fn handle_cstore(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;
        let action = args[0]; // retrieve, search, json_query

        if action == "json_query" {
            return self.send_request(&["open-ils.cstore", "open-ils.cstore.json_query", (args[1])]);
        }

        // retrieve and search require an additional class specifier
        self.args_min_length(args, 3)?;

        // IDL class may either be a class hint (e.g. "aou") or a full
        // fieldmapper name ("actor.org_unit");
        let mut class = args[1].to_string();

        if class.contains("::") {
            class = class.replace("::", ".");
        } else if !class.contains('.') {
            // Caller provided a class hint.  Translate that into
            // the fieldmapper string used by cstore APIs.

            if let Ok(idl_class) = idl::get_class(&class) {
                if let Some(fm) = idl_class.fieldmapper() {
                    class = fm.replace("::", ".");
                } else {
                    return Err(format!("IDL class {class} has no fieldmapper"));
                }
            } else {
                return Err(format!("IDL class {class} does not exist"));
            }
        }

        let method = format!("open-ils.cstore.direct.{class}.{action}");

        let mut new_args = vec!["open-ils.cstore", method.as_str()];
        for s in &args[2..] {
            new_args.push(s);
        }

        self.send_request(new_args.as_slice())
    }

    fn handle_settings(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;
        let subcom = args[0];

        match subcom {
            "get" => self.get_setting(args),
            _ => Err(format!("Unknown 'setting' command: {subcom}")),
        }
    }

    fn _check_for_event(&mut self, v: &EgValue) -> Result<(), String> {
        if let Some(evt) = event::EgEvent::parse(v) {
            if !evt.is_success() {
                return Err(format!("Non-SUCCESS event returned: {evt}"));
            }
        }

        Ok(())
    }

    fn get_setting(&mut self, args: &[&str]) -> Result<(), String> {
        let mut editor = editor::Editor::new(self.client());
        let mut sc = settings::Settings::new(&editor);

        // If the caller requested settings for a specific org unit,
        // use that as the context.  Otherwise, pull what we can
        // from our editor / auth info.
        if args.len() > 2 {
            let org_id = args[2]
                .parse::<i64>().map_err(|_| format!("Invalid org unit ID: {}", args[2]))?;

            sc.set_org_id(org_id);
        } else if let Some(authses) = &self.auth_session {
            editor.set_authtoken(authses.token());
            editor.checkauth()?;
            sc.set_editor(&editor);
        } else {
            Err("Org unit or authtoken required to check settings".to_string())?;
        }

        let name = &args[1];
        let value = sc.get_value(name)?;

        Ok(println!("\n{name} => {value}\n"))
    }

    fn handle_prefs(&mut self, args: &[&str]) -> Result<(), String> {
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
        for pref in [
            "json_print_depth",
            "json_as_wire_protocal",
            "json_hash_slim",
        ] {
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
                    .parse::<u16>().map_err(|e| format!("Invalid value for {pref} {e}"))?;
                self.json_print_depth = value_num;
            }
            "json_as_wire_protocal" => self.json_as_wire_protocal = value.to_lowercase() == "true",
            "json_hash_slim" => self.json_hash_slim = value.to_lowercase() == "true",
            _ => Err(format!("No such pref: {pref}"))?,
        }

        self.get_pref(args)
    }

    fn get_pref(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;
        let pref = args[1];

        let value = match pref {
            "json_print_depth" => EgValue::from(self.json_print_depth),
            "json_as_wire_protocal" => EgValue::from(self.json_as_wire_protocal),
            "json_hash_slim" => EgValue::from(self.json_hash_slim),
            _ => return Err(format!("No such pref: {pref}")),
        };

        println!("{pref} = {value}");
        Ok(())
    }

    fn send_reqauth(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;

        let authtoken = match &self.auth_session {
            Some(s) => EgValue::from(s.token()).dump(),
            None => return Err("reqauth requires an auth token".to_string()),
        };

        let mut params = args.to_vec();
        params.insert(2, authtoken.as_str());

        self.send_request(params.as_slice())
    }

    fn handle_login(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;

        let username = args[0];
        let password = args[1];
        let login_type = args.get(2).unwrap_or(&DEFAULT_LOGIN_TYPE);
        let workstation = if args.len() > 3 { Some(args[3]) } else { None };

        let args = auth::LoginArgs::new(
            username,
            password,
            auth::LoginType::try_from(*login_type)?,
            workstation,
        );

        match auth::Session::login(self.client(), &args)? {
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

    fn introspect(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 1)?;

        let wants_summary = self.command.contains("-summary");

        let service = &args[0];

        println!("\nNOTE: Introspecting Perl services will fail with an unknown IDL class (see error log).\n");

        let mut params = vec![];
        if let Some(prefix) = args.get(1) {
            params.push(EgValue::from(*prefix));
        }

        let method = if wants_summary {
            "opensrf.system.method.all.summary"
        } else {
            "opensrf.system.method.all"
        };

        let mut ses = self.client().session(service);
        let mut req = ses.request(method, params)?;

        while let Some(resp) = req.recv()? {
            if self.command.contains("-names") {
                println!("* {}", resp["api_name"]);
            } else if wants_summary {
                println!("* {}", resp.as_str().unwrap());
            } else {
                self.print_json_record(resp)?;
            }
        }

        Ok(())
    }

    fn send_request(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;

        let mut params: Vec<EgValue> = Vec::new();

        // Use the serde_json stream parser to read the parameters.
        let data = args[2..].join(" ");
        let stream = serde_json::Deserializer::from_str(&data).into_iter::<serde_json::Value>();

        for param_res in stream {
            let p = match param_res {
                Ok(p) => p,
                Err(e) => Err(format!("Cannot parse params: {data} {e}"))?,
            };

            // Translate the serde_json::Value into a EgValue.
            let p_str = match serde_json::to_string(&p) {
                Ok(s) => s,
                Err(e) => Err(format!("Error stringifying: {e}"))?,
            };

            let param = EgValue::parse(&p_str)?;
            params.push(param);
        }

        // We are the entry point for this request.  Give it a log trace.
        Logger::mk_log_trace();

        let mut ses = self.client().session(args[0]);
        let mut req = ses.request(args[1], params)?;

        while let Some(resp) = req.recv()? {
            self.print_json_record(resp)?;
        }

        Ok(())
    }

    fn db_command(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 2)?;

        match args[0].to_lowercase().as_str() {
            "sleep" => self.db_sleep(args[1]),
            "idl" | "idlf" => self.idl_query(args),
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
            None => return Err("'db' command requires --with-database".to_string()),
        };

        let query = "SELECT PG_SLEEP($1)";

        let query_res = db.borrow_mut().client().query(query, &[&secs]);

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
        self.args_min_length(args, 4)?;

        match args[1] {
            "get" => self.idl_get(args),
            "search" => self.idl_search(args),
            _ => Err(format!("Could not parse idl query command: {args:?}")),
        }
    }

    /// Retrieve a single IDL object by its primary key value
    fn idl_get(&mut self, args: &[&str]) -> Result<(), String> {
        let classname = args[2];
        let pkey = args[3];

        let translator = self.db_translator_mut()?;

        let obj = match translator.get_idl_object_by_pkey(classname, &EgValue::from(pkey), None)? {
            Some(o) => o,
            None => return Ok(()),
        };

        if args[0].eq("idlf") {
            self.print_idl_object(&obj)
        } else {
            self.print_json_record(obj)
        }
    }

    /// Retrieve a single IDL object by its primary key value
    fn idl_search(&mut self, args: &[&str]) -> Result<(), String> {
        self.args_min_length(args, 6)?;

        let classname = args[2];
        let fieldname = args[3];
        let operator = args[4];
        let value = args[5];

        let idl_class = idl::get_class(classname)?;

        if idl_class.fields().get(fieldname).is_none() {
            Err(format!("No such IDL field: {fieldname}"))?;
        }

        if !db::is_supported_operator(operator) {
            Err(format!("Invalid query operator: {operator}"))?;
        }

        let value = EgValue::parse(value)?;

        let mut search = idldb::IdlClassSearch::new(classname);

        // Apply some kind of limit here to prevent
        // excessive queries.  TODO: configurable?
        // TODO: support paging in the UI?
        search.set_pager(util::Pager::new(100, 0));

        let mut filter = EgValue::new_object();
        let mut subfilter = EgValue::new_object();
        subfilter[operator] = value;
        filter[fieldname] = subfilter;

        search.set_filter(filter);

        let translator = self.db_translator_mut()?;

        for obj in translator.idl_class_search(&search)? {
            if args[0].eq("idlf") {
                self.print_idl_object(&obj)?;
            } else {
                self.print_json_record(obj)?;
            }
        }

        Ok(())
    }

    fn print_json_record(&mut self, mut obj: EgValue) -> Result<(), String> {
        self.result_count += 1;

        let dumped = if self.json_as_wire_protocal {
            if self.json_print_depth == 0 {
                obj.into_json_value().dump()
            } else {
                obj.into_json_value().pretty(self.json_print_depth)
            }
        } else {
            obj.to_classed_hash();
            if self.json_hash_slim {
                obj.scrub_hash_nulls();
            }
            if self.json_print_depth == 0 {
                obj.dump()
            } else {
                obj.pretty(self.json_print_depth)
            }
        };

        println!("{SEPARATOR}");
        println!("{dumped}");

        Ok(()) // TODO remove?
    }

    fn print_idl_object(&mut self, obj: &EgValue) -> Result<(), String> {
        self.result_count += 1;

        let idl_class = obj
            .idl_class()
            .ok_or_else(|| format!("Not an IDL object value: {}", obj.dump()))?;

        // Get the max field name length for improved formatting.
        let mut maxlen = 0;
        let mut fields = Vec::new();
        for field in idl_class.real_fields_sorted() {
            let fname = field.name();

            if obj[fname].is_null() {
                continue;
            }

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
