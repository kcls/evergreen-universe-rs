//! OpenSRF Syslog
use crate::date;
use crate::osrf::conf;
use crate::util;
use log;
use std::cell::RefCell;
use std::fs;
use std::io::Write;
use std::os::unix::net::UnixDatagram;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};
use syslog;

const SYSLOG_UNIX_PATH: &str = "/dev/log";

// Thread-local version of the current log trace
thread_local! {
    static THREAD_LOCAL_LOG_TRACE: RefCell<String> = RefCell::new(Logger::build_log_trace());
}

/// Main logging structure
///
/// NOTE this logs directly to the syslog UNIX path instead of going through
/// the syslog crate.  This approach gives us much more control.
pub struct Logger {
    logfile: conf::LogFile,
    loglevel: log::LevelFilter,
    facility: syslog::Facility,
    activity_facility: syslog::Facility,
    writer: Option<UnixDatagram>,
    application: String,
}

impl Logger {
    pub fn new(options: &conf::LogOptions) -> Result<Self, String> {
        let file = match options.log_file() {
            Some(f) => f,
            None => return Err(format!("log_file option required")),
        };

        let level = match options.log_level() {
            Some(l) => l,
            None => &log::LevelFilter::Info,
        };

        let facility = match options.syslog_facility() {
            Some(f) => f,
            None => syslog::Facility::LOG_LOCAL0,
        };

        let act_facility = options
            .activity_log_facility()
            .unwrap_or(syslog::Facility::LOG_LOCAL1);

        Ok(Logger {
            logfile: file.clone(),
            loglevel: level.clone(),
            facility: facility.clone(),
            activity_facility: act_facility.clone(),
            writer: None,
            application: Logger::find_app_name(),
        })
    }

    fn find_app_name() -> String {
        if let Ok(p) = std::env::current_exe() {
            if let Some(f) = p.file_name() {
                if let Some(n) = f.to_str() {
                    return n.to_string();
                }
            }
        }

        eprintln!("Cannot determine executable name.  See set_application()");
        return "opensrf".to_string();
    }

    pub fn set_application(&mut self, app: &str) {
        self.application = app.to_string();
    }

    pub fn set_loglevel(&mut self, loglevel: log::LevelFilter) {
        self.loglevel = loglevel
    }

    pub fn set_facility(&mut self, facility: syslog::Facility) {
        self.facility = facility;
    }

    /// Setup our global log handler.
    ///
    /// Attempts to connect to syslog unix socket if possible.
    pub fn init(mut self) -> Result<(), String> {
        match self.logfile {
            conf::LogFile::Syslog => {
                self.writer = match Logger::writer() {
                    Ok(w) => Some(w),
                    Err(e) => {
                        eprintln!("Cannot init Logger: {e}");
                        return Err(format!("Cannot init Logger: {e}"));
                    }
                }
            }
            conf::LogFile::Filename(ref name) => {
                if let Err(e) = fs::File::options()
                    .create(true)
                    .write(true)
                    .append(true)
                    .open(name)
                {
                    let err = format!("Cannot open file for writing: {name} {e}");
                    eprintln!("{err}");
                    return Err(err);
                }
            }
        }

        log::set_max_level(self.loglevel);

        if let Err(e) = log::set_boxed_logger(Box::new(self)) {
            eprintln!("Cannot init Logger: {e}");
            return Err(format!("Cannot init Logger: {e}"));
        }

        Ok(())
    }

    /// Encode the facility and severity as the syslog priority.
    ///
    /// Essentially copied from the syslog crate.
    fn encode_priority(&self, severity: syslog::Severity) -> syslog::Priority {
        return self.facility as u8 | severity as u8;
    }

    pub fn writer() -> Result<UnixDatagram, String> {
        match UnixDatagram::unbound() {
            Ok(socket) => match socket.connect(SYSLOG_UNIX_PATH) {
                Ok(()) => Ok(socket),
                Err(e) => Err(format!("Cannot connect to unix socket: {e}")),
            },
            Err(e) => Err(format!("Cannot connect to unix socket: {e}")),
        }
    }

    /// Create a log trace string from the current time and thread id.
    fn build_log_trace() -> String {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime before UNIX EPOCH!");

        format!("{}-{:0>5}", t.as_millis(), util::thread_id())
    }

    /// Generate and set a thread-local log trace string.
    pub fn mk_log_trace() {
        let t = Logger::build_log_trace();
        Logger::set_log_trace(t);
    }

    /// Set the thread-local log trace string, typically from
    /// a log trace found in an opensrf message.
    pub fn set_log_trace(trace: impl Into<String>) {
        let trace = trace.into();
        THREAD_LOCAL_LOG_TRACE.with(|tr| *tr.borrow_mut() = trace);
    }

    /// Returns a clone of the current log trace.
    ///
    /// Cloning required here.
    pub fn get_log_trace() -> String {
        let mut trace: Option<String> = None;
        THREAD_LOCAL_LOG_TRACE.with(|tr| trace = Some((*tr.borrow()).to_string()));
        trace.unwrap()
    }
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        &metadata.level().to_level_filter() <= &self.loglevel
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        let mut levelname = record.level().to_string();
        let target = if !record.target().is_empty() {
            record.target()
        } else {
            record.module_path().unwrap_or_default()
        };

        // HACK to avoid logging content from the rustyline crate, which
        // is quite chatty.  If this list grows, consider alternative
        // approaches to specifying which module's logs we want to
        // handle.
        if target.starts_with("rustyline") {
            return;
        }

        let mut logmsg = record.args().to_string();

        // This is a hack to support ACTIVITY logging via the existing
        // log::* macros.  Ideally we could use e.g. Notice instead.
        // https://github.com/rust-lang/log/issues/334
        let severity = if format!("{}", record.args()).starts_with("ACT:") {
            // Remove the ACT: tag since it will also be present in the
            // syslog level.
            logmsg = logmsg[4..].to_string();
            levelname = String::from("ACT");
            let facility = self.activity_facility;
            facility as u8 | syslog::Severity::LOG_INFO as u8
        } else {
            self.encode_priority(match levelname.as_str() {
                "DEBUG" | "TRACE" => syslog::Severity::LOG_DEBUG,
                "INFO" => syslog::Severity::LOG_INFO,
                "WARN" => syslog::Severity::LOG_WARNING,
                _ => syslog::Severity::LOG_ERR,
            })
        };

        let mut message = format!(
            "{}{} [{}:{}:{}:{}",
            match self.writer.is_some() {
                true => format!("<{}>", severity),
                _ => format!("{} ", date::epoch_secs()),
            },
            &self.application,
            levelname,
            process::id(),
            target,
            match record.line() {
                Some(l) => l,
                _ => 0,
            }
        );

        // Add the thread-local log trace
        THREAD_LOCAL_LOG_TRACE.with(|tr| message += &format!(":{}] ", *tr.borrow()));

        message += &logmsg;

        if let Some(ref w) = self.writer {
            if w.send(message.as_bytes()).is_ok() {
                return;
            }
        } else if let conf::LogFile::Filename(ref name) = self.logfile {
            if let Ok(mut file) = fs::File::options()
                .create(true)
                .write(true)
                .append(true)
                .open(name)
            {
                message += "\n";
                if file.write_all(message.as_bytes()).is_ok() {
                    return;
                }
            }
        }

        // If all else fails, print the log message.
        println!("{message}");
    }

    fn flush(&self) {}
}
