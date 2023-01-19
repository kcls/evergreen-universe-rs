use super::conf;
///! OpenSRF Syslog
use log;
use std::os::unix::net::UnixDatagram;
use std::process;
use syslog;
use thread_id;

const SYSLOG_UNIX_PATH: &str = "/dev/log";

/// Thread IDs can be many digits, though in practice only the last
/// few digits vary.  In log messages, include only the final
/// TRIM_THREAD_ID characters to differentiate threads.
const TRIM_THREAD_ID: usize = 5;

/// Main logging structure
///
/// NOTE this logs directly to the syslog UNIX path instead of going through
/// the syslog crate.  This approach gives us much more control.
pub struct Logger {
    logfile: conf::LogFile,
    loglevel: log::LevelFilter,
    facility: syslog::Facility,
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

        Ok(Logger {
            logfile: file.clone(),
            loglevel: level.clone(),
            facility: facility.clone(),
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
    pub fn init(mut self) -> Result<(), log::SetLoggerError> {
        match UnixDatagram::unbound() {
            Ok(socket) => match socket.connect(SYSLOG_UNIX_PATH) {
                Ok(()) => self.writer = Some(socket),
                Err(e) => {
                    eprintln!("Cannot connext to unix socket: {e}");
                }
            },
            Err(e) => {
                eprintln!("Cannot connext to unix socket: {e}");
            }
        }

        log::set_max_level(self.loglevel);
        log::set_boxed_logger(Box::new(self))?;

        Ok(())
    }

    /// Encode the facility and severity as the syslog priority.
    ///
    /// Essentially copied from the syslog crate.
    fn encode_priority(&self, severity: syslog::Severity) -> syslog::Priority {
        return self.facility as u8 | severity as u8;
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

        let levelname = record.level().to_string();
        let target = if !record.target().is_empty() {
            record.target()
        } else {
            record.module_path().unwrap_or_default()
        };

        let severity = self.encode_priority(match levelname.to_lowercase().as_str() {
            "debug" | "trace" => syslog::Severity::LOG_DEBUG,
            "info" => syslog::Severity::LOG_INFO,
            "warn" => syslog::Severity::LOG_WARNING,
            _ => syslog::Severity::LOG_ERR,
        });

        let mut tid: String = thread_id::get().to_string();
        if tid.len() > TRIM_THREAD_ID {
            tid = tid.chars().skip(tid.len() - TRIM_THREAD_ID).collect();
        }

        let message = format!(
            "<{}>{} [{}:{}:{}:{}:{}] {}",
            severity,
            &self.application,
            levelname,
            process::id(),
            target,
            match record.line() {
                Some(l) => l,
                _ => 0,
            },
            tid,
            record.args()
        );

        if let Some(ref w) = self.writer {
            if w.send(message.as_bytes()).is_ok() {
                return;
            }
        }

        println!("{message}");
    }

    fn flush(&self) {}
}
