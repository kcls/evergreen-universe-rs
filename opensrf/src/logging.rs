///! OpenSRF Syslog
use super::conf;
use super::util;
use log;
use std::os::unix::net::UnixDatagram;
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};
use syslog;

const SYSLOG_UNIX_PATH: &str = "/dev/log";

/// Main logging structure
///
/// NOTE this logs directly to the syslog UNIX path instead of going through
/// the syslog crate.  This approach gives us much more control.
///
/// TODO: As it stands, there's no way to apply a log trace value to the
/// logger, since the global logger isn't generally writable or accessible
/// to individual threads.  Additionally, each thread will have its own
/// log trace values. Log traces currently have to be passed by the
/// log::* caller within the log message.  Consider alternatives.
///
pub struct Logger {
    _logfile: conf::LogFile,
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
            _logfile: file.clone(),
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
    pub fn init(mut self) -> Result<(), log::SetLoggerError> {
        self.writer = Logger::writer().ok();
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

    pub fn writer() -> Result<UnixDatagram, String> {
        match UnixDatagram::unbound() {
            Ok(socket) => match socket.connect(SYSLOG_UNIX_PATH) {
                Ok(()) => Ok(socket),
                Err(e) => Err(format!("Cannot connect to unix socket: {e}")),
            },
            Err(e) => Err(format!("Cannot connect to unix socket: {e}")),
        }
    }

    /// Generate a log trace string from the epoch time and thread id.
    pub fn mk_log_trace() -> String {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime before UNIX EPOCH!");

        format!("{}{:0>5}", t.as_millis(), util::thread_id())
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

        let mut logmsg = record.args().to_string();

        // This is a hack to support ACTIVITY logging via the existing
        // log::* macros.
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

        let message = format!(
            "<{}>{} [{}:{}:{}:{}:{:0>5}] {}",
            severity,
            &self.application,
            levelname,
            process::id(),
            target,
            match record.line() {
                Some(l) => l,
                _ => 0,
            },
            util::thread_id(),
            logmsg
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
