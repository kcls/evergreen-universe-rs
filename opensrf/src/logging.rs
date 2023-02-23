use super::conf;
///! OpenSRF Syslog
use log;
use std::net::Shutdown;
use std::os::unix::net::UnixDatagram;
use std::panic::Location;
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
    _logfile: conf::LogFile,
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
            _logfile: file.clone(),
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
                Err(e) => Err(format!("Cannot connext to unix socket: {e}")),
            },
            Err(e) => Err(format!("Cannot connext to unix socket: {e}")),
        }
    }

    /// Log activity.
    ///
    /// The stock log crate does not have an "activity" log option or other
    /// option we could use for the purpose.  It's also not possible to
    /// add log levels, short of maintaining a locally patched version.
    ///
    /// Provide an activity() call that requires the user to provide all
    /// the needed data.  Optionally, allow the caller to maintain and
    /// provide their own UnixDatagram so a new connection is not required
    /// with every log message.
    #[track_caller]
    pub fn activity(writer: Option<&UnixDatagram>, conf: &conf::BusClient, msg: &str) {
        // Keep the locally created writer in scope if needed.
        let mut local_writer: Option<UnixDatagram> = None;

        let writer = match writer {
            Some(w) => w,
            None => match Logger::writer() {
                Ok(s) => {
                    local_writer = Some(s);
                    local_writer.as_ref().unwrap()
                }
                Err(e) => {
                    eprintln!("Cannot write to unix socket: {e}");
                    return;
                }
            },
        };

        let app = Logger::find_app_name();
        let caller = Location::caller();
        let line = caller.line();

        // Remove the path portion of the file name.
        let file = caller.file();
        let filename: String;
        if let Some(n) = file.rsplit("/").next() {
            filename = n.to_string();
        } else {
            filename = String::from(file);
        }

        let facility = conf.logging().activity_log_facility().unwrap_or(
            conf.logging()
                .syslog_facility()
                .unwrap_or(syslog::Facility::LOG_LOCAL1),
        );

        let severity = facility as u8 | syslog::Severity::LOG_INFO as u8;
        let levelname = "ACT";

        let mut tid: String = thread_id::get().to_string();
        if tid.len() > TRIM_THREAD_ID {
            tid = tid.chars().skip(tid.len() - TRIM_THREAD_ID).collect();
        }

        let message = format!(
            "<{}>{} [{}:{}:{}:{}:{}] {}",
            severity,
            app,
            levelname,
            process::id(),
            filename,
            line,
            tid,
            msg,
        );

        if writer.send(message.as_bytes()).is_ok() {
            if local_writer.is_some() {
                // If we're using a writer created within this function,
                // shut it down when we're done.
                writer.shutdown(Shutdown::Both).ok();
            }
            return;
        }

        println!("{message}");
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
