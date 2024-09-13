use crate as eg;
use eg::EgResult;
use eg::EgValue;
use glob;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::fmt;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Proto {
    Sftp,
    Ftp,
}

pub struct RemoteAccount {
    host: String,
    port: Option<u16>,
    username: Option<String>,
    password: Option<String>,
    proto: Proto,

    remote_path: Option<String>,

    sftp_session: Option<ssh2::Sftp>,

    /// Connect/read timeout
    timeout: u32,

    /// Full path to an SSH private key file.
    ssh_private_key: Option<String>,
    ssh_private_key_password: Option<String>,
}

impl fmt::Display for RemoteAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,
            "RemoteAccount host={} user={}",
            self.host,
            self.username.as_deref().unwrap_or("")
        )
    }
}

impl RemoteAccount {

    pub fn new(host: &str) -> RemoteAccount {
        RemoteAccount {
            host: host.to_string(),
            port: None,
            username: None,
            password: None,
            proto: Proto::Sftp,
            remote_path: None,
            timeout: 0,
            ssh_private_key: None,
            ssh_private_key_password: None,
            sftp_session: None,
        }
    }

    /// Extract whatever components we can from a URL.
    ///
    /// Example "sftp://localhost"
    pub fn unpack_url_string(&mut self, url: &str) -> EgResult<()> {
        let url = Url::parse(url).map_err(|e| format!("Cannot parse URL: {url} : {e}"))?;

        if let Some(hostname) = url.host_str() {
            if hostname != self.host {
                self.host = hostname.to_string();
            }
        }

        match url.scheme() {
            "sftp" => self.proto = Proto::Sftp,
            "ftp" => self.proto = Proto::Ftp,
            _ => return Err(format!("Unsupported protocol: {}", url.scheme()).into()),
        }

        if !url.username().is_empty() {
            self.set_username(url.username());
        }

        Ok(())
    }

    pub fn set_remote_path(&mut self, remote_path: &str) {
        self.remote_path = Some(remote_path.to_string());
    }

    pub fn set_username(&mut self, username: &str) {
        self.username = Some(username.to_string());
    }

    pub fn set_password(&mut self, password: &str) {
        self.password = Some(password.to_string());
    }

    pub fn set_ssh_private_key(&mut self, keyfile: &str) {
        self.ssh_private_key = Some(keyfile.to_string());
    }

    pub fn set_ssh_private_key_password(&mut self, keypass: &str) {
        self.ssh_private_key_password = Some(keypass.to_string());
    }

    pub fn set_timeout(&mut self, timeout: u32) {
        self.timeout = timeout;
    }

    pub fn connect(&mut self) -> EgResult<()> {
        if self.proto == Proto::Sftp {
            self.connect_sftp()
        } else {
            Err(format!("Unsupported protocol: {:?}", self.proto).into())
        }
    }

    /// Returns a list of file paths matching our remote path and optional glob.
    pub fn ls(&self) -> EgResult<Vec<String>> {
        if self.proto == Proto::Sftp {
            self.ls_sftp()
        } else {
            Err(format!("Unsupported protocol: {:?}", self.proto).into())
        }
    }

    /// Returns a list of files/directories within our remote_path directory.
    ///
    /// If our remote_path contains a file name glob, the list only
    /// includes files that match the glob.
    pub fn ls_sftp(&self) -> EgResult<Vec<String>> {
        let (remote_path, maybe_glob) = self.remote_path_and_glob()?;

        log::info!("{self} listing directory {remote_path}");

        let mut files = Vec::new();

        let dir_path = Path::new(&remote_path);

        if self.sftp_session.is_none() {
            return Err("SFTP session is not connected".into());
        }

        let contents = self.sftp_session.as_ref().unwrap().readdir(dir_path)
            .map_err(|e| format!("{self} cannot list directory {remote_path} : {e}"))?;

        for (file, _) in contents {

            let fullname = match file.to_str() {
                Some(s) => s.to_string(),
                None => {
                    log::warn!("{self} skipping non-stringifiable path: {file:?}");
                    continue;
                }
            };

            if let Some(pattern) = maybe_glob.as_ref() {
                if let Some(file_name) = file.file_name() {
                    if let Some(name) = file_name.to_str() {
                        if pattern.matches(name) {
                            files.push(name.to_string());
                        }
                    } else {
                        log::warn!("{self} skipping non-stringifiable path: {file_name:?}");
                    }
                }
            } else {
                files.push(fullname);
            }
        }

        Ok(files)
    }

    fn connect_sftp(&mut self) -> EgResult<()> {
        let port = self.port.unwrap_or(22);
        let host = self.host.as_str();

        let username = self
            .username
            .as_deref()
            .ok_or("SFTP connection requires a username")?;

        let tcp_result = if self.timeout > 0 {
            todo!();
            /* TODO SocketAddr needs
            TcpStream::connect_timeout((host, port), Duration::from_secs(self.timeout.into()))
            */
        } else {
            TcpStream::connect((host, port))
        };

        let tcp_stream = tcp_result.map_err(|e| format!("Cannot connect to {host} : {e}"))?;

        let mut sess =
            ssh2::Session::new().map_err(|e| format!("Cannot create SFTP session to {host} : {e}"))?;

        sess.set_tcp_stream(tcp_stream);

        sess.handshake()
            .map_err(|e| format!("SFTP handshake failed to {host} : {e}"))?;

        if let Some(keyfile) = self.ssh_private_key.as_ref() {
            sess.userauth_pubkey_file(
                username,
                None,
                Path::new(keyfile),
                self.ssh_private_key_password.as_deref(),
            )
            .map_err(|e| {
                format!(
                    "Public key authentication failed for host {host} and key file {keyfile}: {e}"
                )
            })?;
        } else if let Some(password) = self.password.as_ref() {
            sess.userauth_password(username, password)
                .map_err(|e| format!("Password authentication failed for host {host}: {e}"))?;
        } else {
            return Err("SFTP connection requires an SSH key or password".into());
        }

        if !sess.authenticated() {
            return Err(format!("SFTP connection failed to authenticate with {host}").into());
        }

        let sftp = sess
            .sftp()
            .map_err(|e| format!("Cannot upgrade to SFTP connection for host {host}: {e}"))?;

        self.sftp_session = Some(sftp);

        Ok(())
    }

    /// Return a directory path and a glob pattern if the provided path
    /// contains a glob file name (e.g. /foo/bar/*.edi).   Otherwise,
    /// returns None, meaning the originally provided path is the
    /// one that should be used for send/recv files.
    fn remote_path_and_glob(&self) -> EgResult<(String, Option<glob::Pattern>)> {

        let remote_path = match self.remote_path.as_ref() {
            Some(p) => p,
            None => return Err("RemoteAccont has no remote path to unpack".into()),
        };

        let full_path = Path::new(remote_path);

        // Is there a trailing file name or is it just a directory?
        let filename = match full_path.file_name().map(|f| f.to_str()) {
            Some(Some(f)) => f,
            _ => return Ok((remote_path.to_string(), None)),
        };

        // Does the file name contain a glob star
        if !filename.contains('*') {
            return Ok((remote_path.to_string(), None));
        }

        // It's a glob.

        let glob_pattern = glob::Pattern::new(filename)
            .map_err(|e| format!("Invalid glob pattern: {filename} : {e}"))?;

        let mut path_buf = PathBuf::new();

        // Rebuild the path from its components then trim the globbed filename
        for part in full_path.iter() {
            path_buf.push(part);
        }

        // Remove the filename part
        path_buf.pop();

        Ok((
            path_buf.into_os_string().to_string_lossy().to_string(),
            Some(glob_pattern)
        ))
    }
}
