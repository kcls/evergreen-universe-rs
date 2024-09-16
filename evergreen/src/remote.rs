use crate as eg;
use eg::Editor;
use eg::EgResult;
//use eg::EgValue;
use glob;
use std::env;
use std::fmt;
use std::fs;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
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

    try_typical_ssh_keys: bool,
}

impl fmt::Display for RemoteAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
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
            try_typical_ssh_keys: true,
        }
    }

    /// If read_mode is true, our remote_path = edi account "in_dir",
    /// otherwise our remote_path = edi account "path"
    pub fn from_edi_account(
        editor: &mut Editor,
        account_id: i64,
        read_mode: bool,
    ) -> EgResult<RemoteAccount> {
        let edi_account = editor
            .retrieve("acqedi", account_id)?
            .ok_or_else(|| editor.die_event())?;

        let mut account = RemoteAccount::from_url_string(edi_account["host"].str()?)?;

        account.remote_path = if read_mode {
            edi_account["in_dir"].as_str().map(|s| s.to_string())
        } else {
            edi_account["path"].as_str().map(|s| s.to_string())
        };

        if let Some(username) = edi_account["username"].as_str() {
            account.set_username(username);
        }

        if let Some(password) = edi_account["password"].as_str() {
            account.set_password(password);
        }

        Ok(account)
    }

    /// Extract whatever components we can from a URL.
    ///
    /// Example "sftp://localhost"
    pub fn from_url_string(url: &str) -> EgResult<RemoteAccount> {
        let url = Url::parse(url).map_err(|e| format!("Cannot parse URL: {url} : {e}"))?;

        let hostname = url.host_str().ok_or("URL has no host")?;
        let mut account = RemoteAccount::new(hostname);

        account.proto = if url.scheme() == "sftp" {
            Proto::Sftp
        } else {
            Proto::Ftp
        };

        if !url.username().is_empty() {
            account.set_username(url.username());
        }

        if !url.path().is_empty() {
            account.remote_path = Some(url.path().to_string());
        }

        Ok(account)
    }

    pub fn remote_path(&mut self) -> Option<&str> {
        self.remote_path.as_deref()
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

    /// Set the global timeout for tasks that may block.
    pub fn set_timeout(&mut self, timeout: u32) {
        self.timeout = timeout;
    }

    /// Connect and authenticate with the remote site.
    pub fn connect(&mut self) -> EgResult<()> {
        match self.proto {
            Proto::Sftp => self.connect_sftp(),
            _ => Err(format!("Unsupported protocol: {:?}", self.proto).into()),
        }
    }

    /// Returns a list of file paths matching our remote path and optional glob.
    pub fn ls(&self) -> EgResult<Vec<String>> {
        self.check_connected()?;

        match self.proto {
            Proto::Sftp => self.ls_sftp(),
            _ => Err(format!("Unsupported protocol: {:?}", self.proto).into()),
        }
    }

    /// Fetch a remote file by name, store the contents in a local
    /// file, and return the created File handle.
    pub fn get(&self, remote_file: &str, local_file: &str) -> EgResult<fs::File> {
        self.check_connected()?;

        match self.proto {
            Proto::Sftp => self.get_sftp(remote_file, local_file),
            _ => Err(format!("Unsupported protocol: {:?}", self.proto).into()),
        }
    }

    /// Returns an Err if we're not connected
    pub fn check_connected(&self) -> EgResult<()> {
        match self.proto {
            Proto::Sftp => self.check_connected_sftp(),
            _ => Err(format!("Unsupported protocol: {:?}", self.proto).into()),
        }
    }

    /// Returns an Err if we're not connected
    fn check_connected_sftp(&self) -> EgResult<()> {
        match self.sftp_session {
            Some(_) => Ok(()),
            _ => Err(format!("{self} is not connected to SFTP").into()),
        }
    }

    /// Fetch a remote file by name, store the contents in a local
    /// file, and return the created File handle.
    fn get_sftp(&self, remote_filename: &str, local_filename: &str) -> EgResult<fs::File> {
        let mut remote_file = self
            .sftp_session
            .as_ref()
            .unwrap()
            .open(Path::new(remote_filename))
            .map_err(|e| format!("Cannot open remote file {remote_filename} {e}"))?;

        let mut local_file = fs::File::create(Path::new(local_filename))
            .map_err(|e| format!("Cannot create local file {local_filename} {e}"))?;

        let mut bytes: Vec<u8> = Vec::new();
        remote_file
            .read_to_end(&mut bytes)
            .map_err(|e| format!("Cannot read remote file: {remote_filename} {e}"))?;

        local_file
            .write_all(bytes.as_slice())
            .map_err(|e| format!("Cannot write to local file: {local_filename} {e}"))?;

        Ok(local_file)
    }

    /// Returns a list of files/directories within our remote_path directory.
    ///
    /// If our remote_path contains a file name glob, the list only
    /// includes files that match the glob.
    fn ls_sftp(&self) -> EgResult<Vec<String>> {
        let (remote_path, maybe_glob) = self.remote_path_and_glob()?;

        log::info!("{self} listing directory {remote_path}");

        let mut files = Vec::new();

        let dir_path = Path::new(&remote_path);

        let contents = self
            .sftp_session
            .as_ref()
            .unwrap()
            .readdir(dir_path)
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
                            files.push(fullname);
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
            let sock_addr = format!("{host}:{port}")
                .to_socket_addrs()
                .map_err(|e| format!("Cannot resolve host: {host} : {e}"))?
                .next()
                .ok_or_else(|| format!("Cannot resolve host: {host}"))?;

            TcpStream::connect_timeout(&sock_addr, Duration::from_secs(self.timeout.into()))
        } else {
            TcpStream::connect((host, port))
        };

        let tcp_stream = tcp_result.map_err(|e| format!("Cannot connect to {host} : {e}"))?;

        let mut sess = ssh2::Session::new()
            .map_err(|e| format!("Cannot create SFTP session to {host} : {e}"))?;

        if self.timeout > 0 {
            sess.set_timeout(self.timeout * 1000); // ms
        }

        sess.set_tcp_stream(tcp_stream);

        sess.handshake()
            .map_err(|e| format!("SFTP handshake failed to {host} : {e}"))?;

        if let Some(password) = self.password.as_ref() {
            sess.userauth_password(username, password)
                .map_err(|e| format!("Password auth failed for host {host}: {e}"))?;
        } else {
            self.sftp_key_auth(&mut sess, username)?;
        }

        if !sess.authenticated() {
            return Err(format!("SFTP connection failed to authenticate with {host}").into());
        }

        let sftp = sess
            .sftp()
            .map_err(|e| format!("Cannot upgrade to SFTP connection for host {host}: {e}"))?;

        self.sftp_session = Some(sftp);

        log::info!("Successfully connected to SFTP at {host}");

        Ok(())
    }

    /// Authenticate via ssh key file, trying the file provided and/or
    /// typical local SSH key files.
    fn sftp_key_auth(&self, sess: &mut ssh2::Session, username: &str) -> EgResult<()> {
        let mut key_files = Vec::new();

        if let Some(keyfile) = self.ssh_private_key.as_ref() {
            key_files.push(keyfile.to_string());
        }

        if self.try_typical_ssh_keys {
            if let Some(home) = env::vars().find(|(k, _)| k == "HOME").map(|(_, v)| v) {
                let mut path_buf = PathBuf::new();

                path_buf.push(home);
                path_buf.push(".ssh");
                path_buf.push("id_rsa");

                key_files.push(path_buf.as_os_str().to_string_lossy().to_string());

                path_buf.pop();
                path_buf.push("dsa_rsa");

                key_files.push(path_buf.as_os_str().to_string_lossy().to_string());
            }
        }

        for key_file in key_files {
            log::debug!("Trying key file {key_file}");

            let result = sess.userauth_pubkey_file(
                username,
                None,
                Path::new(&key_file),
                self.ssh_private_key_password.as_deref(),
            );

            if result.is_ok() {
                return Ok(());
            }
        }

        Err("No suitable SSH keys found".into())
    }

    /// Return a directory path and a glob pattern if the provided path
    /// contains a glob file name (e.g. /foo/bar/*.edi).   Otherwise,
    /// returns None, meaning the originally provided path is the
    /// one that should be used for send/recv files.
    fn remote_path_and_glob(&self) -> EgResult<(String, Option<glob::Pattern>)> {
        let remote_path = self.remote_path.as_deref().unwrap_or("/");
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
            Some(glob_pattern),
        ))
    }
}
