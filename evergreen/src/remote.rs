use crate as eg;
use eg::EgResult;
use eg::EgValue;
use glob;
use ssh2::Session;
use std::net::SocketAddr;
use std::net::TcpStream;
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

    sftp_session: Option<ssh2::Sftp>,

    /// Connect/read timeout
    timeout: u32,

    /// Full path to an SSH private key file.
    ssh_private_key: Option<String>,
    ssh_private_key_password: Option<String>,
}

impl Default for RemoteAccount {
    fn default() -> RemoteAccount {
        RemoteAccount {
            host: "".to_string(), // meh
            port: None,
            username: None,
            password: None,
            proto: Proto::Sftp,
            timeout: 0,
            ssh_private_key: None,
            ssh_private_key_password: None,
            sftp_session: None,
        }
    }
}

impl RemoteAccount {
    /// Example "sftp://localhost"
    pub fn from_url_string(url: &str) -> EgResult<RemoteAccount> {
        let url = Url::parse(url).map_err(|e| format!("Cannot parse URL: {url} : {e}"))?;

        let hostname = url
            .host_str()
            .ok_or_else(|| format!("URL contains no host: {url}"))?
            .to_string();

        let mut account = RemoteAccount {
            host: hostname.to_string(),
            ..Default::default()
        };

        account.host = hostname.to_string();

        match url.scheme() {
            "sftp" => account.proto = Proto::Sftp,
            "ftp" => account.proto = Proto::Ftp,
            _ => return Err(format!("Unsupported protocol: {}", url.scheme()).into()),
        }

        if !url.username().is_empty() {
            account.username = Some(url.username().to_string());
        }

        Ok(account)
    }

    pub fn set_username(&mut self, username: &str) {
        self.username = Some(username.to_string());
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
            Session::new().map_err(|e| format!("Cannot create SFTP session to {host} : {e}"))?;

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
}
