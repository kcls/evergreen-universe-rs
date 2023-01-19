use super::addr::ClientAddress;
use super::conf;
use super::message::TransportMessage;
use redis::{Commands, ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use std::fmt;
use std::time;

/// Manages the Redis connection.
pub struct Bus {
    connection: redis::Connection,

    // Every bus connection has a unique client address.
    address: ClientAddress,

    domain: String,
}

impl Bus {
    pub fn new(config: &conf::BusClient) -> Result<Self, String> {
        let info = Bus::connection_info(config)?;

        log::trace!("Bus::new() connecting to {:?}", info);

        let client = match redis::Client::open(info) {
            Ok(c) => c,
            Err(e) => {
                return Err(format!("Error opening Redis connection: {e}"));
            }
        };

        let connection = match client.get_connection() {
            Ok(c) => c,
            Err(e) => Err(format!("Bus connect error: {e}"))?,
        };

        let domain = config.domain().name();
        let addr = ClientAddress::new(domain);

        let bus = Bus {
            domain: domain.to_string(),
            connection,
            address: addr,
        };

        Ok(bus)
    }

    /// Generates the Redis connection Info
    fn connection_info(config: &conf::BusClient) -> Result<ConnectionInfo, String> {
        // Build the connection info by hand because it gives us more
        // flexibility/control than compiling a URL string.

        let redis_con = RedisConnectionInfo {
            db: 0,
            username: Some(config.username().to_string()),
            password: Some(config.password().to_string()),
        };

        let domain = config.domain();
        let con_addr = ConnectionAddr::Tcp(domain.name().to_string(), domain.port());

        Ok(ConnectionInfo {
            addr: con_addr,
            redis: redis_con,
        })
    }

    pub fn address(&self) -> &ClientAddress {
        &self.address
    }

    pub fn set_address(&mut self, addr: &ClientAddress) {
        self.address = addr.clone();
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn connection(&mut self) -> &mut redis::Connection {
        &mut self.connection
    }

    /// Returns at most one String pulled from the queue or None if the
    /// pop times out or is interrupted.
    ///
    /// The string will be whole, unparsed JSON string.
    fn recv_one_chunk(
        &mut self,
        mut timeout: i32,
        recipient: Option<&str>,
    ) -> Result<Option<String>, String> {
        let recipient = match recipient {
            Some(s) => s.to_string(),
            None => self.address().full().to_string(),
        };

        let value: String;

        if timeout == 0 {
            // non-blocking

            // LPOP returns a scalar response.
            value = match self.connection().lpop(&recipient, None) {
                Ok(c) => c,
                Err(e) => match e.kind() {
                    redis::ErrorKind::TypeError => {
                        // Will read a Nil value on timeout.  That's OK.
                        return Ok(None);
                    }
                    _ => return Err(format!("recv_one_chunk failed: {e}")),
                },
            };
        } else {
            // Blocking

            // BLPOP returns the name of the popped list and the value.
            if timeout < 0 {
                // Timeout 0 means block indefinitely in Redis.
                timeout = 0;
            }

            let resp: Vec<String> = match self.connection().blpop(&recipient, timeout as usize) {
                Ok(r) => r,
                Err(e) => return Err(format!("Redis list pop error: {e} recipient={recipient}")),
            };

            if resp.len() == 0 {
                // No message received
                return Ok(None);
            }

            value = resp[1].to_string(); // resp = [key, value]
        }

        log::trace!("recv_one_value() pulled from bus: {}", value);

        Ok(Some(value))
    }

    /// Returns at most one JSON value pulled from the queue or None if
    /// the list pop times out or the pop is interrupted by a signal.
    fn recv_one_value(
        &mut self,
        timeout: i32,
        stream: Option<&str>,
    ) -> Result<Option<json::JsonValue>, String> {
        let json_string = match self.recv_one_chunk(timeout, stream)? {
            Some(s) => s,
            None => {
                return Ok(None);
            }
        };

        log::trace!("{self} read json from the bus: {json_string}");

        match json::parse(&json_string) {
            Ok(json_val) => Ok(Some(json_val)),
            Err(err_msg) => {
                return Err(format!("Error parsing JSON: {:?}", err_msg));
            }
        }
    }

    /// Returns at most one JSON value pulled from the queue.
    ///
    /// Keeps trying until a value is returned or the timeout is exceeded.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Time in seconds to wait for a value.
    ///     A negative value means to block indefinitely.
    ///     0 means do not block.
    pub fn recv_json_value(
        &mut self,
        timeout: i32,
        stream: Option<&str>,
    ) -> Result<Option<json::JsonValue>, String> {
        let mut option: Option<json::JsonValue>;

        if timeout == 0 {
            // See if any data is ready now
            return self.recv_one_value(timeout, stream);
        } else if timeout < 0 {
            // Keep trying until we have a result.
            loop {
                option = self.recv_one_value(timeout, stream)?;
                if let Some(_) = option {
                    return Ok(option);
                }
            }
        }

        // Keep trying until we have a result or exhaust the timeout.

        let mut seconds = timeout;

        while seconds > 0 {
            let now = time::SystemTime::now();

            option = self.recv_one_value(timeout, stream)?;

            match option {
                None => {
                    if seconds < 0 {
                        return Ok(None);
                    }
                    seconds -= now.elapsed().unwrap().as_secs() as i32;
                    continue;
                }
                _ => return Ok(option),
            }
        }

        Ok(None)
    }

    pub fn recv(
        &mut self,
        timeout: i32,
        stream: Option<&str>,
    ) -> Result<Option<TransportMessage>, String> {
        let json_op = self.recv_json_value(timeout, stream)?;

        match json_op {
            Some(ref jv) => Ok(TransportMessage::from_json_value(jv)),
            None => Ok(None),
        }
    }

    /// Sends a TransportMessage to the "to" value in the message.
    pub fn send(&mut self, msg: &TransportMessage) -> Result<(), String> {
        self.send_to(msg, msg.to())
    }

    /// Sends a TransportMessage to the specified ClientAddress, regardless
    /// of what value is in the msg.to() field.
    pub fn send_to(&mut self, msg: &TransportMessage, recipient: &str) -> Result<(), String> {
        let json_str = msg.to_json_value().dump();

        log::trace!("send() writing chunk to={}: {}", recipient, json_str);

        let res: Result<i32, _> = self.connection().rpush(recipient, json_str);

        if let Err(e) = res {
            return Err(format!("Error in send() {e}"));
        }

        Ok(())
    }

    /// Remove all pending data from the stream while leaving the stream intact.
    pub fn clear_stream(&mut self) -> Result<(), String> {
        let sname = self.address().full().to_string();
        self.clear_named_stream(&sname)
    }

    pub fn clear_named_stream(&mut self, stream: &str) -> Result<(), String> {
        log::trace!("Clearing stream {stream}");

        let res: Result<i32, _> = self.connection().del(stream);

        if let Err(e) = res {
            return Err(format!("Error in clear_stream(): {e}"));
        }

        Ok(())
    }

    /// Delete our stream.
    ///
    /// Redis connectionts are closed on free, so no specific
    /// disconnect action is taken.
    pub fn disconnect(&mut self) -> Result<(), String> {
        self.clear_stream()
    }

    // See Redis KEYS command.
    pub fn keys(&mut self, pattern: &str) -> Result<Vec<String>, String> {
        let res: Result<Vec<String>, _> = self.connection().keys(pattern);

        if let Err(e) = res {
            return Err(format!("Error in keys(): {e}"));
        }

        Ok(res.unwrap())
    }

    pub fn llen(&mut self, key: &str) -> Result<i32, String> {
        let res: Result<i32, _> = self.connection().llen(key);

        if let Err(e) = res {
            return Err(format!("Error in llen(): {e}"));
        }

        Ok(res.unwrap())
    }

    pub fn ttl(&mut self, key: &str) -> Result<i32, String> {
        let res: Result<i32, _> = self.connection().ttl(key);

        if let Err(e) = res {
            return Err(format!("Error in ttl(): {e}"));
        }

        Ok(res.unwrap())
    }

    pub fn lrange(&mut self, key: &str,
        start: isize, stop: isize) -> Result<Vec<String>, String> {

        let res: Result<Vec<String>, _> = self.connection().lrange(key, start, stop);

        if let Err(e) = res {
            return Err(format!("Error in lrange(): {e}"));
        }

        Ok(res.unwrap())
    }


    /// Set the expire time on the specified key to 'timeout' seconds from now.
    pub fn set_key_timeout(&mut self, key: &str, timeout: u64) -> Result<i32, String> {
        let res: Result<i32, _> = self.connection().expire(key, timeout as usize);

        if let Err(ref e) = res {
            Err(format!("Error in set_key_timeout(): {e}"))?;
        }

        let val = res.unwrap();
        Ok(val)
    }
}

impl fmt::Display for Bus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Bus {}", self.address())
    }
}
