use crate::osrf::addr::BusAddress;
use crate::osrf::conf;
use crate::osrf::logging::Logger;
use crate::osrf::message::TransportMessage;
use crate::util;
use crate::EgResult;
use redis::{Commands, ConnectionAddr, ConnectionInfo, RedisConnectionInfo};
use std::fmt;

/// Manages a Redis connection.
pub struct Bus {
    connection: redis::Connection,

    /// Every bus connection has a unique client address.
    address: BusAddress,

    /// Name of the router running on our primary domain.
    router_name: String,

    /// Some clients don't need the IDL and all its classes to function
    /// (e.g. the router).  Using raw_data_mode allows for transport
    /// messages to be parsed and serialized without concern for
    /// IDL-classed information stored in the message.
    raw_data_mode: bool,
}

impl Bus {
    pub fn new(config: &conf::BusClient) -> EgResult<Self> {
        let info = Bus::connection_info(config)?;

        log::trace!("Bus::new() connecting to {:?}", info);

        let client = redis::Client::open(info)
            .or_else(|e| Err(format!("Error opening Redis connection: {e}")))?;

        let connection = client
            .get_connection()
            .or_else(|e| Err(format!("Bus connect error: {e}")))?;

        let username = config.username();
        let domain = config.domain().name();
        let addr = BusAddress::for_client(username, domain);

        let bus = Bus {
            connection,
            raw_data_mode: false,
            address: addr,
            router_name: config.router_name().to_string(),
        };

        Ok(bus)
    }

    pub fn set_raw_data_mode(&mut self, on: bool) {
        self.raw_data_mode = on;
    }

    /// Generates the Redis connection Info
    ///
    /// Builds the connection info by hand because it gives us more
    /// flexibility/control than compiling a URL string.
    fn connection_info(config: &conf::BusClient) -> EgResult<ConnectionInfo> {
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

    /// The unique bus address for this bus connection.
    pub fn address(&self) -> &BusAddress {
        &self.address
    }

    /// Apply a new bus address
    pub fn set_address(&mut self, addr: &BusAddress) {
        self.address = addr.clone();
    }

    /// Generates a new BusAddress and applies it to this Bus.
    pub fn generate_address(&mut self) {
        self.address = BusAddress::for_client(self.username(), self.domain());
    }

    /// The name of the router running on our primary domain.
    pub fn router_name(&self) -> &str {
        &self.router_name
    }

    /// Our primary domain
    pub fn domain(&self) -> &str {
        self.address().domain()
    }

    pub fn username(&self) -> &str {
        self.address().username()
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
    ) -> EgResult<Option<String>> {
        let recipient = match recipient {
            Some(s) => s.to_string(),
            None => self.address().as_str().to_string(),
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
                    _ => return Err(format!("recv_one_chunk failed: {e}").into()),
                },
            };
        } else {
            // Blocking

            if timeout < 0 {
                // Timeout 0 means block indefinitely in Redis.
                timeout = 0;
            }

            let mut resp: Vec<String> = self
                .connection()
                .blpop(&recipient, timeout as usize)
                .or_else(|e| Err(format!("Redis blpop error recipient={recipient} : {e}")))?;

            if resp.len() > 1 {
                // BLPOP returns the name of the popped list and the value.
                // resp = [key, value]
                value = resp.remove(1);
            } else {
                // No message received
                return Ok(None);
            }
        }

        log::trace!("recv_one_value() pulled from bus: {}", value);

        Ok(Some(value))
    }

    /// Returns at most one JSON value pulled from the queue or None if
    /// the list pop times out or the pop is interrupted by a signal.
    fn recv_one_value(
        &mut self,
        timeout: i32,
        recipient: Option<&str>,
    ) -> EgResult<Option<json::JsonValue>> {
        let json_string = match self.recv_one_chunk(timeout, recipient)? {
            Some(s) => s,
            None => {
                return Ok(None);
            }
        };

        log::trace!("{self} read json from the bus: {json_string}");

        match json::parse(&json_string) {
            Ok(json_val) => Ok(Some(json_val)),
            Err(err) => Err(format!("Error parsing JSON: {err:?}").into()),
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
        recipient: Option<&str>,
    ) -> EgResult<Option<json::JsonValue>> {
        let mut option: Option<json::JsonValue>;

        if timeout == 0 {
            // See if any data is ready now
            return self.recv_one_value(timeout, recipient);
        } else if timeout < 0 {
            // Keep trying until we have a result.
            loop {
                option = self.recv_one_value(timeout, recipient)?;
                if let Some(_) = option {
                    return Ok(option);
                }
            }
        }

        // Keep trying until we have a result or exhaust the timeout.
        let timer = util::Timer::new(timeout);

        while !timer.done() {
            option = self.recv_one_value(timer.remaining(), recipient)?;

            if option.is_some() {
                return Ok(option);
            }
        }

        Ok(None)
    }

    /// Returns at most one TransportMessage.
    ///
    /// Keeps trying until a value is returned or the timeout is exceeded.
    ///
    /// Avoids exiting with an error on receipt of invalid data from the
    /// network, since exiting early can result in leaving additional
    /// (streamed) invalid messages on the bus for later retrieval,
    /// because presumably the original client request exited instead of
    /// processing all of the messages.
    ///
    /// Invalid messages left on the bus can result in later failures
    /// for unrelated requests as the old invalid messages are pulled
    /// and parsed, resulting in additional early exits.
    ///
    /// Instead, act as if no response was received.  This is still an
    /// error condition that requries repair, but this way the impact is
    /// limited to the failed request.
    ///
    /// This condition was seen in the wild when introspecting Perl
    /// services, which contains unknown "__c" message classes.
    ///
    /// # Arguments
    ///
    /// * `timeout` - Time in seconds to wait for a response.
    ///     A negative value means to block indefinitely.
    ///     0 means do not block.
    /// * `recipient` - Optionally specify the name of the destination
    ///     queue/stream.  This overrides using the bus-specific
    ///     bus address as the recipient.
    pub fn recv(
        &mut self,
        timeout: i32,
        recipient: Option<&str>,
    ) -> EgResult<Option<TransportMessage>> {
        let json_op = self.recv_json_value(timeout, recipient)?;

        if let Some(jv) = json_op {
            match TransportMessage::from_json_value(jv, self.raw_data_mode) {
                Ok(v) => return Ok(Some(v)),
                Err(e) => {
                    log::error!("Error translating JSON value into EgValue: {e}");
                    return Ok(None);
                }
            };
        } else {
            Ok(None)
        }
    }

    /// Sends a TransportMessage to the "to" value in the message.
    pub fn send(&mut self, msg: TransportMessage) -> EgResult<()> {
        // TODO refactor so we can avoi this to_string()
        let to = msg.to().to_string();
        self.send_to(msg, &to)
    }

    /// Sends a TransportMessage to the specified BusAddress, regardless
    /// of what value is in the msg.to() field.
    pub fn send_to(&mut self, msg: TransportMessage, recipient: &str) -> EgResult<()> {
        let mut json_val = msg.into_json_value();

        // Play a little inside baseball here and tag the message
        // with our log trace.  This way the layers above don't have
        // to worry about it.
        json_val["osrf_xid"] = json::from(Logger::get_log_trace());

        let json_str = json_val.dump();

        log::trace!("send() writing chunk to={}: {}", recipient, json_str);

        let res: Result<i32, _> = self.connection().rpush(recipient, json_str);

        if let Err(e) = res {
            return Err(format!("Error in send() {e}").into());
        }

        Ok(())
    }

    /// Returns a list of keys that match the provided pattern.
    pub fn keys(&mut self, pattern: &str) -> EgResult<Vec<String>> {
        let res: Result<Vec<String>, _> = self.connection().keys(pattern);

        if let Err(e) = res {
            return Err(format!("Error in keys(): {e}").into());
        }

        Ok(res.unwrap())
    }

    /// Returns the length of the array specified by 'key'.
    pub fn llen(&mut self, key: &str) -> EgResult<i32> {
        let res: Result<i32, _> = self.connection().llen(key);

        if let Err(e) = res {
            return Err(format!("Error in llen(): {e}").into());
        }

        Ok(res.unwrap())
    }

    /// Returns the time-to-live (in seconds) of the specified key.
    ///
    /// Return -1 if no expire time is set, -2 if no such key exists.
    pub fn ttl(&mut self, key: &str) -> EgResult<i32> {
        let res: Result<i32, _> = self.connection().ttl(key);

        if let Err(e) = res {
            return Err(format!("Error in ttl(): {e}").into());
        }

        Ok(res.unwrap())
    }

    /// Returns an array slice as a Vec of Strings.
    pub fn lrange(&mut self, key: &str, start: isize, stop: isize) -> EgResult<Vec<String>> {
        let res: Result<Vec<String>, _> = self.connection().lrange(key, start, stop);

        if let Err(e) = res {
            return Err(format!("Error in lrange(): {e}").into());
        }

        Ok(res.unwrap())
    }

    /// Set the expire time on the specified key to 'timeout' seconds from now.
    pub fn set_key_timeout(&mut self, key: &str, timeout: u64) -> EgResult<i32> {
        let res: Result<i32, _> = self.connection().expire(key, timeout as usize);

        if let Err(ref e) = res {
            Err(format!("Error in set_key_timeout(): {e}"))?;
        }

        let val = res.unwrap();
        Ok(val)
    }

    /// Remove all pending data from the recipient queue.
    pub fn clear_bus(&mut self) -> EgResult<()> {
        let stream = self.address().as_str().to_string(); // mut borrow
        let res: Result<i32, _> = self.connection().del(stream);

        if let Err(e) = res {
            return Err(format!("Error in queue clear(): {e}").into());
        }

        Ok(())
    }
}

/// Good for debugging / logging
impl fmt::Display for Bus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Bus {}", self.address().as_str())
    }
}

/// Every Bus instance has a unique address which will never be used
/// again.  When this bus instance is dropped, remove any remaining
/// messages destined for this address since otherwise they will linger.
impl Drop for Bus {
    /// Similar to clear_bus but avoids any logging / error reporting.
    fn drop(&mut self) {
        let stream = self.address().as_str().to_string();
        let res: Result<i32, _> = self.connection().del(&stream);
        res.ok();
    }
}
