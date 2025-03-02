//! Handles a single Z39 connected session.
use crate::conf;
use crate::error::LocalError;
use crate::error::LocalResult;
use crate::limits::RateLimiter;
use crate::query::Z39QueryCompiler;

use evergreen as eg;
use z39::types::oid;
use z39::types::pdu::*;

use std::fmt;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const NETWORK_BUFSIZE: usize = 1024;

#[derive(Debug, Default)]
struct BibSearch {
    database_name: Option<String>,
    bib_record_ids: Option<Vec<i64>>,
}

pub(crate) struct Z39Session {
    tcp_stream: TcpStream,
    peer_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    client: eg::Client,
    last_search: Option<BibSearch>,
    limits: Option<Arc<Mutex<RateLimiter>>>,
}

impl fmt::Display for Z39Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Z39Session [{}]", self.peer_addr)
    }
}

impl Z39Session {
    pub fn new(
        tcp_stream: TcpStream,
        peer_addr: SocketAddr,
        bus: eg::osrf::bus::Bus,
        shutdown: Arc<AtomicBool>,
        limits: Option<Arc<Mutex<RateLimiter>>>,
    ) -> Self {
        let client = eg::Client::from_bus(bus);

        Self {
            tcp_stream,
            peer_addr,
            shutdown,
            client,
            limits,
            last_search: None,
        }
    }

    /// Main listen loop
    pub fn listen(&mut self) -> LocalResult<()> {
        log::info!("{self} starting session");

        let mut bytes = Vec::new();

        let mut last_activity = Instant::now();

        let timeout = if conf::global().idle_timeout > 0 {
            Some(Duration::from_secs(conf::global().idle_timeout as u64))
        } else {
            None
        };

        // Read bytes from the TCP stream, feeding them into the BER
        // parser, until a complete message is formed.  Handle the
        // message, rinse and repeat.
        loop {
            let mut buffer = [0u8; NETWORK_BUFSIZE];

            let count = match self.tcp_stream.read(&mut buffer) {
                Ok(c) => c,
                Err(e) => match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        if self.shutdown.load(Ordering::Relaxed) {
                            log::debug!("{self} Shutdown signal received, exiting listen loop");
                            break;
                        }

                        if let Some(duration) = timeout {
                            if (Instant::now() - last_activity) > duration {
                                log::info!("{self} disconnecting on idle timeout");
                                self.send_close(
                                    CloseReason::LackOfActivity,
                                    Some("Timed Out".to_string()),
                                )?;
                                break;
                            }
                        }

                        // Go back and wait for requests to arrive.
                        continue;
                    }
                    _ => {
                        // Connection severed.  we're done.
                        log::info!("{self} Socket closed: {e}");
                        break;
                    }
                },
            };

            if count == 0 {
                // Returning Ok(0) from read for a TcpStream indicates the
                // remote end of the stream was shut down.
                log::debug!("{self} socket shut down by remote endpoint");
                break;
            }

            bytes.extend_from_slice(&buffer[0..count]);

            // Parse the message bytes
            let Some(msg) = Message::from_bytes(&bytes)? else {
                log::debug!("{self} partial message read; more bytes needed");
                continue;
            };

            // Once we have a whole message, count it as activity.
            last_activity = Instant::now();

            // Reset the byte array for the next message cycle.
            bytes.clear();

            // Unless the caller is closing the connection, verify they
            // have not exceeded the configured rate limit.
            if !matches!(msg.payload(), MessagePayload::Close(_)) {
                self.check_activity_limit()?;
            }

            // Handle the message
            let resp = match self.handle_message(msg) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("handle_message() exited with {e}");

                    let (reason, diag) = match e {
                        // Avoid sending internal debug info to the client.
                        LocalError::Internal(_) => (CloseReason::SystemProblem, None),
                        _ => (CloseReason::ProtocolError, Some(e.to_string())),
                    };

                    self.send_close(reason, diag)?;
                    break;
                }
            };

            self.send_reply(resp)?;
        }

        log::info!("{self} session exiting");

        Ok(())
    }

    /// Checks for excessive activity and inserts a sleep when too many
    /// requests have been encountered.
    ///
    /// TODO option to send a Close and subsequently disconnect instead
    /// of pausing when the rate limit is exceeded.
    fn check_activity_limit(&mut self) -> LocalResult<()> {
        let Some(ref limits) = self.limits else {
            return Ok(());
        };

        let permitted = {
            let mut limiter = match limits.lock() {
                Ok(l) => l,
                Err(e) => {
                    // As a safety valve for now, if locking errors
                    // occur, move on and let the activity take place.
                    // Should only happen of another thread paniced with
                    // the lock open.
                    log::error!("{self} limiter lock error: {e}");
                    return Ok(());
                }
            };

            limiter.track_event(&self.peer_addr.ip())
            // lock drops here
        };

        if permitted {
            return Ok(());
        }

        if conf::global().close_on_exceeds_rate {
            log::info!("{self} exceeded rate limit; closing connection");

            self.send_close(
                CloseReason::CostLimit,
                Some("Exceeded Rate Limit".to_string()),
            )

        } else {
            log::info!("{self} exceeded rate limit; pausing");

            let seconds = conf::global().rate_throttle_pause;

            if seconds > 0 {
                std::thread::sleep(Duration::from_secs(seconds.into()));
            }

            Ok(())
        }
    }

    /// Send a Close message to the caller with the provided close reason
    /// and optional diagnostic info.
    fn send_close(&mut self, reason: CloseReason, diag: Option<String>) -> LocalResult<()> {
        log::debug!("{self} sending Close {reason:?} {diag:?}");

        let close = Close {
            close_reason: reason,
            diagnostic_information: diag,
            ..Default::default()
        };

        self.send_reply(Message::from_payload(MessagePayload::Close(close)))
    }

    /// Send message bytes to the caller.
    fn send_reply(&mut self, payload: Message) -> LocalResult<()> {
        let bytes = payload.to_bytes()?;

        log::trace!("{self} replying with bytes: {bytes:?}");

        Ok(self
            .tcp_stream
            .write_all(bytes.as_slice())
            .map_err(|e| e.to_string())?)
    }

    /// Shut down the session's TcpStrean.
    ///
    /// Ignores errors.
    pub fn shutdown(&mut self) {
        self.tcp_stream.shutdown(std::net::Shutdown::Both).ok();
    }

    /// Take the underyling bus from our Evergreen client.
    ///
    /// Panics if self.client has no Bus.
    pub fn take_bus(&mut self) -> eg::osrf::bus::Bus {
        self.client.take_bus()
    }

    /// Z39 message handler.
    ///
    /// Dispatches each message to its handler.
    fn handle_message(&mut self, msg: Message) -> LocalResult<Message> {
        log::debug!("{self} processing message {msg:?}");

        let payload = match &msg.payload {
            MessagePayload::InitializeRequest(r) => self.handle_init_request(r)?,
            MessagePayload::SearchRequest(r) => self.handle_search_request(r)?,
            MessagePayload::PresentRequest(r) => self.handle_present_request(r)?,
            _ => return Err(format!("handle_message() unsupported message type: {msg:?}").into()),
        };

        Ok(Message::from_payload(payload))
    }

    /// Handle the InitializeResponse
    ///
    /// Canned response.
    fn handle_init_request(&mut self, _req: &InitializeRequest) -> LocalResult<MessagePayload> {
        Ok(MessagePayload::InitializeResponse(
            InitializeResponse::default(),
        ))
    }

    /// Perform a bib record search and retain the results in a
    /// BibSearch for subsequent bib retrievals via PresentRequest.
    fn handle_search_request(&mut self, req: &SearchRequest) -> LocalResult<MessagePayload> {
        log::info!("{self} search query: {:?}", req.query);

        let mut resp = SearchResponse::default();

        // See if the caller requested a specific database by name.
        let db_name = if let Some(dbn) = req.database_names.first() {
            let DatabaseName::Name(s) = dbn;
            Some(s.as_str())
        } else {
            None
        };

        let database = conf::global().find_database(db_name)?;

        let result = if database.use_elasticsearch() {
            match self.bib_search_elastic(req, database) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("{self} bib search failed: {e}");
                    return Err(e);
                }
            }
        } else {
            todo!("Native Evergreen support needed");
        };

        let mut bib_search = BibSearch::default();

        // Search succeeded, even if no results were found.
        resp.search_status = true;

        let bib_ids = match result {
            Some(ids) => ids,
            None => {
                self.last_search = Some(bib_search);
                return Ok(MessagePayload::SearchResponse(resp));
            }
        };

        log::info!("Search returned IDs {bib_ids:?}");

        resp.result_count = bib_ids.len() as u32;

        bib_search.database_name = db_name.map(|s| s.to_string());
        bib_search.bib_record_ids = Some(bib_ids);

        self.last_search = Some(bib_search);

        Ok(MessagePayload::SearchResponse(resp))
    }

    /// Search for bib records
    fn bib_search_elastic(
        &mut self,
        req: &SearchRequest,
        database: &conf::Z39Database,
    ) -> LocalResult<Option<Vec<i64>>> {
        let compiler = Z39QueryCompiler::new(database);

        let query = compiler.compile(&req.query)?;

        log::info!("{self} compiled search query: {query}");

        if query.is_empty() {
            return Ok(None);
        }

        let search = eg::hash! {
            "size": database.bib_search_limit(),
            "from": 0, // offset
            "sort": [
                {"_score": "desc"},
                {"id": "desc"},
            ],
            "query": {
                "bool": {
                    "must": {
                        "query_string": {
                            "query": query,
                            "default_operator": "AND",
                            "default_field": "keyword.text*",
                        }
                    }
                }
            }
        };

        let method = "open-ils.search.elastic.bib_search";

        let Some(result) = self
            .client
            .send_recv_one("open-ils.search", method, search)?
        else {
            // Search can return None if the request times out.
            // Treat it as 0 results.
            return Ok(None);
        };

        // Panics if the database returns a non-integer bib ID.
        let bib_ids: Vec<i64> = result["ids"]
            .members()
            .map(|arr| arr[0].int_required())
            .collect();

        Ok(Some(bib_ids))
    }

    /// Collect and return the requests bib records from the preceding SearchRequest.
    fn handle_present_request(&mut self, req: &PresentRequest) -> LocalResult<MessagePayload> {
        let mut resp = PresentResponse::default();

        let Some(search) = self.last_search.as_ref() else {
            log::warn!("{self} PresentRequest called with no search in progress");
            return Ok(MessagePayload::PresentResponse(resp));
        };

        let database = conf::global().find_database(search.database_name.as_deref())?;

        let num_requested = req.number_of_records_requested as usize;
        let mut start_point = req.reset_set_start_point as usize;

        // subtract 1 without underflowing; neat.
        start_point = start_point.saturating_sub(1);

        let bib_record_ids = match search.bib_record_ids.as_ref() {
            Some(ids) => ids,
            None => return Ok(MessagePayload::PresentResponse(resp)),
        };

        if num_requested == 0 || start_point >= bib_record_ids.len() {
            log::warn!("{self} PresentRequest requested 0 records");
            return Ok(MessagePayload::PresentResponse(resp));
        }

        let max = if start_point + num_requested <= bib_record_ids.len() {
            start_point + num_requested
        } else {
            bib_record_ids.len()
        };

        let bib_ids = &bib_record_ids[start_point..max];

        resp.records = Some(self.collect_bib_records(req, bib_ids, database)?);

        Ok(MessagePayload::PresentResponse(resp))
    }

    fn collect_bib_records(
        &self,
        req: &PresentRequest,
        bib_ids: &[i64],
        database: &conf::Z39Database,
    ) -> LocalResult<Records> {
        log::info!("{self} collecting bib records {bib_ids:?}");

        // For now we only support binary and XML.
        let mut as_xml = false;
        let mut response_syntax = oid::for_marc21();

        if let Some(syntax) = req.preferred_record_syntax.as_ref() {
            if oid::is_marcxml_identifier(syntax) {
                as_xml = true;
                response_syntax = syntax.clone();
            }
        }

        let mut records = Vec::new();
        let mut editor = eg::Editor::new(&self.client);

        for bib_id in bib_ids {
            let bytes = self.get_one_record(&mut editor, *bib_id, as_xml, database)?;

            // Z39 PresentResponse messages include bib records packaged
            // in an ASN.1 External element

            let oc = z39::types::OctetString::from(bytes);

            let mut external = ExternalMessage::new(Encoding::OctetAligned(oc));

            external.direct_reference = Some(response_syntax.clone());

            let npr = NamePlusRecord::new(Record::RetrievalRecord(External(external)));

            records.push(npr);
        }

        Ok(Records::ResponseRecords(records))
    }

    /// Retrieve one bib record from the database, format it, optionally
    /// add holdings, and return its bytes.
    fn get_one_record(
        &self,
        editor: &mut eg::Editor,
        bib_id: i64,
        as_xml: bool,
        database: &conf::Z39Database,
    ) -> LocalResult<Vec<u8>> {
        let mut bre = editor
            .retrieve("bre", bib_id)?
            .ok_or_else(|| editor.die_event())?;

        let marc_xml = bre["marc"].take_string().unwrap();

        // For consistency in responses, first translate all records into
        // a marc Record, before migrating them to their final form.
        let mut rec = marctk::Record::from_xml(&marc_xml)
            .next() // Option
            .ok_or_else(|| format!("Could not parse MARC xml for record {bib_id}"))??;

        if database.include_holdings() {
            self.append_record_holdings(bib_id, database, &mut rec)?;
        }

        if as_xml {
            Ok(rec.to_xml_string().into_bytes())
        } else {
            Ok(rec.to_binary()?)
        }
    }

    /// Append holdings fields to a bib record.
    fn append_record_holdings(
        &self,
        bib_id: i64,
        database: &conf::Z39Database,
        rec: &mut marctk::Record,
    ) -> LocalResult<()> {
        let query = eg::hash! {
            "select": {
                "acp":["id", "barcode", "price", "ref", "holdable", "opac_visible", "copy_number"],
                "ccm": [{"column": "name", "alias": "circ_modifier"}],
                "ccs": [{"column": "name", "alias": "status"}],
                "acpl": [{"column": "name", "alias": "location"}],
                "circ_lib": [{"column": "name", "alias": "circ_lib"}],
                "owning_lib": [{"column": "name", "alias": "owning_lib"}],
                "acn": [{"column": "label", "alias": "call_number"}],
                "acnp": [{"column": "label", "alias": "call_number_prefix"}],
                "acns": [{"column": "label", "alias": "call_number_suffix"}]
            },
            "from": {
                "acp": {
                    "acn": {
                        "join": {
                            "bre": {},
                            "owning_lib": {
                                "class": "aou",
                                "fkey": "owning_lib",
                                "field": "id"
                            },
                            "acnp": {},
                            "acns": {}
                        }
                    },
                    "ccm": {},
                    "acpl": {},
                    "ccs": {},
                    "circ_lib": {
                        "class": "aou",
                        "fkey": "circ_lib",
                        "field": "id"
                    }
                }
            },
            "where": {
                "+acp": {"deleted": "f", "opac_visible": "t"},
                "+acpl": {"deleted": "f", "opac_visible": "t"},
                "+ccs": {"opac_visible": "t"},
                "+acn": {"deleted": "f"},
                "+bre": {"deleted": "f", "id": bib_id},
                "+circ_lib": {"opac_visible": "t"}
            },
            "order_by": [{
                "class": "acp",
                "field": "create_date",
                "direction": "asc"
            }],
            "limit": database.max_item_count()
        };

        let mut ses = self.client.session("open-ils.cstore");
        let mut req = ses.request("open-ils.cstore.json_query", vec![query])?;

        /*
        {"price":"17.00",
        "opac_visible":"t",
        "circ_lib":"Sammamish",
        "owning_lib":"Sammamish",
        "id":7618543,
        "ref":"f",
        "holdable":"t",
        "location":"Easy Reader",
        "label":"",
        "status":"Available",
        "copy_number":null,
        "circ_modifier":"Book",
        "barcode":"30000017112669"}
        */

        // TODO need to confirm these fields.
        while let Some(copy) = req.recv()? {
            let call_number = format!(
                "{}{}{}",
                copy["call_number_prefix"].as_str().unwrap_or(""),
                copy["call_number"].str()?,
                copy["call_number_suffix"].as_str().unwrap_or("")
            );

            let mut field = marctk::Field::new(database.holdings_tag())?;
            let _ = field.add_subfield("a", copy["circ_lib"].str()?);
            let _ = field.add_subfield("b", copy["location"].str()?);
            let _ = field.add_subfield("h", call_number);
            let _ = field.add_subfield("p", copy["barcode"].str()?);
            let _ = field.add_subfield("r", copy["status"].str()?);
            let _ = field.add_subfield("w", copy["circ_modifier"].str()?);

            rec.insert_data_field(field);
        }

        Ok(())
    }
}
