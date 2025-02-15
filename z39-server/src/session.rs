//! Handles a single Z39 connected session.
use crate::conf;
use crate::query::Z39QueryCompiler;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;

use z39::message::*;

use std::fmt;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const NETWORK_BUFSIZE: usize = 1024;

struct BibSearch {
    database_name: Option<String>,
    bib_record_ids: Vec<i64>,
}

pub(crate) struct Z39Session {
    tcp_stream: TcpStream,
    peer_addr: String,
    shutdown: Arc<AtomicBool>,
    client: eg::Client,
    last_search: Option<BibSearch>,
}

impl fmt::Display for Z39Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Z39Session [{}]", self.peer_addr)
    }
}

impl Z39Session {
    pub fn new(
        tcp_stream: TcpStream,
        peer_addr: String,
        bus: eg::osrf::bus::Bus,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        let client = eg::Client::from_bus(bus);

        Self {
            tcp_stream,
            peer_addr,
            shutdown,
            client,
            last_search: None,
        }
    }

    /// Main listen loop
    pub fn listen(&mut self) -> EgResult<()> {
        log::info!("{self} starting session");

        let mut bytes = Vec::new();

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

            // Reset the byte array for the next message cycle.
            bytes.clear();

            // Handle the message
            let resp = match self.handle_message(msg) {
                Ok(r) => r,
                Err(e) => {
                    log::error!("handle_message() exited with {e}");

                    // For now, errors produced by the session
                    // are simple strings, so we have no way to
                    // differentiate client protocol issues, vs internal
                    // server errors, etc.  Just return a generic
                    // SystemProblem response.  TODO granular Err types
                    // or is that overkill?

                    let close = Close {
                        close_reason: CloseReason::SystemProblem,
                        ..Default::default()
                    };

                    Message::from_payload(MessagePayload::Close(close))
                }
            };

            let bytes = resp.to_bytes()?;

            log::trace!("{self} replying with {bytes:?}");

            self.tcp_stream
                .write_all(bytes.as_slice())
                .map_err(|e| e.to_string())?;
        }

        log::info!("{self} session exiting");

        Ok(())
    }

    /// Shut down the sesion's TcpStrean.
    pub fn shutdown(&mut self) {
        self.tcp_stream.shutdown(std::net::Shutdown::Both).ok();
    }

    /// Panics if self.client has no Bus.
    pub fn take_bus(&mut self) -> eg::osrf::bus::Bus {
        self.client.take_bus()
    }

    fn handle_message(&mut self, msg: Message) -> EgResult<Message> {
        log::debug!("{self} processing message {msg:?}");

        let payload = match &msg.payload {
            MessagePayload::InitializeRequest(r) => self.handle_init_request(r)?,
            MessagePayload::SearchRequest(r) => self.handle_search_request(r)?,
            MessagePayload::PresentRequest(r) => self.handle_present_request(r)?,
            _ => return Err(format!("handle_message() unsupported message type: {msg:?}").into()),
        };

        Ok(Message::from_payload(payload))
    }

    fn handle_init_request(&mut self, _req: &InitializeRequest) -> EgResult<MessagePayload> {
        Ok(MessagePayload::InitializeResponse(
            InitializeResponse::default(),
        ))
    }

    fn handle_search_request(&mut self, req: &SearchRequest) -> EgResult<MessagePayload> {
        let mut resp = SearchResponse::default();

        log::info!("{self} search query: {:?}", req.query);

        // See if the caller requested a specific database by name.
        let db_name = if let Some(dbn) = req.database_names.first() {
            let DatabaseName::Name(s) = dbn;
            Some(s.as_str())
        } else {
            None
        };

        let database = conf::global().find_database(db_name)?;

        let compiler = Z39QueryCompiler::new(database);

        let query = match compiler.compile(&req.query) {
            Ok(q) => q,
            Err(e) => {
                log::error!("{self} could not compile search query: {e}");
                return Ok(MessagePayload::SearchResponse(resp));
            }
        };

        log::info!("{self} compiled search query: {query}");

        if query.is_empty() {
            return Ok(MessagePayload::SearchResponse(resp));
        }

        // Quick and dirty!
        let mut options = EgValue::new_object();
        options["limit"] = database.max_bib_count().into();

        let Ok(Some(search_result)) = self.client.send_recv_one(
            "open-ils.search",
            "open-ils.search.biblio.multiclass.query.staff",
            vec![options, EgValue::from(query)],
        ) else {
            return Ok(MessagePayload::SearchResponse(resp));
        };

        let bib_ids: Vec<i64> = search_result["ids"]
            .members()
            .map(|arr| arr[0].int_required())
            .collect();

        log::info!("Search returned IDs {bib_ids:?}");

        resp.result_count = bib_ids.len() as u32;
        resp.search_status = true;

        self.last_search = Some(BibSearch {
            database_name: db_name.map(|s| s.to_string()),
            bib_record_ids: bib_ids,
        });

        Ok(MessagePayload::SearchResponse(resp))
    }

    fn handle_present_request(&mut self, req: &PresentRequest) -> EgResult<MessagePayload> {
        let mut resp = PresentResponse::default();

        let Some(search) = self.last_search.as_ref() else {
            log::warn!("{self} PresentRequest called with no search in progress");
            return Ok(MessagePayload::PresentResponse(resp));
        };

        let database = conf::global().find_database(search.database_name.as_deref())?;

        let num_requested = req.number_of_records_requested as usize;
        let mut start_point = req.reset_set_start_point as usize;

        // subtract 1 without overflowing; neat.
        start_point = start_point.saturating_sub(1);

        if num_requested == 0 || start_point >= search.bib_record_ids.len() {
            log::warn!("{self} PresentRequest requested 0 records");
            return Ok(MessagePayload::PresentResponse(resp));
        }

        let max = if start_point + num_requested <= search.bib_record_ids.len() {
            start_point + num_requested
        } else {
            search.bib_record_ids.len()
        };

        let bib_ids = &search.bib_record_ids[start_point..max];

        resp.records = Some(self.collect_bib_records(req, bib_ids, database)?);

        Ok(MessagePayload::PresentResponse(resp))
    }

    fn collect_bib_records(
        &self,
        req: &PresentRequest,
        bib_ids: &[i64],
        database: &conf::Z39Database,
    ) -> EgResult<Records> {
        log::info!("{self} collecting bib records {bib_ids:?}");

        // For now we only support binary and XML.
        let mut as_xml = false;
        let mut response_syntax = z39::message::marc21_identifier();

        if let Some(syntax) = req.preferred_record_syntax.as_ref() {
            if z39::message::is_marcxml_identifier(syntax) {
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

            let oc = z39::message::octet_string(bytes);

            let mut external = ExternalMessage::new(Encoding::OctetAligned(oc));

            external.direct_reference = Some(response_syntax.clone());

            let npr = NamePlusRecord::new(Record::RetrievalRecord(External(external)));

            records.push(npr);
        }

        Ok(Records::ResponseRecords(records))
    }

    fn get_one_record(
        &self,
        editor: &mut eg::Editor,
        bib_id: i64,
        as_xml: bool,
        database: &conf::Z39Database,
    ) -> EgResult<Vec<u8>> {
        let mut bre = editor
            .retrieve("bre", bib_id)?
            .ok_or_else(|| editor.die_event())?;

        let marc_xml = bre["marc"]
            .take_string()
            .ok_or_else(|| format!("Invalid bib record: {bib_id}"))?;

        let bytes = if as_xml && !database.include_holdings() {
            // Return the XML directly from the database
            marc_xml.into_bytes()
        } else {
            // Translate the native MARC XML into MARC binary.

            let mut rec = marctk::Record::from_xml(&marc_xml)
                .next() // Option
                .ok_or_else(|| format!("Could not parse MARC xml for record {bib_id}"))??;

            if database.include_holdings() {
                self.append_record_holdings(bib_id, database, &mut rec)?;
            }

            if as_xml {
                rec.to_xml_string().into_bytes()
            } else {
                rec.to_binary()?
            }
        };

        Ok(bytes)
    }

    /// Append holdings data to a bib record.
    fn append_record_holdings(
        &self,
        bib_id: i64,
        database: &conf::Z39Database,
        rec: &mut marctk::Record,
    ) -> EgResult<()> {
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
