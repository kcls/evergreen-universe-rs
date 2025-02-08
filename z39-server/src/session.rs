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

            // Let the worker do its thing
            let resp = self.handle_message(msg)?;

            // Turn the response into bytes
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

        let compiler = Z39QueryCompiler;

        let query = compiler.compile(&req.query)?;

        // Quick and dirty!
        let mut options = EgValue::new_object();
        options["limit"] = 10.into();

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
            bib_record_ids: bib_ids,
        });

        Ok(MessagePayload::SearchResponse(resp))
    }

    fn handle_present_request(&mut self, req: &PresentRequest) -> EgResult<MessagePayload> {
        let mut resp = PresentResponse::default();
        // TODO result offset

        let Some(search) = self.last_search.as_ref() else {
            log::warn!("{self} PresentRequest called with no search in progress");
            return Ok(MessagePayload::PresentResponse(resp));
        };

        let num_requested = req.number_of_records_requested as usize;
        let mut start_point = req.reset_set_start_point as usize;

        // subtract 1 without overflowing
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

        resp.records = Some(self.collect_bib_records(req, bib_ids)?);

        Ok(MessagePayload::PresentResponse(resp))
    }

    fn collect_bib_records(&self, req: &PresentRequest, bib_ids: &[i64]) -> EgResult<Records> {
        log::info!("{self} collecting bib records {bib_ids:?}");

        let mut records = Vec::new();
        let mut editor = eg::Editor::new(&self.client);

        for bib_id in bib_ids {
            let bre = editor.retrieve("bre", *bib_id)?.unwrap(); // todo
            let rec = marctk::Record::from_xml(bre["marc"].str()?)
                .next()
                .unwrap()
                .unwrap(); // TODO

            let mut wants_xml = false;

            if let Some(syntax) = req.preferred_record_syntax.as_ref() {
                wants_xml = **syntax == OID_MARCXML; // TODO make this easier
            }

            let bytes = if wants_xml {
                rec.to_xml_string().into_bytes()
            } else {
                rec.to_binary()?
            };

            let oc = octet_string(bytes); // from z39; reconsider

            let mut external = ExternalMessage::new(Encoding::OctetAligned(oc));
            external.direct_reference = if wants_xml {
                Some(marcxml_identifier())
            } else {
                Some(marc21_identifier())
            };

            let npr = NamePlusRecord::new(Record::RetrievalRecord(External(external)));
            records.push(npr);
        }

        Ok(Records::ResponseRecords(records))
    }
}
