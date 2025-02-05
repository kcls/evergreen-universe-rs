use z39::message::*;
use evergreen as eg;

use crate::query::Z39QueryCompiler;

use std::fmt;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const NETWORK_BUFSIZE: usize = 1024;

/// Retain info on the most recently executed search so we can
/// reply with result across subsequent PresentRequest messages.
pub struct BibSearch {
    search_request: SearchRequest,
    bib_record_ids: Vec<i64>,
    limit: usize,
    offset: usize,
}

pub struct Z39Session {
    pub tcp_stream: TcpStream,
    pub peer_addr: String,
    pub shutdown: Arc<AtomicBool>,
    pub client: eg::Client,
    pub last_search: Option<BibSearch>,
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
        shutdown: Arc<AtomicBool>,
        client: eg::Client
    ) -> Self {
        Self {
            tcp_stream,
            peer_addr,
            shutdown,
            client,
            last_search: None,
        }
    }

    fn handle_message(&mut self, message: Message) -> Result<(), String> {
        log::debug!("{self} processing message {message:?}");

        match &message.payload {
            MessagePayload::InitializeRequest(r) => self.handle_init_request(r),
            MessagePayload::SearchRequest(r) => self.handle_search_request(r),
            MessagePayload::PresentRequest(r) => self.handle_present_request(r),
            _ => Err(format!("handle_message() unsupported message type: {message:?}")),
        }
    }

    fn handle_init_request(&mut self, _req: &InitializeRequest) -> Result<(), String> {
        // Canned "what we support" reply
        self.reply(MessagePayload::InitializeResponse(InitializeResponse::default()))
    }

    fn handle_search_request(&mut self, req: &SearchRequest) -> Result<(), String> {
        let mut resp = SearchResponse::default();

        log::info!("{self} search query: {:?}", req.query);

        let compiler = Z39QueryCompiler::default();

        // TODO put all the data collection in separate function so we can
        // simply respond with search success yes/no on Err's instead of
        // exiting this function ungracefully.
        let query = compiler.compile(&req.query)?;

        // Quick and dirty!
        let mut options = eg::EgValue::new_object();
        options["limit"] = 10.into();

        let Ok(Some(search_result)) = self.client.send_recv_one(
            "open-ils.search",
            "open-ils.search.biblio.multiclass.query.staff",
            vec![options, eg::EgValue::from(query)]
        ) else {
            return self.reply(MessagePayload::SearchResponse(resp));
        };

        let bib_ids: Vec<i64> = search_result["ids"]
            .members()
            .map(|arr| arr[0].int_required())
            .collect();

        log::info!("Search returned IDs {bib_ids:?}");

        resp.result_count = bib_ids.len() as u32;
        resp.search_status = true;

        self.last_search = Some(
            BibSearch {
                search_request: req.clone(),
                bib_record_ids: bib_ids,
                limit: 10, // TODO
                offset: 0,
            }
        );

        self.reply(MessagePayload::SearchResponse(resp))
    }


    fn handle_present_request(&mut self, req: &PresentRequest) -> Result<(), String> {
        let mut resp = PresentResponse::default();
        // TODO result offset

        let Some(search) = self.last_search.as_ref() else {
            log::warn!("{self} PresentRequest called with no search in progress");
            return self.reply(MessagePayload::PresentResponse(resp));
        };

        let num_requested = req.number_of_records_requested as usize;
        let mut start_point = req.reset_set_start_point as usize;

        if start_point > 0 {
            // Start point is 1-based.
            start_point -= 1;
        }

        if num_requested == 0 || start_point >= search.bib_record_ids.len() {
            log::warn!("{self} PresentRequest requested 0 records");
            return self.reply(MessagePayload::PresentResponse(resp));
        }

        let max = if start_point + num_requested <= search.bib_record_ids.len() {
            start_point + num_requested
        } else {
            search.bib_record_ids.len()
        };
            
        let bib_ids = &search.bib_record_ids[start_point..max];

        resp.records = Some(self.collect_bib_records(bib_ids)?);

        self.reply(MessagePayload::PresentResponse(resp))
    }

    fn collect_bib_records(&self, bib_ids: &[i64]) -> Result<Records, String> {
        log::info!("{self} collecting bib records {bib_ids:?}");

        let mut records = Vec::new();
        let mut editor = eg::Editor::new(&self.client);

        for bib_id in bib_ids {
            let bre = editor.retrieve("bre", *bib_id)?.unwrap(); // todo
            let rec = marctk::Record::from_xml(bre["marc"].str()?).next().unwrap().unwrap(); // TODO
            let bytes = rec.to_binary()?;

            let oc = octet_string(bytes); // from z39; reconsider

            let mut external = ExternalMessage::new(Encoding::OctetAligned(oc));
            external.direct_reference = Some(marc21_identifier());

            let npr = NamePlusRecord::new(Record::RetrievalRecord(External(external)));
            records.push(npr);
        }

        Ok(Records::ResponseRecords(records))
    }


    /// Drop a set of bytes onto the wire.
    fn reply(&mut self, payload: MessagePayload) -> Result<(), String> {
        let bytes = Message::from_payload(payload).to_bytes()?;

        log::trace!("{self} replying with {bytes:?}");

        self.tcp_stream.write_all(bytes.as_slice()).map_err(|e| e.to_string())
    }

    pub fn listen(&mut self) -> Result<(), String> {
        log::info!("{self} starting session");

        let mut bytes = Vec::new();
        let mut buffer = [0u8; NETWORK_BUFSIZE];

        // Read bytes from the TCP stream, feeding them into the BER
        // parser, until a complete message is formed.  Handle the
        // message, rinse and repeat.
        loop {

            let _count = match self.tcp_stream.read(&mut buffer) {
                Ok(c) => c,
                Err(e) => match e.kind() {
                    std::io::ErrorKind::WouldBlock => {
                        if self.shutdown.load(Ordering::Relaxed) {
                            log::debug!("Shutdown signal received, exiting listen loop");
                            break;
                        }
                        // Go back and wait for reqeusts to arrive.
                        continue;
                    }
                    _ => {
                        // Connection severed.  Likely the caller disconnected.
                        log::info!("Socket closed: {e}");
                        break;
                    }
                }
            };

            bytes.extend_from_slice(&buffer);

            let msg = match Message::from_bytes(&bytes) {
                Ok(maybe) => match maybe {
                    Some(m) => {
                        bytes.clear();
                        m
                    }
                    None => continue, // more bytes needed
                },
                Err(e) => {
                    log::error!("cannot parse message: {e} {bytes:?}");
                    break;
                }
            };

            if let Err(e) = self.handle_message(msg) {
                log::error!("cannot handle message: {e} {bytes:?}");
                break;
            }
        }

        log::info!("session exiting");

        self.tcp_stream.shutdown(std::net::Shutdown::Both).ok();

        Ok(())
    }
}
