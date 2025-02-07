use evergreen as eg;
use eg::EgResult;

use z39::message::*;
use z39::bib1;
use z39::server::Z39Server;
use z39::server::Z39Worker;

use std::fmt;

const OP_NOT_SUPPORTED: &str = "Operation not supported";

// TODO move this into a config file.
// See /openils/conf/dgo.conf for example
const BIB1_ATTR_QUERY_MAP: &[(u32, &str)] = &[
	(4, "title"),
	(7, "identifier|isbn"),
	(8, "keyword"),
	(21, "subject"),
	(1003, "author"),
	(1007, "identifier"),
	(1018, "keyword|publisher"),
];

/// Compiler for Z39 queries.
///
/// The z39-server module creates z39::message's which we then have to handle,
/// the most complicated of whic is the SearchRequest message, which contains
/// the actual search queries.  This mod translates those into queries
/// that can be used by Evergreen.
#[derive(Debug, Default)]
pub struct Z39QueryCompiler;

impl Z39QueryCompiler {
    /// Translate a Z39 Query into a query string that can be sent to Evergreen
	pub fn compile(&self, query: &z39::message::Query) -> EgResult<String> {
        match query {
            Query::Type1(ref rpn_query) => self.compile_rpn_structure(&rpn_query.rpn),
            _ => Err(OP_NOT_SUPPORTED.into()),
        }
	}

    fn compile_rpn_structure(&self, structure: &RpnStructure) -> EgResult<String> {
        match structure {
            RpnStructure::Op(ref op) => self.compile_rpn_operand(op),
            RpnStructure::RpnOp(ref op) => self.compile_rpn_op(op),
        }
    }

    fn compile_rpn_operand(&self, op: &Operand) -> EgResult<String> {
        match op {
            Operand::AttrTerm(ref attr_term) => self.compile_attributes_plus_term(attr_term),
            Operand::ResultSet(_) => Err(OP_NOT_SUPPORTED.into()),
            Operand::ResultAttr(_) => Err(OP_NOT_SUPPORTED.into()),
        }
    }

    fn compile_rpn_op(&self, op: &RpnOp) -> EgResult<String> {
        let rpn1 = self.compile_rpn_structure(&op.rpn1)?;
        let rpn2 = self.compile_rpn_structure(&op.rpn2)?;

        let joiner = match &op.op {
            Operator::And => "AND",
            Operator::Or => "OR",
            _ => return Err(format!("Operator not supported: {op:?}").into()),
        };

        Ok(format!("({rpn1} {joiner} {rpn2})"))
    }

    fn compile_attributes_plus_term(&self, attr_term: &AttributesPlusTerm) -> EgResult<String> {
        if attr_term.attributes.is_empty() {
            return Err("AttributesPlusTerm.attribute required".into());
        }

        // This needs more thought re: integrating attributes.

        let mut s = "".to_string();

        for attr in &attr_term.attributes {
            let attr_type: bib1::Attribute = attr.attribute_type.try_into()?;

            match attr_type {
                bib1::Attribute::Use => s += &self.compile_use_attribute(attr, &attr_term.term)?,
                _ => return Err(OP_NOT_SUPPORTED.into()),
            }
        }

        Ok(s)
    }

    fn compile_use_attribute(&self, attr: &AttributeElement, term: &Term) -> EgResult<String> {

        let field = match attr.attribute_value {
            AttributeValue::Numeric(n) => {
                if let Some((_code, field)) = BIB1_ATTR_QUERY_MAP.iter().find(|(c, _)| c == &n) {
                    field
                } else {
                    // Default to keyword when no mapping is found.  todo.
                    "keyword"
                }
            }
            _ => return Err(OP_NOT_SUPPORTED.into()),
        };

        let value = match term {
            Term::General(ref v) => std::str::from_utf8(v).map_err(|e| e.to_string())?.to_string(),
            Term::Numeric(n) => format!("{n}"),
            Term::CharacterString(ref v) => v.to_string(),
            _ => return Err(OP_NOT_SUPPORTED.into()),
        };

        Ok(format!("{field}:{value}"))
    }
}

#[test]
fn test_compile_rpn_structure() {

    let rpn_struct = RpnStructure::RpnOp(Box::new(
        RpnOp {
            rpn1: RpnStructure::Op(
                Operand::AttrTerm(
                    AttributesPlusTerm {
                        attributes: vec![bib1::Use::Author.as_z39_attribute_element()],
                        term: Term::General("martin".as_bytes().into()) 
                    }
                )
            ),
            rpn2: RpnStructure::Op(
                Operand::AttrTerm(
                    AttributesPlusTerm {
                        attributes: vec![bib1::Use::Title.as_z39_attribute_element()],
                        term: Term::General("thrones".as_bytes().into()) 
                    }
                )
            ),
            op: Operator::And,
        }
    ));

    let compiler = Z39QueryCompiler::default();
    let s = compiler.compile_rpn_structure(&rpn_struct).unwrap();

    assert_eq!(s, "(author:martin AND title:thrones)");
}

/// Z39 Message handler

struct EgZ39Worker;

impl fmt::Display for EgZ39Worker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO we want to log the IP addresses as well.
        write!(f, "Z39")
    }
}

impl Z39Worker for EgZ39Worker {

    fn handle_message(&mut self, msg: Message) -> Result<Message, String> {
        log::debug!("{self} processing message {msg:?}");

        let payload = match &msg.payload {
            MessagePayload::InitializeRequest(r) => self.handle_init_request(r)?,
            MessagePayload::SearchRequest(r) => self.handle_search_request(r)?,
            MessagePayload::PresentRequest(r) => self.handle_present_request(r)?,
            _ => return Err(format!("handle_message() unsupported message type: {msg:?}")),
        };

        Ok(Message::from_payload(payload))
    }
}

impl EgZ39Worker {

    fn handle_init_request(&mut self, _req: &InitializeRequest) -> Result<MessagePayload, String> {
        Ok(MessagePayload::InitializeResponse(InitializeResponse::default()))
    }

    fn handle_search_request(&mut self, req: &SearchRequest) -> Result<MessagePayload, String> {
        todo!();

        /*
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
        */
    }


    fn handle_present_request(&mut self, req: &PresentRequest) -> Result<MessagePayload, String> {
        todo!();

        /*
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

        resp.records = Some(self.collect_bib_records(req, bib_ids)?);

        self.reply(MessagePayload::PresentResponse(resp))
        */
    }

    fn collect_bib_records(&self, req: &PresentRequest, bib_ids: &[i64]) -> Result<Records, String> {

        todo!();
    

    /*
        log::info!("{self} collecting bib records {bib_ids:?}");

        let mut records = Vec::new();
        let mut editor = eg::Editor::new(&self.client);

        for bib_id in bib_ids {
            let bre = editor.retrieve("bre", *bib_id)?.unwrap(); // todo
            let rec = marctk::Record::from_xml(bre["marc"].str()?).next().unwrap().unwrap(); // TODO

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
        */
    }
}

fn main() {

    let settings = z39::Settings {
        implementation_id: Some("EG".to_string()),
        implementation_name: Some("Evergreen".to_string()),
        implementation_version: Some("0.1.0".to_string()),
        ..Default::default()
    };

    settings.apply();

    // TODO command line, etc.
    let tcp_listener = eg::util::tcp_listener(
        "127.0.0.1",
        2210,
        3,
    )
    .unwrap(); // todo error reporting

    Z39Server::start(tcp_listener, || Box::new(EgZ39Worker {}));
}


