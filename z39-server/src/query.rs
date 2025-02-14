use z39::bib1;
use z39::message::*;
use crate::conf;

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

// TODO setting
fn use_elastic_search() -> bool {
    true
}

/// Compiler for Z39 queries.
///
/// The z39-server module creates z39::message's which we then have to
/// handle, the most complicated of which (so far) is the SearchRequest
/// message, which contains the search queries.  This mod translates
/// those into queries that can be used by Evergreen.
pub struct Z39QueryCompiler<'a> {
    database: Option<&'a conf::Z39Database>
}

impl<'a> Z39QueryCompiler<'a> {
    pub fn new(database: Option<&'a conf::Z39Database>) -> Self {
        Self { database }
    }

    /// Translate a Z39 Query into a query string that can be sent to Evergreen
    pub fn compile(&self, query: &z39::message::Query) -> Result<String, String> {
        match query {
            Query::Type1(ref rpn_query) => self.compile_rpn_structure(&rpn_query.rpn),
            _ => Err(format!("Query type not supported: {query:?}")),
        }
    }

    fn compile_rpn_structure(&self, structure: &RpnStructure) -> Result<String, String> {
        match structure {
            RpnStructure::Op(ref op) => self.compile_rpn_operand(op),
            RpnStructure::RpnOp(ref op) => self.compile_rpn_op(op),
        }
    }

    fn compile_rpn_operand(&self, op: &Operand) -> Result<String, String> {
        match op {
            Operand::AttrTerm(ref attr_term) => self.compile_attributes_plus_term(attr_term),
            _ => Err(format!("Operand not supported: {op:?}")),
        }
    }

    fn compile_rpn_op(&self, op: &RpnOp) -> Result<String, String> {
        let rpn1 = self.compile_rpn_structure(&op.rpn1)?;
        let rpn2 = self.compile_rpn_structure(&op.rpn2)?;

        let joiner = if use_elastic_search() {
            match &op.op {
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::AndNot => "AND NOT",
                _ => return Err(format!("Operator not supported: {op:?}")),
            }
        } else {
            todo!("Native Evergreen support needed");
        };

        Ok(format!("({rpn1} {joiner} {rpn2})"))
    }

    /// Collect the search term, search index, and related attributes
    /// into a search component, e.g. id|isbn:1231231231231
    fn compile_attributes_plus_term(
        &self,
        attr_term: &AttributesPlusTerm,
    ) -> Result<String, String> {
        let search_term = match &attr_term.term {
            Term::General(ref v) => std::str::from_utf8(v)
                .map_err(|e| e.to_string())?
                .to_string(),
            Term::Numeric(n) => format!("{n}"),
            Term::CharacterString(ref v) => v.to_string(),
            _ => return Err(format!("Unsupported Term variant: {:?}", attr_term.term)),
        };

        if search_term.is_empty() {
            return Err(format!("Search term is empty: {attr_term:?}"));
        }

        // Log when attributes are sent by the caller that we are going
        // to ignore becuase we don't support them yet (or don't care).
        fn log_unused_attr(a: &AttributeElement) {
            if let Ok(s) = bib1::stringify_attribute(a) {
                log::warn!("Attribute {s} is currently ignored");
            } else {
                log::error!("Unexpected attribute: {a:?}");
            }
        }

        let mut search_index = None;

        for attr in &attr_term.attributes {
            let attr_type: bib1::Attribute = attr.attribute_type.try_into()?;

            match attr_type {
                bib1::Attribute::Use => match &attr.attribute_value {
                    AttributeValue::Numeric(n) => {
                        if let Some((_code, field)) =
                            BIB1_ATTR_QUERY_MAP.iter().find(|(c, _)| c == n)
                        {
                            search_index = Some(field);
                        }
                    }
                    _ => log_unused_attr(attr),
                },
                /*
                bib1::Attribute::Truncation => match &attr.attribute_value {
                    AttributeValue::Numeric(n) => match bib1::Truncation::try_from(*n)? {
                        bib1::Truncation::RightTruncation => search_term += "*",
                        bib1::Truncation::LeftTruncation => search_term = format!("*{search_term}"),
                        bib1::Truncation::LeftAndRightTruncation => {
                            search_term = format!("*{search_term}*")
                        }
                        _ => log_unused_attr(attr),
                    },
                    _ => log_unused_attr(attr),
                },
                */
                _ => log_unused_attr(attr),
            }
        }

        // If we receive no guidance on where to search, do a keyword search.
        let si = search_index.unwrap_or(&"keyword");

        if search_term.contains(' ') {
            Ok(format!("{si}:({search_term})"))
        } else {
            Ok(format!("{si}:{search_term}"))
        }
    }
}

#[test]
fn test_compile_rpn_structure() {
    let rpn_struct = RpnStructure::RpnOp(Box::new(RpnOp {
        rpn1: RpnStructure::Op(Operand::AttrTerm(AttributesPlusTerm {
            attributes: vec![bib1::Use::Author.as_z39_attribute_element()],
            term: Term::General("martin".as_bytes().into()),
        })),
        rpn2: RpnStructure::Op(Operand::AttrTerm(AttributesPlusTerm {
            attributes: vec![bib1::Use::Title.as_z39_attribute_element()],
            term: Term::General("thrones".as_bytes().into()),
        })),
        op: Operator::And,
    }));

    let compiler = Z39QueryCompiler::default();
    let s = compiler.compile_rpn_structure(&rpn_struct).unwrap();

    assert_eq!(s, "(author:martin AND title:thrones)");
}
