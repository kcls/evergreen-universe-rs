use crate::conf;
use crate::error::LocalError;
use crate::error::LocalResult;
use z39::types::bib1;
use z39::types::pdu::*;

/// Compiler for Z39 queries.
///
/// The z39-server module creates z39_types::message's which we then have to
/// handle, the most complicated of which (so far) is the SearchRequest
/// message, which contains the search queries.  This mod translates
/// those into queries that can be used by Evergreen.
pub struct Z39QueryCompiler<'a> {
    database: &'a conf::Z39Database,
}

impl<'a> Z39QueryCompiler<'a> {
    pub fn new(database: &'a conf::Z39Database) -> Self {
        Self { database }
    }

    /// Translate a Z39 Query into a query string that can be sent to Evergreen
    pub fn compile(&self, query: &Query) -> LocalResult<String> {
        match query {
            Query::Type1(ref rpn_query) => self.compile_rpn_structure(&rpn_query.rpn),
            _ => Err(LocalError::NotSupported(format!("Query type: {query:?}"))),
        }
    }

    fn compile_rpn_structure(&self, structure: &RpnStructure) -> LocalResult<String> {
        match structure {
            RpnStructure::Op(ref op) => self.compile_rpn_operand(op),
            RpnStructure::RpnOp(ref op) => self.compile_rpn_op(op),
        }
    }

    fn compile_rpn_operand(&self, op: &Operand) -> LocalResult<String> {
        match op {
            Operand::AttrTerm(ref attr_term) => self.compile_attributes_plus_term(attr_term),
            _ => Err(LocalError::NotSupported(format!("Operand: {op:?}"))),
        }
    }

    fn compile_rpn_op(&self, op: &RpnOp) -> LocalResult<String> {
        let rpn1 = self.compile_rpn_structure(&op.rpn1)?;
        let rpn2 = self.compile_rpn_structure(&op.rpn2)?;

        let joiner = if self.database.use_elasticsearch() {
            match &op.op {
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::AndNot => "AND NOT",
                _ => return Err(LocalError::NotSupported(format!("Operator: {op:?}"))),
            }
        } else {
            todo!("Native Evergreen support needed");
        };

        Ok(format!("({rpn1} {joiner} {rpn2})"))
    }

    /// Collect the search term, search index, and related attributes
    /// into a search component, e.g. id|isbn:1231231231231
    fn compile_attributes_plus_term(&self, attr_term: &AttributesPlusTerm) -> LocalResult<String> {
        let search_term = match &attr_term.term {
            Term::General(ref v) => z39::types::octet_string_as_str(v)?.to_string(),
            Term::Numeric(n) => format!("{n}"),
            Term::CharacterString(ref v) => v.to_string(),
            _ => {
                return Err(LocalError::NotSupported(format!(
                    "Term variant: {:?}",
                    attr_term.term
                )));
            }
        };

        if search_term.is_empty() {
            return Err(LocalError::NoSearchTerm(format!(
                "Search term is empty: {attr_term:?}"
            )));
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
        let mut bib1_index_value = None;
        let mut is_not_equal = false;

        // Handle the no-op/default attributes explicitly so we can
        // avoid logging that they are ignored.
        for attr in &attr_term.attributes {
            let attr_type: bib1::Attribute = attr.attribute_type.try_into()?;

            match attr_type {
                bib1::Attribute::Use => match &attr.attribute_value {
                    AttributeValue::Numeric(n) => {
                        bib1_index_value = Some(n);
                        search_index = self.database.bib1_index_map_value(*n);
                    }
                    _ => log_unused_attr(attr),
                },

                bib1::Attribute::Relation => match &attr.attribute_value {
                    AttributeValue::Numeric(n) => match bib1::Relation::try_from(*n)? {
                        bib1::Relation::Equal => {} // no-op
                        bib1::Relation::NotEqual => is_not_equal = true,
                        _ => log_unused_attr(attr),
                    },
                    _ => log_unused_attr(attr),
                },

                bib1::Attribute::Structure => match &attr.attribute_value {
                    AttributeValue::Numeric(n) => match bib1::Structure::try_from(*n)? {
                        bib1::Structure::WordList => {} // no-op
                        _ => log_unused_attr(attr),
                    },
                    _ => log_unused_attr(attr),
                },

                bib1::Attribute::Position => match &attr.attribute_value {
                    AttributeValue::Numeric(n) => match bib1::Position::try_from(*n)? {
                        bib1::Position::AnyPositionInField => {} // no-op
                        _ => log_unused_attr(attr),
                    },
                    _ => log_unused_attr(attr),
                },

                bib1::Attribute::Completeness => match &attr.attribute_value {
                    AttributeValue::Numeric(n) => match bib1::Completeness::try_from(*n)? {
                        bib1::Completeness::IncompleteSubfield => {} // no-op
                        _ => log_unused_attr(attr),
                    },
                    _ => log_unused_attr(attr),
                },

                // Largely avoiding truncation support, unless we learn
                // later that it's needed, since it's not generally
                // required to get reasonable results, and a Z39 server
                // is not expected to be the primary source for nuanced
                // querying.  Keeping sample code in place in case it's
                // useful later.
                bib1::Attribute::Truncation => match &attr.attribute_value {
                    AttributeValue::Numeric(n) => match bib1::Truncation::try_from(*n)? {
                        bib1::Truncation::DoNotTruncate => {} // no-op
                        _ => log_unused_attr(attr),
                    },
                    _ => log_unused_attr(attr),
                },
                _ => log_unused_attr(attr),
            }
        }

        let index = search_index
            .or(self.database.default_index())
            .ok_or_else(|| {
                LocalError::NoSuchSearchIndex(format!(
                    "No search index configured for Use attribute={bib1_index_value:?}"
                ))
            })?;

        let mut search = if search_term.contains(' ') {
            format!("{index}:({search_term})")
        } else {
            format!("{index}:{search_term}")
        };

        if is_not_equal {
            search = format!("NOT ({search})");
        }

        Ok(search)
    }
}

#[test]
fn test_compile_rpn_structure() {
    let rpn_struct = RpnStructure::RpnOp(Box::new(RpnOp {
        rpn1: RpnStructure::Op(Operand::AttrTerm(AttributesPlusTerm {
            attributes: vec![bib1::Use::Author.into()],
            term: Term::General("martin".as_bytes().into()),
        })),
        rpn2: RpnStructure::Op(Operand::AttrTerm(AttributesPlusTerm {
            attributes: vec![bib1::Use::Title.into()],
            term: Term::General("thrones".as_bytes().into()),
        })),
        op: Operator::And,
    }));

    let mut db = conf::Z39Database::default();
    db.set_use_elasticsearch(true);
    db.bib1_index_map_mut().insert(4, "title".to_string());
    db.bib1_index_map_mut().insert(1003, "author".to_string());

    let compiler = Z39QueryCompiler { database: &db };

    let s = compiler.compile_rpn_structure(&rpn_struct).unwrap();

    assert_eq!(s, "(author:martin AND title:thrones)");
}
