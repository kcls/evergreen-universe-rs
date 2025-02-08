use z39::message::*;
use z39::bib1;

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
	pub fn compile(&self, query: &z39::message::Query) -> Result<String, String> {
        match query {
            Query::Type1(ref rpn_query) => self.compile_rpn_structure(&rpn_query.rpn),
            _ => Err(OP_NOT_SUPPORTED.into()),
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
            Operand::ResultSet(_) => Err(OP_NOT_SUPPORTED.into()),
            Operand::ResultAttr(_) => Err(OP_NOT_SUPPORTED.into()),
        }
    }

    fn compile_rpn_op(&self, op: &RpnOp) -> Result<String, String> {
        let rpn1 = self.compile_rpn_structure(&op.rpn1)?;
        let rpn2 = self.compile_rpn_structure(&op.rpn2)?;

        let joiner = match &op.op {
            Operator::And => "AND",
            Operator::Or => "OR",
            _ => return Err(format!("Operator not supported: {op:?}").into()),
        };

        Ok(format!("({rpn1} {joiner} {rpn2})"))
    }

    fn compile_attributes_plus_term(&self, attr_term: &AttributesPlusTerm) -> Result<String, String> {
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

    fn compile_use_attribute(&self, attr: &AttributeElement, term: &Term) -> Result<String, String> {

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
