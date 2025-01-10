use crate::bib1;

#[derive(Debug, PartialEq, Clone)]
enum Joiner {
    And,
    Or,
}

impl TryFrom<&str> for Joiner {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.eq_ignore_ascii_case("and") {
            Ok(Joiner::And)
        } else if s.eq_ignore_ascii_case("or") {
            Ok(Joiner::Or)
        } else {
            Err(format!("Unknown join type: {s}"))
        }
    }
}

/// Search value with its attributes
/// @attr 1=7 @attr 4=6 @attr 5=1 "testisbn"
#[derive(Debug, PartialEq, Clone)]
struct Clause {
    attrs: Vec<bib1::Attr>,
    value: String,
}

#[derive(Debug, PartialEq, Clone)]
enum Content {
    Joiner(Joiner),
    Clause(Clause),
}

#[derive(Debug, PartialEq, Clone)]
struct Node {
    content: Content,
    left_child: Option<Box<Node>>,
    right_child: Option<Box<Node>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Query {
    query_string: Option<String>,
    tree: Node,
}

/*
fn error(parts: &Vec<String>) -> Result<Node, String> {
    Err(format!("Invalid query parts: {parts:?}"))
}
*/

impl Query {
    /// Create a Query from a query string.
    pub fn from_query_str(s: &str) -> Result<Self, String> {
        // Collect our string pieces into a list of Contents
        let mut parts: Vec<String> = s.split(' ').map(|s| s.to_lowercase()).collect();

        let mut contents = Vec::new();

        while !parts.is_empty() {
            if parts[0] == "@and" || parts[0] == "@or" {
                let part = parts.remove(0);
                contents.push(Content::Joiner(Joiner::try_from(&part[1..])?));
            } else {
                contents.push(Content::Clause(Query::build_clause(&mut parts)?));
            }
        }

        println!("Contents: {contents:?}");

        todo!();

        /*

        let node = Query::read_parts(&mut query_parts)?;

        let mut q = Query {
            query_string: Some(s.to_string()),
            tree: node,
        };

        Ok(q)
        */
    }

    /// Construct a cluase from a set of query parts.
    fn build_clause(parts: &mut Vec<String>) -> Result<Clause, String> {
        let mut attrs = Vec::new();

        let mut in_attr = false;
        while !parts.is_empty() {
            let part = parts.remove(0);

            println!("part: {part}");

            if part == "@attr" {
                in_attr = true;
            } else if in_attr {
                attrs.push(bib1::Attr::try_from(part.as_str())?);
                in_attr = false;
            } else if part.starts_with('@') {
                // Should not get here.
                break;
            } else {
                // The value is the final token in the clause
                return Ok(Clause {
                    attrs,
                    value: part.to_string(),
                });
            }
        }

        Err(format!(
            "Clause parsing completed without a search value: {attrs:?}"
        ))
    }

    /*
    fn read_parts(parts: &mut Vec<String>) -> Result<Node, String> {
        if let Some(token) = parts.get(0) {
            if token == "@and" || token == "@or" {
                Query::joiner(parts)
            } else {
                Query::value(parts)
            }
        } else {
            error(parts)
        }
    }

    fn joiner(parts: &mut Vec<String>) -> Result<Node, String> {
        let join_type = if parts.len() > 0 {
            parts.remove(0)
        } else {
            return error(parts);
        };

        let join_value = Joiner::try_from(join_type.as_str())?;

        let node = Node {
            content: Content::Joiner(join_value),
            left_child: None,
            right_child: None,
        };

        Ok(node)
    }

    fn value(parts: &mut Vec<String>) -> Result<Node, String> {
        /*
        let mut attrs = Vec::new();

        while parts.len() > 0 {
            let token = parts.remove(0);

            if part == "@attr" {
                if let Some(attr_
            }

        let mut node = Node {
            content: Content::Clause(Clause { attrs: Vec::new(),
            left_child: None,
            right_child: None,
        }
        */

        todo!();
    }
    */
}

#[test]
fn test_query_str() {
    let s = r#"@and @and @attr 1=7 @attr 4=6 @attr 5=1 "testisbn" @attr 1=4 @attr 4=6 @attr 5=1 "testtitle" @attr 1=1003 @attr 4=6 @attr 5=1 "testauthor""#;
    let q = Query::from_query_str(s).unwrap();
}
