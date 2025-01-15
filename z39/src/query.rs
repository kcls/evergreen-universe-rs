use crate::bib1;
use std::fmt;

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
struct SearchClause {
    attrs: Vec<bib1::Attr>,
    value: String,
}

/*
impl fmt::Display for SearchClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let attrs: String = self
            .attrs
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<String>>()
            .join(" ");

        write!(f, "{attrs} {}", self.value)
    }
}
*/

#[derive(Debug, PartialEq, Clone)]
enum Content {
    Joiner(Joiner),
    SearchClause(SearchClause),
}

#[derive(Debug, PartialEq, Clone)]
struct Node {
    content: Content,
    left_node: Option<Box<Node>>,
    right_node: Option<Box<Node>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Query {
    source_query_string: Option<String>,
    tree: Node,
}

impl fmt::Display for SearchClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {

        let attrs: String = self
            .attrs
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<String>>()
            .join(" ");

        write!(f, "{attrs} {}", self.value)
    }
}

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
                contents.push(Content::SearchClause(Query::build_search_clause(
                    &mut parts,
                )?));
            }
        }

        let q = Query {
            source_query_string: Some(s.to_string()),
            tree: Query::next_node_from_content_list(&mut contents)?,
        };

        Ok(q)
    }

    fn next_node_from_content_list(contents: &mut Vec<Content>) -> Result<Node, String> {
        if contents.is_empty() {
            return Err("node_from_content_list() ran out of tokens to process".to_string());
        }

        let content = contents.remove(0);

        let node = if let Content::Joiner(ref joiner) = content {
            Node {
                content,
                left_node: Some(Box::new(Query::next_node_from_content_list(contents)?)),
                right_node: Some(Box::new(Query::next_node_from_content_list(contents)?)),
            }
        } else {
            Node {
                content,
                left_node: None,
                right_node: None,
            }
        };

        Ok(node)
    }

    /// Construct a clause from a set of query parts, removing parts from
    /// the parts array as we go.
    fn build_search_clause(parts: &mut Vec<String>) -> Result<SearchClause, String> {
        let mut attrs = Vec::new();

        let mut in_attr = false;
        while !parts.is_empty() {
            let part = parts.remove(0);

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
                return Ok(SearchClause {
                    attrs,
                    value: part.to_string(),
                });
            }
        }

        Err(format!(
            "SearchClause parsing completed without a search value: {attrs:?}"
        ))
    }
}

#[test]
fn test_query_str() {
    let s = r#"@and @and @attr 1=7 @attr 4=6 @attr 5=1 "testisbn" @attr 1=4 @attr 4=6 @attr 5=1 "testtitle" @attr 1=1003 @attr 4=6 @attr 5=1 "testauthor""#;
    let q = Query::from_query_str(s).unwrap();
    //println!("{q}");
}
