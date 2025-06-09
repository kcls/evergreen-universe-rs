use crate::{Field, Subfield};
use std::ops::RangeInclusive;

pub struct FieldQuery {
    pub field_filter: Box<dyn Fn(&&Field) -> bool>,
}

impl From<RangeInclusive<i64>> for FieldQuery {
    fn from(range: RangeInclusive<i64>) -> Self {
        FieldQuery {
            field_filter: Box::new(move |f: &&Field| match f.tag().parse::<i64>() {
                Ok(tag_number) => range.contains(&tag_number),
                Err(_) => false,
            }),
        }
    }
}

impl From<&str> for FieldQuery {
    fn from(spec_input: &str) -> Self {
        let specs: Vec<String> = spec_input.split(':').map(str::to_owned).collect();
        FieldQuery {
            field_filter: Box::new(move |f: &&Field| specs.iter().any(|spec| f.matches_spec(spec))),
        }
    }
}

/// Mutable variant of [`FieldQuery`]
pub struct FieldQueryMut {
    pub field_filter: Box<dyn FnMut(&&mut Field) -> bool>,
}

impl From<RangeInclusive<i64>> for FieldQueryMut {
    fn from(range: RangeInclusive<i64>) -> Self {
        FieldQueryMut {
            field_filter: Box::new(move |f: &&mut Field| match f.tag().parse::<i64>() {
                Ok(tag_number) => range.contains(&tag_number),
                Err(_) => false,
            }),
        }
    }
}

impl From<&str> for FieldQueryMut {
    fn from(spec_input: &str) -> Self {
        let specs: Vec<String> = spec_input.split(':').map(str::to_owned).collect();
        FieldQueryMut {
            field_filter: Box::new(move |f: &&mut Field| {
                specs.iter().any(|spec| f.matches_spec(spec))
            }),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ComplexSpecification {
    tag: String,
    ind1: Option<char>,
    ind2: Option<char>,
    subfields: Vec<char>,
}

impl ComplexSpecification {
    pub fn subfield_filter(&self) -> impl Fn(&Subfield, &Field) -> bool + use<'_> {
        |subfield: &Subfield, field: &Field| match subfield.code().chars().next() {
            Some(first_char) => self.subfields.contains(&first_char) && self.matches_field(field),
            None => false,
        }
    }

    pub fn matches_field(&self, field: &Field) -> bool {
        field.matches_spec(&self.tag) && self.ind1_matches(field) && self.ind2_matches(field)
    }

    fn ind1_matches(&self, field: &Field) -> bool {
        match self.ind1 {
            Some(i) => {
                i == '*' // * is a wildcard that matches anything
                    || field.ind1() == i.to_string()
                    || field.ind1().trim().is_empty() && i == '_' // _ can represent an empty indicator
            }
            None => true,
        }
    }

    fn ind2_matches(&self, field: &Field) -> bool {
        match self.ind2 {
            Some(i) => {
                i == '*'
                    || field.ind2() == i.to_string()
                    || field.ind1().trim().is_empty() && i == '_'
            }
            None => true,
        }
    }

    fn parse_indicators_and_subfields(
        remainder: &mut impl Iterator<Item = char>,
    ) -> (Option<char>, Option<char>, Vec<char>) {
        // if first character is '(', that means that
        // there are indicators specified within the
        // parens
        let first = remainder.next();
        let ind1 = match first {
            Some('(') => remainder.next(),
            _ => None,
        };
        let ind2 = match first {
            Some('(') => remainder.next(),
            _ => None,
        };
        if first == Some('(') {
            remainder.next(); // Consume the closing paren
        }
        let subfields: Vec<char> = match first {
            Some('(') => remainder.collect(),
            _ => first.into_iter().chain(remainder).collect(),
        };

        (ind1, ind2, subfields)
    }
}

impl From<&str> for ComplexSpecification {
    fn from(raw_spec: &str) -> ComplexSpecification {
        let mut rest = raw_spec.chars();
        let tag = rest.by_ref().take(3).collect::<String>();
        let (ind1, ind2, subfields) = Self::parse_indicators_and_subfields(&mut rest);
        ComplexSpecification {
            tag,
            ind1,
            ind2,
            subfields,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Record;

    #[test]
    fn test_can_filter_by_inclusive_range() {
        let query = FieldQuery::from(600..=699);

        let record = Record::from_breaker(
            r#"=600 10$aZhang, Heng, $d 78-139 $v Juvenile literature.
=650 \0$aEarthquakes $v Juvenile literature.
=955 \0$a1234"#,
        )
        .unwrap();
        let filter = query.field_filter;
        let mut filtered = record.fields().iter().filter(filter);
        assert_eq!(filtered.next().unwrap().tag(), "600");
        assert_eq!(filtered.next().unwrap().tag(), "650");
        assert!(filtered.next().is_none());
    }

    #[test]
    fn test_can_filter_by_string_slice() {
        let query = FieldQuery::from("x50");

        let record = Record::from_breaker(
            r#"=150 \\$aLion
=450 \\$aPanthera leo
=550 \\$wg$aPanthera
=953 \\$axx00$bec11"#,
        )
        .unwrap();
        let filter = query.field_filter;
        let mut filtered = record.fields().iter().filter(filter);
        assert_eq!(filtered.next().unwrap().tag(), "150");
        assert_eq!(filtered.next().unwrap().tag(), "450");
        assert_eq!(filtered.next().unwrap().tag(), "550");
        assert!(filtered.next().is_none());
    }

    #[test]
    fn test_can_filter_by_string_slice_with_multiple_specs() {
        let query = FieldQuery::from("600:9XX");

        let record = Record::from_breaker(
            r#"=600 10$aZhang, Heng, $d 78-139 $v Juvenile literature.
=650 \0$aEarthquakes $v Juvenile literature.
=955 \0$a1234"#,
        )
        .unwrap();
        let filter = query.field_filter;
        let mut filtered = record.fields().iter().filter(filter);
        assert_eq!(filtered.next().unwrap().tag(), "600");
        assert_eq!(filtered.next().unwrap().tag(), "955");
        assert!(filtered.next().is_none());
    }

    #[test]
    fn test_if_filter_ignores_junk_and_non_numeric_tags() {
        let query = FieldQuery::from("6XX:ABC$DEF");

        // Some records have funky tags.
        let record =
            Record::from_breaker(r#"=ABC \0$aEarthquakes $v Juvenile literature."#).unwrap();
        let filter = query.field_filter;
        let mut filtered = record.fields().iter().filter(filter);
        assert!(filtered.next().is_none());
    }

    #[test]
    fn test_can_create_complex_spec() {
        let spec = ComplexSpecification::from("245a");
        assert_eq!(spec.tag, "245");
        assert_eq!(spec.ind1, None);
        assert_eq!(spec.ind2, None);
        assert_eq!(spec.subfields, vec!['a']);

        let indicator_spec = ComplexSpecification::from("6xx(01)av");
        assert_eq!(indicator_spec.tag, "6xx");
        assert_eq!(indicator_spec.ind1, Some('0'));
        assert_eq!(indicator_spec.ind2, Some('1'));
        assert_eq!(indicator_spec.subfields, vec!['a', 'v']);
    }

    #[test]
    fn test_can_create_subfield_filter() {
        let mut field = Field::new("245").unwrap();
        let _ = field.add_subfield("a", "My title");
        let _ = field.add_subfield("b", "My subtitle");

        let spec = ComplexSpecification::from("245a");
        let mut filtered = field
            .subfields()
            .iter()
            .filter(|sf| spec.subfield_filter()(&sf, &field));
        assert_eq!(filtered.next().unwrap().code(), "a");
        assert_eq!(filtered.next(), None);
    }

    #[test]
    fn test_can_match_fields_to_complex_specifications() {
        let mut field = Field::new("245").unwrap();
        let _ = field.set_ind1("1");
        let _ = field.set_ind2("4");

        assert!(ComplexSpecification::from("245a").matches_field(&field));
        assert!(ComplexSpecification::from("2xxa").matches_field(&field));
        assert!(ComplexSpecification::from("245(14)a").matches_field(&field));
        assert!(ComplexSpecification::from("245(1*)a").matches_field(&field));
        assert!(!ComplexSpecification::from("245(00)a").matches_field(&field));
        assert!(!ComplexSpecification::from("650(14)").matches_field(&field));
    }
}
