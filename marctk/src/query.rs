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
pub struct ComplexSpecification<'a> {
    tag: &'a str,
    ind1: Option<char>,
    ind2: Option<char>,
    subfields: &'a str,
}

impl<'a> ComplexSpecification<'a> {
    pub fn subfield_filter(&self) -> impl Fn(&Subfield, &Field) -> bool + use<'_> {
        |subfield: &Subfield, field: &Field| match subfield.code().chars().next() {
            Some(first_char) => self.subfields.contains(first_char) && self.matches_field(field),
            None => false,
        }
    }

    #[inline]
    pub fn matches_field(&self, field: &Field) -> bool {
        field.matches_spec(self.tag) && self.ind1_matches(field) && self.ind2_matches(field)
    }

    fn indicator_matches(
        field: &Field,
        query_indicator: Option<char>,
        field_indicator_fn: fn(&Field) -> &str,
    ) -> bool {
        match query_indicator {
            Some(query_indicator) => {
                let field_indicator = field_indicator_fn(field).chars().next();
                field_indicator.is_some_and(|field_indicator| {
                    query_indicator == '*'
                        || field_indicator == query_indicator
                        // '_' can represent an empty indicator
                        || field_indicator.is_whitespace() && query_indicator == '_'
                })
            }
            None => true,
        }
    }

    fn ind1_matches(&self, field: &Field) -> bool {
        Self::indicator_matches(field, self.ind1, Field::ind1)
    }

    fn ind2_matches(&self, field: &Field) -> bool {
        Self::indicator_matches(field, self.ind2, Field::ind2)
    }

    fn parse_indicators_and_subfields(spec: &'a str) -> (Option<char>, Option<char>, &'a str) {
        let mut remainder = spec.chars();
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
        let subfields = match first {
            Some('(') => &spec[4..],
            _ => spec,
        };

        (ind1, ind2, subfields)
    }
}

#[derive(Debug)]
pub struct QueryIsBadlyFormatted;

impl<'a> TryFrom<&'a str> for ComplexSpecification<'a> {
    type Error = QueryIsBadlyFormatted;

    fn try_from(query: &'a str) -> Result<Self, Self::Error> {
        let tag = query.get(0..3).ok_or(QueryIsBadlyFormatted)?;
        let rest = query.get(3..).ok_or(QueryIsBadlyFormatted)?;
        let (ind1, ind2, subfields) = Self::parse_indicators_and_subfields(rest);
        Ok(ComplexSpecification {
            tag,
            ind1,
            ind2,
            subfields,
        })
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
        let spec = ComplexSpecification::try_from("245a").unwrap();
        assert_eq!(spec.tag, "245");
        assert_eq!(spec.ind1, None);
        assert_eq!(spec.ind2, None);
        assert_eq!(spec.subfields, "a");

        let indicator_spec = ComplexSpecification::try_from("6xx(01)av").unwrap();
        assert_eq!(indicator_spec.tag, "6xx");
        assert_eq!(indicator_spec.ind1, Some('0'));
        assert_eq!(indicator_spec.ind2, Some('1'));
        assert_eq!(indicator_spec.subfields, "av");
    }

    #[test]
    fn test_can_create_subfield_filter() {
        let mut field = Field::new("245").unwrap();
        let _ = field.add_subfield("a", "My title");
        let _ = field.add_subfield("b", "My subtitle");

        let spec = ComplexSpecification::try_from("245a").unwrap();
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

        assert!(ComplexSpecification::try_from("245a")
            .unwrap()
            .matches_field(&field));
        assert!(ComplexSpecification::try_from("2xxa")
            .unwrap()
            .matches_field(&field));
        assert!(ComplexSpecification::try_from("245(14)a")
            .unwrap()
            .matches_field(&field));
        assert!(ComplexSpecification::try_from("245(1*)a")
            .unwrap()
            .matches_field(&field));
        assert!(!ComplexSpecification::try_from("245(00)a")
            .unwrap()
            .matches_field(&field));
        assert!(!ComplexSpecification::try_from("650(14)")
            .unwrap()
            .matches_field(&field));
    }

    #[test]
    fn test_can_match_fields_to_complex_specifications_with_empty_indicator() {
        let mut field = Field::new("100").unwrap();
        let _ = field.set_ind1("0");
        let _ = field.set_ind2(" ");

        assert!(ComplexSpecification::try_from("100(0_)")
            .unwrap()
            .matches_field(&field));
        assert!(ComplexSpecification::try_from("100(0*)")
            .unwrap()
            .matches_field(&field));
    }
}
