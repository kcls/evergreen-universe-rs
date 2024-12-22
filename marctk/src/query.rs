use std::ops::RangeInclusive;

use crate::Field;

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
}
