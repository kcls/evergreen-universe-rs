#![cfg(feature = "marc21_bibliographic")]
use crate::{Field, Record};

impl Record {
    /// # Examples
    ///
    /// ```
    /// use marctk::Record;
    /// let record = Record::from_breaker(r#"=LDR 02322cam a2200445u  4500
    ///=245 10$aRobot / $c Jan Pieńkowski.
    ///=520 \\$aA robot family's everyday life is described and illustrated in a dutiful young robot's letter home inquiring how everyone is doing."#)
    ///     .unwrap();
    /// assert_eq!(record.main_title(), Some("Robot / ".to_string()));
    /// ```
    pub fn main_title(&self) -> Option<String> {
        Some(
            self.get_fields("245")
                .first()?
                .first_subfield("a")?
                .content()
                .to_owned(),
        )
    }

    /// An alternative to [`get_fields`] that returns parallel 880 fields,
    /// which are also known as
    /// [alternate graphic representations](https://www.loc.gov/marc/bibliographic/bd880.html)
    /// in the MARC21 standard.
    ///
    /// In records where catalogers have entered both the original script
    /// of a language and a Romanized version, `get_parallel_fields` will
    /// return the original script, while `get_fields` will return the
    /// Romanized version.
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Record;
    /// let record = Record::from_breaker(r#"=LDR 02322cam a2200445u  4500
    ///=100 1\$6880-01 $a Shukrī Tabrīzī, Shukr Allāh, $d 18th century, $e author.
    ///=880 1\$6100-01/(3/r $a شکری تبریزی، شکرالله."#)
    ///     .unwrap();
    /// let original_script_fields = record.get_parallel_fields("100");
    /// assert_eq!(original_script_fields.len(), 1);
    /// let field = original_script_fields.first().unwrap();
    /// assert_eq!(
    ///     field.first_subfield("a").unwrap().content(),
    ///     " شکری تبریزی، شکرالله."
    /// );
    /// ```
    /// [`get_fields`]: crate::Record::get_fields
    pub fn get_parallel_fields(&self, tag: &str) -> Vec<&Field> {
        let parallel_field_matches = {
            |field: &Field| match field.first_subfield("6") {
                Some(subfield) => subfield.content().starts_with(tag),
                None => false,
            }
        };
        self.fields
            .iter()
            .filter(|f| f.tag() == "880" && parallel_field_matches(f))
            .collect()
    }
}
