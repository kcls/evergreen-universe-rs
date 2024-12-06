//! Routines for reading and writing MARC Breaker text
use super::Controlfield;
use super::Field;
use super::Record;
use super::Subfield;

const MARC_BREAKER_SF_DELIMITER: &str = "$";
const MARC_BREAKER_SF_DELIMITER_ESCAPE: &str = "{dollar}";

/// Replace bare subfield delimiter values with their escaped version.
pub fn escape_to_breaker(value: &str) -> String {
    value.replace(MARC_BREAKER_SF_DELIMITER, MARC_BREAKER_SF_DELIMITER_ESCAPE)
}

/// Replace escaped subfield delimiter values with the bare version.
pub fn unescape_from_breaker(value: &str) -> String {
    value.replace(MARC_BREAKER_SF_DELIMITER_ESCAPE, MARC_BREAKER_SF_DELIMITER)
}

impl Controlfield {
    /// Generate breaker text for a [`Controlfield`]
    pub fn to_breaker(&self) -> String {
        if !self.content().is_empty() {
            format!("={} {}", self.tag(), escape_to_breaker(self.content()))
        } else {
            format!("={}", self.tag())
        }
    }
}

impl Subfield {
    /// Generate breaker text for a [`Subfield`]
    pub fn to_breaker(&self) -> String {
        format!(
            "${}{}",
            escape_to_breaker(self.code()),
            escape_to_breaker(self.content()),
        )
    }
}

impl Field {
    /// Generate breaker text for a [`Field`]
    pub fn to_breaker(&self) -> String {
        let mut s = format!(
            "={} {}{}",
            self.tag(),
            if self.ind1() == " " {
                "\\"
            } else {
                self.ind1()
            },
            if self.ind2() == " " {
                "\\"
            } else {
                self.ind2()
            },
        );

        for sf in self.subfields() {
            s += sf.to_breaker().as_str();
        }

        s
    }
}

impl Record {
    /// Generate breaker text for a [`Record`]
    ///
    /// # References
    ///
    /// * <https://www.loc.gov/marc/makrbrkr.html>
    pub fn to_breaker(&self) -> String {
        let mut s = format!("=LDR {}", &escape_to_breaker(self.leader()));

        for cfield in self.control_fields() {
            s += format!("\n{}", cfield.to_breaker()).as_str();
        }

        for field in self.fields() {
            s += format!("\n{}", field.to_breaker()).as_str();
        }

        s
    }

    /// Create a MARC [`Record`] from a MARC Breaker string.
    ///
    /// Assumes one record per input string.
    pub fn from_breaker(breaker: &str) -> Result<Self, String> {
        let mut record = Record::new();

        for line in breaker.lines() {
            record.add_breaker_line(line)?;
        }

        Ok(record)
    }

    /// Create a MARC [`Record`] from a file containing MARC Breaker text.
    ///
    /// Assumes one record per file.
    pub fn from_breaker_file(filename: &str) -> Result<Self, String> {
        let breaker = std::fs::read_to_string(filename)
            .map_err(|e| format!("Error reading breaker file: {e}"))?;
        Record::from_breaker(&breaker)
    }

    /// Process one line of breaker text and append the result to [`self`]
    fn add_breaker_line(&mut self, line: &str) -> Result<(), String> {
        let mut len = line.len();
        if len < 4 {
            // Skip unusable lines
            return Ok(());
        }

        // Step past the opening '=' character
        let line = &line[1..];
        len -= 1;

        let tag = &line[..3];

        if tag.eq("LDR") {
            if len > 4 {
                self.set_leader(&line[4..])?;
            }
            return Ok(());
        }

        if tag < "010" {
            let content = if len > 4 {
                unescape_from_breaker(&line[4..])
            } else {
                "".to_string()
            };
            let cf = Controlfield::new(tag, content)?;
            self.control_fields_mut().push(cf);
            return Ok(());
        }

        let mut field = Field::new(tag)?;

        // There is a space between the tag and the 1st indicator.

        if len > 4 {
            field.set_ind1(line[4..5].replace('\\', " "))?;
        }

        if len > 5 {
            field.set_ind2(line[5..6].replace('\\', " "))?;
        }

        if len > 6 {
            for sf in line[6..].split(MARC_BREAKER_SF_DELIMITER) {
                if sf.is_empty() {
                    continue;
                }
                let code = &sf[..1];
                let content = &sf[1..]; // maybe ""
                field.subfields_mut().push(Subfield::new(code, content)?);
            }
        }

        self.fields_mut().push(field);

        Ok(())
    }


}

#[cfg(test)]
mod breaker_tests {
    #[test]
    fn test_add_breaker_line() {
        let mut record = crate::Record::default();

        assert!(record.add_breaker_line("=LDR too short").is_err());

        record.add_breaker_line("=100 11$aSunshine$b$csquashes").unwrap();
        assert_eq!(record.get_field_values("100", "a")[0], "Sunshine");
        assert_eq!(record.get_field_values("100", "b")[0], "");
    }
}

