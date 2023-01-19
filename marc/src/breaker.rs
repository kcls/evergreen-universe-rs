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
    pub fn to_breaker(&self) -> String {
        if self.content.len() > 0 {
            format!("{} {}", self.tag, escape_to_breaker(&self.content))
        } else {
            format!("{}", self.tag)
        }
    }
}

impl Subfield {
    pub fn to_breaker(&self) -> String {
        format!(
            "${}{}",
            escape_to_breaker(&self.code),
            escape_to_breaker(&self.content),
        )
    }
}

impl Field {
    pub fn to_breaker(&self) -> String {
        let mut s = format!(
            "{} {}{}",
            self.tag,
            match &self.ind1 {
                ' ' => '\\',
                _ => self.ind1,
            },
            match &self.ind2 {
                ' ' => '\\',
                _ => self.ind2,
            },
        );

        for sf in &self.subfields {
            s += sf.to_breaker().as_str();
        }

        s
    }
}

impl Record {
    /// Creates the MARC Breaker representation of this record as a String.
    pub fn to_breaker(&self) -> String {
        let mut s = format!("LDR {}", &escape_to_breaker(&self.leader));

        for cfield in &self.control_fields {
            s += format!("\n{}", cfield.to_breaker()).as_str();
        }

        for field in &self.fields {
            s += format!("\n{}", field.to_breaker()).as_str();
        }

        s
    }

    /// Creates a new MARC Record from a MARC Breaker string.
    pub fn from_breaker(breaker: &str) -> Result<Self, String> {
        let mut record = Record::new();

        for line in breaker.lines() {
            record.add_breaker_line(line)?;
        }

        Ok(record)
    }

    /// Process one line of breaker text
    fn add_breaker_line(&mut self, line: &str) -> Result<(), String> {
        let len = line.len();

        if len < 3 {
            // Skip invalid lines
            return Ok(());
        }

        let tag = &line[..3];

        if tag.eq("LDR") {
            if len > 4 {
                self.set_leader(&line[4..])?;
            }
            return Ok(());
        }

        // There is a space between the tag and the 1st indicator.

        if tag < "010" {
            let mut cf = Controlfield::new(tag, None)?;
            if len > 4 {
                cf.set_content(unescape_from_breaker(&line[4..]).as_str());
            }
            self.control_fields.push(cf);
            return Ok(());
        }

        let mut field = Field::new(tag)?;

        if len > 4 {
            field.set_ind1(&line[4..5].replace("\\", " "))?;
        }

        if len > 5 {
            field.set_ind2(&line[5..6].replace("\\", " "))?;
        }

        if len > 6 {
            for sf in line[6..].split(MARC_BREAKER_SF_DELIMITER) {
                if sf.len() == 0 {
                    continue;
                }
                let mut subfield = Subfield::new(&sf[..1], None)?;
                if sf.len() > 1 {
                    subfield.set_content(unescape_from_breaker(&sf[1..]).as_str());
                }
                field.subfields.push(subfield);
            }
        }

        self.fields.push(field);

        Ok(())
    }
}
