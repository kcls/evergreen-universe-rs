use super::Controlfield;
use super::Field;
use super::Record;
use super::Subfield;

// b"$"
const MARC_BREAKER_SF_DELIMITER_STR: &str = "$";
const MARC_BREAKER_SF_DELIMITER: &[u8] = &[36];

// b"{dollar}"
const MARC_BREAKER_SF_DELIMITER_ESCAPE: &[u8] = &[123, 100, 111, 108, 108, 97, 114, 125];

/// Returns the index of a matching subsequence of bytes
/// ```
/// use marc::breaker::replace_byte_sequence;
///
/// let s = b"hello joe";
/// let v = replace_byte_sequence(s, b"ll", b"jj");
/// assert_eq!(v, b"hejjo joe");
///
/// let v = replace_byte_sequence(s, b"he", b"HE");
/// assert_eq!(v, b"HEllo joe");
///
/// let v = replace_byte_sequence(s, b"joe", b"xx");
/// assert_eq!(v, b"hello xx");
///
/// let v = replace_byte_sequence(s, b"o", b"Z");
/// assert_eq!(v, b"hellZ jZe")
/// ```
pub fn replace_byte_sequence(source: &[u8], target: &[u8], replace: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();

    let source_len = source.len();
    let target_len = target.len();

    let mut index = 0;

    while index < source_len {
        let part = &source[index..];

        if part.len() >= target_len {
            if &part[..target_len] == target {
                result.extend(replace);
                index += target_len;
                continue
            }
        }

        // No match; add the next byte
        result.push(part[0]);

        index += 1;
    }

    result
}

/// Replace bare subfield delimiter values with their escaped version.
pub fn escape_to_breaker(value: &[u8]) -> Vec<u8> {
    replace_byte_sequence(value, MARC_BREAKER_SF_DELIMITER, MARC_BREAKER_SF_DELIMITER_ESCAPE)
}

/// Replace escaped subfield delimiter values with the bare version.
pub fn unescape_from_breaker(value: &[u8]) -> Vec<u8> {
    replace_byte_sequence(value, MARC_BREAKER_SF_DELIMITER_ESCAPE, MARC_BREAKER_SF_DELIMITER)
}


impl Controlfield {
    pub fn to_breaker(&self) -> String {
        format!("{}{}",
            self.tag,
            String::from_utf8_lossy(&escape_to_breaker(&self.content))
        )
    }
}


impl Subfield {
    pub fn to_breaker(&self) -> String {
        format!("{}{}{}",
            MARC_BREAKER_SF_DELIMITER_STR,
            self.code,
            String::from_utf8_lossy(&escape_to_breaker(&self.content))
        )
    }
}

/*

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
*/
