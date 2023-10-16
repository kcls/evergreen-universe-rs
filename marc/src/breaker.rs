use super::Controlfield;
use super::Field;
use super::Leader;
use super::Tag;
use super::Record;
use super::Subfield;

// b"$"
pub const MARC_BREAKER_SF_DELIMITER_STR: &str = "$";
pub const MARC_BREAKER_SF_DELIMITER: &[u8] = &[36];

// b"{dollar}"
pub const MARC_BREAKER_SF_DELIMITER_ESCAPE: &[u8] = &[123, 100, 111, 108, 108, 97, 114, 125];

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
/// ```
/// use marc::breaker::escape_to_breaker;
///
/// let e = escape_to_breaker(b"My money is $9.25 or $0.00");
/// assert_eq!(b"My money is {dollar}9.25 or {dollar}0.00", e.as_slice());
/// ```
pub fn escape_to_breaker(value: &[u8]) -> Vec<u8> {
    replace_byte_sequence(value, MARC_BREAKER_SF_DELIMITER, MARC_BREAKER_SF_DELIMITER_ESCAPE)
}

/// Replace escaped subfield delimiter values with the bare version.
/// ```
/// use marc::breaker::unescape_from_breaker;
///
/// let e = unescape_from_breaker(b"My money is {dollar}9.25 or {dollar}0.00");
/// assert_eq!(b"My money is $9.25 or $0.00", e.as_slice());
/// ```
pub fn unescape_from_breaker(value: &[u8]) -> Vec<u8> {
    replace_byte_sequence(value, MARC_BREAKER_SF_DELIMITER_ESCAPE, MARC_BREAKER_SF_DELIMITER)
}


impl Controlfield {
    pub fn to_breaker(&self) -> String {
        format!("={} {}",
            self.tag,
            String::from_utf8_lossy(&escape_to_breaker(&self.content))
        )
    }
}


impl Subfield {
    pub fn to_breaker(&self) -> String {
        format!("{}{}{}",
            MARC_BREAKER_SF_DELIMITER_STR,
            self.code as char,
            String::from_utf8_lossy(&escape_to_breaker(&self.content))
        )
    }
}

impl Field {
    pub fn to_breaker(&self) -> String {
        let mut s = format!("={} {}{}",
            self.tag,
            if self.ind1 as char == ' ' { '\\' } else { self.ind1 as char },
            if self.ind2 as char == ' ' { '\\' } else { self.ind2 as char },
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
        let mut s = format!("=LDR {}", String::from_utf8_lossy(
            &escape_to_breaker(self.leader.value().as_slice())));

        for cfield in &self.control_fields {
            s += &format!("\n{}", cfield.to_breaker());
        }

        for field in &self.fields {
            s += &format!("\n{}", field.to_breaker());
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
        if line.len() == 0 {
            return Ok(());
        }

        // skip the opening "="
        let line = &line[1..];
        let len = line.len();

        if len < 3 {
            // Skip invalid lines
            return Ok(());
        }

        let tag = &line[..3];

        if tag.eq("LDR") {
            if len > 4 {
                let leader: Leader = line[4..].try_into()?;
                self.set_leader(leader);
            }
            return Ok(());
        }

        // There is a space between the tag and the 1st indicator.
        let tag: Tag = tag.try_into()?;

        if tag.is_control_field() {
            let mut cf = Controlfield::new(tag, &[]);
            if len > 4 {
                cf.set_content(unescape_from_breaker(&line[4..].as_bytes()).as_slice());
            }
            self.control_fields.push(cf);
            return Ok(());
        }

        let mut field = Field::new(tag);

        if len > 4 {
            let ind = &line[4..5].replace("\\", " ");
            let bytes = ind.as_bytes();
            if bytes.len() > 1 {
                // Can happen if an indicator is a non-ascii character.
                return Err(format!("Invalid indicator bytes: {bytes:?}"));
            }
            field.set_ind1(bytes[0]);
        }

        if len > 5 {
            let ind = &line[5..6].replace("\\", " ");
            let bytes = ind.as_bytes();
            if bytes.len() > 1 {
                // Can happen if an indicator is a non-ascii character.
                return Err(format!("Invalid indicator bytes: {bytes:?}"));
            }
            field.set_ind2(bytes[0]);
        }

        if len > 6 {
            for sf in line[6..].split(MARC_BREAKER_SF_DELIMITER_STR) {
                if sf.len() == 0 {
                    continue;
                }
                let bytes = &sf[..1].as_bytes();
                if bytes.len() > 1 {
                    return Err(format!("Invalid subfield code bytes: {bytes:?}"));
                }
                let mut subfield = Subfield::new(bytes[0], &[]);
                if sf.len() > 1 {
                    subfield.set_content(unescape_from_breaker(&sf[1..].as_bytes()).as_slice());
                }
                field.subfields.push(subfield);
            }
        }

        self.fields.push(field);

        Ok(())
    }
}
