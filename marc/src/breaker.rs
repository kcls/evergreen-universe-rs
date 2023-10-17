/// Extend marc::Record and its components with tools for reading/writing
/// MARC breaker text.
///
/// Breaker text is assumed to be UTF8.
use super::{Tag, Leader, Subfield, Field, ControlField, Record};
use super::util;

// b"$"
pub const MARC_BREAKER_SF_DELIMITER_STR: &str = "$";
pub const MARC_BREAKER_SF_DELIMITER: &[u8] = &[b'$'];
// b"{dollar}"
pub const MARC_BREAKER_SF_DELIMITER_ESCAPE: &[u8] = &[123, 100, 111, 108, 108, 97, 114, 125];

/// Replace bare subfield delimiter values with their escaped version.
/// ```
/// use marc::breaker::escape_to_breaker;
///
/// let e = escape_to_breaker(b"My money is $9.25 or $0.00");
/// assert_eq!(b"My money is {dollar}9.25 or {dollar}0.00", e.as_slice());
/// ```
pub fn escape_to_breaker(value: &[u8]) -> Vec<u8> {
    util::replace_byte_sequence(
        value,
        MARC_BREAKER_SF_DELIMITER,
        MARC_BREAKER_SF_DELIMITER_ESCAPE
    )
}

/// Replace escaped subfield delimiter values with the bare version.
/// ```
/// use marc::breaker::unescape_from_breaker;
///
/// let e = unescape_from_breaker(b"My money is {dollar}9.25 or {dollar}0.00");
/// assert_eq!(b"My money is $9.25 or $0.00", e.as_slice());
/// ```
pub fn unescape_from_breaker(value: &[u8]) -> Vec<u8> {
    util::replace_byte_sequence(
        value,
        MARC_BREAKER_SF_DELIMITER_ESCAPE,
        MARC_BREAKER_SF_DELIMITER
    )
}

impl ControlField {
    pub fn to_breaker(&self) -> String {
        format!("={} {}",
            String::from_utf8_lossy(self.tag().value()),
            String::from_utf8_lossy(&escape_to_breaker(self.content()))
        )
    }
}

impl Subfield {
    pub fn to_breaker(&self) -> String {
        format!("{}{}{}",
            MARC_BREAKER_SF_DELIMITER_STR,
            self.code() as char,
            String::from_utf8_lossy(&escape_to_breaker(self.content()))
        )
    }
}


impl Field {
    pub fn to_breaker(&self) -> String {
        let ind1 = self.ind1() as char;
        let ind2 = self.ind2() as char;

        let mut s = format!("={} {}{}",
            String::from_utf8_lossy(self.tag().value()),
            if ind1 == ' ' { '\\' } else { ind1 },
            if ind2 == ' ' { '\\' } else { ind2 }
        );
        for sf in self.subfields() {
            s += &sf.to_breaker();
        }

        s
    }
}

impl Record {
    /// Creates the MARC Breaker representation of this record as a String.
    pub fn to_breaker(&self) -> String {
        let mut s = format!("=LDR {}", String::from_utf8_lossy(
            &escape_to_breaker(self.leader().value())));

        for cfield in self.control_fields() {
            s += &format!("\n{}", cfield.to_breaker());
        }

        for field in self.fields() {
            s += &format!("\n{}", field.to_breaker());
        }

        s
    }

    /// Creates a new MARC Record from a MARC Breaker string.
    pub fn from_breaker(breaker: &str) -> Result<Self, String> {
        let mut record = Record::default();

        for line in breaker.lines() {
            record.add_breaker_line(line);
        }

        Ok(record)
    }


    /// Process one line of breaker text
    fn add_breaker_line(&mut self, line: &str) {
        let line_bytes = line.as_bytes();
        let mut len = line_bytes.len();

        if len == 0 {
            return;
        }

        // Skip the opening '='
        let line_bytes = &line_bytes[1..];
        len -= 1;

        if len < 3 {
            // Not enough content to do anything with.
            return;
        }

        let tag = &line_bytes[0..3];
        if tag == b"LDR" {
            if len > 4 {
                let mut leader = Leader::default();
                leader.set_value(&line_bytes[4..]);
                self.set_leader(leader);
            }
            return;
        }

        // There is a space between the tag and the 1st indicator.
        let tag = Tag::from(&[tag[0], tag[1], tag[2]]);

        if tag.is_control_tag() {
            let mut cf = ControlField::new(tag, &[]);
            if len > 4 {
                cf.set_content(unescape_from_breaker(&line_bytes[4..]).as_slice());
            }
            self.control_fields_mut().push(cf);
            return;
        }

        if !tag.is_data_tag() {
            // Tag is something funk, not a LDR, control field or data field
            // Ignore it.
            return;
        }

        let mut field = Field::new(tag);

        // index 3 is a space between the tag and first indicator.

        if len > 4 {
            let mut ind = line_bytes[4] as char;
            if ind == '\\' { ind = ' '; }
            field.set_ind1(ind as u8);
        }

        if len > 5 {
            let mut ind = line_bytes[5] as char;
            if ind == '\\' { ind = ' '; }
            field.set_ind2(ind as u8);
        }

        if len > 6 {

            for sf in line_bytes[6..].split(|b| b == &MARC_BREAKER_SF_DELIMITER[0]) {
                if sf.len() == 0 {
                    continue;
                }
                let mut subfield = Subfield::new(sf[0], &[]);
                if sf.len() > 1 {
                    subfield.set_content(unescape_from_breaker(&sf[1..]).as_slice());
                }
                field.add_subfield(subfield);
            }
        }

        self.fields_mut().push(field);
    }
}
