use crate::record;
use crate::util;
use crate::{Controlfield, Field, Leader, Record, Subfield, Tag};

/// Adds string-based getter and setter functions for ease of use.
/// The cost of this convenience is greater RAM/CPU consumption,
/// since the data are crosswalked to/from bytes

impl Tag {
    pub fn from_str(tag: &str) -> Result<Tag, String> {
        let tag_bytes = util::utf8_to_bytes(tag, Some(record::TAG_LEN))?;
        Ok(Tag::new(&[tag_bytes[0], tag_bytes[1], tag_bytes[2]]))
    }
}

impl Leader {
    pub fn from_str(leader: &str) -> Result<Leader, String> {
        let bytes = util::utf8_to_bytes(leader, Some(record::LEADER_LEN))?;
        let mut lb: [u8; record::LEADER_LEN] = [0; record::LEADER_LEN];
        lb.clone_from_slice(bytes.as_slice());
        Ok(Leader::new(lb))
    }
}

impl Controlfield {
    pub fn from_strs(tag: &str, content: &str) -> Result<Controlfield, String> {
        let tag = Tag::from_str(tag)?;
        let content_bytes = util::utf8_to_bytes(content, None)?;
        Ok(Controlfield::new(tag, content_bytes.as_slice()))
    }

    pub fn content_string(&self) -> Result<String, String> {
        util::bytes_to_utf8(self.content())
    }

    pub fn set_content_string(&mut self, content: &str) -> Result<(), String> {
        let bytes = util::utf8_to_bytes(content, None)?;
        self.set_content(bytes.as_slice());
        Ok(())
    }
}

impl Subfield {
    pub fn from_strs(code: &str, content: &str) -> Result<Subfield, String> {
        let code = util::utf8_to_bytes(code, Some(1))?;
        let content = util::utf8_to_bytes(content, None)?;
        Ok(Subfield::new(code[0], content.as_slice()))
    }
    pub fn content_string(&self) -> Result<String, String> {
        util::bytes_to_utf8(self.content())
    }
}

impl Field {
    pub fn from_strs(
        tag: &str,
        ind1: &str,
        ind2: &str,
        subfields: &[(&str, &str)],
    ) -> Result<Field, String> {
        let mut field = Field::new(Tag::from_str(tag)?);

        field.set_ind1(util::utf8_to_bytes(ind1, Some(1))?[0]);
        field.set_ind2(util::utf8_to_bytes(ind2, Some(1))?[0]);

        for (code, content) in subfields {
            field.add_subfield(Subfield::from_strs(code, content)?);
        }

        Ok(field)
    }
}

impl Record {
    pub fn value_strings(&self, tag: &str, subfield: &str) -> Result<Vec<String>, String> {
        let tag = Tag::from_str(tag)?;
        let code = util::utf8_to_bytes(subfield, Some(1))?[0];

        let mut values = Vec::new();

        for field in self.fields().iter().filter(|f| f.tag() == &tag) {
            for sf in field.subfields().iter().filter(|sf| sf.code() == code) {
                values.push(sf.content_string()?);
            }
        }

        Ok(values)
    }
}
