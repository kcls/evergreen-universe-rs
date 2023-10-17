use crate::record;
use crate::util;
/// Adds string-based getter and setter functions for ease of use.
/// The cost of this convenience is greater RAM/CPU consumption,
/// since the data are crosswalked to/from byte arrays.
use crate::{ControlField, Field, Leader, Record, Subfield, Tag};

impl Tag {
    pub fn to_string(&self) -> Result<String, String> {
        util::bytes_to_utf8(self.value())
    }
    pub fn from_str(tag: &str) -> Result<Tag, String> {
        let tag_bytes = util::utf8_to_bytes(tag, Some(record::TAG_LEN))?;
        Ok(Tag::from(tag_bytes.as_slice()))
    }
}

impl Leader {
    pub fn from_str(leader: &str) -> Result<Leader, String> {
        let bytes = util::utf8_to_bytes(leader, Some(record::LEADER_LEN))?;
        Ok(Leader::from(bytes.as_slice()))
    }

    pub fn to_string(&self) -> Result<String, String> {
        util::bytes_to_utf8(self.value())
    }
}

impl ControlField {
    pub fn from_strs(tag: &str, content: &str) -> Result<ControlField, String> {
        let tag = Tag::from_str(tag)?;
        let content_bytes = util::utf8_to_bytes(content, None)?;
        Ok(ControlField::new(tag, content_bytes.as_slice()))
    }

    pub fn tag_string(&self) -> Result<String, String> {
        util::bytes_to_utf8(self.tag().value())
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

    pub fn code_string(&self) -> Result<String, String> {
        util::bytes_to_utf8(&[self.code()])
    }

    pub fn content_string(&self) -> Result<String, String> {
        util::bytes_to_utf8(self.content())
    }
}

impl Field {
    pub fn from_tag_str(tag: &str) -> Result<Field, String> {
        Ok(Field::new(Tag::from_str(tag)?))
    }

    pub fn set_ind1_from_str(&mut self, ind: &str) -> Result<(), String> {
        Ok(self.set_ind1(util::utf8_to_bytes(ind, Some(1))?[0]))
    }

    pub fn set_ind2_from_str(&mut self, ind: &str) -> Result<(), String> {
        Ok(self.set_ind2(util::utf8_to_bytes(ind, Some(1))?[0]))
    }

    pub fn first_subfield_from_str(&self, code: &str) -> Result<Option<&Subfield>, String> {
        let code = util::utf8_to_bytes(code, Some(1))?[0];
        Ok(self
            .subfields()
            .iter()
            .filter(|sf| sf.code() == code)
            .next())
    }

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
    pub fn fields_from_str(&self, tag: &str) -> Result<Vec<&Field>, String> {
        let tag = Tag::from_str(tag)?;
        Ok(self
            .fields()
            .iter()
            .filter(|f| f.tag() == &tag)
            .collect::<Vec<&Field>>())
    }
}
