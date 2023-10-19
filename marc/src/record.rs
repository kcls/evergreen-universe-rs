///! Models a MARC record with associated components.
const TAG_SIZE: usize = 3;
const LEADER_SIZE: usize = 24;
const SF_CODE_SIZE: usize = 1;
pub const DEFAULT_LEADER: &str = "                        ";

fn check_byte_count(s: &str, len: usize) -> Result<(), String> {
    let byte_len = s.bytes().len();
    if byte_len != len {
        return Err(format!(
            "Invalid byte count for string s={s} wanted={len} found={byte_len}"));
    }
    Ok(())
}

/// MARC Control Field whose tag value is < "010"
#[derive(Debug, Clone, PartialEq)]
pub struct Controlfield {
    tag: String,
    content: String,
}

impl Controlfield {
    pub fn new(tag: &str, content: Option<&str>) -> Result<Self, String> {
        if tag.bytes().len() != TAG_SIZE {
            return Err(format!("Invalid tag: '{tag}' bytelen={}", tag.bytes().len()));
        }
        Ok(Controlfield {
            tag: tag.to_string(),
            content: match content {
                Some(c) => c.to_string(),
                _ => String::new(),
            },
        })
    }
    pub fn tag(&self) -> &str {
        &self.tag
    }
    pub fn content(&self) -> &str {
        &self.content
    }
    pub fn set_content(&mut self, content: &str) {
        self.content = content.to_string();
    }
}

/// A single subfield code + value pair
#[derive(Debug, Clone, PartialEq)]
pub struct Subfield {
    code: String,
    content: String,
}

impl Subfield {
    pub fn check_code(code: &str) -> Result<(), String> {
        if code.bytes().len() != SF_CODE_SIZE {
            return Err(format!(
                "Invalid subfield code: '{code}' bytelen={}", code.bytes().len()));
        }
        Ok(())
    }

    pub fn new(code: &str, content: &str) -> Result<Self, String> {
        check_byte_count(code, 1)?;
        Ok(Subfield {
            code: String::from(code),
            content: content.to_string(),
        })
    }
    pub fn content(&self) -> &str {
        &self.content
    }
    pub fn set_content(&mut self, content: &str) {
        self.content = String::from(content);
    }
    pub fn code(&self) -> &str {
        &self.code
    }
    pub fn set_code(&mut self, code: &str) -> Result<(), String> {
        check_byte_count(code, 1)?;
        self.code = String::from(code);
        Ok(())
    }
}

/// A MARC Data Field with tag, indicators, and subfields.
#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    tag: String,
    ind1: Option<String>,
    ind2: Option<String>,
    subfields: Vec<Subfield>,
}

impl Field {
    pub fn new(tag: &str) -> Result<Self, String> {
        check_byte_count(tag, TAG_SIZE)?;
        Ok(Field {
            tag: tag.to_string(),
            ind1: None,
            ind2: None,
            subfields: Vec::new(),
        })
    }
    pub fn tag(&self) -> &str {
        &self.tag
    }
    pub fn ind1(&self) -> &str {
        self.ind1.as_deref().unwrap_or(" ")
    }
    pub fn ind2(&self) -> &str {
        self.ind2.as_deref().unwrap_or(" ")
    }
    pub fn subfields(&self) -> &Vec<Subfield> {
        &self.subfields
    }
    pub fn subfields_mut(&mut self) -> &mut Vec<Subfield> {
        &mut self.subfields
    }
    pub fn set_ind1(&mut self, ind: &str) -> Result<(), String> {
        check_byte_count(ind, 1)?;
        self.ind1 = Some(ind.to_string());
        Ok(())
    }
    pub fn set_ind2(&mut self, ind: &str) -> Result<(), String> {
        check_byte_count(ind, 1)?;
        self.ind2 = Some(ind.to_string());
        Ok(())
    }
    pub fn get_subfields(&self, code: &str) -> Vec<&Subfield> {
        self.subfields.iter().filter(|f| f.code() == code).collect()
    }

    pub fn get_subfields_mut(&mut self, code: &str) -> Vec<&mut Subfield> {
        self.subfields
            .iter_mut()
            .filter(|f| f.code() == code)
            .collect()
    }

    pub fn add_subfield(&mut self, code: &str, content: &str) -> Result<(), String> {
        self.subfields.push(Subfield::new(code, content)?);
        Ok(())
    }

    /// Remove the first subfield with the specified code.
    pub fn remove_first_subfield(&mut self, code: &str) -> Option<Subfield> {
        if let Some(index) = self.subfields.iter().position(|s| s.code.eq(code)) {
            return Some(self.subfields.remove(index));
        }

        None
    }

    /// Remove all subfields with the specified code
    pub fn remove_subfields(&mut self, code: &str) -> Vec<Subfield> {
        let mut removed: Vec<Subfield> = Vec::new();

        loop {
            if let Some(index) = self.subfields.iter().position(|s| s.code.eq(code)) {
                removed.push(self.subfields.remove(index));
            } else {
                break;
            }
        }

        removed
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    leader: String,
    control_fields: Vec<Controlfield>,
    fields: Vec<Field>,
}

/// A MARC record with leader, control fields, and data fields.
impl Record {
    pub fn new() -> Self {
        Record {
            leader: DEFAULT_LEADER.to_string(),
            control_fields: Vec::new(),
            fields: Vec::new(),
        }
    }

    pub fn leader(&self) -> &str {
        &self.leader
    }

    /// Apply a leader value from a str
    ///
    /// Returns Err if the value is not composed of the correct number
    /// of bytes.
    pub fn set_leader(&mut self, leader: &str) -> Result<(), String> {
        if leader.bytes().len() != LEADER_SIZE {
            return Err(format!(
                "Invalid leader: '{leader}' bytelen={}", leader.bytes().len()));
        }

        self.leader = leader.to_string();
        Ok(())
    }

    /// Apply a leader value from a set of bytes
    ///
    /// Returns Err if the value is not composed of the correct number
    /// of bytes.
    pub fn set_leader_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        match std::str::from_utf8(bytes) {
            Ok(leader) => {
                self.set_leader(leader)?;
                return Ok(());
            }
            Err(e) => Err(format!(
                "Cannot translate leader to UTF-8 {:?} {}",
                bytes, e
            )),
        }
    }

    pub fn control_fields(&self) -> &Vec<Controlfield> {
        &self.control_fields
    }
    pub fn control_fields_mut(&mut self) -> &mut Vec<Controlfield> {
        &mut self.control_fields
    }
    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }
    pub fn fields_mut(&mut self) -> &mut Vec<Field> {
        &mut self.fields
    }

    pub fn get_control_fields(&self, tag: &str) -> Vec<&Controlfield> {
        self.control_fields
            .iter()
            .filter(|f| f.tag() == tag)
            .collect()
    }

    pub fn get_fields(&self, tag: &str) -> Vec<&Field> {
        self.fields.iter().filter(|f| f.tag() == tag).collect()
    }

    pub fn get_fields_mut(&mut self, tag: &str) -> Vec<&mut Field> {
        self.fields.iter_mut().filter(|f| f.tag() == tag).collect()
    }

    /// Add a control field with data.
    ///
    /// Controlfields are those with tag 001 .. 009
    pub fn add_control_field(&mut self, tag: &str, content: &str) -> Result<(), String> {
        let mut field = Controlfield::new(tag, Some(content))?;

        if tag >= "010" || tag <= "000" {
            return Err(format!("Invalid control field tag: '{tag}'"));
        }

        field.set_content(content);

        // Insert the field at the logical position in the record.

        let mut pos = 0;
        for (idx, f) in self.control_fields.iter().enumerate() {
            pos = idx;
            if f.tag.as_str() > tag {
                break;
            }
        }

        if pos == self.control_fields.len() {
            self.control_fields.push(field);
        } else {
            self.control_fields.insert(pos, field);
        }

        Ok(())
    }

    pub fn insert_field(&mut self, field: Field) {
        match self.fields().iter().position(|f| f.tag() > field.tag()) {
            Some(idx) => self.fields_mut().insert(idx, field),
            None => self.fields_mut().push(field),
        }
    }

    /// Add a new datafield with the given tag, indicators, and list of
    /// subfields.
    ///
    /// * `subfields` - List of subfield code, followed by subfield value.
    ///     e.g. vec![("a", "My Title"), ("b", "More Title Stuff")]
    pub fn add_data_field(
        &mut self,
        tag: &str,
        ind1: &str,
        ind2: &str,
        subfields: &[(&str, &str)],
    ) -> Result<(), String> {
        if tag < "010" {
            return Err(format!("Invalid data field tag: '{tag}'"));
        }

        let mut field = Field::new(tag)?;
        field.set_ind1(ind1)?;
        field.set_ind2(ind2)?;

        for (code, value) in subfields {
            field.subfields_mut().push(Subfield::new(code, value)?);
        }

        self.insert_field(field);
        Ok(())
    }

    /// Returns a list of values for the specified tag and subfield.
    pub fn get_values(&self, tag: &str, sfcode: &str) -> Vec<&str> {
        let mut vec = Vec::new();
        for field in self.get_fields(tag) {
            for sf in field.get_subfields(sfcode) {
                vec.push(sf.content.as_str());
            }
        }
        vec
    }

    /// Remove all occurrences of fields with the provided tag.
    pub fn remove_fields(&mut self, tag: &str) {
        loop {
            if let Some(pos) = self.fields.iter().position(|f| f.tag() == tag) {
                self.fields.remove(pos);
            } else {
                // No more fields to remove.
                return;
            }
        }
    }
}
