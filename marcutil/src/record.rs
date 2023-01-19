///! Models a MARC record with associated components.
const TAG_SIZE: usize = 3;
const LEADER_SIZE: usize = 24;
const SF_CODE_SIZE: usize = 1;
pub const DEFAULT_LEADER: &str = "                        ";

/// MARC Control Field whose tag value is < "010"
#[derive(Debug, Clone)]
pub struct Controlfield {
    pub tag: String,
    pub content: String,
}

impl Controlfield {
    pub fn new(tag: &str, content: Option<&str>) -> Result<Self, String> {
        if tag.bytes().len() != TAG_SIZE {
            return Err(format!("Invalid tag: {tag}"));
        }
        Ok(Controlfield {
            tag: tag.to_string(),
            content: match content {
                Some(c) => c.to_string(),
                _ => String::new(),
            },
        })
    }

    pub fn set_content(&mut self, content: &str) {
        self.content = content.to_string();
    }
}

/// A single subfield code + value pair
#[derive(Debug, Clone)]
pub struct Subfield {
    pub code: String,
    pub content: String,
}

impl Subfield {
    pub fn new(code: &str, content: Option<&str>) -> Result<Self, String> {
        if code.bytes().len() != SF_CODE_SIZE {
            return Err(format!("Invalid subfield code: {code}"));
        }

        Ok(Subfield {
            code: String::from(code),
            content: match content {
                Some(c) => c.to_string(),
                _ => String::new(),
            },
        })
    }

    pub fn set_content(&mut self, content: &str) {
        self.content = String::from(content);
    }
}

/// A MARC Data Field with tag, indicators, and subfields.
#[derive(Debug, Clone)]
pub struct Field {
    pub tag: String,
    pub ind1: char,
    pub ind2: char,
    pub subfields: Vec<Subfield>,
}

impl Field {
    pub fn new(tag: &str) -> Result<Self, String> {
        if tag.bytes().len() != TAG_SIZE {
            return Err(format!("Invalid tag: {tag}"));
        }

        Ok(Field {
            tag: tag.to_string(),
            ind1: ' ',
            ind2: ' ',
            subfields: Vec::new(),
        })
    }

    pub fn set_ind1(&mut self, ind: &str) -> Result<(), String> {
        self.set_ind(ind, true)
    }

    pub fn set_ind2(&mut self, ind: &str) -> Result<(), String> {
        self.set_ind(ind, false)
    }

    fn set_ind(&mut self, ind: &str, first: bool) -> Result<(), String> {
        let bytes = ind.as_bytes();

        let i = match ind.bytes().len() {
            2.. => {
                return Err(format!("Invalid indicator value: '{ind}'"));
            }
            1 => bytes[0] as char,
            _ => ' ',
        };

        match first {
            true => self.ind1 = i,
            false => self.ind2 = i,
        }

        Ok(())
    }

    pub fn get_subfields(&self, code: &str) -> Vec<&Subfield> {
        self.subfields.iter().filter(|f| f.code.eq(code)).collect()
    }

    pub fn get_subfields_mut(&mut self, code: &str) -> Vec<&mut Subfield> {
        self.subfields
            .iter_mut()
            .filter(|f| f.code.eq(code))
            .collect()
    }

    pub fn add_subfield(&mut self, code: &str, content: Option<&str>) -> Result<(), String> {
        self.subfields.push(Subfield::new(code, content)?);
        Ok(())
    }

    /// Remove the first subfield with the specified code.
    pub fn remove_first_subfield(&mut self, code: &str) -> Option<Subfield> {
        if let Some(index) = self.subfields.iter().position(|s| s.code.eq(code)) {
            return Some(self.subfields.remove(index))
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

#[derive(Debug, Clone)]
pub struct Record {
    pub leader: String,
    pub control_fields: Vec<Controlfield>,
    pub fields: Vec<Field>,
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

    /// Apply a leader value from a str
    ///
    /// Returns Err if the value is not composed of the correct number
    /// of bytes.
    pub fn set_leader(&mut self, leader: &str) -> Result<(), String> {
        if leader.bytes().len() != LEADER_SIZE {
            return Err(format!("Invalid leader: {leader}"));
        }

        self.leader = leader.to_string();
        Ok(())
    }

    /// Apply a leader value from a str
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

    pub fn get_control_fields(&self, tag: &str) -> Vec<&Controlfield> {
        self.control_fields
            .iter()
            .filter(|f| f.tag.eq(tag))
            .collect()
    }

    pub fn get_fields(&self, tag: &str) -> Vec<&Field> {
        self.fields.iter().filter(|f| f.tag.eq(tag)).collect()
    }

    pub fn get_fields_mut(&mut self, tag: &str) -> Vec<&mut Field> {
        self.fields.iter_mut().filter(|f| f.tag.eq(tag)).collect()
    }

    pub fn add_control_field(&mut self, tag: &str, content: &str) -> Result<(), String> {
        let mut field = Controlfield::new(tag, Some(content))?;

        if tag >= "010" {
            return Err(format!("Invalid control field tag: {tag}"));
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

    pub fn add_data_field(
        &mut self,
        tag: &str,
        ind1: &str,
        ind2: &str,
        subfields: Vec<&str>,
    ) -> Result<(), String> {
        if tag < "010" {
            return Err(format!("Invalid data field tag: {tag}"));
        }

        let mut field = Field::new(tag)?;
        field.set_ind1(ind1)?;
        field.set_ind2(ind2)?;

        let mut sf_op: Option<Subfield> = None;

        for part in subfields {
            if sf_op.is_none() {
                sf_op = Some(Subfield::new(part, None)?);
            } else {
                let mut sf = sf_op.unwrap();
                sf.set_content(part);
                field.subfields.push(sf);
                sf_op = None;
            }
        }

        // Insert the field at the logical position in the record.

        let mut pos = 0;
        for (idx, f) in self.fields.iter().enumerate() {
            pos = idx;
            if f.tag.as_str() > tag {
                break;
            }
        }

        if pos == self.fields.len() {
            self.fields.push(field);
        } else {
            self.fields.insert(pos, field);
        }

        Ok(())
    }

    pub fn get_values(&self, tag: &str, sfcode: &str) -> Vec<&str> {
        let mut vec = Vec::new();
        for field in self.get_fields(tag) {
            for sf in field.get_subfields(sfcode) {
                vec.push(sf.content.as_str());
            }
        }
        vec
    }
}
