///! Models a MARC record with associated components.
use std::fmt;
const TAG_SIZE: usize = 3;
const LEADER_SIZE: usize = 24;
const SPACE_U8: u8 = 32;
pub const DEFAULT_LEADER: &str = "                        ";

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Tag {
    value: [u8; 3],
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}",
            self.value[0] as char, self.value[1] as char, self.value[2] as char)
    }
}

impl TryFrom<&[u8]> for Tag {
    type Error = String;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != TAG_SIZE {
            return Err(format!("Invalid tag: {value:?}"));
        }
        Ok(Tag {
            value: [
                value[0],
                value[1],
                value[2],
            ]
        })
    }
}

impl TryFrom<&str> for Tag {
    type Error = String;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Tag::try_from(value.as_bytes())
    }
}

impl Tag {
    pub fn is_control_field(&self) -> bool {
        self.value[0] as char == '0' && self.value[1] as char == '0'
    }
    pub fn is_data_field(&self) -> bool {
        self.value[0] as char > '0' || self.value[1] as char > '0'
    }

    pub fn to_string(&self) -> String {
        String::from_utf8_lossy(&self.value).to_string()
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Leader {
    value: [u8; LEADER_SIZE],
}

impl Leader {
    pub fn default() -> Leader {
        Leader {
            value: [' ' as u8; LEADER_SIZE],
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Leader, String> {
        if bytes.len() != LEADER_SIZE {
            return Err(format!("Invalid leader: {:?}", bytes));
        }

        let mut value = [0; LEADER_SIZE];
        for (idx, val) in bytes.iter().enumerate() {
            value[idx] = *val;
        }

        Ok(Leader { value })
    }

    pub fn to_string(&self) -> String {
        String::from_utf8_lossy(&self.value).to_string()
    }
}


/// MARC Control Field whose tag value is < "010"
#[derive(Debug, Clone)]
pub struct Controlfield {
    pub tag: Tag,
    pub content: Vec<u8>,
}

impl Controlfield {
    pub fn new(tag: Tag, content: &[u8]) -> Controlfield {
        Controlfield {
            tag,
            content: content.to_vec()
        }
    }

    pub fn set_content(&mut self, content: &[u8]) {
        self.content = content.to_vec()
    }
}

/// A single subfield code + value pair
#[derive(Debug, Clone)]
pub struct Subfield {
    pub code: u8,
    pub content: Vec<u8>,
}

impl Subfield {
    pub fn new(code: u8, content: &[u8]) -> Subfield {
        Subfield {
            code,
            content: content.to_vec(),
        }
    }

    pub fn set_content(&mut self, content: &[u8]) {
        self.content = content.to_vec()
    }
}

/// A MARC Data Field with tag, indicators, and subfields.
#[derive(Debug, Clone)]
pub struct Field {
    pub tag: Tag,
    pub ind1: u8,
    pub ind2: u8,
    pub subfields: Vec<Subfield>,
}

impl Field {
    pub fn new(tag: Tag) -> Self {
        Field {
            tag,
            ind1: SPACE_U8,
            ind2: SPACE_U8,
            subfields: Vec::new(),
        }
    }

    pub fn set_ind1(&mut self, ind: u8) {
        self.ind1 = ind;
    }

    pub fn set_ind2(&mut self, ind: u8) {
        self.ind2 = ind;
    }

    pub fn get_subfields(&self, code: u8) -> Vec<&Subfield> {
        self.subfields.iter().filter(|f| f.code == code).collect()
    }

    pub fn get_subfields_mut(&mut self, code: u8) -> Vec<&mut Subfield> {
        self.subfields
            .iter_mut()
            .filter(|f| f.code == code)
            .collect()
    }

    pub fn add_subfield(&mut self, code: u8, content: &[u8]) {
        self.subfields.push(Subfield::new(code, content))
    }

    /// Remove the first subfield with the specified code.
    pub fn remove_first_subfield(&mut self, code: u8) -> Option<Subfield> {
        if let Some(index) = self.subfields.iter().position(|s| s.code == code) {
            return Some(self.subfields.remove(index));
        }

        None
    }

    /// Remove all subfields with the specified code and return them.
    pub fn remove_subfields(&mut self, code: u8) -> Vec<Subfield> {
        let mut removed: Vec<Subfield> = Vec::new();

        loop {
            if let Some(index) = self.subfields.iter().position(|s| s.code == code) {
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
    pub leader: Leader,
    pub control_fields: Vec<Controlfield>,
    pub fields: Vec<Field>,
}

/// A MARC record with leader, control fields, and data fields.
impl Record {
    pub fn new() -> Self {
        Record {
            leader: Leader::default(),
            control_fields: Vec::new(),
            fields: Vec::new(),
        }
    }

    pub fn set_leader(&mut self, leader: Leader) {
        self.leader = leader;
    }

    pub fn get_control_fields(&self, tag: &Tag) -> Vec<&Controlfield> {
        self.control_fields
            .iter()
            .filter(|t| &t.tag == tag)
            .collect()
    }

    pub fn get_fields(&self, tag: &Tag) -> Vec<&Field> {
        self.fields.iter().filter(|f| &f.tag == tag).collect()
    }

    pub fn get_fields_mut(&mut self, tag: &Tag) -> Vec<&mut Field> {
        self.fields.iter_mut().filter(|f| &f.tag == tag).collect()
    }

    /// Add a control field with data.
    ///
    /// Controlfields are those with tag 001 .. 009
    pub fn add_control_field(&mut self, tag: Tag, content: &[u8]) -> Result<(), String> {
        if !tag.is_control_field() {
            return Err(format!("Invalid Controlfield tag: {tag}"));
        }

        // Insert the field at the logical position in the record.
        let mut pos = 0;
        for (idx, f) in self.control_fields.iter().enumerate() {
            pos = idx;
            if f.tag > tag {
                break;
            }
        }

        let field = Controlfield::new(tag, content);

        if pos == self.control_fields.len() {
            self.control_fields.push(field);
        } else {
            self.control_fields.insert(pos, field);
        }

        Ok(())
    }

    /// Add a new datafield with the given tag, indicators, and list of
    /// subfields.
    ///
    /// * `subfields` - List of subfield code, followed by subfield value.
    ///     e.g. vec!["a", "My Title", "b", "More Title Stuff"]
    pub fn add_data_field(
        &mut self,
        tag: Tag,
        ind1: u8,
        ind2: u8,
        subfields: &[(u8, &[u8])],
    ) -> Result<(), String> {
        if !tag.is_data_field() {
            return Err(format!("Invalid data field tag: '{tag}'"));
        }

        // Insert the field at the logical position in the record.
        let mut pos = 0;
        for (idx, f) in self.fields.iter().enumerate() {
            pos = idx;
            if f.tag > tag {
                break;
            }
        }

        let mut field = Field::new(tag);
        field.set_ind1(ind1);
        field.set_ind2(ind2);

        for set in subfields {
            // set == (code, value)
            field.subfields.push(Subfield::new(set.0, set.1));
        }

        if pos == self.fields.len() {
            self.fields.push(field);
        } else {
            self.fields.insert(pos, field);
        }

        Ok(())
    }

    /// Returns a list of values for the specified tag and subfield.
    pub fn get_values(&self, tag: &Tag, sfcode: u8) -> Vec<&[u8]> {
        let mut vec = Vec::new();
        for field in self.get_fields(tag) {
            for sf in field.get_subfields(sfcode) {
                vec.push(sf.content.as_slice())
            }
        }
        vec
    }

    /// Remove all occurrences of fields with the provided tag.
    pub fn remove_fields(&mut self, tag: &Tag) {
        loop {
            if let Some(pos) = self.fields.iter().position(|f| &f.tag == tag) {
                self.fields.remove(pos);
            } else {
                return;
            }
        }
    }
}
