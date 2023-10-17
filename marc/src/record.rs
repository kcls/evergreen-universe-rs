pub const U8_ZERO: u8 = '0' as u8;
pub const U8_SPACE: u8 = ' ' as u8;

pub const LEADER_LEN: usize = 24;
pub const TAG_LEN: usize = 3;

#[derive(Debug, Clone, PartialEq)]
pub struct Tag {
    value: [u8; TAG_LEN],
}

impl Tag {
    pub fn new(value: &[u8; TAG_LEN]) -> Tag {
        Tag {
            value: *value
        }
    }
    pub fn value(&self) -> &[u8; TAG_LEN] {
        &self.value
    }
    pub fn is_control_tag(&self) -> bool {
        self.value[0] == U8_ZERO && self.value[1] == U8_ZERO
    }
    pub fn is_data_tag(&self) -> bool {
        self.value[0] > U8_ZERO || self.value[1] > U8_ZERO
    }
}

impl From<&[u8; TAG_LEN]> for Tag {
    fn from(value: &[u8; TAG_LEN]) -> Tag {
        Tag::new(value)
    }
}

/// Translate a byte slice into a Tag.
/// Panics if the slice is the wrong length
impl From<&[u8]> for Tag {
    fn from(value: &[u8]) -> Tag {
        if value.len() != TAG_LEN {
            panic!("Invalid slice for tag: {value:?}");
        }
        let v = [value[0], value[1], value[2]];
        Tag::new(&v)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Leader {
    value: [u8; LEADER_LEN],
}

impl Leader {
    pub fn new(bytes: [u8; LEADER_LEN]) -> Leader {
        Leader {
            value: bytes
        }
    }
    pub fn default() -> Leader {
        Leader {
            value: [U8_SPACE; LEADER_LEN],
        }
    }

    /// Returns the char at the specified zero-based index.
    /// Panics if index exceeds LEADER_LEN.
    pub fn char_at(&self, index: usize) -> char {
        self.value[index] as char
    }

    pub fn value(&self) -> &[u8; LEADER_LEN] {
        &self.value
    }
    pub fn value_mut(&mut self) -> &mut [u8; LEADER_LEN] {
        &mut self.value
    }
    /// Set the leader content from the provided bytes.
    /// If the value is too short, remainging slots are left as-is.
    /// If the value is too long, extra bytes are ignored.
    pub fn set_value(&mut self, value: &[u8]) {
        for (idx, val) in value.iter().enumerate() {
            self.value[idx] = *val;
        }
    }
}

/// Translate a byte slice into a Leader.
/// Panics if the slice is the wrong length
impl From<&[u8]> for Leader {
    fn from(value: &[u8]) -> Leader {
        if value.len() != LEADER_LEN {
            panic!("Invalid slice for leader: {value:?}");
        }
        let mut leader = Leader::default();
        leader.set_value(value);
        leader
    }
}



#[derive(Debug, Clone, PartialEq)]
pub struct ControlField {
    tag: Tag,
    content: Vec<u8>,
}

impl ControlField {
    pub fn new(tag: Tag, content: &[u8]) -> ControlField {
        ControlField {
            tag,
            content: content.to_vec(),
        }
    }
    pub fn tag(&self) -> &Tag {
        &self.tag
    }
    pub fn content(&self) -> &[u8] {
        self.content.as_slice()
    }
    pub fn set_content(&mut self, content: &[u8]) {
        self.content.clear();
        self.content.extend(content);
    }
}

#[derive(Debug)]
pub struct Subfield {
    code: u8,
    content: Vec<u8>,
}

impl Subfield {
    pub fn new(code: u8, content: &[u8]) -> Subfield {
        Subfield {
            code,
            content: content.to_vec(),
        }
    }
    pub fn code(&self) -> u8 {
        self.code
    }
    pub fn code_char(&self) -> char {
        self.code as char
    }
    pub fn content(&self) -> &[u8] {
        self.content.as_slice()
    }
    pub fn set_content(&mut self, content: &[u8]) {
        self.content.clear();
        self.content.extend(content);
    }
}

#[derive(Debug)]
pub struct Field {
    tag: Tag,
    ind1: u8,
    ind2: u8,
    subfields: Vec<Subfield>
}

impl Field {
    pub fn new(tag: Tag) -> Field {
        Field {
            tag,
            ind1: U8_SPACE,
            ind2: U8_SPACE,
            subfields: Vec::new()
        }
    }
    pub fn subfields(&self) -> &[Subfield] {
        self.subfields.as_slice()
    }
    pub fn subfields_mut(&mut self) -> &mut Vec<Subfield> {
        &mut self.subfields
    }
    pub fn tag(&self) -> &Tag {
        &self.tag
    }
    pub fn set_tag(&mut self, tag: Tag) {
        self.tag = tag;
    }
    pub fn ind1(&self) -> u8 {
        self.ind1
    }
    pub fn ind1_char(&self) -> char {
        self.ind1 as char
    }
    pub fn ind2_char(&self) -> char {
        self.ind2 as char
    }
    pub fn set_ind1(&mut self, ind: u8) {
        self.ind1 = ind;
    }
    pub fn ind2(&self) -> u8 {
        self.ind2
    }
    pub fn set_ind2(&mut self, ind: u8) {
        self.ind2 = ind;
    }

    pub fn first_subfield(&self, code: u8) -> Option<&Subfield> {
        self.subfields.iter().filter(|s| s.code == code).next()
    }

    pub fn add_subfield(&mut self, sf: Subfield) {
        self.subfields.push(sf);
    }

    /// Add one or more subfields as a collection of raw data.
    pub fn add_subfield_data(&mut self, subfields: &[(u8, &[u8])]) {
        for (code, content) in subfields {
            self.add_subfield(Subfield::new(*code, content));
        }
    }
}

#[derive(Debug)]
pub struct Record {
    leader: Leader,
    control_fields: Vec<ControlField>,
    fields: Vec<Field>,
}

impl Record {
    pub fn default() -> Record {
        Record {
            leader: Leader::default(),
            control_fields: Vec::new(),
            fields: Vec::new(),
        }
    }
    pub fn leader(&self) -> &Leader {
        &self.leader
    }
    pub fn leader_mut(&mut self) -> &mut Leader {
        &mut self.leader
    }
    pub fn set_leader(&mut self, leader: Leader) {
        self.leader = leader;
    }

    pub fn control_fields(&self) -> &[ControlField] {
        self.control_fields.as_slice()
    }
    pub fn control_fields_mut(&mut self) -> &mut Vec<ControlField> {
        &mut self.control_fields
    }

    pub fn fields(&self) -> &[Field] {
        self.fields.as_slice()
    }
    pub fn fields_mut(&mut self) -> &mut Vec<Field> {
        &mut self.fields
    }

    pub fn first_field(&self, tag: Tag) -> Option<&Field> {
        self.fields.iter().filter(|f| f.tag == tag).next()
    }

    /// Remove the first occurrence of a field with the matching tag
    /// and return the Field.
    pub fn remove_first_field(&mut self, tag: Tag) -> Option<Field> {
        if let Some(pos) = self.fields.iter().position(|f| f.tag == tag) {
            self.fields.remove(pos);
        }
        None
    }

    /// Remove all occurrences of fields with the provided tag and
    /// return the number of fields removed.
    pub fn remove_fields(&mut self, tag: Tag) -> u64 {
        let mut removed = 0;
        loop {
            if let Some(pos) = self.fields.iter().position(|f| f.tag == tag) {
                self.fields.remove(pos);
                removed += 1;
            } else {
                break
            }
        }
        removed
    }

    /// Insert a new control field in tag order
    pub fn insert_control_field(&mut self, field: ControlField) {
        match self.control_fields.iter().position(|f| f.tag == field.tag) {
            Some(idx) => self.control_fields.insert(idx, field),
            None => self.control_fields.push(field),
        };
    }

    /// Insert a new field in tag order
    pub fn insert_field(&mut self, field: Field) {
        match self.fields.iter().position(|f| f.tag == field.tag) {
            Some(idx) => self.fields.insert(idx, field),
            None => self.fields.push(field),
        };
    }
}

