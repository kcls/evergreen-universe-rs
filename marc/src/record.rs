use std::fmt;
/// MARC Record and components
///
/// Assumptions:
///
/// Tag, Leader, indicator, and subfield code values are assumed to be
/// ASCII bytes.  If not, and the values are stringified (e.g. writing
/// to breaker, xml, terminal, etc.), the output will be garbled.
///
/// Subfield & Controlfield content strings are not assumed to be
/// anything, except in the utf8 module which treats them as utf8
/// strings.

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
        Tag { value: *value }
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

    /// Stringified Tag.  Assumes ASCII bytes.
    pub fn to_string(&self) -> String {
        format!(
            "{}{}{}",
            self.value[0] as char, self.value[1] as char, self.value[2] as char
        )
    }
}

impl fmt::Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl From<&[u8; TAG_LEN]> for Tag {
    fn from(value: &[u8; TAG_LEN]) -> Tag {
        Tag::new(value)
    }
}

impl From<[u8; TAG_LEN]> for Tag {
    fn from(value: [u8; TAG_LEN]) -> Tag {
        Tag { value }
    }
}

impl From<&Tag> for Tag {
    fn from(t: &Tag) -> Tag {
        t.clone()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Leader {
    value: [u8; LEADER_LEN],
}

impl Leader {
    pub fn new(bytes: [u8; LEADER_LEN]) -> Leader {
        Leader { value: bytes }
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

    /// Stringified leader.  Assumes ASCII bytes.
    pub fn to_string(&self) -> String {
        let mut s = String::new();
        for i in 0..24 {
            s += &format!("{}", self.char_at(i));
        }
        s
    }
}

impl fmt::Display for Leader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Controlfield {
    tag: Tag,
    content: Vec<u8>,
}

impl Controlfield {
    pub fn new<T>(tag: T, content: &[u8]) -> Controlfield
    where
        T: Into<Tag>,
    {
        let tag = tag.into();
        Controlfield {
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

#[derive(Debug, Clone, PartialEq)]
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
    pub fn code_string(&self) -> String {
        format!("{}", self.code_char())
    }
    pub fn content(&self) -> &[u8] {
        self.content.as_slice()
    }
    pub fn set_content(&mut self, content: &[u8]) {
        self.content.clear();
        self.content.extend(content);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    tag: Tag,
    ind1: u8,
    ind2: u8,
    subfields: Vec<Subfield>,
}

impl Field {
    pub fn new<T>(tag: T) -> Field
    where
        T: Into<Tag>,
    {
        let tag = tag.into();
        Field {
            tag,
            ind1: U8_SPACE,
            ind2: U8_SPACE,
            subfields: Vec::new(),
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

    pub fn matching_subfields(&self, code: u8) -> Vec<&Subfield> {
        self.subfields.iter().filter(|s| s.code == code).collect()
    }

    pub fn matching_subfields_mut(&mut self, code: u8) -> Vec<&mut Subfield> {
        self.subfields
            .iter_mut()
            .filter(|s| s.code == code)
            .collect()
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

    /// Remove all occurrences of subfields with the provided code.
    pub fn remove_subfields(&mut self, code: u8) {
        loop {
            if let Some(pos) = self.subfields.iter().position(|sf| sf.code == code) {
                self.subfields.remove(pos);
            } else {
                return;
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    leader: Leader,
    control_fields: Vec<Controlfield>,
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

    pub fn control_fields(&self) -> &[Controlfield] {
        self.control_fields.as_slice()
    }
    pub fn control_fields_mut(&mut self) -> &mut Vec<Controlfield> {
        &mut self.control_fields
    }

    pub fn fields(&self) -> &[Field] {
        self.fields.as_slice()
    }
    pub fn fields_mut(&mut self) -> &mut Vec<Field> {
        &mut self.fields
    }

    pub fn matching_fields<T>(&self, tag: T) -> Vec<&Field>
    where
        T: Into<Tag>,
    {
        let tag = tag.into();
        self.fields.iter().filter(|f| f.tag == tag).collect()
    }

    pub fn matching_fields_mut<T>(&mut self, tag: T) -> Vec<&mut Field>
    where
        T: Into<Tag>,
    {
        let tag = tag.into();
        self.fields.iter_mut().filter(|f| f.tag == tag).collect()
    }

    /// Remove all occurrences of fields with the provided tag.
    pub fn remove_fields<T>(&mut self, tag: T)
    where
        T: Into<Tag>,
    {
        let tag = tag.into();
        loop {
            if let Some(pos) = self.fields.iter().position(|f| f.tag == tag) {
                self.fields.remove(pos);
            } else {
                return;
            }
        }
    }

    /// Insert a new control field in tag order
    pub fn insert_control_field(&mut self, field: Controlfield) {
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

    /// Returns all values for fields with the provided tag and subfield.
    pub fn values<T>(&self, tag: T, sfcode: u8) -> Vec<&[u8]>
    where
        T: Into<Tag>,
    {
        let tag = tag.into();
        let mut values = Vec::new();

        for field in self.fields() {
            if field.tag == tag {
                for sf in field.subfields() {
                    if sf.code() == sfcode {
                        values.push(sf.content());
                    }
                }
            }
        }

        values
    }
}
