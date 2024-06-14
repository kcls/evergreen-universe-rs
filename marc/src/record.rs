///! Models a MARC record with associated components.
const TAG_SIZE: usize = 3;
const LEADER_SIZE: usize = 24;
const CODE_SIZE: usize = 1;
const DEFAULT_LEADER: &str = "                        ";
const DEFAULT_INDICATOR: &str = " ";

/// Verifies the provided string is composed of 'len' number of bytes.
fn check_byte_count(s: &str, len: usize) -> Result<(), String> {
    let byte_len = s.bytes().len();
    if byte_len != len {
        return Err(format!(
            "Invalid byte count for string s={s} wanted={len} found={byte_len}"
        ));
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
    /// Create a Controlfield with the provided tag and content.
    ///
    /// * `tag` - Must have the correct byte count.
    ///
    /// # Examples
    ///
    /// ```
    /// let control_field = marc::Controlfield::new("008", "12345").unwrap();
    /// assert_eq!(control_field.tag(), "008");
    /// ```
    /// ```
    /// let control_field = marc::Controlfield::new("010", "12345");
    ///
    /// assert_eq!(control_field.is_err(), true);
    /// assert_eq!(control_field.unwrap_err(), "Invalid Controlfield tag: 010");
    /// ```
    pub fn new(tag: impl Into<String>, content: impl Into<String>) -> Result<Self, String> {
        let tag = tag.into();
        check_byte_count(&tag, TAG_SIZE)?;

        if tag.as_str() < "000" || tag.as_str() > "009" {
            return Err(format!("Invalid Controlfield tag: {tag}"));
        }

        Ok(Controlfield {
            tag,
            content: content.into(),
        })
    }

    /// Get the tag
    ///
    /// # Examples
    ///
    /// ```
    /// use marc::Controlfield;
    ///
    /// let control_field = Controlfield::new("008", "12345").unwrap();
    /// assert_eq!(control_field.tag(), "008");
    /// ```
    pub fn tag(&self) -> &str {
        &self.tag
    }

    /// Get the content
    ///
    /// # Examples
    ///
    /// ```
    /// use marc::Controlfield;
    ///
    /// let control_field = Controlfield::new("008", "12345").unwrap();
    /// assert_eq!(control_field.content(), "12345");
    /// ```
    pub fn content(&self) -> &str {
        &self.content
    }

    /// Set the Controlfield content.
    ///
    /// # Examples
    ///
    /// ```
    /// use marc::Controlfield;
    ///
    /// let mut control_field = Controlfield::new("008", "12345").unwrap();
    /// control_field.set_content("6789");
    /// assert_eq!(control_field.content(), "6789");
    /// ```
    pub fn set_content(&mut self, content: impl Into<String>) {
        self.content = content.into();
    }
}

/// A single subfield code + value pair
#[derive(Debug, Clone, PartialEq)]
pub struct Subfield {
    code: String,
    content: String,
}

impl Subfield {
    /// Create a Subfield with the provided code and content.
    ///
    /// * `code` - Must have the correct byte count.
    ///
    /// # Examples
    ///
    /// ```
    /// use marc::Subfield;
    /// let subfield: Subfield = match Subfield::new("a", "Στη σκιά της πεταλούδας") {
    ///   Ok(sf) => sf,
    ///   Err(e) => panic!("Subfield::new() failed with: {}", e),
    /// };
    /// assert_eq!(subfield.content(), "Στη σκιά της πεταλούδας");
    /// ```
    ///
    /// ```should_panic
    /// use marc::Subfield;
    /// Subfield::new("🦋", "Στη σκιά της πεταλούδας").unwrap();
    /// ```
    ///
    pub fn new(code: impl Into<String>, content: impl Into<String>) -> Result<Self, String> {
        let code = code.into();
        check_byte_count(&code, CODE_SIZE)?;
        Ok(Subfield {
            code,
            content: content.into(),
        })
    }
    /// Get the Subfield content.
    pub fn content(&self) -> &str {
        &self.content
    }
    /// Set the Subfield content.
    ///
    /// # Examples
    ///
    /// ```
    /// use marc::Subfield;
    /// let mut subfield: Subfield = Subfield::new("a", "potato").unwrap();
    /// subfield.set_content("cheese");
    /// assert_eq!(subfield.content(), "cheese");
    /// ```
    ///
    pub fn set_content(&mut self, content: impl Into<String>) {
        self.content = content.into();
    }
    /// Get the Subfield code.
    ///
    /// # Examples
    ///
    /// ```
    /// use marc::Subfield;
    /// let subfield: Subfield = Subfield::new("a", "potato").unwrap();
    /// assert_eq!(subfield.code(), "a");
    /// ```
    ///
    pub fn code(&self) -> &str {
        &self.code
    }
    /// Set the Subfield code.
    ///
    /// # Examples
    ///
    /// ```
    /// use marc::Subfield;
    /// let mut subfield: Subfield = Subfield::new("a", "potato").unwrap();
    /// subfield.set_code("q");
    /// assert_eq!(subfield.code(), "q");
    /// ```
    ///
    /// ```should_panic
    /// use marc::Subfield;
    /// let mut subfield: Subfield = Subfield::new("a", "potato").unwrap();
    /// subfield.set_code("🥔").unwrap();
    /// ```
    ///
    pub fn set_code(&mut self, code: impl Into<String>) -> Result<(), String> {
        let code: String = code.into();
        check_byte_count(&code, CODE_SIZE)?;
        self.code = code;
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
    /// Create a Field with the provided tag.
    ///
    /// * `tag` - Must have the correct byte count.
    ///
    /// # Examples
    ///
    /// ```
    /// use marc::record::Field;
    ///
    /// let field: Field = match Field::new("245") {
    ///   Ok(f) => f,
    ///   Err(e) => panic!("Field::new() failed with: {}", e),
    /// };
    /// assert_eq!(field.tag(), "245");
    /// assert_eq!(field.ind1(), " ");
    /// assert_eq!(field.ind2(), " ");
    /// assert_eq!(field.subfields().len(), 0);
    /// ```
    ///
    pub fn new(tag: impl Into<String>) -> Result<Self, String> {
        let tag = tag.into();
        check_byte_count(&tag, TAG_SIZE)?;

        if tag.as_str() < "010" || tag.as_str() > "999" {
            // Of note, OCLC sometimes creates MARC records with data
            // fields using the tag "DAT".  For our purposes, the only
            // thing that really matters is the byte count (checked
            // above), so just warn for unexpected tags.
            eprintln!("Unexpected tag for data field: '{tag}'");
        }

        Ok(Field {
            tag,
            ind1: None,
            ind2: None,
            subfields: Vec::new(),
        })
    }
    /// Get the tag
    pub fn tag(&self) -> &str {
        &self.tag
    }
    /// Get the value of indicator-1, defaulting to DEFAULT_INDICATOR.
    pub fn ind1(&self) -> &str {
        self.ind1.as_deref().unwrap_or(DEFAULT_INDICATOR)
    }
    /// Get the value of indicator-2, defaulting to DEFAULT_INDICATOR.
    pub fn ind2(&self) -> &str {
        self.ind2.as_deref().unwrap_or(DEFAULT_INDICATOR)
    }
    /// Get the full list of subfields
    pub fn subfields(&self) -> &Vec<Subfield> {
        &self.subfields
    }
    /// Get a mutable list of subfields.
    pub fn subfields_mut(&mut self) -> &mut Vec<Subfield> {
        &mut self.subfields
    }

    /// Set the indicator-1 value.
    ///
    /// * `ind` - Must have the correct byte count.
    pub fn set_ind1(&mut self, ind: impl Into<String>) -> Result<(), String> {
        let ind = ind.into();
        check_byte_count(&ind, CODE_SIZE)?;
        self.ind1 = Some(ind);
        Ok(())
    }

    /// Set the indicator-2 value.
    ///
    /// * `ind` - Must have the correct byte count.
    pub fn set_ind2(&mut self, ind: impl Into<String>) -> Result<(), String> {
        let ind = ind.into();
        check_byte_count(&ind, CODE_SIZE)?;
        self.ind2 = Some(ind);
        Ok(())
    }

    /// Get a list of subfields with the provided code.
    pub fn get_subfields(&self, code: &str) -> Vec<&Subfield> {
        self.subfields.iter().filter(|f| f.code() == code).collect()
    }

    pub fn first_subfield(&self, code: &str) -> Option<&Subfield> {
        self.subfields.iter().find(|f| f.code() == code)
    }

    pub fn has_subfield(&self, code: &str) -> bool {
        self.subfields.iter().any(|f| f.code() == code)
    }

    /// Get a mutable list of subfields with the provided code.
    pub fn get_subfields_mut(&mut self, code: &str) -> Vec<&mut Subfield> {
        self.subfields
            .iter_mut()
            .filter(|f| f.code() == code)
            .collect()
    }

    /// Adds a new Subfield to this field using the provided code and content.
    ///
    /// * `code` - Must have the correct byte count.
    pub fn add_subfield(
        &mut self,
        code: impl Into<String>,
        content: impl Into<String>,
    ) -> Result<(), String> {
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

    /// Remove all subfields with the specified code and returns
    /// the count of removed subfields.
    pub fn remove_subfields(&mut self, code: &str) -> usize {
        let mut removed = 0;

        loop {
            if let Some(index) = self.subfields.iter().position(|s| s.code.eq(code)) {
                self.subfields.remove(index);
                removed += 1;
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
impl Default for Record {
    fn default() -> Self {
        Self::new()
    }
}

impl Record {
    /// Create a new Record with a default leader and no content.
    pub fn new() -> Self {
        Record {
            leader: DEFAULT_LEADER.to_string(),
            control_fields: Vec::new(),
            fields: Vec::new(),
        }
    }

    /// Get the leader as a string.
    pub fn leader(&self) -> &str {
        &self.leader
    }

    /// Apply a leader value from a str
    ///
    /// Returns Err if the value is not composed of the correct number
    /// of bytes.
    pub fn set_leader(&mut self, leader: impl Into<String>) -> Result<(), String> {
        let leader = leader.into();
        check_byte_count(&leader, LEADER_SIZE)?;
        self.leader = leader;
        Ok(())
    }

    /// Apply a leader value from a set of bytes
    ///
    /// Returns Err if the value is not composed of the correct number
    /// of bytes.
    pub fn set_leader_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        let s = std::str::from_utf8(bytes).map_err(|e| format!(
                "Leader is not a valid UTF-8 string: {e} bytes={bytes:?}"
            ))?;
        self.set_leader(s)
    }

    /// Get the full list of control fields.
    pub fn control_fields(&self) -> &Vec<Controlfield> {
        &self.control_fields
    }
    /// Get the full list of control fields, mutable.
    pub fn control_fields_mut(&mut self) -> &mut Vec<Controlfield> {
        &mut self.control_fields
    }
    /// Get the full list of fields.
    pub fn fields(&self) -> &Vec<Field> {
        &self.fields
    }
    /// Get the full list of fields, mutable.
    pub fn fields_mut(&mut self) -> &mut Vec<Field> {
        &mut self.fields
    }

    /// Return a list of control fields with the provided tag.
    pub fn get_control_fields(&self, tag: &str) -> Vec<&Controlfield> {
        self.control_fields
            .iter()
            .filter(|f| f.tag() == tag)
            .collect()
    }

    /// Return a list of fields with the provided tag.
    pub fn get_fields(&self, tag: &str) -> Vec<&Field> {
        self.fields.iter().filter(|f| f.tag() == tag).collect()
    }

    /// Return a mutable list of fields with the provided tag.
    pub fn get_fields_mut(&mut self, tag: &str) -> Vec<&mut Field> {
        self.fields.iter_mut().filter(|f| f.tag() == tag).collect()
    }

    /// Add a control field with data.
    ///
    /// Controlfields are those with tag 001 .. 009
    pub fn add_control_field(&mut self, tag: &str, content: &str) -> Result<(), String> {
        if tag >= "010" || tag <= "000" {
            return Err(format!("Invalid control field tag: '{tag}'"));
        }
        self.insert_control_field(Controlfield::new(tag, content)?);
        Ok(())
    }

    /// Insert a control field in tag order
    pub fn insert_control_field(&mut self, field: Controlfield) {
        match self
            .control_fields()
            .iter()
            .position(|f| f.tag() > field.tag())
        {
            Some(idx) => self.control_fields_mut().insert(idx, field),
            None => self.control_fields_mut().push(field),
        }
    }

    /// Insert a data field in tag order
    pub fn insert_field(&mut self, field: Field) -> usize {
        match self.fields().iter().position(|f| f.tag() > field.tag()) {
            Some(idx) => {
                self.fields_mut().insert(idx, field);
                idx
            }
            None => {
                self.fields_mut().push(field);
                0
            }
        }
    }

    /// Create a new Field with the provided tag, insert it into the
    /// record, then return a mut ref so the field may be additionally
    /// modified.
    pub fn add_data_field(&mut self, tag: impl Into<String>) -> Result<&mut Field, String> {
        let pos = self.insert_field(Field::new(tag)?);
        Ok(self.fields_mut().get_mut(pos).unwrap())
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

    /// Remove all occurrences of control fields with the provided tag.
    pub fn remove_control_fields(&mut self, tag: &str) {
        loop {
            if let Some(pos) = self.control_fields.iter().position(|f| f.tag() == tag) {
                self.control_fields.remove(pos);
            } else {
                // No more fields to remove.
                return;
            }
        }
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
