//! Base MARC record model and associated components.
const TAG_SIZE: usize = 3;
const LEADER_SIZE: usize = 24;
const CODE_SIZE: usize = 1;
const DEFAULT_LEADER: &str = "                        ";
const DEFAULT_INDICATOR: &str = " ";

/// Verifies the provided string is composed of 'len' number of bytes.
fn check_byte_count(s: &str, len: usize) -> Result<(), String> {
    let byte_len = s.as_bytes().len();
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
    /// let control_field = marctk::Controlfield::new("008", "12345").unwrap();
    /// assert_eq!(control_field.tag(), "008");
    /// ```
    /// ```
    /// let control_field = marctk::Controlfield::new("010", "12345");
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
    /// use marctk::Controlfield;
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
    /// use marctk::Controlfield;
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
    /// use marctk::Controlfield;
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
    /// use marctk::Subfield;
    /// let subfield: Subfield = match Subfield::new("a", "Στη σκιά της πεταλούδας") {
    ///   Ok(sf) => sf,
    ///   Err(e) => panic!("Subfield::new() failed with: {}", e),
    /// };
    /// assert_eq!(subfield.content(), "Στη σκιά της πεταλούδας");
    /// ```
    ///
    /// ```should_panic
    /// use marctk::Subfield;
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
    /// use marctk::Subfield;
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
    /// use marctk::Subfield;
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
    /// use marctk::Subfield;
    /// let mut subfield: Subfield = Subfield::new("a", "potato").unwrap();
    /// subfield.set_code("q");
    /// assert_eq!(subfield.code(), "q");
    /// ```
    ///
    /// ```should_panic
    /// use marctk::Subfield;
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
    /// use marctk::Field;
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

    /// Get the first occurrence of the subfield with the provided code,
    /// if one is present.
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Field;
    ///
    /// let mut field: Field =  Field::new("245").unwrap();
    /// assert!(field.first_subfield("a").is_none());
    ///
    /// field.add_subfield("a", "First one").unwrap();
    /// field.add_subfield("a", "Second one").unwrap();
    ///
    /// assert_eq!(field.first_subfield("a").unwrap().content(), "First one");
    /// ```
    pub fn first_subfield(&self, code: &str) -> Option<&Subfield> {
        self.subfields.iter().find(|f| f.code() == code)
    }

    /// True if a subfield with the provided code is present.
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Field;
    ///
    /// let mut field: Field =  Field::new("245").unwrap();
    /// assert!(!field.has_subfield("a"));
    ///
    /// field.add_subfield("a", "My title").unwrap();
    ///
    /// assert!(field.has_subfield("a"));
    /// ```
    pub fn has_subfield(&self, code: &str) -> bool {
        self.subfields.iter().any(|f| f.code() == code)
    }

    /// Get a mutable list of subfields with the provided code.
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Field;
    ///
    /// let mut field: Field =  Field::new("245").unwrap();
    /// field.add_subfield("a", "First one").unwrap();
    /// field.add_subfield("a", "Second one").unwrap();
    ///
    /// for mut subfield in field.get_subfields_mut("a") {
    ///   subfield.set_content(subfield.content().to_uppercase());
    /// }
    ///
    /// assert_eq!(field.first_subfield("a").unwrap().content(), "FIRST ONE");
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Field;
    ///
    /// let mut field: Field =  Field::new("245").unwrap();
    /// field.add_subfield("a", "First one").unwrap();
    /// field.add_subfield("a", "Second one").unwrap();
    /// assert_eq!(field.subfields().len(), 2);
    ///
    /// assert_eq!(field.remove_first_subfield("a").unwrap().content(), "First one");
    /// assert_eq!(field.subfields().len(), 1);
    /// assert_eq!(field.first_subfield("a").unwrap().content(), "Second one");
    /// ```
    pub fn remove_first_subfield(&mut self, code: &str) -> Option<Subfield> {
        if let Some(index) = self.subfields.iter().position(|s| s.code.eq(code)) {
            return Some(self.subfields.remove(index));
        }

        None
    }

    /// Remove all subfields with the specified code and returns
    /// the count of removed subfields.
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Field;
    /// let mut field = Field::new("505").unwrap();
    /// let _ = field.add_subfield("t", "Chapter 1 /");
    /// let _ = field.add_subfield("r", "Cool author --");
    /// let _ = field.add_subfield("t", "Chapter 2.");
    /// assert_eq!(field.subfields().len(), 3);
    ///
    /// assert_eq!(field.remove_subfields("t"), 2);
    ///
    /// assert_eq!(field.subfields().len(), 1);
    /// ```
    pub fn remove_subfields(&mut self, code: &str) -> usize {
        let mut removed = 0;

        while let Some(index) = self.subfields.iter().position(|s| s.code.eq(code)) {
            self.subfields.remove(index);
            removed += 1;
        }

        removed
    }

    /// # Examples
    ///
    /// ```
    /// use marctk::Field;
    /// let field = Field::new("505").unwrap();
    /// assert!(field.matches_spec("505"));
    /// assert!(field.matches_spec("5xx"));
    /// assert!(field.matches_spec("50x"));
    /// assert!(field.matches_spec("5x5"));
    /// assert!(field.matches_spec("x05"));
    /// assert!(field.matches_spec("5XX"));
    ///
    /// assert!(!field.matches_spec("6xx"));
    /// assert!(!field.matches_spec("LDR"));
    /// assert!(!field.matches_spec("invalid spec"));
    /// ```
    pub fn matches_spec(&self, spec: &str) -> bool {
        if spec.len() != 3 {
            return false;
        };
        spec.chars()
            .zip(self.tag().chars())
            .all(|(spec_char, tag_char)| {
                spec_char.to_ascii_lowercase() == 'x' || spec_char == tag_char
            })
    }
}

/// A MARC record with leader, control fields, and data fields.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    leader: String,
    control_fields: Vec<Controlfield>,
    fields: Vec<Field>,
}

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

    /// Apply a leader value.
    ///
    /// Returns Err if the value is not composed of the correct number
    /// of bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Record;
    /// let mut record = Record::default();
    /// assert!(record.set_leader("too short").is_err());
    /// assert!(record.set_leader("just right              ").is_ok());
    /// ```
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
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Record;
    /// let mut record = Record::default();
    /// assert!(record.set_leader_bytes("too short".as_bytes()).is_err());
    /// assert!(record.set_leader_bytes("just right              ".as_bytes()).is_ok());
    /// ```
    pub fn set_leader_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        let s = std::str::from_utf8(bytes)
            .map_err(|e| format!("Leader is not a valid UTF-8 string: {e} bytes={bytes:?}"))?;
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

    /// Add a new control field with the provided tag and content and
    /// insert it in tag order.
    ///
    /// Controlfields are those with tag 001 .. 009
    ///
    /// Err if the tag is invalid.
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Record;
    /// let mut record = Record::default();
    /// assert!(record.add_control_field("011", "foo").is_err());
    /// assert!(record.add_control_field("002", "bar").is_ok());
    /// assert!(record.add_control_field("001", "bar").is_ok());
    ///
    /// // should be sorted by tag.
    /// assert_eq!(record.control_fields()[0].tag(), "001");
    /// ```
    pub fn add_control_field(&mut self, tag: &str, content: &str) -> Result<(), String> {
        self.insert_control_field(Controlfield::new(tag, content)?);
        Ok(())
    }

    /// Insert a [`Controlfield`] in tag order.
    pub fn insert_control_field(&mut self, field: Controlfield) {
        if let Some(idx) = self
            .control_fields()
            .iter()
            .position(|f| f.tag() > field.tag())
        {
            self.control_fields_mut().insert(idx, field);
        } else {
            self.control_fields_mut().push(field);
        }
    }

    /// Insert a [`Field`] in tag order
    pub fn insert_data_field(&mut self, field: Field) -> usize {
        if let Some(idx) = self.fields().iter().position(|f| f.tag() > field.tag()) {
            self.fields_mut().insert(idx, field);
            idx
        } else {
            self.fields_mut().push(field);
            0
        }
    }

    /// Create a new Field with the provided tag, insert it into the
    /// record in tag order, then return a mut ref to the new field.
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Record;
    /// let mut record = Record::default();
    /// assert!(record.add_data_field("245").is_ok());
    /// assert!(record.add_data_field("240").is_ok());
    /// assert!(record.add_data_field("1234").is_err());
    ///
    /// assert_eq!(record.fields()[0].tag(), "240");
    /// ```
    pub fn add_data_field(&mut self, tag: impl Into<String>) -> Result<&mut Field, String> {
        let pos = self.insert_data_field(Field::new(tag)?);
        Ok(self.fields_mut().get_mut(pos).unwrap())
    }

    /// Returns a list of values for the specified tag and subfield.
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Record;
    /// let mut record = Record::default();
    /// let field = record.add_data_field("650").expect("added field");
    /// field.add_subfield("a", "foo");
    /// field.add_subfield("a", "bar");
    ///
    /// let field = record.add_data_field("650").expect("added field");
    /// field.add_subfield("a", "baz");
    ///
    /// let values = record.get_field_values("650", "a");
    ///
    /// assert_eq!(values.len(), 3);
    /// assert_eq!(values[1], "bar");
    /// ```
    pub fn get_field_values(&self, tag: &str, sfcode: &str) -> Vec<&str> {
        let mut vec = Vec::new();
        for field in self.get_fields(tag) {
            for sf in field.get_subfields(sfcode) {
                vec.push(sf.content.as_str());
            }
        }
        vec
    }

    /// Remove all occurrences of control fields with the provided tag.
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Record;
    /// let mut record = Record::default();
    /// let _ = record.add_control_field("008", "stuffandsuch").unwrap();
    /// let _ = record.add_control_field("008", "morestuffandsuch").unwrap();
    ///
    /// assert_eq!(record.get_control_fields("008").len(), 2);
    ///
    /// record.remove_control_fields("007");
    /// assert_eq!(record.get_control_fields("008").len(), 2);
    ///
    /// record.remove_control_fields("008");
    /// assert!(record.get_fields("008").is_empty());
    /// ```
    pub fn remove_control_fields(&mut self, tag: &str) {
        while let Some(pos) = self.control_fields.iter().position(|f| f.tag() == tag) {
            self.control_fields.remove(pos);
        }
    }

    /// Remove all occurrences of fields with the provided tag.
    ///
    /// # Examples
    ///
    /// ```
    /// use marctk::Record;
    /// let mut record = Record::default();
    /// let field = record.add_data_field("650").unwrap();
    /// field.add_subfield("a", "Art");
    /// field.add_subfield("a", "Science");
    ///
    /// assert_eq!(record.get_fields("650").len(), 1);
    ///
    /// record.remove_fields("200");
    /// assert_eq!(record.get_fields("650").len(), 1);
    ///
    /// record.remove_fields("650");
    /// assert!(record.get_fields("650").is_empty());
    /// ```
    pub fn remove_fields(&mut self, tag: &str) {
        while let Some(pos) = self.fields.iter().position(|f| f.tag() == tag) {
            self.fields.remove(pos);
        }
    }

    /// # Examples
    ///
    /// ```
    /// use marctk::Record;
    /// let record = Record::from_breaker(
    ///     r#"=600 10$aZhang, Heng, $d 78-139 $v Juvenile literature.
    /// =650 \0$aEarthquakes $v Juvenile literature.
    /// =955 \0$a1234"#
    /// ).unwrap();
    ///
    /// let mut some_fields = record.extract_fields(600..=699);
    /// assert_eq!(some_fields.next().unwrap().tag(), "600");
    /// assert_eq!(some_fields.next().unwrap().tag(), "650");
    /// assert!(some_fields.next().is_none());
    ///
    /// let mut more_fields = record.extract_fields("9xx");
    /// assert_eq!(more_fields.next().unwrap().tag(), "955");
    /// assert!(more_fields.next().is_none());
    /// ```
    pub fn extract_fields(
        &self,
        query: impl Into<crate::query::Query>,
    ) -> impl Iterator<Item = &Field> {
        self.fields().iter().filter(query.into().field_filter)
    }
}
