///! IDL Parser
///!
///! Creates an in-memory representation of the IDL file.
///!
///! Parser is wrapped in an Arc<Parser> since it's read-only and
///! practically all areas of EG code need a reference to it.
use crate::EgResult;
use json::JsonValue;
//use opensrf::classified;
//use opensrf::client::DataSerializer;
use roxmltree;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::sync::Arc;
use std::cell::RefCell;

// TODO move a lot of the object-specific functions/methods into EgValue.

thread_local! {
    static THREAD_LOCAL_IDL: RefCell<Option<Arc<Parser>>> = RefCell::new(None);
}

const _OILS_NS_BASE: &str = "http://opensrf.org/spec/IDL/base/v1";
const OILS_NS_OBJ: &str = "http://open-ils.org/spec/opensrf/IDL/objects/v1";
const OILS_NS_PERSIST: &str = "http://open-ils.org/spec/opensrf/IDL/persistence/v1";
const OILS_NS_REPORTER: &str = "http://open-ils.org/spec/opensrf/IDL/reporter/v1";
const AUTO_FIELDS: [&str; 3] = ["isnew", "ischanged", "isdeleted"];


/// Every thread needs its own copy of the Arc<Parser>
pub fn set_thread_idl(idl: &Arc<Parser>) {
    THREAD_LOCAL_IDL.with(|p| *p.borrow_mut() = Some(idl.clone()));
}

pub fn get_class(classname: &str) -> Option<Arc<Class>> {
    let mut idl_class: Option<Arc<Class>> = None;

    THREAD_LOCAL_IDL.with(|p| idl_class = p.borrow()
        .as_ref()
        .expect("Thread Local IDL Required")
        .classes()
        .get(classname)
        .map(|c| c.clone()) // Arc::clone()
    );

    idl_class
}



/// Various forms an IDL-classed object can take internally and on
/// the wire.
#[derive(Debug, Clone, PartialEq)]
pub enum DataFormat {
    /// Traditional hash with a class key an array payload.
    Fieldmapper,
    /// IDL objects modeled as key/value pairs in a flat hash, with one
    /// reserved hash key of "_classname" to contain the short IDL class
    /// key.  No NULL values are included.
    Hash,
    /// Same as 'Hash' with NULL values included.  Useful for seeing
    /// all of the key names for an IDL object, regardless of
    /// whether a value is present for every key.
    HashFull,
}

impl From<&str> for DataFormat {
    fn from(s: &str) -> DataFormat {
        match s {
            "hash" => Self::Hash,
            "hashfull" => Self::HashFull,
            _ => Self::Fieldmapper,
        }
    }
}

impl DataFormat {
    pub fn is_hash(&self) -> bool {
        self == &Self::Hash || self == &Self::HashFull
    }
}

/// Key where IDL class name/hint value is stored on unpacked JSON objects.
/// OpenSRF has its own class key used for storing class names on
/// packed (array-based) JSON objects, which is separate.
//pub const CLASSNAME_KEY: &str = "_classname";

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Id,
    Int,
    Float,
    Text,
    Bool,
    Link,
    Money,
    OrgUnit,
    Timestamp,
}

impl DataType {
    pub fn is_numeric(&self) -> bool {
        match *self {
            Self::Id | Self::Int | Self::Float | Self::Money | Self::OrgUnit => true,
            _ => false,
        }
    }
}

impl From<&str> for DataType {
    fn from(s: &str) -> Self {
        match s {
            "id" => Self::Id,
            "int" => Self::Int,
            "float" => Self::Float,
            "text" => Self::Text,
            "bool" => Self::Bool,
            "timestamp" => Self::Timestamp,
            "money" => Self::Money,
            "org_unit" => Self::OrgUnit,
            "link" => Self::Link,
            _ => Self::Text,
        }
    }
}

impl From<&DataType> for &'static str {
    fn from(d: &DataType) -> Self {
        match *d {
            DataType::Id => "id",
            DataType::Int => "int",
            DataType::Float => "float",
            DataType::Text => "text",
            DataType::Bool => "bool",
            DataType::Timestamp => "timestamp",
            DataType::Money => "money",
            DataType::OrgUnit => "org_unit",
            DataType::Link => "link",
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s: &str = self.into();
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    name: String,
    label: String,
    datatype: DataType,
    i18n: bool,
    array_pos: usize,
    is_virtual: bool,
    suppress_controller: Option<String>,
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Field: name={} datatype={} virtual={} label={}",
            self.name, self.datatype, self.is_virtual, self.label
        )
    }
}

impl Field {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn label(&self) -> &str {
        &self.label
    }
    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }
    pub fn i18n(&self) -> bool {
        self.i18n
    }
    pub fn array_pos(&self) -> usize {
        self.array_pos
    }
    pub fn is_virtual(&self) -> bool {
        self.is_virtual
    }
    pub fn suppress_controller(&self) -> Option<&str> {
        self.suppress_controller.as_deref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RelType {
    HasA,
    HasMany,
    MightHave,
    Unset,
}

impl From<&RelType> for &str {
    fn from(rt: &RelType) -> &'static str {
        match *rt {
            RelType::HasA => "has_a",
            RelType::HasMany => "has_many",
            RelType::MightHave => "might_have",
            RelType::Unset => "unset",
        }
    }
}

impl From<&str> for RelType {
    fn from(s: &str) -> Self {
        match s {
            "has_a" => Self::HasA,
            "has_many" => Self::HasMany,
            "might_have" => Self::MightHave,
            _ => Self::Unset,
        }
    }
}

impl fmt::Display for RelType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s: &str = self.into();
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Link {
    field: String,
    reltype: RelType,
    key: String,
    map: Option<String>,
    class: String,
}

impl Link {
    pub fn field(&self) -> &str {
        &self.field
    }
    pub fn reltype(&self) -> RelType {
        self.reltype
    }
    pub fn key(&self) -> &str {
        &self.key
    }
    pub fn map(&self) -> Option<&str> {
        if let Some(map) = self.map.as_ref() {
            if map == "" || map == " " {
                None
            } else {
                self.map.as_deref()
            }
        } else {
            None
        }
    }
    pub fn class(&self) -> &str {
        &self.class
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Class {
    classname: String,
    label: String,
    field_safe: bool,
    read_only: bool,

    /// Name of primary key column
    pkey: Option<String>,

    /// Name of the column to use for the human label value
    selector: Option<String>,

    fieldmapper: Option<String>,
    fields: HashMap<String, Field>,
    links: HashMap<String, Link>,
    tablename: Option<String>,
    source_definition: Option<String>,
    controller: Option<String>,
    is_virtual: bool,
}

impl Class {
    pub fn pkey(&self) -> Option<&str> {
        self.pkey.as_deref()
    }
    pub fn pkey_field(&self) -> Option<&Field> {
        if let Some(pk) = self.pkey() {
            self.fields().values().filter(|f| f.name().eq(pk)).next()
        } else {
            None
        }
    }

    pub fn selector(&self) -> Option<&str> {
        self.selector.as_deref()
    }

    pub fn source_definition(&self) -> Option<&str> {
        self.source_definition.as_deref()
    }

    pub fn classname(&self) -> &str {
        &self.classname
    }
    pub fn label(&self) -> &str {
        &self.label
    }
    pub fn fields(&self) -> &HashMap<String, Field> {
        &self.fields
    }
    pub fn fieldmapper(&self) -> Option<&str> {
        self.fieldmapper.as_deref()
    }
    pub fn links(&self) -> &HashMap<String, Link> {
        &self.links
    }
    pub fn tablename(&self) -> Option<&str> {
        self.tablename.as_deref()
    }

    pub fn get_field(&self, name: &str) -> Option<&Field> {
        self.fields.get(name)
    }

    pub fn controller(&self) -> Option<&str> {
        self.controller.as_deref()
    }
    pub fn is_virtual(&self) -> bool {
        self.is_virtual
    }

    /// Vec of non-virutal fields.
    pub fn real_fields(&self) -> Vec<&Field> {
        let mut fields: Vec<&Field> = Vec::new();
        for (_, field) in self.fields().into_iter() {
            if !field.is_virtual() {
                fields.push(field);
            }
        }
        fields
    }

    /// Vec of non-virutal fields sorted by name.
    pub fn real_fields_sorted(&self) -> Vec<&Field> {
        let mut fields = self.real_fields();
        fields.sort_by(|a, b| a.name().cmp(b.name()));
        fields
    }

    /// Vec of non-virutal field names sorted alphabetically.
    pub fn real_field_names_sorted(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.real_fields().iter().map(|f| f.name()).collect();
        names.sort();
        names
    }

    /// Vec of all field names, unsorted.
    pub fn field_names(&self) -> Vec<&str> {
        self.fields().keys().map(|f| f.as_str()).collect()
    }

    pub fn has_real_field(&self, field: &str) -> bool {
        self.fields()
            .values()
            .filter(|f| f.name().eq(field) && !f.is_virtual())
            .next()
            .is_some()
    }

    pub fn has_field(&self, field: &str) -> bool {
        self.fields().get(field).is_some()
    }

    pub fn get_real_field(&self, field: &str) -> Option<&Field> {
        self.fields()
            .values()
            .filter(|f| f.name().eq(field) && !f.is_virtual())
            .next()
    }
}

impl fmt::Display for Class {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Class: class={} fields={} links={} label={} ",
            self.classname,
            self.fields.len(),
            self.links.len(),
            self.label
        )
    }
}

pub struct Parser {
    classes: HashMap<String, Arc<Class>>,
}

impl fmt::Debug for Parser {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IDLParser")
    }
}
impl Parser {
    /// All of our IDL classes keyed on classname/hint (e.g. "aou")
    pub fn classes(&self) -> &HashMap<String, Arc<Class>> {
        &self.classes
    }

    /// Parse the IDL from a file
    pub fn parse_file(filename: &str) -> EgResult<Arc<Parser>> {
        let xml = match fs::read_to_string(filename) {
            Ok(x) => x,
            Err(e) => Err(format!("Cannot parse IDL file '{filename}': {e}"))?,
        };

        Parser::parse_string(&xml)
    }

    /// Parse the IDL as a string
    pub fn parse_string(xml: &str) -> EgResult<Arc<Parser>> {
        let doc = match roxmltree::Document::parse(xml) {
            Ok(d) => d,
            Err(e) => Err(format!("Error parsing XML string for IDL: {e}"))?,
        };

        let mut parser = Parser {
            classes: HashMap::new(),
        };

        for root_node in doc.root().children() {
            if root_node.tag_name().name() == "IDL" {
                for class_node in root_node.children() {
                    if class_node.node_type() == roxmltree::NodeType::Element
                        && class_node.tag_name().name() == "class"
                    {
                        parser.add_class(&class_node);
                    }
                }
            }
        }

        Ok(Arc::new(parser))
    }

    fn add_class(&mut self, node: &roxmltree::Node) {
        let name = node.attribute("id").unwrap(); // required

        let label = match node.attribute((OILS_NS_REPORTER, "label")) {
            Some(l) => l.to_string(),
            None => name.to_string(),
        };

        let tablename = match node.attribute((OILS_NS_PERSIST, "tablename")) {
            Some(v) => Some(v.to_string()),
            None => None,
        };

        let fieldmapper = match node.attribute((OILS_NS_OBJ, "fieldmapper")) {
            Some(v) => Some(v.to_string()),
            None => None,
        };

        let field_safe = match node.attribute((OILS_NS_PERSIST, "field_safe")) {
            Some(v) => v.to_lowercase().eq("true"),
            None => false,
        };

        let read_only = match node.attribute((OILS_NS_PERSIST, "readonly")) {
            Some(v) => v.to_lowercase().eq("true"),
            None => false,
        };

        let controller = match node.attribute("controller") {
            Some(v) => Some(v.to_string()),
            None => None,
        };

        let is_virtual: bool = match node.attribute((OILS_NS_PERSIST, "virtual")) {
            Some(i) => i == "true",
            None => false,
        };

        let mut class = Class {
            tablename,
            fieldmapper,
            field_safe,
            read_only,
            controller,
            classname: name.to_string(),
            label: label,
            is_virtual,
            source_definition: None,
            fields: HashMap::new(),
            links: HashMap::new(),
            selector: None,
            pkey: None,
        };

        let mut field_array_pos = 0;

        for child in node
            .children()
            .filter(|n| n.node_type() == roxmltree::NodeType::Element)
        {
            if child.tag_name().name() == "fields" {
                class.pkey = match child.attribute((OILS_NS_PERSIST, "primary")) {
                    Some(v) => Some(v.to_string()),
                    None => None,
                };

                for field_node in child
                    .children()
                    .filter(|n| n.node_type() == roxmltree::NodeType::Element)
                    .filter(|n| n.tag_name().name() == "field")
                {
                    self.add_field(&mut class, field_array_pos, &field_node);
                    field_array_pos += 1;
                }
            } else if child.tag_name().name() == "links" {
                for link_node in child
                    .children()
                    .filter(|n| n.node_type() == roxmltree::NodeType::Element)
                    .filter(|n| n.tag_name().name() == "link")
                {
                    self.add_link(&mut class, &link_node);
                }
            }

            if child.tag_name().name() == "source_definition" {
                class.source_definition = child.text().map(|t| t.to_string());
            }
        }

        self.add_auto_fields(&mut class, field_array_pos);

        //self.classes.insert(class.classname.to_string(), class.clone());
        self.classes
            .insert(class.classname.to_string(), Arc::new(class));
    }

    fn add_auto_fields(&self, class: &mut Class, mut pos: usize) {
        for field in AUTO_FIELDS {
            class.fields.insert(
                field.to_string(),
                Field {
                    name: field.to_string(),
                    label: field.to_string(),
                    datatype: DataType::Bool,
                    i18n: false,
                    array_pos: pos,
                    is_virtual: true,
                    suppress_controller: None,
                },
            );

            pos += 1;
        }
    }

    fn add_field(&self, class: &mut Class, pos: usize, node: &roxmltree::Node) {
        let label = match node.attribute((OILS_NS_REPORTER, "label")) {
            Some(l) => l.to_string(),
            None => "".to_string(),
        };

        let datatype: DataType = match node.attribute((OILS_NS_REPORTER, "datatype")) {
            Some(dt) => dt.into(),
            None => DataType::Text,
        };

        if let Some(selector) = node.attribute((OILS_NS_REPORTER, "selector")) {
            class.selector = Some(selector.to_string());
        };

        let i18n: bool = match node.attribute((OILS_NS_PERSIST, "i18n")) {
            Some(i) => i == "true",
            None => false,
        };

        let is_virtual: bool = match node.attribute((OILS_NS_PERSIST, "virtual")) {
            Some(i) => i == "true",
            None => false,
        };

        let suppress_controller = node
            .attribute((OILS_NS_PERSIST, "suppress_controller"))
            .map(|c| c.to_string());

        let field = Field {
            name: node.attribute("name").unwrap().to_string(),
            label,
            datatype,
            i18n,
            array_pos: pos,
            is_virtual,
            suppress_controller,
        };

        class.fields.insert(field.name.to_string(), field);
    }

    fn add_link(&self, class: &mut Class, node: &roxmltree::Node) {
        let reltype: RelType = match node.attribute("reltype") {
            Some(rt) => rt.into(),
            None => RelType::Unset,
        };

        let map = match node.attribute("map") {
            Some(s) => Some(s.to_string()),
            None => None,
        };

        let field = match node.attribute("field") {
            Some(v) => v.to_string(),
            None => {
                log::warn!("IDL links is missing 'field' attribute");
                return;
            }
        };

        let key = match node.attribute("key") {
            Some(v) => v.to_string(),
            None => {
                log::warn!("IDL links is missing 'key' attribute");
                return;
            }
        };

        let lclass = match node.attribute("class") {
            Some(v) => v.to_string(),
            None => {
                log::warn!("IDL links is missing 'class' attribute");
                return;
            }
        };

        let link = Link {
            field,
            key,
            map,
            reltype,
            class: lclass,
        };

        class.links.insert(link.field.to_string(), link);
    }

    /*
    /// Replace Object or Array values on an IDL object with the
    /// scalar primary key value of the linked object (real fields)
    /// or null (virtual fields).
    pub fn de_flesh_object(&self, obj: &mut JsonValue) -> EgResult<()> {
        let cname = obj[CLASSNAME_KEY]
            .as_str()
            .ok_or_else(|| format!("Not an IDL object: {}", obj.dump()))?;

        let idl_class = self
            .classes
            .get(cname)
            .ok_or_else(|| format!("Not an IDL class: {cname}"))?;

        for (name, field) in idl_class.fields().iter() {
            let value = &obj[name];
            if value.is_object() || value.is_array() {
                if field.is_virtual() {
                    // Virtual fields can be fully cleared.
                    obj[name] = JsonValue::Null;
                } else {
                    if let Ok(val) = self.get_pkey_value(value) {
                        // Replace fleshed real fields with their pkey.
                        obj[name] = val;
                    } else {
                        // This is a real IDL field fleshed with an object
                        // that does not have a primary key value.
                        Err(format!(
                            "Cannot de-flesh.
                            Linked object has no primary key: {}",
                            value.dump()
                        ))?;
                    }
                }
            }
        }

        Ok(())
    }

    /// Returns the value at the specified key OR the primary key value
    /// of the object fleshed at the specified key.
    ///
    /// Return value may be JsonValue::Null;
    pub fn de_flesh_value(&self, value: &JsonValue) -> EgResult<JsonValue> {
        if value.is_object() {
            self.get_pkey_value(value)
        } else if value.is_array() {
            Err(format!("Cannot de_flesh_value an array").into())
        } else {
            Ok(value.clone())
        }
    }

    pub fn get_class_and_pkey(&self, obj: &JsonValue) -> EgResult<(String, JsonValue)> {
        let classname = match obj[CLASSNAME_KEY].as_str() {
            Some(c) => c.to_string(),
            None => Err(format!("JsonValue cannot be blessed into an idl::EgValue"))?,
        };

        Ok((classname, self.get_pkey_value(obj)?))
    }

    /// Returns the primary key value for an IDL object, which may
    /// be JsonValue::Null if no value is present.
    ///
    /// Returns Err of the object is not an IDL object or the IDL class
    /// in question has no primary key field.
    pub fn get_pkey_value(&self, obj: &JsonValue) -> EgResult<JsonValue> {
        if !self.is_idl_object(obj) {
            return Err(format!("Not an IDL object: {}", obj.dump()).into());
        }

        // these data known good from above is_idl_object check
        let classname = obj[CLASSNAME_KEY].as_str().unwrap();
        let idlclass = self.classes.get(classname).unwrap();

        if let Some(pkey_field) = idlclass.pkey_field() {
            return Ok(obj[pkey_field.name()].clone());
        } else {
            return Err(format!("IDL class {classname} has no primary key field").into());
        }
    }

    pub fn get_classname(&self, obj: &JsonValue) -> EgResult<String> {
        match obj[CLASSNAME_KEY].as_str() {
            Some(s) => Ok(s.to_string()),
            None => Err(format!("Not an IDL object: {}", obj.dump()).into()),
        }
    }

    /// Get the primary key field and value from an IDL object if one exists.
    /// Note the pkey value may be JSON NULL.
    pub fn get_pkey_info(&self, obj: &JsonValue) -> Option<(&Field, JsonValue)> {
        if !self.is_idl_object(obj) {
            return None;
        }

        // these data known good from above is_idl_object check
        let classname = obj[CLASSNAME_KEY].as_str().unwrap();
        let idlclass = self.classes.get(classname).unwrap();

        if let Some(pkey_field) = idlclass.pkey_field() {
            return Some((pkey_field, obj[pkey_field.name()].clone()));
        }

        None
    }

    /// Create the seed of an IDL object with the requested class.
    pub fn create(&self, classname: &str) -> EgResult<JsonValue> {
        if !self.classes.contains_key(classname) {
            Err(format!("Invalid IDL class: {classname}"))?;
        }

        let mut obj = JsonValue::new_object();
        obj[CLASSNAME_KEY] = json::from(classname);

        Ok(obj)
    }

    /// Stamp the object with the requested class and confirm any
    /// existing fields are valid for the class.
    pub fn create_from(&self, classname: &str, mut obj: JsonValue) -> EgResult<JsonValue> {
        if !obj.is_object() {
            Err(format!("IDL cannot create_from() on a non-object"))?;
        }

        let _ = self
            .classes
            .get(classname)
            .ok_or_else(|| format!("IDL no such class {classname}"))?;

        /*
        // There may be cases where we want to attach misc. fields
        // to an IDL object.  We could provide an option to this
        // method to scrub unknown fields...shrug.
        for (field, _) in obj.entries() {
            if !idlclass.fields().contains_key(field) {
                Err(format!("IDL class {classname} has no field {field}"))?;
            }
        }
        */

        obj[CLASSNAME_KEY] = json::from(classname);

        Ok(obj)
    }

    /// Verify a JSON object is a properly-formatted IDL object with no
    /// misspelled field names.
    ///
    /// Field names beginning with "_" will not be checked.
    pub fn verify_object(&self, obj: &JsonValue) -> EgResult<()> {
        if !obj.is_object() {
            return Err(format!("IDL value is not an object: {}", obj.dump()).into());
        }

        let cname = match obj[CLASSNAME_KEY].as_str() {
            Some(c) => c,
            None => return Err(format!("IDL object has no class name: {}", obj.dump()).into()),
        };

        let idl_class = match self.classes.get(cname) {
            Some(c) => c,
            None => return Err(format!("No such IDL class: {cname}").into()),
        };

        for (key, _) in obj.entries() {
            // Ignore keys that start with _ as that's a supported method
            // for attaching values to IDL objects with ad-hoc keys.
            if !key.starts_with("_") {
                if !idl_class.fields.contains_key(key) {
                    return Err(format!("IDL class {cname} has no such field: {key}").into());
                }
            }
        }

        Ok(())
    }

    /// Translate and IDL-classed flat hash into an array-based
    /// Fieldmapper object, recursively.
    pub fn encode(&self, mut value: JsonValue) -> JsonValue {
        if !value.is_array() && !value.is_object() {
            return value;
        }

        if self.is_idl_object(&value) {
            // Extract the class -- hash_to_array does not need the
            // translated object to have the class key (hence the
            // 'class' param requirement).
            let class_json = value[CLASSNAME_KEY].take();
            let class = class_json.as_str().unwrap();
            let mut array = self.hash_to_array(&class, value);

            let mut new_arr = JsonValue::new_array();
            while array.len() > 0 {
                new_arr.push(self.pack(array.array_remove(0))).ok();
            }

            return classified::ClassifiedJson::classify(new_arr, &class);
        }

        if value.is_array() {
            let mut arr = JsonValue::new_array();
            while value.len() > 0 {
                arr.push(self.pack(value.array_remove(0))).ok();
            }

            arr
        } else if value.is_object() {
            let mut hash = JsonValue::new_object();

            for (k, v) in value.entries_mut() {
                hash.insert(k, self.pack(v.take())).ok();
            }

            hash
        } else {
            value // should not get here
        }
    }

    /// Translate an array-based Fieldmapper object into an IDL-classed
    /// flat hash object, recursively.
    pub fn decode(&self, value: JsonValue) -> JsonValue {
        if !value.is_array() && !value.is_object() {
            return value;
        }

        let mut obj: JsonValue;

        if classified::ClassifiedJson::can_declassify(&value) {
            let mut unpacked = classified::ClassifiedJson::declassify(value).unwrap();
            let json_arr = unpacked.take_json();
            if json_arr.is_array() {
                obj = self.array_to_hash(unpacked.class(), json_arr);
            } else {
                // Occasionally we're asked to decode classed hashes
                // which are part of the OpenSRF messaging for certain
                // internal tasks, e.g. method introspection.
                obj = json_arr;
            }
        } else {
            obj = value;
        }

        if obj.is_array() {
            let mut arr = JsonValue::new_array();
            while obj.len() > 0 {
                arr.push(self.unpack(obj.array_remove(0))).unwrap();
            }

            return arr;
        } else if obj.is_object() {
            let mut hash = JsonValue::new_object();
            for (k, v) in obj.entries_mut() {
                hash.insert(k, self.unpack(v.take())).ok();
            }
            return hash;
        }

        obj
    }
    */

    /// Translates a set of path-based flesh definition into a flesh
    /// object that can be used by cstore, etc.
    ///
    /// E.g. 'jub', ['lineitems.lineitem_details.owning_lib', 'lineitems.lineitem_details.fund']
    ///
    pub fn field_paths_to_flesh(&self, base_class: &str, paths: &[&str]) -> EgResult<JsonValue> {
        let mut flesh = json::object! {"flesh_fields": {}};
        let mut flesh_depth = 1;

        let base_idl_class = self
            .classes()
            .get(base_class)
            .ok_or_else(|| format!("No such IDL class: {base_class}"))?;

        for path in paths {
            let mut idl_class = base_idl_class;

            for (idx, fieldname) in path.split(".").enumerate() {
                let cname = idl_class.classname();

                let link_field = idl_class
                    .links()
                    .get(fieldname)
                    .ok_or_else(|| format!("Class '{cname}' cannot flesh '{fieldname}'"))?;

                let flesh_fields = &mut flesh["flesh_fields"];

                if flesh_fields[cname].is_null() {
                    flesh_fields[cname] = json::array![];
                }

                if !flesh_fields[cname].contains(fieldname) {
                    flesh_fields[cname].push(fieldname).expect("Is Array");
                }

                if flesh_depth < idx + 1 {
                    flesh_depth = idx + 1;
                }

                idl_class = self
                    .classes()
                    .get(link_field.class())
                    .ok_or_else(|| format!("No such IDL class: {}", link_field.class()))?;
            }
        }

        flesh["flesh"] = json::from(flesh_depth);

        Ok(flesh)
    }
}

/*
impl DataSerializer for Parser {
    /// Replaces IDL-classed arrays with classed hashes
    fn unpack(&self, value: JsonValue) -> JsonValue {
        self.decode(value)
    }

    /// Replaces IDL-classed flat hashes with Fieldmapper (array payload)
    /// hashes.
    fn pack(&self, value: JsonValue) -> JsonValue {
        self.encode(value)
    }
}
*/

/// Remove the class designation and any auto-fields, resulting in a
/// vanilla hash.
/*
pub fn unbless(hash: &mut JsonValue) {
    hash.remove(CLASSNAME_KEY);
    for field in AUTO_FIELDS {
        hash.remove(field);
    }
}
*/

/// Remove NULL values from JSON objects (hashes) recursively.
///
/// Does not remove NULL Array values, since that would change value
/// positions, but may modify a hash/object which is a member of an
/// array.
pub fn scrub_hash_nulls(mut value: json::JsonValue) -> json::JsonValue {
    if value.is_object() {
        let mut hash = json::JsonValue::new_object();
        for (k, v) in value.entries_mut() {
            let scrubbed = scrub_hash_nulls(v.take());
            if !scrubbed.is_null() {
                hash.insert(&k, scrubbed).ok();
            }
        }

        hash
    } else if let json::JsonValue::Array(mut list) = value {
        let mut arr = json::JsonValue::new_array();

        for val in list.drain(..) {
            let scrubbed = scrub_hash_nulls(val);
            arr.push(scrubbed).unwrap();
        }

        arr
    } else {
        value
    }
}
