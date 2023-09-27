use crate::result::EgResult;
///! IDL Parser
///!
///! Creates an in-memory representation of the IDL file.
///!
///! Parser is wrapped in an Arc<Parser> since it's read-only and
///! practically all areas of EG code need a reference to it.
use json::JsonValue;
use log::warn;
use opensrf::classified;
use opensrf::client::DataSerializer;
use roxmltree;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::ops::Index;
use std::sync::Arc;

const _OILS_NS_BASE: &str = "http://opensrf.org/spec/IDL/base/v1";
const OILS_NS_OBJ: &str = "http://open-ils.org/spec/opensrf/IDL/objects/v1";
const OILS_NS_PERSIST: &str = "http://open-ils.org/spec/opensrf/IDL/persistence/v1";
const OILS_NS_REPORTER: &str = "http://open-ils.org/spec/opensrf/IDL/reporter/v1";

const AUTO_FIELDS: [&str; 3] = ["isnew", "ischanged", "isdeleted"];

/// Key where IDL class name/hint value is stored on unpacked JSON objects.
/// OpenSRF has its own class key used for storing class names on
/// packed (array-based) JSON objects, which is separate.
pub const CLASSNAME_KEY: &str = "_classname";

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

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    name: String,
    label: String,
    datatype: DataType,
    i18n: bool,
    array_pos: usize,
    is_virtual: bool, // vim at least thinks 'virtual' is reserved
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
}

#[derive(Debug, Clone, PartialEq)]
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
        write!(f, "{}", self)
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

#[derive(Debug, Clone, PartialEq)]
pub struct Class {
    classname: String,
    label: String,
    field_safe: bool,
    read_only: bool,

    /// Name of primary key column
    pkey: Option<String>,

    fieldmapper: Option<String>,
    fields: HashMap<String, Field>,
    links: HashMap<String, Link>,
    tablename: Option<String>,
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

    pub fn has_real_field(&self, field: &str) -> bool {
        self.fields()
            .values()
            .filter(|f| f.name().eq(field) && !f.is_virtual())
            .next()
            .is_some()
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

/// NOTE: experiment
/// Create an Instance wrapper around a JsonValue to enforce
/// IDL field access (and maybe more, we'll see).
pub fn wrap(idl: Arc<Parser>, v: JsonValue) -> EgResult<Instance> {
    let classname = match v[CLASSNAME_KEY].as_str() {
        Some(c) => c.to_string(),
        None => Err(format!("JsonValue cannot be blessed into an idl::Instance"))?,
    };

    Ok(Instance {
        classname,
        idl,
        value: v,
    })
}

pub struct Instance {
    classname: String,
    value: JsonValue,
    idl: Arc<Parser>,
}

impl Instance {
    pub fn inner(&self) -> &JsonValue {
        &self.value
    }
    pub fn classname(&self) -> &str {
        &self.classname
    }
}

/// Ensures field access fails on unknown IDL class fields.
impl Index<&str> for Instance {
    type Output = JsonValue;
    fn index(&self, key: &str) -> &Self::Output {
        if let Some(_) = self
            .idl
            .classes()
            .get(&self.classname)
            .unwrap()
            .fields()
            .get(key)
        {
            &self.value[key]
        } else {
            panic!("IDL class {} has no field {key}", self.classname);
        }
    }
}

pub struct Parser {
    classes: HashMap<String, Class>,
}

impl fmt::Debug for Parser {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IDLParser")
    }
}
impl Parser {
    /// Create a ref to a DataSerializer suitable for OpenSRF
    /// data packing and unpacking.
    pub fn as_serializer(idlref: &Arc<Parser>) -> Arc<dyn DataSerializer> {
        idlref.clone()
    }

    pub fn classes(&self) -> &HashMap<String, Class> {
        &self.classes
    }

    pub fn parse_file(filename: &str) -> EgResult<Arc<Parser>> {
        let xml = match fs::read_to_string(filename) {
            Ok(x) => x,
            Err(e) => Err(format!("Cannot parse IDL file '{filename}': {e}"))?,
        };

        Parser::parse_string(&xml)
    }

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
            fields: HashMap::new(),
            links: HashMap::new(),
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
        }

        self.add_auto_fields(&mut class, field_array_pos);

        self.classes.insert(class.classname.to_string(), class);
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

        let i18n: bool = match node.attribute((OILS_NS_PERSIST, "i18n")) {
            Some(i) => i == "true",
            None => false,
        };

        let is_virtual: bool = match node.attribute((OILS_NS_PERSIST, "virtual")) {
            Some(i) => i == "true",
            None => false,
        };

        let field = Field {
            name: node.attribute("name").unwrap().to_string(),
            label,
            datatype,
            i18n,
            array_pos: pos,
            is_virtual,
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
                warn!("IDL links is missing 'field' attribute");
                return;
            }
        };

        let key = match node.attribute("key") {
            Some(v) => v.to_string(),
            None => {
                warn!("IDL links is missing 'key' attribute");
                return;
            }
        };

        let lclass = match node.attribute("class") {
            Some(v) => v.to_string(),
            None => {
                warn!("IDL links is missing 'class' attribute");
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

    /// Converts an IDL-classed array into a hash whose keys match
    /// the values defined in the IDL for this class, consuming the
    /// array as it goes.
    ///
    /// Includes a _classname key with the IDL class.
    fn array_to_hash(&self, class: &str, mut value: JsonValue) -> JsonValue {
        let fields = &self.classes.get(class).unwrap().fields;

        let mut hash = JsonValue::new_object();

        hash.insert(CLASSNAME_KEY, json::from(class)).unwrap();

        for (name, field) in fields {
            hash.insert(name, value[field.array_pos].take()).unwrap();
        }

        hash
    }

    /// Converts an IDL-classed hash into an IDL-classed array, whose
    /// array positions match the IDL field position, consuming the
    /// hash as it goes.
    fn hash_to_array(&self, class: &str, mut hash: JsonValue) -> JsonValue {
        let fields = &self.classes.get(class).unwrap().fields;

        // Translate the fields hash into a sorted array
        let mut sorted = fields.values().collect::<Vec<&Field>>();
        sorted.sort_by_key(|f| f.array_pos);

        let mut array = JsonValue::new_array();

        for field in sorted {
            array.push(hash[&field.name].take()).unwrap();
        }

        array
    }

    /// Returns true if the provided value is shaped like an IDL-blessed
    /// object and has a valid IDL class name.
    pub fn is_idl_object(&self, obj: &JsonValue) -> bool {
        if obj.is_object() {
            if let Some(cname) = obj[CLASSNAME_KEY].as_str() {
                if self.classes.get(cname).is_some() {
                    return true;
                }
            }
        }

        false
    }

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
                    if let Some(val) = self.get_pkey_value(value) {
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

    pub fn get_class_and_pkey(&self, obj: &JsonValue) -> EgResult<(String, Option<JsonValue>)> {
        let classname = match obj[CLASSNAME_KEY].as_str() {
            Some(c) => c.to_string(),
            None => Err(format!("JsonValue cannot be blessed into an idl::Instance"))?,
        };

        Ok((classname, self.get_pkey_value(obj)))
    }

    pub fn get_pkey_value(&self, obj: &JsonValue) -> Option<JsonValue> {
        self.get_pkey_info(obj).map(|(_, v)| v)
    }

    pub fn get_classname(&self, obj: &JsonValue) -> EgResult<String> {
        match obj[CLASSNAME_KEY].as_str() {
            Some(s) => Ok(s.to_string()),
            None => Err(format!("Not an IDL object: {}", obj.dump()).into()),
        }
    }

    /// Get the primary key field and value from an IDL object if one exists.
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
    /// Field names begging with "_" will not be checked.
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
}

impl DataSerializer for Parser {
    /// Replaces IDL-classed arrays with classed hashes
    fn unpack(&self, value: JsonValue) -> JsonValue {
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
                panic!("IDL-encoded objects should be arrays");
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
            loop {
                let key = match obj.entries().next() {
                    Some((k, _)) => k.to_owned(),
                    None => break,
                };
                hash.insert(&key, self.unpack(obj.remove(&key))).ok();
            }

            return hash;
        }

        obj
    }

    /// Replaces IDL-classed flat hashes with Fieldmapper (array payload)
    /// hashes.
    fn pack(&self, mut value: JsonValue) -> JsonValue {
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

            loop {
                let key = match value.entries().next() {
                    Some((k, _)) => k.to_owned(),
                    None => break,
                };
                hash.insert(&key, self.pack(value.remove(&key))).ok();
            }

            hash
        } else {
            value // should not get here
        }
    }
}

/// Remove the class designation and any auto-fields, resulting in a
/// vanilla hash.
pub fn unbless(hash: &mut JsonValue) {
    hash.remove(CLASSNAME_KEY);
    for field in AUTO_FIELDS {
        hash.remove(field);
    }
}

/// Remove NULL values from JSON objects (hashes) recursively.
///
/// Does not remove NULL Array values, since that would change value
/// positions, but may modify a hash/object which is a member of an
/// array.
pub fn scrub_hash_nulls(mut value: json::JsonValue) -> json::JsonValue {
    if value.is_object() {
        let mut hash = json::JsonValue::new_object();
        loop {
            let key = match value.entries().next() {
                Some((k, _)) => k.to_owned(),
                None => break,
            };

            let scrubbed = scrub_hash_nulls(value.remove(&key));
            if !scrubbed.is_null() {
                hash.insert(&key, scrubbed).unwrap();
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
