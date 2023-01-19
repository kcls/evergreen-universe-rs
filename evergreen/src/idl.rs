///! IDL Parser
///!
///! Creates an in-memory representation of the IDL file.
///!
///! Parser is wrapped in an Arc<Parser> since it's read-only and
///! practically all areas of EG code need a reference to it.
use json;
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
}

impl Class {
    pub fn pkey(&self) -> Option<&str> {
        self.pkey.as_deref()
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
pub fn wrap(idl: Arc<Parser>, v: json::JsonValue) -> Result<Instance, String> {
    let classname = match v[CLASSNAME_KEY].as_str() {
        Some(c) => c.to_string(),
        None => return Err(format!("JsonValue cannot be blessed into an idl::Instance")),
    };

    Ok(Instance {
        classname,
        idl,
        value: v,
    })
}

pub struct Instance {
    classname: String,
    value: json::JsonValue,
    idl: Arc<Parser>,
}

impl Instance {
    pub fn inner(&self) -> &json::JsonValue {
        &self.value
    }
    pub fn classname(&self) -> &str {
        &self.classname
    }
}

/// Ensures field access fails on unknown IDL class fields.
impl Index<&str> for Instance {
    type Output = json::JsonValue;
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

impl Parser {
    /// Create a ref to a DataSerializer suitable for OpenSRF
    /// data packing and unpacking.
    pub fn as_serializer(idlref: &Arc<Parser>) -> Arc<dyn DataSerializer> {
        idlref.clone()
    }

    pub fn classes(&self) -> &HashMap<String, Class> {
        &self.classes
    }

    pub fn parse_file(filename: &str) -> Result<Arc<Parser>, String> {
        let xml = match fs::read_to_string(filename) {
            Ok(x) => x,
            Err(e) => {
                return Err(format!("Cannot parse IDL file '{filename}': {e}"));
            }
        };

        Parser::parse_string(&xml)
    }

    pub fn parse_string(xml: &str) -> Result<Arc<Parser>, String> {
        let doc = match roxmltree::Document::parse(xml) {
            Ok(d) => d,
            Err(e) => {
                return Err(format!("Error parsing XML string for IDL: {e}"));
            }
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

        let mut class = Class {
            tablename,
            fieldmapper,
            field_safe,
            read_only,
            classname: name.to_string(),
            label: label,
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
            label: label,
            datatype: datatype,
            i18n: i18n,
            array_pos: pos,
            is_virtual: is_virtual,
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
            map: map,
            class: lclass,
            reltype: reltype,
        };

        class.links.insert(link.field.to_string(), link);
    }

    /// Converts an IDL-classed array into a hash whose keys match
    /// the values defined in the IDL for this class.
    ///
    /// Includes a _classname key with the IDL class.
    fn array_to_hash(&self, class: &str, value: &json::JsonValue) -> json::JsonValue {
        let fields = &self.classes.get(class).unwrap().fields;

        let mut hash = json::JsonValue::new_object();

        hash.insert(CLASSNAME_KEY, json::from(class)).ok();

        for (name, field) in fields {
            hash.insert(name, value[field.array_pos].clone()).ok();
        }

        hash
    }

    /// Converts and IDL-classed hash into an IDL-classed array, whose
    /// array positions match the IDL field positions.
    fn hash_to_array(&self, class: &str, hash: &json::JsonValue) -> json::JsonValue {
        let fields = &self.classes.get(class).unwrap().fields;

        // Translate the fields hash into a sorted array
        let mut sorted = fields.values().collect::<Vec<&Field>>();
        sorted.sort_by_key(|f| f.array_pos);

        let mut array = json::JsonValue::new_array();

        for field in sorted {
            array.push(hash[&field.name].clone()).ok();
        }

        array
    }

    /// Returns true if the provided value is shaped like an IDL-blessed
    /// object and has a valid IDL class name.
    pub fn is_idl_object(&self, obj: &json::JsonValue) -> bool {
        if obj.is_object() {
            if let Some(cname) = obj[CLASSNAME_KEY].as_str() {
                if self.classes.get(cname).is_some() {
                    return true;
                }
            }
        }

        false
    }

    pub fn get_pkey_value(&self, obj: &json::JsonValue) -> Option<String> {

        if !self.is_idl_object(obj) {
            return None;
        }

        // these data known good from above is_idl_object check
        let classname = obj[CLASSNAME_KEY].as_str().unwrap();
        let idlclass = self.classes.get(classname).unwrap();

        if let Some(pkey_field) = idlclass.pkey() {
            if obj.has_key(pkey_field) {
                return Some(format!("{}", obj[pkey_field]));
            }
        }

        None
    }
}

impl DataSerializer for Parser {
    /// Creates a clone of the provided JsonValue, replacing any
    /// IDL-classed arrays with classed hashes.
    fn unpack(&self, value: &json::JsonValue) -> json::JsonValue {
        if !value.is_array() && !value.is_object() {
            return value.clone();
        }

        let obj: json::JsonValue;

        if let Some(unpacked) = classified::ClassifiedJson::declassify(value) {
            if unpacked.json().is_array() {
                obj = self.array_to_hash(unpacked.class(), unpacked.json());
            } else {
                panic!("IDL-encoded objects should be arrays");
            }
        } else {
            obj = value.clone();
        }

        if obj.is_array() {
            let mut arr = json::JsonValue::new_array();

            for child in obj.members() {
                arr.push(self.unpack(&child)).ok();
            }

            return arr;
        } else if obj.is_object() {
            let mut hash = json::JsonValue::new_object();

            for (key, val) in obj.entries() {
                hash.insert(key, self.unpack(&val)).ok();
            }

            return hash;
        }

        obj
    }

    /// Creates a clone of the provided JsonValue, replacing any
    /// IDL-classed hashes with IDL-classed arrays.
    fn pack(&self, value: &json::JsonValue) -> json::JsonValue {
        if !value.is_array() && !value.is_object() {
            return value.clone();
        }

        if value.is_object() && value.has_key(CLASSNAME_KEY) {
            let class = value[CLASSNAME_KEY].as_str().unwrap();
            let array = self.hash_to_array(&class, &value);

            let mut new_arr = json::JsonValue::new_array();

            for child in array.members() {
                new_arr.push(self.pack(&child)).ok();
            }

            return classified::ClassifiedJson::classify(&new_arr, &class);
        }

        if value.is_array() {
            let mut arr = json::JsonValue::new_array();

            for child in value.members() {
                arr.push(self.pack(&child)).ok();
            }

            arr
        } else if value.is_object() {
            let mut hash = json::JsonValue::new_object();

            for (key, val) in value.entries() {
                hash.insert(key, self.pack(&val)).ok();
            }

            hash
        } else {
            value.clone() // should not get here
        }
    }
}
