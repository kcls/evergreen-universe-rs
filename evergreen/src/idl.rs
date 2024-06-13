//! Creates an in-memory representation of the fieldmapper IDL.
use crate as eg;
use crate::EgResult;
use crate::EgValue;
use roxmltree;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::sync::Arc;
use std::sync::OnceLock;

/// Parse the IDL once and store it here, making it accessible to all
/// threads as a read-only value.
static GLOBAL_IDL: OnceLock<Parser> = OnceLock::new();

const _OILS_NS_BASE: &str = "http://opensrf.org/spec/IDL/base/v1";
const OILS_NS_OBJ: &str = "http://open-ils.org/spec/opensrf/IDL/objects/v1";
const OILS_NS_PERSIST: &str = "http://open-ils.org/spec/opensrf/IDL/persistence/v1";
const OILS_NS_REPORTER: &str = "http://open-ils.org/spec/opensrf/IDL/reporter/v1";
const AUTO_FIELDS: [&str; 3] = ["isnew", "ischanged", "isdeleted"];

/// Returns a ref to the global IDL parser instance
pub fn parser() -> &'static Parser {
    if let Some(idl) = GLOBAL_IDL.get() {
        idl
    } else {
        log::error!("IDL Required");
        panic!("IDL Required")
    }
}

/// Returns a ref to an IDL class by classname.
///
/// Err is returned if no such classes exists.
pub fn get_class(classname: &str) -> EgResult<&Arc<Class>> {
    parser()
        .classes
        .get(classname)
        .ok_or_else(|| format!("No such IDL class: {classname}").into())
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
    /// Store each class in an Arc so it's easier for components
    /// to have an owned ref to the Class, which comes in handy quite
    /// a bit.
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

    /// Load the IDL from a file.
    ///
    /// Returns an Err if the IDL has already been parsed and loaded, in
    /// part to discourage unnecessary reparsing, which is a heavy job.
    pub fn load_file(filename: &str) -> EgResult<()> {
        let xml = match fs::read_to_string(filename) {
            Ok(x) => x,
            Err(e) => Err(format!("Cannot parse IDL file '{filename}': {e}"))?,
        };

        let p = Parser::parse_string(&xml)?;

        if GLOBAL_IDL.set(p).is_err() {
            return Err(format!("Cannot initialize IDL more than once").into());
        }

        Ok(())
    }

    /// Parse the IDL as a string
    fn parse_string(xml: &str) -> EgResult<Parser> {
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

        Ok(parser)
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
                log::warn!(
                    "IDL link for class '{}' has no 'field' attr",
                    class.classname
                );
                return;
            }
        };

        let key = match node.attribute("key") {
            Some(v) => v.to_string(),
            None => {
                log::warn!("IDL link for class '{}' has no 'key' attr", class.classname);
                return;
            }
        };

        let lclass = match node.attribute("class") {
            Some(v) => v.to_string(),
            None => {
                log::warn!(
                    "IDL link for class '{}' has no 'class' attr",
                    class.classname
                );
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

    /// Translates a set of path-based flesh definition into a flesh
    /// object that can be used by cstore, etc.
    ///
    /// E.g. 'jub', ['lineitems.lineitem_details.owning_lib', 'lineitems.lineitem_details.fund']
    ///
    pub fn field_paths_to_flesh(&self, base_class: &str, paths: &[&str]) -> EgResult<EgValue> {
        let mut flesh = eg::hash! {"flesh_fields": {}};
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
                    flesh_fields[cname] = eg::array![];
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

        flesh["flesh"] = EgValue::from(flesh_depth);

        Ok(flesh)
    }

    #[deprecated(note = "See EgValue::create()")]
    pub fn create_from(&self, classname: &str, v: EgValue) -> EgResult<EgValue> {
        EgValue::create(classname, v)
    }

    #[deprecated(note = "See EgValue::is_blessed()")]
    pub fn is_idl_object(&self, v: &EgValue) -> bool {
        v.is_blessed()
    }

    #[deprecated(note = "See EgValue::pkey_value()")]
    pub fn get_pkey_value(&self, v: &EgValue) -> EgResult<EgValue> {
        if let Some(v) = v.pkey_value() {
            Ok(v.clone())
        } else {
            Err(format!("Cannot determine pkey value: {}", v.dump()).into())
        }
    }
}
