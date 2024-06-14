use std::fs::File;
use std::io::BufReader;
use std::io::Cursor;
use xml::attribute::OwnedAttribute;
use xml::reader::{EventReader, XmlEvent};

use super::Controlfield;
use super::Field;
use super::Record;
use super::Subfield;

const MARCXML_NAMESPACE: &str = "http://www.loc.gov/MARC21/slim";
const MARCXML_XSI_NAMESPACE: &str = "http://www.w3.org/2001/XMLSchema-instance";
const MARCXML_SCHEMA_LOCATION: &str =
    "http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd";

/// Replace non-ASCII characters and special characters with escaped
/// XML entities.
///
/// * is_attr - If true, also escape single and double quotes.
///
/// ```
/// use marc::xml;
/// assert_eq!(xml::escape_xml("<'É'>", false).as_str(), "&lt;'&#xC9;'&gt;");
/// assert_eq!(xml::escape_xml("<'É'>", true).as_str(), "&lt;&apos;&#xC9;&apos;&gt;");
/// ```
pub fn escape_xml(value: &str, is_attr: bool) -> String {
    let mut buf = String::new();
    for c in value.chars() {
        if c == '&' {
            buf.push_str("&amp;");
        } else if c == '\'' && is_attr {
            buf.push_str("&apos;");
        } else if c == '"' && is_attr {
            buf.push_str("&quot;");
        } else if c == '>' {
            buf.push_str("&gt;");
        } else if c == '<' {
            buf.push_str("&lt;");
        } else if c > '~' {
            let ord: u32 = c.into();
            buf.push_str(format!("&#x{ord:X};").as_str());
        } else {
            buf.push(c);
        }
    }

    buf
}

fn format(formatted: bool, value: &mut String, depth: u8) {
    if formatted {
        value.push('\n');
        for _ in 0..depth {
            value.push(' ');
        }
    }
}

pub struct XmlOptions {
    pub formatted: bool,
    pub with_xml_declaration: bool,
}

struct XmlParseContext {
    record: Record,
    in_cfield: bool,
    in_subfield: bool,
    in_leader: bool,
    record_complete: bool,
    doc_complete: bool,
}

pub enum XmlRecordIterator {
    FileReader(EventReader<BufReader<File>>),
    ByteReader(EventReader<Cursor<Vec<u8>>>),
}

impl Iterator for XmlRecordIterator {
    type Item = Result<Record, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut context = XmlParseContext {
            record: Record::new(),
            in_cfield: false,
            in_subfield: false,
            in_leader: false,
            record_complete: false,
            doc_complete: false,
        };

        self.read_next(&mut context).transpose()
    }
}

impl XmlRecordIterator {
    pub fn from_file(filename: &str) -> Result<Self, String> {
        match File::open(filename) {
            Ok(file) => Ok(XmlRecordIterator::FileReader(EventReader::new(
                BufReader::new(file),
            ))),
            Err(e) => Err(format!("Cannot read MARCXML file: {filename} {e}")),
        }
    }

    pub fn from_string(xml: &str) -> Self {
        XmlRecordIterator::ByteReader(EventReader::new(Cursor::new(xml.as_bytes().to_vec())))
    }

    /// Pull the next Record from the data source.
    fn read_next(&mut self, context: &mut XmlParseContext) -> Result<Option<Record>, String> {
        loop {
            let evt_res = match *self {
                XmlRecordIterator::FileReader(ref mut reader) => reader.next(),
                XmlRecordIterator::ByteReader(ref mut reader) => reader.next(),
            };

            let evt = evt_res.map_err(|e| format!("Error processing XML: {e}"))?;

            if let Err(e) = self.handle_xml_event(context, evt) {
                return Err(format!("Error processing XML: {e}"));
            }

            if context.record_complete {
                // Return the compiled record and replace it with a new one.
                return Ok(Some(std::mem::replace(&mut context.record, Record::new())));
            } else if context.doc_complete {
                // If we had a doc in progress, discard it.
                context.record = Record::new();

                // All done.  Get outta here.
                return Ok(None);
            }
        }
    }

    /// Process a single XML read event
    fn handle_xml_event(
        &mut self,
        context: &mut XmlParseContext,
        evt: XmlEvent,
    ) -> Result<(), String> {
        let record = &mut context.record;

        match evt {
            XmlEvent::StartElement {
                name, attributes, ..
            } => {
                self.handle_start_element(context, name.local_name.as_str(), &attributes)?;
            }

            XmlEvent::Characters(ref characters) => {
                if context.in_leader {
                    record.set_leader(characters)?;
                    context.in_leader = false;
                } else if context.in_cfield {
                    if let Some(cf) = record.control_fields_mut().last_mut() {
                        cf.set_content(characters);
                    }
                    context.in_cfield = false;
                } else if context.in_subfield {
                    if let Some(field) = record.fields_mut().last_mut() {
                        if let Some(subfield) = field.subfields_mut().last_mut() {
                            subfield.set_content(characters);
                        }
                    }
                    context.in_subfield = false;
                }
            }

            XmlEvent::EndElement { name, .. } => match name.local_name.as_str() {
                "record" => context.record_complete = true,
                _ => {}
            },

            XmlEvent::EndDocument => {
                context.doc_complete = true;
            }

            _ => {}
        }

        Ok(())
    }

    fn handle_start_element(
        &mut self,
        context: &mut XmlParseContext,
        name: &str,
        attributes: &Vec<OwnedAttribute>,
    ) -> Result<(), String> {
        let record = &mut context.record;

        match name {
            "leader" => context.in_leader = true,

            "controlfield" => {
                if let Some(t) = attributes
                    .iter().find(|a| a.name.local_name.eq("tag"))
                {
                    record
                        .control_fields_mut()
                        .push(Controlfield::new(&t.value, "")?);
                    context.in_cfield = true;
                } else {
                    return Err("Controlfield has no tag".to_string());
                }
            }

            "datafield" => {
                let mut field = match attributes
                    .iter().find(|a| a.name.local_name.eq("tag"))
                {
                    Some(attr) => Field::new(&attr.value)?,
                    None => {
                        return Err("Data field has no tag".to_string());
                    }
                };

                for attr in attributes {
                    match attr.name.local_name.as_str() {
                        "ind1" => field.set_ind1(&attr.value)?,
                        "ind2" => field.set_ind2(&attr.value)?,
                        _ => {}
                    }
                }

                record.fields_mut().push(field);
            }

            "subfield" => {
                let field_op = record.fields_mut().last_mut();

                if field_op.is_none() {
                    return Err("Encounted <subfield/> without a field".to_string());
                }

                let field = field_op.unwrap();
                for attr in attributes {
                    if attr.name.local_name.eq("code") {
                        context.in_subfield = true;
                        field.subfields_mut().push(Subfield::new(&attr.value, "")?);
                        break;
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }
}

impl Record {
    /// Returns an iterator over the XML file which emits Records.
    pub fn from_xml_file(filename: &str) -> Result<XmlRecordIterator, String> {
        XmlRecordIterator::from_file(filename)
    }

    /// Returns an iterator over the XML string which emits Records.
    pub fn from_xml(xml: &str) -> XmlRecordIterator {
        XmlRecordIterator::from_string(xml)
    }

    /// Creates the XML representation of a MARC record as a String.
    pub fn to_xml(&self) -> Result<String, String> {
        self.to_xml_ops(&XmlOptions {
            formatted: false,
            with_xml_declaration: true,
        })
    }

    /// Creates the XML representation of a MARC record as a formatted
    /// string using 2-space indentation.
    pub fn to_xml_formatted(&self) -> Result<String, String> {
        self.to_xml_ops(&XmlOptions {
            formatted: true,
            with_xml_declaration: true,
        })
    }

    pub fn to_xml_ops(&self, options: &XmlOptions) -> Result<String, String> {
        // We could use XmlWriter here, but manual creation works fine
        // and offers more flexibility.

        let mut xml = match options.with_xml_declaration {
            true => String::from(r#"<?xml version="1.0"?>"#),
            _ => String::new(),
        };

        // Document root

        if options.formatted {
            xml += &format!(
                "\n<record\n  xmlns=\"{}\"\n  xmlns:xsi=\"{}\"\n  xsi:schemaLocation=\"{}\">",
                MARCXML_NAMESPACE, MARCXML_XSI_NAMESPACE, MARCXML_SCHEMA_LOCATION
            );
        } else {
            xml += &format!(
                r#"<record xmlns="{}" xmlns:xsi="{}" xsi:schemaLocation="{}">"#,
                MARCXML_NAMESPACE, MARCXML_XSI_NAMESPACE, MARCXML_SCHEMA_LOCATION
            );
        }

        // Leader

        format(options.formatted, &mut xml, 2);
        xml += &format!("<leader>{}</leader>", &escape_xml(self.leader(), false));

        // Control Fields

        for cfield in self.control_fields() {
            format(options.formatted, &mut xml, 2);

            xml += &format!(
                r#"<controlfield tag="{}">{}</controlfield>"#,
                escape_xml(cfield.tag(), true),
                escape_xml(cfield.content(), false),
            );
        }

        // Data Fields

        for field in self.fields() {
            format(options.formatted, &mut xml, 2);

            xml += &format!(
                r#"<datafield tag="{}" ind1="{}" ind2="{}">"#,
                escape_xml(field.tag(), true),
                escape_xml(field.ind1(), true),
                escape_xml(field.ind2(), true),
            );

            for sf in field.subfields() {
                format(options.formatted, &mut xml, 4);

                xml += &format!(
                    r#"<subfield code="{}">{}</subfield>"#,
                    &escape_xml(sf.code(), true),
                    &escape_xml(sf.content(), false)
                );
            }

            format(options.formatted, &mut xml, 2);

            xml += "</datafield>";
        }

        format(options.formatted, &mut xml, 0);

        xml += "</record>";

        Ok(xml)
    }
}
