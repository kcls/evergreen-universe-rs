//! Partial values for Z39.50 Bib-1 attribute set.
//! https://www.loc.gov/z3950/agency/bib1.html

#[derive(Debug, PartialEq, Clone)]
pub enum Structure {
    Phrase = 1,
    Word = 2,
    Key = 3,
    Year = 4,
    DateNormalized = 5,
    WordList = 6,
    DateUnNormalized = 100,
    NameNormalized = 101,
    NameUnNormalized = 102,
    Structure = 103,
    Urx = 104,
    FreeFormText = 105,
    DocumentText = 106,
    LocalLumber = 107,
    String = 108,
    NumericString = 109,
}

impl TryFrom<u64> for Structure {
    type Error = String;

    fn try_from(n: u64) -> Result<Self, Self::Error> {
        let s = match n {
            1 => Self::Phrase,
            2 => Self::Word,
            3 => Self::Key,
            4 => Self::Year,
            5 => Self::DateNormalized,
            6 => Self::WordList,
            100 => Self::DateUnNormalized,
            101 => Self::NameNormalized,
            102 => Self::NameUnNormalized,
            103 => Self::Structure,
            104 => Self::Urx,
            105 => Self::FreeFormText,
            106 => Self::DocumentText,
            107 => Self::LocalLumber,
            108 => Self::String,
            109 => Self::NumericString,
            _ => return Err(format!("Unknown Structure value: {n}")),
        };

        Ok(s)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Truncation {
    Right = 1,
    Left = 2,
    LeftAndRight = 3,
    NoTruncation = 100,
    Process = 101,
    RegExpr1 = 102,
    RegExpr2 = 103,
}

impl TryFrom<u64> for Truncation {
    type Error = String;

    fn try_from(n: u64) -> Result<Self, Self::Error> {
        let t = match n {
            1 => Self::Right,
            2 => Self::Left,
            3 => Self::LeftAndRight,
            100 => Self::NoTruncation,
            101 => Self::Process,
            102 => Self::RegExpr1,
            103 => Self::RegExpr2,
            _ => return Err(format!("Unknown Truncation value: {n}")),
        };

        Ok(t)
    }
}

/// Use values.
///
/// For now, just store the raw number value instead of listing
/// all of the options.
#[derive(Debug, PartialEq, Clone)]
pub enum Use {
    Value(u16),
}

impl TryFrom<u64> for Use {
    type Error = String;

    fn try_from(n: u64) -> Result<Self, Self::Error> {
        if let Ok(n16) = n.try_into() {
            Ok(Use::Value(n16))
        } else {
            Err(format!("Invalid Use value: {n}"))
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum AttrType {
    Use = 1,
    Relation = 2,
    Position = 3,
    Structure = 4,
    Truncation = 5,
    Completeness = 6,
    Sorting = 7,
}

impl TryFrom<u64> for AttrType {
    type Error = String;

    fn try_from(n: u64) -> Result<Self, Self::Error> {
        let a = match n {
            1 => Self::Use,
            2 => Self::Relation,
            3 => Self::Position,
            4 => Self::Structure,
            5 => Self::Truncation,
            6 => Self::Completeness,
            7 => Self::Sorting,
            _ => return Err(format!("Unknown attribute type: {n}")),
        };

        Ok(a)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum AttrValue {
    Use(Use),
    Structure(Structure),
    Truncation(Truncation),
    // TODO
}

#[derive(Debug, PartialEq, Clone)]
pub struct Attr {
    attr_type: AttrType,
    attr_value: AttrValue,
}

impl TryFrom<&str> for Attr {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let err = |s| format!("Invalid Attr string: {s}");

        let mut parts = s.split('=');

        // str values
        let attr_type = parts.next().ok_or_else(|| err(s))?;
        let attr_value = parts.next().ok_or_else(|| err(s))?;

        // numeric values
        let attr_type = attr_type.parse::<u64>().map_err(|_| err(s))?;
        let attr_value = attr_value.parse::<u64>().map_err(|_| err(s))?;

        let attr_type = AttrType::try_from(attr_type)?;

        let attr_value = match attr_type {
            AttrType::Use => AttrValue::Use(Use::try_from(attr_value)?),
            AttrType::Relation => todo!(),
            AttrType::Position => todo!(),
            AttrType::Structure => AttrValue::Structure(Structure::try_from(attr_value)?),
            AttrType::Truncation => AttrValue::Truncation(Truncation::try_from(attr_value)?),
            AttrType::Completeness => todo!(),
            AttrType::Sorting => todo!(),
        };

        Ok(Attr {
            attr_type,
            attr_value,
        })
    }
}
