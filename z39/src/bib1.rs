//! Bib1 Attribute Set Types
//!
//! https://www.loc.gov/z3950/agency/defns/bib1.html
use crate::message::*;
use strum::IntoEnumIterator; // 0.17.1
use strum_macros::EnumIter; // 0.17.1

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum Attribute {
    Use = 1,
    Relation = 2,
    Position = 3,
    Structure = 4,
    Truncation = 5,
    Completeness = 6,
    Sorting = 7,
}

impl TryFrom<u32> for Attribute {
    type Error = String;

    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| format!("No Attribute '{n}'"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum Use {
    PersonalName = 1,
    CorporateName = 2,
    ConferenceName = 3,
    Title = 4,
    TitleSeries = 5,
    TitleUniform = 6,
    ISBN = 7,
    ISSN = 8,
/*
    9     LC-card-number
    10    BNB-card-number
    11    BGF-number
    12    Local-number
    13    Dewey-classification
    14    UDC-classification
    15    Bliss-classification
    16    LC-call-number
    17    NLM-call-number
    18    NAL-call-number
    19    MOS-call-number
    20    Local-classification
    21    Subject-heading
    22    Subject-Rameau
    23    BDI-index-subject
    24    INSPEC-subject
    25    MESH-subject
    26    PA-subject
    27    LC-subject-heading
    28    RVM-subject-heading
    29    Local-subject-index
    30    Date
    31    Date-of-publication
    32    Date-of-acquisition
    33    Title-key
    34    Title-collective
    35    Title-parallel
    36    Title-cover
    37    Title-added-title-page
    38    Title-caption
    39    Title-running
    40    Title-spine
    41    Title-other-variant
    42    Title-former
    43    Title-abbreviated
    44    Title-expanded
    45    Subject-precis
    46    Subject-rswk
    47    Subject-subdivision
    48    Number-natl-biblio
    49    Number-legal-deposit
    50    Number-govt-pub
    51    Number-music-publisher
    52    Number-db
    53    Number-local-call
    54    Code-language
    55    Code-geographic
    56    Code-institution
    57    Name-and-title
    58    Name-geographic
    59    Place-publication
    60    CODEN
    61    Microform-generation
    62    Abstract
    63    Note
    1000  Author-title
    1001  Record-type
    1002  Name
*/
    Author = 1003,
/*
    1004  Author-name-personal
    1005  Author-name-corporate
    1006  Author-name-conference
    1007  Identifier-standard
    1008  Subject-LC-childrens
    1009  Subject-name-personal
    1010  Body-of-text
    1011  Date/time-added-to-db
    1012  Date/time-last-modified
    1013  Authority/format-id
    1014  Concept-text
    1015  Concept-reference
    1016  Any
    1017  Server-choice
    1018  Publisher
    1019  Record-source
    1020  Editor
    1021  Bib-level
    1022  Geographic-class
    1023  Indexed-by
    1024  Map-scale
    1025  Music-key
    1026  Related-periodical
    1027  Report-number
    1028  Stock-number
    1030  Thematic-number
    1031  Material-type
    1032  Doc-id
    1033  Host-item
    1034  Content-type
    1035  Anywhere
    1036  Author-Title-Subject
*/
}

impl TryFrom<u32> for Use {
    type Error = String;
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| format!("No bib1::Use '{n}'"))
    }
}

impl Use {
    pub fn as_z39_attribute_element(&self) -> AttributeElement {
        AttributeElement {
            attribute_set: None, // bib1 is implicit
            attribute_type: Attribute::Use as u32, 
            attribute_value: AttributeValue::Numeric(*self as u32), 
        }
    }
}


#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum Relation {
    LessThan = 1,
    LessThanOrEqual = 2,
    Equal = 3,
    GreaterOrEqual = 4,
    GreaterThan = 5,
    NotEqual = 6,
    Phonetic = 100,
    Stem = 101,
    Relevance = 102,
    AlwaysMatches = 103
}

impl TryFrom<u32> for Relation {
    type Error = String;
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| format!("No bib1::Relation '{n}'"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum Position {
    FirstInField = 1,
    FirstInSsubfield = 2,
    AnyPositionInFfield = 3,
}

impl TryFrom<u32> for Position {
    type Error = String;
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| format!("No bib1::Position '{n}'"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
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
    LocalNumber = 107,
    String = 108,
    NumericString = 109,
}

impl TryFrom<u32> for Structure {
    type Error = String;
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| format!("No bib1::Structure '{n}'"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum Truncation {
    RightTruncation = 1,
    LeftTruncation = 2,
    LeftAndRightTruncation = 3,
    DoNotTruncate = 100,
    Process = 101,
    RegExpr1 = 102,
    RegExpr2 = 103,
    ProcessAlt = 104, // ??
}

impl TryFrom<u32> for Truncation {
    type Error = String;
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| format!("No bib1::Truncation '{n}'"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Completeness {
    IncompleteSubfield = 1,
    CompleteSubfield = 2,
    CompleteField = 3,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Sorting {
    Ascending = 1,
    Descending = 2,
}

impl TryFrom<u32> for Sorting {
    type Error = String;
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        match n {
            1 => Ok(Self::Ascending),
            2 => Ok(Self::Descending),
            _ => Err(format!("No bib1::Sorting '{n}'"))
        }
    }
}  

