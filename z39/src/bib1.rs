//! Bib1 Attribute Set Types
//!
//! https://www.loc.gov/z3950/agency/defns/bib1.html
use crate::message::*;

use strum::IntoEnumIterator;
use strum_macros::EnumIter;

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum Attribute {
    Use = 1,
    Relation,
    Position,
    Structure,
    Truncation,
    Completeness,
    Sorting,
}

impl TryFrom<u32> for Attribute {
    type Error = String;
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| format!("No bib1::Attribute '{n}'"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum Use {
    PersonalName = 1,
    CorporateName,
    ConferenceName,
    Title,
    TitleSeries,
    TitleUniform,
    Isbn,
    Issn,
    LcCardNumber,
    BnbCardNumber,
    BgfNumber,
    LocalNumber,
    DeweyClassification,
    UdcClassification,
    BlissClassification,
    LcCallNumber,
    NlmCallNumber,
    NalCallNumber,
    MosCallNumber,
    LocalClassification,
    SubjectHeading,
    SubjectRameau,
    BdiIndexSubject,
    InspecSubject,
    MeshSubject,
    PaSubject,
    LcSubjectHeading,
    RvmSubjectHeading,
    LocalSubjectIndex,
    Date,
    DateOfPublication,
    DateOfAcquisition,
    TitleKey,
    TitleCollective,
    TitleParallel,
    TitleCover,
    TitleAddedTitlePage,
    TitleCaption,
    TitleRunning,
    TitleSpine,
    TitleOtherVariant,
    TitleFormer,
    TitleAbbreviated,
    TitleExpanded,
    SubjectPrecis,
    SubjectRswk,
    SubjectSubdivision,
    NumberNatlBiblio,
    NumberLegalDeposit,
    NumberGovtPub,
    NumberMusicPublisher,
    NumberDb,
    NumberLocalCall,
    CodeLanguage,
    CodeGeographic,
    CodeInstitution,
    NameAndTitle,
    NameGeographic,
    PlacePublication,
    Coden,
    MicroformGeneration,
    Abstract,
    Note,
    AuthorTitle = 1000,
    RecordType,
    Name,
    Author,
    AuthorNamePersonal,
    AuthorNameCorporate,
    AuthorNameConference,
    IdentifierStandard,
    SubjectLcChildrens,
    SubjectNamePersonal,
    BodyOfText,
    DateTimeAddedToDb,
    DateTimeLastModified,
    AuthorityFormatId,
    ConceptText,
    ConceptReference,
    Any,
    ServerChoice,
    Publisher,
    RecordSource,
    Editor,
    BibLevel,
    GeographicClass,
    IndexedBy,
    MapScale,
    MusicKey,
    RelatedPeriodical,
    ReportNumber,
    StockNumber,
    ThematicNumber,
    MaterialType,
    DocId,
    HostItem,
    ContentType,
    Anywhere,
    AuthorTitleSubject,
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
    LessThanOrEqual,
    Equal,
    GreaterOrEqual,
    GreaterThan,
    NotEqual,
    Phonetic = 100,
    Stem,
    Relevance,
    AlwaysMatches,
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
    FirstInSsubfield,
    AnyPositionInFfield,
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
    Word,
    Key,
    Year,
    DateNormalized,
    WordList,
    DateUnNormalized = 100,
    NameNormalized,
    NameUnNormalized,
    Structure,
    Urx,
    FreeFormText,
    DocumentText,
    LocalNumber,
    String,
    NumericString,
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
    LeftTruncation,
    LeftAndRightTruncation,
    DoNotTruncate = 100,
    Process,
    RegExpr1,
    RegExpr2,
    ProcessAlt, // ??
}

impl TryFrom<u32> for Truncation {
    type Error = String;
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| format!("No bib1::Truncation '{n}'"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum Completeness {
    IncompleteSubfield = 1,
    CompleteSubfield,
    CompleteField,
}

impl TryFrom<u32> for Completeness {
    type Error = String;
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| format!("No bib1::Completeness '{n}'"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum Sorting {
    Ascending = 1,
    Descending,
}

impl TryFrom<u32> for Sorting {
    type Error = String;
    fn try_from(n: u32) -> Result<Self, Self::Error> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| format!("No bib1::Sorting '{n}'"))
    }
}
