//! Bib1 Attribute Set Types and Values
//!
//! https://www.loc.gov/z3950/agency/defns/bib1.html
use crate::error::{LocalError, LocalResult};
use crate::types::pdu::*;

// Make working with the larger enums easier.
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
    type Error = LocalError;
    fn try_from(n: u32) -> LocalResult<Self> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| LocalError::ProtocolError(format!("No bib1::Attribute '{n}'")))
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

impl From<Use> for AttributeElement {
    fn from(u: Use) -> Self {
        Self {
            attribute_set: None,
            attribute_type: Attribute::Use as u32,
            attribute_value: AttributeValue::Numeric(u as u32),
        }
    }
}

impl TryFrom<u32> for Use {
    type Error = LocalError;
    fn try_from(n: u32) -> LocalResult<Self> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| LocalError::ProtocolError(format!("No bib1::Use '{n}'")))
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
    type Error = LocalError;
    fn try_from(n: u32) -> LocalResult<Self> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| LocalError::ProtocolError(format!("No bib1::Relation '{n}'")))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum Position {
    FirstInField = 1,
    FirstInSubfield,
    AnyPositionInField,
}

impl TryFrom<u32> for Position {
    type Error = LocalError;
    fn try_from(n: u32) -> LocalResult<Self> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| LocalError::ProtocolError(format!("No bib1::Position '{n}'")))
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
    type Error = LocalError;
    fn try_from(n: u32) -> LocalResult<Self> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| LocalError::ProtocolError(format!("No bib1::Structure '{n}'")))
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
    type Error = LocalError;
    fn try_from(n: u32) -> LocalResult<Self> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| LocalError::ProtocolError(format!("No bib1::Truncation '{n}'")))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum Completeness {
    IncompleteSubfield = 1,
    CompleteSubfield,
    CompleteField,
}

impl TryFrom<u32> for Completeness {
    type Error = LocalError;
    fn try_from(n: u32) -> LocalResult<Self> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| LocalError::ProtocolError(format!("No bib1::Completeness '{n}'")))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, EnumIter)]
pub enum Sorting {
    Ascending = 1,
    Descending,
}

impl TryFrom<u32> for Sorting {
    type Error = LocalError;
    fn try_from(n: u32) -> LocalResult<Self> {
        Self::iter()
            .find(|a| *a as u32 == n)
            .ok_or_else(|| LocalError::ProtocolError(format!("No bib1::Sorting '{n}'")))
    }
}

/// Turn a z39 AttributeElement into a string of the form AttributeType=AttributeValue,
/// where type and value are derived from the enum labels.
///
/// Complex attribute values are debug-stringified in place.
///
/// ```
/// use z39_types::bib1::*;
/// use z39_types::message::*;
///
/// let attr = AttributeElement {
///     attribute_set: None,
///     attribute_type: Attribute::Structure as u32,
///     attribute_value: AttributeValue::Numeric(Structure::WordList as u32),
/// };
///
/// assert_eq!(stringify_attribute(&attr).unwrap(), "Structure=WordList");
/// ```
pub fn stringify_attribute(a: &AttributeElement) -> LocalResult<String> {
    let attr_type = Attribute::try_from(a.attribute_type)?;

    let numeric_value = match &a.attribute_value {
        AttributeValue::Numeric(n) => *n,
        AttributeValue::Complex(c) => return Ok(format!("{attr_type:?}={c:?}")),
    };

    Ok(match attr_type {
        Attribute::Use => format!("{attr_type:?}={:?}", Use::try_from(numeric_value)?),
        Attribute::Relation => format!("{attr_type:?}={:?}", Relation::try_from(numeric_value)?),
        Attribute::Position => format!("{attr_type:?}={:?}", Position::try_from(numeric_value)?),
        Attribute::Structure => format!("{attr_type:?}={:?}", Structure::try_from(numeric_value)?),
        Attribute::Truncation => {
            format!("{attr_type:?}={:?}", Truncation::try_from(numeric_value)?)
        }
        Attribute::Completeness => {
            format!("{attr_type:?}={:?}", Completeness::try_from(numeric_value)?)
        }
        Attribute::Sorting => format!("{attr_type:?}={:?}", Sorting::try_from(numeric_value)?),
    })
}
