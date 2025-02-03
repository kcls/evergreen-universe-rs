//! Bib1 Attribute Set Types
//!
//! https://www.loc.gov/z3950/agency/defns/bib1.html


#[derive(Debug, Clone, Copy, PartialEq)]
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
        match n {
            1 => Ok(Self::Use),
            2 => Ok(Self::Relation),
            3 => Ok(Self::Position),
            4 => Ok(Self::Structure),
            5 => Ok(Self::Truncation),
            6 => Ok(Self::Completeness),
            7 => Ok(Self::Sorting),
            _ => Err(format!("Unsupported Attribute: {n}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Relation {
/*
    1 Less than
    2 Less than or equal
    3 Equal
    4 Greater or equal
    5 Greater than
    6 Not equal
    100 Phonetic
    101 Stem
    102 Relevance
    103 AlwaysMatches
*/
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Position {
/*
    1 First in field
    2 First in subfield
    3 Any position in field
*/
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Structure {
/*
    1 Phrase
    2 Word
    3 Key
    4 Year
    5 Date (normalized)
    6 Word list
    100 Date (un-normalized)
    101 Name (normalized)
    102 Name (un-normalized)
    103 Structure
    104 Urx
    105 Free-form-text
    106 Document-text
    107 Local-number
    108 String
    109 Numeric-string
*/
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Truncation {
/*
    1 Right truncation
    2 Left truncation
    3 Left and right truncation
    100 Do not truncate
    101 Process # in search term  . regular #=.*
    102 RegExpr-1
    103 RegExpr-2
    104 Process # ?n . regular: #=., ?n=.{0,n} or ?=.* Z39.58
*/
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
   

