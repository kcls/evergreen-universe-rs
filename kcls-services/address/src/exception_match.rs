//! Address-exception matching logic for the kcls.address.exception.matches
//! API.
//!
//! This module is intentionally free of OpenSRF / database dependencies so the
//! matching rules can be unit-tested directly.  The OpenSRF handler in
//! methods.rs loads the config.usr_address_exception ("cuae") rows, converts
//! them to [`ExceptionRecord`]s, and calls [`match_exceptions`].
//!
//! Rules (all comparisons are case-insensitive):
//! * A null/None value on either side (search or DB) is a match worth 0 points.
//! * city, state, and post_code, when present on both sides, must match
//!   exactly (1 point each).
//! * street1 / street2, when present on both sides, match when the in-database
//!   value starts with the searched value (street1 = 3 points; street2 = 5
//!   points, but only when street1 also matched, else 0).  A searched street2
//!   also matches with its leading designator removed (e.g. "Apt 4" -> "4").
//! * If no street2 is supplied, a secondary is derived from street1 by
//!   splitting on "apt", "unit", "bldg", or "#".
//! * Any present-on-both field that fails its comparison excludes the record.
//! * The best match is the one with the most points.

/// Trim + lowercase a value for comparison; empty becomes None.
fn norm(v: &Option<String>) -> Option<String> {
    v.as_ref()
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
}

/// The address values being searched for.
#[derive(Debug, Default, Clone)]
pub struct AddressSearch {
    pub street1: Option<String>,
    pub street2: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub post_code: Option<String>,
}

/// A candidate address-exception row: its id plus searchable fields.
#[derive(Debug, Default, Clone)]
pub struct ExceptionRecord {
    pub id: i64,
    pub street1: Option<String>,
    pub street2: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub post_code: Option<String>,
}

/// A matched exception and its score.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExceptionMatch {
    pub id: i64,
    pub score: i32,
}

/// The best match plus any other matches (best excluded), best-first.
#[derive(Debug, Default, Clone)]
pub struct MatchResult {
    pub best: Option<ExceptionMatch>,
    pub others: Vec<ExceptionMatch>,
}

/// Outcome of comparing a single field.
#[derive(Debug, PartialEq)]
enum FieldMatch {
    /// Null on either side: matches but scores nothing.
    Null,
    /// Present on both sides and matched.
    Positive,
    /// Present on both sides but did not match; excludes the record.
    Miss,
}

fn match_exact(search: &Option<String>, db: &Option<String>) -> FieldMatch {
    match (norm(search), norm(db)) {
        (None, _) | (_, None) => FieldMatch::Null,
        (Some(s), Some(d)) => {
            if s == d {
                FieldMatch::Positive
            } else {
                FieldMatch::Miss
            }
        }
    }
}

fn match_prefix(search: &Option<String>, db: &Option<String>) -> FieldMatch {
    // The in-database value must start with the searched value.
    match (norm(search), norm(db)) {
        (None, _) | (_, None) => FieldMatch::Null,
        (Some(s), Some(d)) => {
            if d.starts_with(&s) {
                FieldMatch::Positive
            } else {
                FieldMatch::Miss
            }
        }
    }
}

/// Match street2, trying the searched value as-is and, if that fails, again
/// with a leading secondary designator ("apt", "unit", "bldg", "#") removed --
/// so a searched "Apt 4" also matches an in-database "4".
fn match_street2(search: &Option<String>, db: &Option<String>) -> FieldMatch {
    let direct = match_prefix(search, db);
    if direct != FieldMatch::Miss {
        // Null (empty on a side) or Positive: nothing more to try.
        return direct;
    }

    // Direct comparison missed; retry with the designator stripped.
    if let Some(stripped) = search.as_deref().and_then(strip_designator) {
        if match_prefix(&Some(stripped), db) == FieldMatch::Positive {
            return FieldMatch::Positive;
        }
    }

    FieldMatch::Miss
}

/// Return the portion of `value` after a secondary designator, or None when it
/// has no designator (or nothing follows it).
fn strip_designator(value: &str) -> Option<String> {
    find_designator(value)
        .map(|(idx, len)| value[idx + len..].trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Find the earliest secondary designator in `street1`.  Word designators
/// ("apt", "unit", "bldg") must be bounded by non-alphanumerics; "#" matches
/// anywhere.  Returns (byte offset, byte length) of the designator.
///
/// NOTE: assumes ASCII address text so byte offsets from the lowercased copy
/// line up with the original.
fn find_designator(street1: &str) -> Option<(usize, usize)> {
    let lower = street1.to_lowercase();
    let bytes = lower.as_bytes();
    let mut candidates: Vec<(usize, usize)> = Vec::new();

    if let Some(i) = lower.find('#') {
        candidates.push((i, 1));
    }

    for w in ["apt", "unit", "bldg"] {
        let mut start = 0;
        while let Some(rel) = lower[start..].find(w) {
            let i = start + rel;
            let end = i + w.len();
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
            let after_ok = end >= bytes.len() || !bytes[end].is_ascii_alphanumeric();
            if before_ok && after_ok {
                candidates.push((i, w.len()));
                break;
            }
            start = end;
        }
    }

    candidates.into_iter().min_by_key(|(i, _)| *i)
}

/// When no street2 is supplied, derive one from street1 by splitting on a
/// secondary designator.  The portion after the designator becomes street2;
/// the portion before becomes street1.
pub fn split_secondary(search: &AddressSearch) -> AddressSearch {
    let mut out = search.clone();

    // Only when street2 is empty and street1 has a value.
    if norm(&out.street2).is_some() {
        return out;
    }
    let Some(street1) = out.street1.clone() else {
        return out;
    };

    if let Some((idx, len)) = find_designator(&street1) {
        let before = street1[..idx].trim().to_string();
        let after = street1[idx + len..].trim().to_string();

        out.street1 = if before.is_empty() { None } else { Some(before) };
        out.street2 = if after.is_empty() { None } else { Some(after) };
    }

    out
}

/// Score one record against the search.  Returns None when the record is not a
/// match (a present-on-both field failed its comparison).
fn score_record(search: &AddressSearch, rec: &ExceptionRecord) -> Option<i32> {
    let city = match_exact(&search.city, &rec.city);
    let state = match_exact(&search.state, &rec.state);
    let post = match_exact(&search.post_code, &rec.post_code);
    let s1 = match_prefix(&search.street1, &rec.street1);
    let s2 = match_street2(&search.street2, &rec.street2);

    if city == FieldMatch::Miss
        || state == FieldMatch::Miss
        || post == FieldMatch::Miss
        || s1 == FieldMatch::Miss
        || s2 == FieldMatch::Miss
    {
        return None;
    }

    let mut score = 0;
    if city == FieldMatch::Positive {
        score += 1;
    }
    if state == FieldMatch::Positive {
        score += 1;
    }
    if post == FieldMatch::Positive {
        score += 1;
    }

    let street1_matched = s1 == FieldMatch::Positive;
    if street1_matched {
        score += 3;
    }

    // street2 only scores when street1 also positively matched.
    if street1_matched && s2 == FieldMatch::Positive {
        score += 5;
    }

    Some(score)
}

/// Match the search against all records, returning the best match plus the
/// other matches, ordered best-first (input order breaks ties).
pub fn match_exceptions(search: &AddressSearch, records: &[ExceptionRecord]) -> MatchResult {
    let search = split_secondary(search);

    let mut matches: Vec<ExceptionMatch> = records
        .iter()
        .filter_map(|rec| {
            score_record(&search, rec).map(|score| ExceptionMatch { id: rec.id, score })
        })
        .collect();

    // Highest score first; sort is stable so input order breaks ties.
    matches.sort_by(|a, b| b.score.cmp(&a.score));

    let mut iter = matches.into_iter();
    let best = iter.next();
    let others = iter.collect();

    MatchResult { best, others }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(v: &str) -> Option<String> {
        Some(v.to_string())
    }

    fn search(street1: Option<String>, street2: Option<String>, city: Option<String>,
        state: Option<String>, post_code: Option<String>) -> AddressSearch {
        AddressSearch { street1, street2, city, state, post_code }
    }

    fn rec(id: i64, street1: Option<String>, street2: Option<String>, city: Option<String>,
        state: Option<String>, post_code: Option<String>) -> ExceptionRecord {
        ExceptionRecord { id, street1, street2, city, state, post_code }
    }

    #[test]
    fn exact_fields_are_case_insensitive() {
        let se = search(None, None, s("Bellevue"), s("wa"), s("98004"));
        let re = rec(1, None, None, s("BELLEVUE"), s("WA"), s("98004"));
        // city + state + post_code = 3 points.
        assert_eq!(match_exceptions(&se, &[re]).best, Some(ExceptionMatch { id: 1, score: 3 }));
    }

    #[test]
    fn exact_mismatch_excludes_record() {
        let se = search(None, None, s("Bellevue"), s("WA"), s("98004"));
        let re = rec(1, None, None, s("Seattle"), s("WA"), s("98004"));
        assert_eq!(match_exceptions(&se, &[re]).best, None);
    }

    #[test]
    fn null_on_either_side_matches_for_zero_points() {
        // Search has only city; record has only state.  Each field is a
        // null-match somewhere, so it matches with 0 points.
        let se = search(None, None, s("Kent"), None, None);
        let re = rec(1, None, None, None, s("WA"), None);
        assert_eq!(match_exceptions(&se, &[re]).best, Some(ExceptionMatch { id: 1, score: 0 }));
    }

    #[test]
    fn street1_prefix_matches_db_starts_with_search() {
        let se = search(s("123 main"), None, None, None, None);
        let re = rec(1, s("123 Main St"), None, None, None, None);
        // street1 = 3 points.
        assert_eq!(match_exceptions(&se, &[re.clone()]).best, Some(ExceptionMatch { id: 1, score: 3 }));

        // The reverse (search longer than DB) does not match.
        let se2 = search(s("123 Main St Suite"), None, None, None, None);
        assert_eq!(match_exceptions(&se2, &[re]).best, None);
    }

    #[test]
    fn street2_only_scores_when_street1_matched() {
        // street1 null-match (record has none) -> street2 scores 0.
        let se = search(None, s("4"), None, None, None);
        let re = rec(1, None, s("4B"), None, None, None);
        assert_eq!(match_exceptions(&se, &[re]).best, Some(ExceptionMatch { id: 1, score: 0 }));

        // With street1 matched, street2 adds 5 (3 + 5 = 8).
        let se2 = search(s("123 main"), s("4"), None, None, None);
        let re2 = rec(2, s("123 Main St"), s("4B"), None, None, None);
        assert_eq!(match_exceptions(&se2, &[re2]).best, Some(ExceptionMatch { id: 2, score: 8 }));
    }

    #[test]
    fn provided_street2_matches_with_or_without_designator() {
        // DB stores the bare secondary; a searched street2 that includes the
        // designator still matches (as-is fails, stripped succeeds).
        let re = rec(1, s("123 Main St"), s("4"), None, None, None);

        let with_designator = search(s("123 main"), s("Apt 4"), None, None, None);
        assert_eq!(match_exceptions(&with_designator, &[re.clone()]).best,
            Some(ExceptionMatch { id: 1, score: 8 }));

        // The bare value still matches too.
        let bare = search(s("123 main"), s("4"), None, None, None);
        assert_eq!(match_exceptions(&bare, &[re]).best,
            Some(ExceptionMatch { id: 1, score: 8 }));
    }

    #[test]
    fn secondary_extracted_from_street1_when_street2_absent() {
        // "123 Main St Apt 4" -> street1 "123 Main St", street2 "4".
        let se = search(s("123 Main St Apt 4"), None, None, None, None);
        let re = rec(1, s("123 Main St"), s("4"), None, None, None);
        // street1 (3) + street2 (5) = 8.
        assert_eq!(match_exceptions(&se, &[re]).best, Some(ExceptionMatch { id: 1, score: 8 }));
    }

    #[test]
    fn secondary_designators_variants() {
        for raw in ["10 Elm # 2", "10 Elm Unit 2", "10 Elm bldg 2"] {
            let out = split_secondary(&search(s(raw), None, None, None, None));
            assert_eq!(out.street1.as_deref(), Some("10 Elm"), "street1 for {raw}");
            assert_eq!(out.street2.as_deref(), Some("2"), "street2 for {raw}");
        }
    }

    #[test]
    fn secondary_split_ignores_words_containing_designator() {
        // "Aptos" should not be split on "apt".
        let out = split_secondary(&search(s("100 Aptos Ave"), None, None, None, None));
        assert_eq!(out.street1.as_deref(), Some("100 Aptos Ave"));
        assert_eq!(out.street2, None);
    }

    #[test]
    fn provided_street2_is_not_overwritten() {
        let out = split_secondary(&search(s("123 Main St Apt 4"), s("9"), None, None, None));
        assert_eq!(out.street1.as_deref(), Some("123 Main St Apt 4"));
        assert_eq!(out.street2.as_deref(), Some("9"));
    }

    #[test]
    fn best_match_has_the_most_points() {
        let se = search(s("123 main"), s("4"), s("Kent"), s("WA"), s("98030"));

        // Full match: street1(3) + street2(5) + city(1) + state(1) + post(1) = 11.
        let full = rec(1, s("123 Main St"), s("4B"), s("Kent"), s("WA"), s("98030"));
        // Weaker: street1 only (3).
        let weak = rec(2, s("123 Main St"), None, None, None, None);

        let result = match_exceptions(&se, &[weak.clone(), full.clone()]);
        assert_eq!(result.best, Some(ExceptionMatch { id: 1, score: 11 }));
        assert_eq!(result.others, vec![ExceptionMatch { id: 2, score: 3 }]);
    }
}
