use regex::Regex;
use std::sync::OnceLock;

/// Store these globally to avoid repititive regex recompilation.
static REGEX_CONTROL_CODES: OnceLock<Regex> = OnceLock::new();
static REGEX_PUNCTUATION: OnceLock<Regex> = OnceLock::new();
static REGEX_MULTI_SPACES: OnceLock<Regex> = OnceLock::new();

const REGEX_CONTROL_CODES_PATTERN: &str = r#"[\p{Cc}\p{Cf}\p{Co}\p{Lm}\p{Mc}\p{Me}\p{Mn}]"#;
const REGEX_PUNCTUATION_PATTERN: &str =
    r#"[\p{Pc}\p{Pd}\p{Pe}\p{Pf}\p{Pi}\p{Po}\p{Ps}\p{Sk}\p{Sm}\p{So}\p{Zl}\p{Zp}\p{Zs}]"#;

/// As is, this struct is no longer necessary but retained for backwards compat.
pub struct Normalizer {}

impl Normalizer {
    pub fn init() {
        if REGEX_CONTROL_CODES.get().is_some() {
            return;
        }
        // The above check should make the outer unwrap()'s below succeed.
        REGEX_CONTROL_CODES
            .set(Regex::new(REGEX_CONTROL_CODES_PATTERN).unwrap())
            .unwrap();
        REGEX_PUNCTUATION
            .set(Regex::new(REGEX_PUNCTUATION_PATTERN).unwrap())
            .unwrap();
        REGEX_MULTI_SPACES.set(Regex::new("\\s+").unwrap()).unwrap();
    }

    pub fn new() -> Normalizer {
        Normalizer::init();
        Normalizer {}
    }

    pub fn naco_normalize_once(value: &str) -> String {
        Normalizer::new().naco_normalize(value)
    }

    /// See Evergreen/Open-ILS/src/perlmods/lib/OpenILS/Utils/Normalize.pm
    ///
    /// # Examples
    ///
    /// ```
    /// use evergreen::norm::Normalizer;
    ///
    /// let normalizer = Normalizer::new();
    /// assert_eq!(normalizer.naco_normalize("Café"), normalizer.naco_normalize("cafe"));
    /// assert_eq!(
    ///     normalizer.naco_normalize(concat!("\u{009C}", "Pushkin")),
    ///     normalizer.naco_normalize("Pushkin")
    /// );
    /// assert_eq!(
    ///     normalizer.naco_normalize(concat!("Library", "\u{009C}")),
    ///     normalizer.naco_normalize("Library")
    /// );
    /// assert_eq!(normalizer.naco_normalize("‘Hello’"), normalizer.naco_normalize("Hello"));
    /// assert_eq!(normalizer.naco_normalize("Ægis"), normalizer.naco_normalize("aegis"));
    /// ```
    pub fn naco_normalize(&self, value: &str) -> String {
        self.normalize_codes(&self.normalize_substitutions(value))
    }

    fn normalize_substitutions(&self, value: &str) -> String {
        let value = value
            .to_uppercase()
            // Start/End of string characters
            .replace("\u{0098}", "")
            .replace("\u{009C}", "")
            // Single-quote-like characters
            .replace("\u{2018}", "'")
            .replace("\u{2019}", "'")
            .replace("\u{201B}", "'")
            .replace("\u{FF07}", "'")
            .replace("\u{201A}", "'")
            // Double-quote-like characters
            .replace("\u{201C}", "\"")
            .replace("\u{201D}", "\"")
            .replace("\u{201F}", "\"")
            .replace("\u{FF0C}", "\"")
            .replace("\u{201E}", "\"")
            .replace("\u{2E42}", "\"");

        // TODO
        // https://docs.rs/icu_normalizer/1.0.0/icu_normalizer/
        // $str = NFKD($str);

        // Additional substitutions
        value
            .replace("\u{00C6}", "AE")
            .replace("\u{00DE}", "TH")
            .replace("\u{0152}", "OE")
            .replace("\u{0110}", "D")
            .replace("\u{00D0}", "D")
            .replace("\u{00D8}", "O")
            .replace("\u{0141}", "L")
            .replace("\u{0142}", "l")
            .replace("\u{2113}", "")
            .replace("\u{02BB}", "")
            .replace("\u{02BC}", "")
    }

    fn normalize_codes(&self, value: &str) -> String {
        let mut value = if let Some(reg) = REGEX_CONTROL_CODES.get() {
            reg.replace_all(&value, "").into_owned()
        } else {
            unreachable!();
        };

        // Set aside some chars for safe keeping.
        value = value
            .replace("+", "\u{01}")
            .replace("&", "\u{02}")
            .replace("@", "\u{03}")
            .replace("\u{266D}", "\u{04}")
            .replace("\u{266F}", "\u{05}")
            .replace("#", "\u{06}");

        if let Some(reg) = REGEX_PUNCTUATION.get() {
            value = reg.replace_all(&value, " ").into_owned();
        }

        // Now put them back
        value = value
            .replace("\u{01}", "+")
            .replace("\u{02}", "&")
            .replace("\u{03}", "@")
            .replace("\u{04}", "\u{266D}")
            .replace("\u{05}", "\u{266F}")
            .replace("\u{06}", "#");

        // TODO decimal digits

        /*
        $str =~ tr/\x{0660}-\x{0669}\x{06F0}-\x{06F9}\x{07C0}-\x{07C9}\x{0966}-\x{096F}\x{09E6}-\x{09EF}\x{0A66}-\x{0A6F}\x{0AE6}-\x{0AEF}\x{0B66}-\x{0B6F}\x{0BE6}-\x{0BEF}\x{0C66}-\x{0C6F}\x{0CE6}-\x{0CEF}\x{0D66}-\x{0D6F}\x{0E50}-\x{0E59}\x{0ED0}-\x{0ED9}\x{0F20}-\x{0F29}\x{1040}-\x{1049}\x{1090}-\x{1099}\x{17E0}-\x{17E9}\x{1810}-\x{1819}\x{1946}-\x{194F}\x{19D0}-\x{19D9}\x{1A80}-\x{1A89}\x{1A90}-\x{1A99}\x{1B50}-\x{1B59}\x{1BB0}-\x{1BB9}\x{1C40}-\x{1C49}\x{1C50}-\x{1C59}\x{A620}-\x{A629}\x{A8D0}-\x{A8D9}\x{A900}-\x{A909}\x{A9D0}-\x{A9D9}\x{AA50}-\x{AA59}\x{ABF0}-\x{ABF9}\x{FF10}-\x{FF19}/0-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-9/;
        */

        if let Some(reg) = REGEX_MULTI_SPACES.get() {
            value = reg.replace_all(&value, " ").into_owned();
        }

        // leaing / trailing spaces
        value.trim().to_lowercase()
    }
}
