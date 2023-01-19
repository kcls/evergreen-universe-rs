use regex::Regex;

const REGEX_CONTROL_CODES: &str =
    r#"[\p{Cc}\p{Cf}\p{Co}\p{Lm}\p{Mc}\p{Me}\p{Mn}]"#;
const REGEX_PUNCTUATION: &str =
    r#"[\p{Pc}\p{Pd}\p{Pe}\p{Pf}\p{Pi}\p{Po}\p{Ps}\p{Sk}\p{Sm}\p{So}\p{Zl}\p{Zp}\p{Zs}]"#;

/// Container for precompiled regexes so we're not forced to compile
/// them repetitively, which is very innefficient.
pub struct Normalizer {
    regex_control_codes: Regex,
    regex_puncutation: Regex,
    regex_multi_spaces: Regex,
}

impl Normalizer {
    pub fn new() -> Normalizer {
        Normalizer {
            regex_control_codes: Regex::new(REGEX_CONTROL_CODES).unwrap(),
            regex_puncutation: Regex::new(REGEX_PUNCTUATION).unwrap(),
            regex_multi_spaces: Regex::new("\\s+").unwrap(),
        }
    }

    pub fn naco_normalize_once(value: &str) -> String {
        Normalizer::new().naco_normalize(value)
    }

    // See Evergreen/Open-ILS/src/perlmods/lib/OpenILS/Utils/Normalize.pm
    pub fn naco_normalize(&self, value: &str) -> String {
        self.normalize_codes(&self.normalize_substitutions(value))
    }

    fn normalize_substitutions(&self, value: &str) -> String {

        let value = value.to_uppercase()
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
            .replace("\u{2E42}", "\"")
        ;

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

        let mut value = self.regex_control_codes.replace_all(&value, "").into_owned();

        // Set aside some chars we want to keep for safe keeping.
        value = value
            .replace("+", "\u{01}")
            .replace("&", "\u{02}")
            .replace("@", "\u{03}")
            .replace("\u{266D}", "\u{04}")
            .replace("\u{266F}", "\u{05}")
            .replace("#", "\u{06}")
        ;

        value = self.regex_puncutation.replace_all(&value, " ").into_owned();

        // Now put them back
        value = value
            .replace("\u{01}", "+")
            .replace("\u{02}", "&")
            .replace("\u{03}", "@")
            .replace("\u{04}", "\u{266D}")
            .replace("\u{05}", "\u{266F}")
            .replace("\u{06}", "#")
        ;

        // TODO decimal digits

        /*
        $str =~ tr/\x{0660}-\x{0669}\x{06F0}-\x{06F9}\x{07C0}-\x{07C9}\x{0966}-\x{096F}\x{09E6}-\x{09EF}\x{0A66}-\x{0A6F}\x{0AE6}-\x{0AEF}\x{0B66}-\x{0B6F}\x{0BE6}-\x{0BEF}\x{0C66}-\x{0C6F}\x{0CE6}-\x{0CEF}\x{0D66}-\x{0D6F}\x{0E50}-\x{0E59}\x{0ED0}-\x{0ED9}\x{0F20}-\x{0F29}\x{1040}-\x{1049}\x{1090}-\x{1099}\x{17E0}-\x{17E9}\x{1810}-\x{1819}\x{1946}-\x{194F}\x{19D0}-\x{19D9}\x{1A80}-\x{1A89}\x{1A90}-\x{1A99}\x{1B50}-\x{1B59}\x{1BB0}-\x{1BB9}\x{1C40}-\x{1C49}\x{1C50}-\x{1C59}\x{A620}-\x{A629}\x{A8D0}-\x{A8D9}\x{A900}-\x{A909}\x{A9D0}-\x{A9D9}\x{AA50}-\x{AA59}\x{ABF0}-\x{ABF9}\x{FF10}-\x{FF19}/0-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-90-9/;
        */

        value = self.regex_multi_spaces.replace_all(&value, " ").into_owned();

        // leaing / trailing spaces
        value.trim().to_lowercase()
    }
}

