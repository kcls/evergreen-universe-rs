/// Replaces every occurrence of a sequence of bytes with another sequence
/// of bytes and returns the final collection of bytes.
/// ```
/// use marc::util;
///
/// let s = b"hello joe";
/// let v = util::replace_byte_sequence(s, b"ll", b"jj");
/// assert_eq!(v, b"hejjo joe");
///
/// let v = util::replace_byte_sequence(s, b"he", b"HE");
/// assert_eq!(v, b"HEllo joe");
///
/// let v = util::replace_byte_sequence(s, b"joe", b"xx");
/// assert_eq!(v, b"hello xx");
///
/// let v = util::replace_byte_sequence(s, b"o", b"Z");
/// assert_eq!(v, b"hellZ jZe")
/// ```
pub fn replace_byte_sequence(source: &[u8], target: &[u8], replace: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();

    let source_len = source.len();
    let target_len = target.len();

    let mut index = 0;

    while index < source_len {
        let part = &source[index..];

        if part.len() >= target_len {
            if &part[..target_len] == target {
                result.extend(replace);
                index += target_len;
                continue;
            }
        }

        // No match; add the next byte
        result.push(part[0]);

        index += 1;
    }

    result
}

pub fn bytes_to_utf8(bytes: &[u8]) -> Result<String, String> {
    match std::str::from_utf8(bytes) {
        Ok(s) => Ok(s.to_string()),
        Err(e) => Err(format!("Invalid utf8 byte sequence: {bytes:?} {e}")),
    }
}

pub fn utf8_to_bytes(s: &str, length: Option<usize>) -> Result<Vec<u8>, String> {
    let bytes = s.as_bytes();
    if let Some(len) = length {
        if bytes.len() != len {
            return Err(format!("Byte length does not match: {bytes:?} {len}"));
        }
    }
    Ok(bytes.to_vec())
}

/// bytes => String => usize
/// TODO do this without the intermediate String
pub fn bytes_to_usize(bytes: &[u8]) -> Result<usize, String> {
    match std::str::from_utf8(&bytes) {
        Ok(bytes_str) => match bytes_str.parse::<usize>() {
            Ok(num) => Ok(num),
            Err(e) => Err(format!(
                "Error translating string to usize str={bytes_str} {e}"
            )),
        },
        Err(e) => Err(format!("Error translating bytes to string: {bytes:?} {e}")),
    }
}