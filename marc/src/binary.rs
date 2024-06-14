use super::Controlfield;
use super::Field;
use super::Record;
use super::Subfield;
use std::fs::File;
use std::io::prelude::*;

const END_OF_FIELD: u8 = 30; // '\x1E';
const END_OF_RECORD: u8 = 29; // '\x1D';
const RECORD_SIZE_ENTRY: usize = 5;
const LEADER_SIZE: usize = 24;
const DATA_OFFSET_START: usize = 12;
const DATA_OFFSET_SIZE: usize = 5;
const DATA_LENGTH_SIZE: usize = 4;
const DIRECTORY_ENTRY_LEN: usize = 12;
const SUBFIELD_SEPARATOR: &str = "\x1F";
const MAX_RECORD_BYTES: usize = 99999;

/// Parses a binary MARC file and emits [`Record`] values.
pub struct BinaryRecordIterator {
    file: File,
}

impl Iterator for BinaryRecordIterator {
    type Item = Result<Record, String>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut bytes: Vec<u8> = Vec::new();

        loop {
            // Read bytes from the file until we hit an END_OF_RECORD byte.
            // Pass the read bytes to the Record binary data parser.

            let mut buf: [u8; 1] = [0];
            match self.file.read(&mut buf) {
                Ok(count) => {
                    if count == 1 {
                        bytes.push(buf[0]);
                        if buf[0] == END_OF_RECORD {
                            break;
                        }
                    } else {
                        break; // EOF
                    }
                }
                Err(e) => {
                    return Some(Err(format!("Error reading file: {:?} {}", self.file, e)));
                }
            }
        }

        if !bytes.is_empty() {
            match Record::from_binary(bytes.as_slice()) {
                Ok(r) => return Some(Ok(r)),
                Err(e) => return Some(Err(format!("Error processing bytes: {:?} {}", bytes, e))),
            }
        }

        None
    }
}

impl BinaryRecordIterator {
    pub fn new(filename: &str) -> Result<Self, String> {
        let file = match File::open(filename) {
            Ok(f) => f,
            Err(e) => return Err(format!("Cannot read MARC file: {filename} {e}")),
        };

        Ok(BinaryRecordIterator { file })
    }
}

/// bytes => String => usize
fn bytes_to_usize(bytes: &[u8]) -> Result<usize, String> {
    match std::str::from_utf8(bytes) {
        Ok(bytes_str) => match bytes_str.parse::<usize>() {
            Ok(num) => Ok(num),
            Err(e) => Err(format!(
                "Error translating string to usize str={bytes_str} {e}"
            )),
        },
        Err(e) => Err(format!("Error translating bytes to string: {bytes:?} {e}")),
    }
}

/// Models the position/size data for a single, variable-length (control
/// or data) field.
pub struct DirectoryEntry {
    tag: String,
    field_start_idx: usize,
    field_end_idx: usize,
}

impl DirectoryEntry {
    /// Create a new directory entry from a set of bytes and positional
    /// information.
    ///
    /// * `which` - Which entry is this in the directory
    /// * `data_start_idx` - Where in the record as a whole does the data
    ///     we care about start.
    /// * `dir_bytes` - Bytes of this directory entry.
    ///
    /// #REFERENCES
    ///
    /// * <https://www.loc.gov/marc/bibliographic/bddirectory.html>
    pub fn new(which: usize, data_start_idx: usize, dir_bytes: &[u8]) -> Result<Self, String> {
        let start = which * DIRECTORY_ENTRY_LEN;
        let end = start + DIRECTORY_ENTRY_LEN;
        let bytes = &dir_bytes[start..end];

        let entry_str = match std::str::from_utf8(bytes) {
            Ok(s) => s,
            Err(e) => return Err(format!("Invalid directory bytes: {:?} {}", bytes, e)),
        };

        let field_tag = &entry_str[0..3];
        let field_len_str = &entry_str[3..7];
        let field_pos_str = &entry_str[7..12];

        let field_len = match field_len_str.parse::<usize>() {
            Ok(l) => l,
            Err(e) => return Err(format!("Invalid data length value {} {}", field_len_str, e)),
        };

        // Where does this field start in the record as a whole
        let field_start_idx = match field_pos_str.parse::<usize>() {
            Ok(l) => l,
            Err(e) => {
                return Err(format!(
                    "Invalid data position value {} {}",
                    field_pos_str, e
                ));
            }
        };

        let start = field_start_idx + data_start_idx;
        let last = start + field_len - 1; // Discard END_OF_FIELD char

        Ok(DirectoryEntry {
            tag: field_tag.to_string(),
            field_start_idx: start,
            field_end_idx: last,
        })
    }
}

impl Record {
    /// Returns an iterator over MARC records produced from a binary file.
    pub fn from_binary_file(filename: &str) -> Result<BinaryRecordIterator, String> {
        BinaryRecordIterator::new(filename)
    }

    /// Creates a single MARC Record from a series of bytes.
    ///
    /// # References
    ///
    /// * <https://www.loc.gov/marc/bibliographic/bdleader.html>
    /// * <https://www.loc.gov/marc/bibliographic/bddirectory.html>
    pub fn from_binary(rec_bytes: &[u8]) -> Result<Record, String> {
        let mut record = Record::new();

        let rec_byte_count = rec_bytes.len();

        if rec_byte_count < LEADER_SIZE {
            return Err(format!("Binary record is too short: {:?}", rec_bytes));
        }

        let leader_bytes = &rec_bytes[0..LEADER_SIZE];

        // Reported size of the record byte chunk
        let size_bytes = &leader_bytes[0..RECORD_SIZE_ENTRY];

        // Repported size of the record as a number
        let rec_size = bytes_to_usize(size_bytes)?;

        if rec_byte_count != rec_size {
            return Err(format!(
                "Record has incorrect size reported={} real={}",
                rec_size, rec_byte_count
            ));
        }

        record.set_leader_bytes(leader_bytes)?;

        // Where in this pile of bytes do the control/data fields tart.
        let data_offset_bytes =
            &leader_bytes[DATA_OFFSET_START..(DATA_OFFSET_START + DATA_OFFSET_SIZE)];

        let data_start_idx = bytes_to_usize(data_offset_bytes)?;

        // The full directory as bytes.
        // -1 to skip the END_OF_FIELD
        let dir_bytes = &rec_bytes[LEADER_SIZE..(data_start_idx - 1)];

        // Directory byte length should be divisible by the directry entry length.
        let dir_len = dir_bytes.len();
        if dir_len == 0 || dir_len % DIRECTORY_ENTRY_LEN != 0 {
            return Err(format!("Invalid directory length {}", dir_len));
        }

        // How many directory entries are in this record.
        let dir_count = dir_bytes.len() / DIRECTORY_ENTRY_LEN;
        let mut dir_idx = 0;

        while dir_idx < dir_count {
            let dir_entry = DirectoryEntry::new(dir_idx, data_start_idx, dir_bytes)?;

            if let Err(e) = record.process_directory_entry(rec_bytes, rec_byte_count, &dir_entry) {
                return Err(format!(
                    "Error processing directory entry index={} {}",
                    dir_idx, e
                ));
            }

            dir_idx += 1;
        }

        Ok(record)
    }

    /// Unpack a single control field / data field and append to the
    /// record in progress.
    ///
    /// #REFERENCES
    ///
    /// * <https://www.loc.gov/marc/bibliographic/bddirectory.html>
    fn process_directory_entry(
        &mut self,
        rec_bytes: &[u8],      // full record as bytes
        rec_byte_count: usize, // full size of record
        dir_entry: &DirectoryEntry,
    ) -> Result<(), String> {
        if (dir_entry.field_end_idx) >= rec_byte_count {
            return Err(format!(
                "Field length exceeds length of record for tag={}",
                dir_entry.tag
            ));
        }

        // Extract the bytes for this directory entry from the directory.
        let field_bytes = &rec_bytes[dir_entry.field_start_idx..dir_entry.field_end_idx];

        // Turn said bytes into a string
        let field_str = match std::str::from_utf8(field_bytes) {
            Ok(s) => s,
            Err(e) => {
                return Err(format!(
                    "Field data is not UTF-8 compatible: {:?} {}",
                    field_bytes, e
                ));
            }
        };

        if dir_entry.tag.as_str() < "010" {
            let content = if !field_str.is_empty() { field_str } else { "" };

            let cf = Controlfield::new(&dir_entry.tag, content)?;
            self.control_fields_mut().push(cf);
            return Ok(());
        }

        // 3-bytes for tag
        // 1 byte for indicator 1
        // 1 byte for indicator 2
        let mut field = Field::new(&dir_entry.tag)?;

        field.set_ind1(&field_str[..1])?;
        field.set_ind2(&field_str[1..2])?;

        // Split the remainder on the subfield separator and
        // build Field's from them.
        let field_parts: Vec<&str> = field_str.split(SUBFIELD_SEPARATOR).collect();

        for part in &field_parts[1..] {
            // skip the initial SUBFIELD_SEPARATOR
            let sf = Subfield::new(&part[..1], if part.len() > 1 { &part[1..] } else { "" })?;
            field.subfields_mut().push(sf);
        }

        self.fields_mut().push(field);

        Ok(())
    }

    /// Generates the binary form of a MARC record as a vector of bytes.
    ///
    /// # Examples
    /// ```
    /// use marc::Record;
    /// let mut my_record = Record::new();
    /// my_record
    ///     .add_data_field("245")
    ///     .unwrap()
    ///     .add_subfield("a", "My favorite book")
    ///     .unwrap();
    /// assert_eq!(
    ///     my_record.to_binary().unwrap(),
    ///     "00059       00037       245002100000\x1E  \x1FaMy favorite book\x1E\x1D".as_bytes()
    /// );
    /// ```
    pub fn to_binary(&self) -> Result<Vec<u8>, String> {
        let mut bytes: Vec<u8> = Vec::new();

        bytes.append(&mut self.leader().as_bytes().to_vec());

        // Directory
        let num_dirs = self.build_directory(&mut bytes);

        // End-of-field after Directory
        bytes.push(END_OF_FIELD);

        self.add_data_fields(&mut bytes);

        // End-of-record after all data fields are added
        bytes.push(END_OF_RECORD);

        // Make sure the size and data offset for the leader match.
        self.sync_leader(num_dirs, &mut bytes)?;

        Ok(bytes)
    }

    /// Compile the directory entries for the control fields and data fields.
    ///
    /// #REFERENCES
    ///
    /// * <https://www.loc.gov/marc/bibliographic/bddirectory.html>
    fn build_directory(&self, bytes: &mut Vec<u8>) -> usize {
        let mut num_dirs = 0;
        let mut prev_end_idx = 0;

        for field in self.control_fields() {
            num_dirs += 1;

            let mut field_len = field.content().as_bytes().len();

            field_len += 1; // end of field terminator

            // Our directory entry as a string.
            let s = format!(
                "{}{:0w1$}{:0w2$}",
                field.tag(),
                field_len,
                prev_end_idx, // our starting point
                w1 = DATA_LENGTH_SIZE,
                w2 = DATA_OFFSET_SIZE
            );

            bytes.append(&mut s.as_bytes().to_vec());

            prev_end_idx += field_len;
        }

        for field in self.fields() {
            num_dirs += 1;

            let mut field_len = 3; // ind1 + ind2 + field terminator
            for sf in field.subfields() {
                field_len += 2; // sf code + separator
                field_len += sf.content().as_bytes().len();
            }

            // Our directory entry as a string.
            let s = format!(
                "{}{:0w1$}{:0w2$}",
                field.tag(),
                field_len,
                prev_end_idx, // our starting point
                w1 = DATA_LENGTH_SIZE,
                w2 = DATA_OFFSET_SIZE
            );

            bytes.append(&mut s.as_bytes().to_vec());

            prev_end_idx += field_len;
        }

        num_dirs
    }

    /// Appends the binary forms of the control fields and data fields.
    fn add_data_fields(&self, bytes: &mut Vec<u8>) {
        // Now append the actual data
        for field in self.control_fields() {
            bytes.append(&mut field.content().as_bytes().to_vec());
            bytes.push(END_OF_FIELD);
        }

        for field in self.fields() {
            let s = format!("{}{}", field.ind1(), field.ind2());
            bytes.append(&mut s.as_bytes().to_vec());

            for sf in field.subfields() {
                let s = format!("{}{}{}", SUBFIELD_SEPARATOR, sf.code(), sf.content());
                bytes.append(&mut s.as_bytes().to_vec());
            }

            bytes.push(END_OF_FIELD);
        }
    }

    /// Sync the byte count and data offset values in the leader to
    /// match the record just created.
    fn sync_leader(&self, num_dirs: usize, bytes: &mut [u8]) -> Result<(), String> {
        let blen = bytes.len();

        if blen > MAX_RECORD_BYTES {
            return Err(format!(
                "MARC byte count {blen} too large for binary encoding"
            ));
        }

        let size_str = format!("{:0w$}", blen, w = RECORD_SIZE_ENTRY);
        let size_bytes = size_str.as_bytes();

        bytes[0..RECORD_SIZE_ENTRY].copy_from_slice(size_bytes);

        // Set the start index of the body of the record
        let data_start_idx = LEADER_SIZE + (num_dirs * DIRECTORY_ENTRY_LEN) + 1; // end-of-field
        let data_start_str = format!("{:0w$}", data_start_idx, w = DATA_OFFSET_SIZE);

        let dstart = DATA_OFFSET_START;
        let dend = dstart + DATA_OFFSET_SIZE;

        bytes[dstart..dend].copy_from_slice(data_start_str.as_bytes());

        Ok(())
    }
}
