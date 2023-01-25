use super::session::Session;

// NOTE some of these could live in opensrf.

impl Session {
    /// Translate a number or numeric-string into a number.
    ///
    /// Values returned from the database vary in stringy-ness.
    pub fn parse_id(&self, value: &json::JsonValue) -> Result<i64, String> {
        if let Some(n) = value.as_i64() {
            return Ok(n);
        } else if let Some(s) = value.as_str() {
            if let Ok(n) = s.parse::<i64>() {
                return Ok(n);
            }
        }
        Err(format!("Invalid numeric value: {}", value))
    }

    /// Translate a number or numeric-string into a number.
    ///
    /// Values returned from the database vary in stringy-ness.
    pub fn parse_float(&self, value: &json::JsonValue) -> Result<f64, String> {
        if let Some(n) = value.as_f64() {
            return Ok(n);
        } else if let Some(s) = value.as_str() {
            if let Ok(n) = s.parse::<f64>() {
                return Ok(n);
            }
        }
        Err(format!("Invalid float value: {}", value))
    }

    // The server returns a variety of true-ish values.
    pub fn parse_bool(&self, value: &json::JsonValue) -> bool {
        if let Some(n) = value.as_i64() {
            n != 0
        } else if let Some(s) = value.as_str() {
            s.len() > 0 && (s[..1].eq("t") || s[..1].eq("T"))
        } else if let Some(b) = value.as_bool() {
            b
        } else {
            log::warn!("Unexpected boolean value: {value}");
            false
        }
    }

    /// This one comes up a lot...
    ///
    /// Assumes copy is fleshed out to the bib simple_record.
    pub fn get_copy_title_author(
        &self,
        copy: &json::JsonValue,
    ) -> Result<(Option<String>, Option<String>), String> {
        let mut resp = (None, None);

        if self.parse_id(&copy["call_number"]["id"])? == -1 {
            if let Some(title) = copy["dummy_title"].as_str() {
                resp.0 = Some(title.to_string());
            }
            if let Some(author) = copy["dummy_author"].as_str() {
                resp.1 = Some(author.to_string());
            }

            return Ok(resp);
        }

        let simple_rec = &copy["call_number"]["record"]["simple_record"];

        if let Some(title) = simple_rec["title"].as_str() {
            resp.0 = Some(title.to_string());
        }
        if let Some(author) = simple_rec["author"].as_str() {
            resp.1 = Some(author.to_string());
        }

        Ok(resp)
    }
}
