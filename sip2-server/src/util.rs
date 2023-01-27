use super::session::Session;
use chrono::prelude::*;
use chrono::DateTime;

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

    pub fn org_id_from_sn(&mut self, sn: &str) -> Result<Option<i64>, String> {
        if let Some(id) = self.org_sn_cache().get(sn) {
            return Ok(Some(*id));
        }

        let orgs = self.editor_mut().search("aou", json::object! {shortname: sn})?;

        if orgs.len() > 0 {
            let org = &orgs[0];
            let id = self.parse_id(&org["id"])?;
            self.org_sn_cache_mut().insert(sn.to_string(), id);
            return Ok(Some(id));
        }

        return Ok(None)
    }

    pub fn org_sn_from_id(&mut self, org_id: i64) -> Result<Option<String>, String> {
        for (sn, id) in self.org_sn_cache() {
            if id == &org_id {
                return Ok(Some(sn.to_string()));
            }
        }

        let org = match self.editor_mut().retrieve("aou", org_id)? {
            Some(o) => o,
            None => return Ok(None)
        };

        let sn = org["shortname"].as_str().unwrap();
        self.org_sn_cache_mut().insert(sn.to_string(), org_id);

        Ok(Some(sn.to_string()))
    }

    /// Panics if this session is not authenticated.
    pub fn get_ws_org_id(&self) -> Result<i64, String> {

        let requestor = self.editor().requestor()
            .ok_or(format!("Editor requestor is unset"))?;

        let mut field = &requestor["ws_ou"];
        if field.is_null() {
            field = &requestor["home_ou"];
        };

        self.parse_id(field)
    }

    pub fn get_user_and_card(&mut self, user_id: i64) -> Result<Option<json::JsonValue>, String> {

        let ops = json::object! {
            flesh: 1,
            flesh_fields: {au: ["card"]}
        };

        self.editor_mut().retrieve_with_ops("au", user_id, ops)
    }

    pub fn format_user_name(&self, user: &json::JsonValue) -> String {
        let mut name = String::new();

        if let Some(n) = user["first_given_name"].as_str() {
            name += n;
        }

        if let Some(n) = user["second_given_name"].as_str() {
            name += &format!(" {n}");
        }

        if let Some(n) = user["first_given_name"].as_str() {
            name += &format!(" {n}");
        }

        name
    }

    /// Create a DateTime from a Postgres date string.
    ///
    /// chrono has a parse_from_rfc3339() function, but it does
    /// not like time zones without colons.  Dates, amiright?
    pub fn parse_pg_date(&self, pg_iso_date: &str) -> Result<DateTime<FixedOffset>, String> {
        DateTime::parse_from_str(pg_iso_date, "%Y-%m-%dT%H:%M:%S%z")
            .or_else(|e| Err(format!("Invalid expire date: {e} {pg_iso_date}")))
    }
}

