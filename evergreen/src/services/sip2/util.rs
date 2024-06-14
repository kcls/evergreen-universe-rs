use crate::session::Session;
use eg::result::EgResult;
use eg::EgValue;
use evergreen as eg;

const PATRON_NAME_PARTS: [&str; 3] = ["first_given_name", "second_given_name", "family_name"];

impl Session {
    /// Extract the title and author info from a copy object.
    ///
    /// Assumes copy is fleshed to the bib with flat_display_entries.
    pub fn get_copy_title_author(
        &self,
        copy: &EgValue,
    ) -> EgResult<(Option<String>, Option<String>)> {
        let mut resp = (None, None);

        if copy["call_number"].id()? == -1 {
            if let Some(title) = copy["dummy_title"].as_str() {
                resp.0 = Some(title.to_string());
            }
            if let Some(author) = copy["dummy_author"].as_str() {
                resp.1 = Some(author.to_string());
            }

            return Ok(resp);
        }

        resp.0 = self.get_bib_display_value(copy, "title");
        resp.1 = self.get_bib_display_value(copy, "author");

        Ok(resp)
    }

    /// Extract the display field value for the provided field (e.g. "title"),
    /// honoring the related SIP setting to override the default display
    /// field when present.
    fn get_bib_display_value(&self, copy: &EgValue, field: &str) -> Option<String> {
        let setting = format!("{field}_display_field");
        let display_entries = &copy["call_number"]["record"]["flat_display_entries"];

        let display_field =
            if let Some(Some(df)) = self.config().settings().get(&setting).map(|v| v.as_str()) {
                df
            } else {
                field
            };

        if let Some(Some(value)) = display_entries
            .members()
            .filter(|e| e["name"].as_str() == Some(display_field))
            .map(|e| e["value"].as_str())
            .next()
        {
            return Some(value.to_string());
        }

        None
    }

    /// Get an org unit (by cache or net) via its ID.
    pub fn org_from_id(&mut self, id: i64) -> EgResult<Option<&EgValue>> {
        if self.org_cache().contains_key(&id) {
            return Ok(self.org_cache().get(&id));
        }

        if let Some(org) = self.editor().retrieve("aou", id)? {
            self.org_cache_mut().insert(id, org);
            return Ok(self.org_cache().get(&id));
        }

        Ok(None)
    }

    /// Get an org unit (by cache or net) via its shortname.
    pub fn org_from_sn(&mut self, sn: &str) -> EgResult<Option<&EgValue>> {
        for (id, org) in self.org_cache() {
            if org["shortname"].as_str().unwrap().eq(sn) {
                return Ok(self.org_cache().get(id));
            }
        }

        let mut orgs = self.editor().search("aou", eg::hash! {"shortname": sn})?;

        if let Some(org) = orgs.pop() {
            let id = org.id()?;
            self.org_cache_mut().insert(id, org);
            return Ok(self.org_cache().get(&id));
        }

        return Ok(None);
    }

    /// Fetch a user account with card fleshed.
    pub fn get_user_and_card(&mut self, user_id: i64) -> EgResult<Option<EgValue>> {
        let ops = eg::hash! {
            "flesh": 1,
            "flesh_fields": {"au": ["card"]}
        };

        self.editor().retrieve_with_ops("au", user_id, ops)
    }

    /// Format a patron name for display.
    pub fn format_user_name(&self, user: &EgValue) -> String {
        let mut name = String::new();

        // Reverse priority of pref name vs non-pref name.
        // This likely affects only KCLS.
        let inverse = self.config().setting_is_true("patron_inverse_pref_names");

        for part in PATRON_NAME_PARTS {
            let name_op = if inverse {
                user[part]
                    .as_str()
                    .or_else(|| user[&format!("pref_{part}")].as_str())
            } else {
                user[&format!("pref_{part}")]
                    .as_str()
                    .or_else(|| user[part].as_str())
            };

            if let Some(n) = name_op {
                if !n.is_empty() {
                    if !name.is_empty() {
                        name.push(' ');
                    }
                    name.push_str(n);
                }
            }
        }

        name
    }

    /// Format an address as a single line value
    pub fn format_address(&self, address: &EgValue) -> String {
        let mut addr = String::new();

        let parts = [
            "street1",
            "street2",
            "city",
            "state",
            "country",
            "post_code",
        ];

        for &part in &parts {
            if let Some(v) = address[part].as_str() {
                if !v.is_empty() {
                    if !addr.is_empty() {
                        addr.push(' ');
                    }
                    addr.push_str(v);
                }
            }
        }

        addr
    }

    /// Add a stat cat value to a message using the provided code.
    pub fn _format_stat_cat_sip_field(
        &self,
        code: &str,
        value: &str,
        format_op: Option<&str>,
    ) -> Option<sip2::Field> {
        if let Some(format) = format_op {
            let flen = format.len();

            // Regex formats are couched in "|" wrappers.
            if flen > 1 && format.starts_with("|") && format.ends_with("|") {
                // Got a regex.
                todo!();
            } else {
                // Non-regex values are assumed to be sprint-style
                // format strings.
                // TODO requires https://docs.rs/sprintf/latest/sprintf/
                todo!();
            }
        }

        if value.len() > 0 {
            Some(sip2::Field::new(code, value))
        } else {
            None
        }
    }
}
