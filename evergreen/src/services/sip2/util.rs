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
        &mut self,
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

        let title_setting = "title_display_field";
        let author_setting = "author_display_field";

        let mut title_field_setting = None;
        let mut author_field_setting = None;

        if let Some(Some(field)) = self
            .config()
            .settings()
            .get(title_setting)
            .map(|v| v.as_str())
        {
            title_field_setting = Some(field.to_string());
        }

        if let Some(Some(field)) = self
            .config()
            .settings()
            .get(author_setting)
            .map(|v| v.as_str())
        {
            author_field_setting = Some(field.to_string());
        }

        let title_field = title_field_setting.as_deref().unwrap_or("title");
        let author_field = author_field_setting.as_deref().unwrap_or("author");

        let query = eg::hash! {
            "source": copy["call_number"]["record"].int()?,
            "name": [title_field, author_field],
        };

        let mut entries = self.editor().search("mfde", query)?;

        for entry in entries.iter_mut() {
            if entry["name"].str()? == title_field {
                resp.0 = entry["value"].take_string();
            } else if entry["name"].str()? == author_field {
                resp.1 = entry["value"].take_string();
            }
        }

        Ok(resp)
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

        Ok(None)
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
            if flen > 1 && format.starts_with('|') && format.ends_with('|') {
                // Got a regex.
                todo!();
            } else {
                // Non-regex values are assumed to be sprint-style
                // format strings.
                // TODO requires https://docs.rs/sprintf/latest/sprintf/
                todo!();
            }
        }

        if !value.is_empty() {
            Some(sip2::Field::new(code, value))
        } else {
            None
        }
    }
}
