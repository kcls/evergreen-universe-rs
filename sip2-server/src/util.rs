use super::session::Session;
use eg::result::EgResult;
use evergreen as eg;

impl Session {
    /// This one comes up a lot...
    ///
    /// Assumes copy is fleshed out to the bib simple_record.
    pub fn get_copy_title_author(
        &self,
        copy: &json::JsonValue,
    ) -> EgResult<(Option<String>, Option<String>)> {
        let mut resp = (None, None);

        if eg::util::json_int(&copy["call_number"]["id"])? == -1 {
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

    pub fn org_from_id(&mut self, id: i64) -> EgResult<Option<&json::JsonValue>> {
        if self.org_cache().contains_key(&id) {
            return Ok(self.org_cache().get(&id));
        }

        if let Some(org) = self.editor_mut().retrieve("aou", id)? {
            self.org_cache_mut().insert(id, org);
            return Ok(self.org_cache().get(&id));
        }

        Ok(None)
    }

    pub fn org_from_sn(&mut self, sn: &str) -> EgResult<Option<&json::JsonValue>> {
        for (id, org) in self.org_cache() {
            if org["shortname"].as_str().unwrap().eq(sn) {
                return Ok(self.org_cache().get(id));
            }
        }

        let orgs = self
            .editor_mut()
            .search("aou", json::object! {shortname: sn})?;

        if orgs.len() > 0 {
            let org = &orgs[0];
            let id = eg::util::json_int(&org["id"])?;
            self.org_cache_mut().insert(id, orgs[0].to_owned());
            return Ok(self.org_cache().get(&id));
        }

        return Ok(None);
    }

    /// Panics if this session is not authenticated.
    pub fn get_ws_org_id(&self) -> EgResult<i64> {
        let requestor = self
            .editor()
            .requestor()
            .ok_or(format!("Editor requestor is unset"))?;

        let mut field = &requestor["ws_ou"];
        if field.is_null() {
            field = &requestor["home_ou"];
        };

        eg::util::json_int(field)
    }

    pub fn get_user_and_card(&mut self, user_id: i64) -> EgResult<Option<json::JsonValue>> {
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

        if let Some(n) = user["family_name"].as_str() {
            name += &format!(" {n}");
        }

        name
    }

    /// Format an address as a single line value
    pub fn format_address(&self, address: &json::JsonValue) -> String {
        let mut addr = String::new();
        if let Some(v) = address["street1"].as_str() {
            addr += v;
        }
        if let Some(v) = address["street2"].as_str() {
            if v.len() > 0 {
                addr += &format!(" {v}");
            }
        }
        if let Some(v) = address["city"].as_str() {
            addr += &format!(" {v}");
        }
        if let Some(v) = address["state"].as_str() {
            if v.len() > 0 {
                addr += &format!(", {v}");
            }
        }
        if let Some(v) = address["country"].as_str() {
            if v.len() > 0 {
                addr += &format!(" {v}");
            }
        }
        if let Some(v) = address["post_code"].as_str() {
            addr += &format!(" {v}");
        }

        addr
    }

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
