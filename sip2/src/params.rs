#![allow(dead_code)]

/// Collection of friendly-named SIP request parameters for common tasks.
///
/// This is not a complete set of friendly-ified parameters.  Just a start.
#[derive(Debug, Clone)]
pub struct ParamSet {
    institution: Option<String>,
    terminal_pwd: Option<String>,
    sip_user: Option<String>,
    sip_pass: Option<String>,
    location: Option<String>,
    patron_id: Option<String>,
    patron_pwd: Option<String>,
    item_id: Option<String>,
    start_item: Option<usize>,
    end_item: Option<usize>,

    /// Indicates which position (if any) of the patron summary string
    /// that should be set to 'Y' (i.e. activated).  Only one summary
    /// index may be activated per message.  Positions are zero-based.
    summary: Option<usize>,
}

impl ParamSet {
    pub fn new() -> Self {
        ParamSet {
            institution: None,
            terminal_pwd: None,
            sip_user: None,
            sip_pass: None,
            location: None,
            patron_id: None,
            patron_pwd: None,
            item_id: None,
            start_item: None,
            end_item: None,
            summary: None,
        }
    }

    pub fn institution(&self) -> &Option<String> {
        &self.institution
    }
    pub fn terminal_pwd(&self) -> &Option<String> {
        &self.terminal_pwd
    }
    pub fn sip_user(&self) -> &Option<String> {
        &self.sip_user
    }
    pub fn sip_pass(&self) -> &Option<String> {
        &self.sip_pass
    }
    pub fn location(&self) -> &Option<String> {
        &self.location
    }
    pub fn patron_id(&self) -> &Option<String> {
        &self.patron_id
    }
    pub fn patron_pwd(&self) -> &Option<String> {
        &self.patron_pwd
    }
    pub fn item_id(&self) -> &Option<String> {
        &self.item_id
    }
    pub fn start_item(&self) -> &Option<usize> {
        &self.start_item
    }
    pub fn end_item(&self) -> &Option<usize> {
        &self.end_item
    }
    pub fn summary(&self) -> &Option<usize> {
        &self.summary
    }

    // ---

    pub fn set_institution(&mut self, value: &str) -> &mut Self {
        self.institution = Some(value.to_string());
        self
    }
    pub fn set_terminal_pwd(&mut self, value: &str) -> &mut Self {
        self.terminal_pwd = Some(value.to_string());
        self
    }
    pub fn set_sip_user(&mut self, value: &str) -> &mut Self {
        self.sip_user = Some(value.to_string());
        self
    }
    pub fn set_sip_pass(&mut self, value: &str) -> &mut Self {
        self.sip_pass = Some(value.to_string());
        self
    }
    pub fn set_location(&mut self, value: &str) -> &mut Self {
        self.location = Some(value.to_string());
        self
    }
    pub fn set_patron_id(&mut self, value: &str) -> &mut Self {
        self.patron_id = Some(value.to_string());
        self
    }
    pub fn set_patron_pwd(&mut self, value: &str) -> &mut Self {
        self.patron_pwd = Some(value.to_string());
        self
    }
    pub fn set_item_id(&mut self, value: &str) -> &mut Self {
        self.item_id = Some(value.to_string());
        self
    }
    pub fn set_start_item(&mut self, value: usize) -> &mut Self {
        self.start_item = Some(value);
        self
    }
    pub fn set_end_item(&mut self, value: usize) -> &mut Self {
        self.end_item = Some(value);
        self
    }
    pub fn set_summary(&mut self, value: usize) -> &mut Self {
        self.summary = Some(value);
        self
    }
}
