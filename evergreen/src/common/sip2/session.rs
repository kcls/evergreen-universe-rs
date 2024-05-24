use crate as eg;
use eg::EgValue;
use eg::EgResult;
use eg::Editor;
use std::collections::HashMap;

/// Supported Messages (BX)
///
/// Currently hard-coded, since it's based on availabilty of
/// functionality in the code, but it could be moved into the database
/// to limit access for specific setting groups.
const INSTITUTION_SUPPORTS: [&'static str; 16] = [
    "Y", // patron status request,
    "Y", // checkout,
    "Y", // checkin,
    "N", // block patron,
    "Y", // acs status,
    "N", // request sc/acs resend,
    "Y", // login,
    "Y", // patron information,
    "N", // end patron session,
    "Y", // fee paid,
    "Y", // item information,
    "N", // item status update,
    "N", // patron enable,
    "N", // hold,
    "Y", // renew,
    "N", // renew all,
];

pub struct Config {
    institution: String,
    supports: [&'static str; 16],
    settings: HashMap<String, EgValue>,
}

pub struct Session<'a> {
    editor: &'a mut Editor,
    seskey: String,
    sip_account: EgValue,
    config: Option<Config>,
}

impl<'a> Session<'a> {
    pub fn new(editor: &'a mut Editor, seskey: &str, sip_account: EgValue) -> Self {
        Session {
            editor,
            seskey: seskey.to_string(),
            sip_account,
            config: None,
        }
    }

    pub fn load_config(&mut self) -> EgResult<()> {
        let flesh = eg::hash! {
            "flesh": 1,
            "flesh_fields": {
                "sipsetg": ["settings"]
            }
        };

        let group = self.editor.retrieve_with_ops(
            "sipsetg",
            self.sip_account["setting_group"].int()?,
            flesh
        )?.ok_or_else(|| self.editor.die_event())?;

        let mut config = Config {
            institution: group["institution"].string()?,
            supports: INSTITUTION_SUPPORTS,
            settings: HashMap::new(),
        };

        for setting in group["settings"].members() {
            config.settings.insert(
                setting["name"].string()?,
                EgValue::parse(setting["value"].str()?)?,
            );
        };

        Ok(())
    }
}
