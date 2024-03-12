///! Settings Client Module
///
use crate::client::Client;
use crate::{EgResult, EgValue};
use std::sync::Arc;

const SETTINGS_TIMEOUT: i32 = 10;

pub struct SettingsClient;

impl SettingsClient {
    /// Fetch the host config for our host.
    ///
    /// If force is set, it is passed to opensrf.settings to override
    /// any caching.
    pub fn get_host_settings(client: &Client, force: bool) -> EgResult<HostSettings> {
        let mut ses = client.session("opensrf.settings");

        let mut req = ses.request(
            "opensrf.settings.host_config.get",
            vec![EgValue::from(client.config().hostname()), EgValue::from(force)],
        )?;

        if let Some(s) = req.recv_with_timeout(SETTINGS_TIMEOUT)? {
            Ok(HostSettings { settings: s })
        } else {
            Err(format!("Settings server returned no response!").into())
        }
    }
}

/// Read-only wrapper around a JSON blob of server setting values, which
/// provides accessor methods for pulling setting values.
pub struct HostSettings {
    settings: EgValue
}

impl HostSettings {
    /// Returns the full host settings config as a JsonValue.
    pub fn settings(&self) -> &EgValue {
        &self.settings
    }

    /// Returns the JsonValue at the specified path.
    ///
    /// Panics of the host config has not yet been retrieved.
    ///
    /// E.g. sclient.value("apps/opensrf.settings/unix_config/max_children");
    pub fn value(&self, slashpath: &str) -> &EgValue {
        let mut value = self.settings();
        for part in slashpath.split("/") {
            value = &value[part]; // -> JsonValue::Null if key is not found.
        }

        value
    }

    pub fn into_shared(self) -> Arc<Self> {
        Arc::new(self)
    }
}
