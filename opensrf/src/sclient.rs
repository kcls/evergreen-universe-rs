///! Settings Client Module
///
use super::client::Client;
use std::sync::Arc;

const SETTINGS_TIMEOUT: i32 = 10;

pub struct SettingsClient;

impl SettingsClient {
    /// Fetch the host config for our host.
    ///
    /// If force is set, it is passed to opensrf.settings to override
    /// any caching.
    pub fn get_host_settings(client: &Client, force: bool) -> Result<HostSettings, String> {
        let mut ses = client.session("opensrf.settings");

        let mut req = ses.request(
            "opensrf.settings.default_config.get",
            vec![json::from(client.config().hostname()), json::from(force)],
        )?;

        if let Some(s) = req.recv(SETTINGS_TIMEOUT)? {
            Ok(HostSettings { settings: s })
        } else {
            Err(format!("Settings server returned no response!"))
        }
    }
}

/// Read-only wrapper around a JSON blob of server setting values, which
/// provides accessor methods for pulling setting values.
pub struct HostSettings {
    settings: json::JsonValue,
}

impl HostSettings {
    /// Returns the full host settings config as a JsonValue.
    pub fn settings(&self) -> &json::JsonValue {
        &self.settings
    }

    /// Returns the JsonValue at the specified path.
    ///
    /// Panics of the host config has not yet been retrieved.
    ///
    /// E.g. sclient.value("apps/opensrf.settings/unix_config/max_children");
    pub fn value(&self, slashpath: &str) -> &json::JsonValue {
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
