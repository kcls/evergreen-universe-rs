//! Host Settings Module
use crate::osrf::conf;
use crate::Client;
use crate::EgResult;
use crate::EgValue;
use std::sync::OnceLock;

const SETTINGS_TIMEOUT: u64 = 10;

/// If we fetch host settings, they will live here.
/// They may be fetched and stored at most one time.
static OSRF_HOST_CONFIG: OnceLock<HostSettings> = OnceLock::new();

/// Read-only wrapper around a JSON blob of server setting values, which
/// provides accessor methods for pulling setting values.
pub struct HostSettings {
    settings: EgValue,
}

impl HostSettings {
    /// True if the host settings have been loaded.
    pub fn is_loaded() -> bool {
        OSRF_HOST_CONFIG.get().is_some()
    }

    /// Fetch the host config for our host and store the result in
    /// our global host settings.
    ///
    pub fn load(client: &Client) -> EgResult<()> {
        let mut ses = client.session("opensrf.settings");

        let mut req = ses.request(
            "opensrf.settings.host_config.get",
            conf::config().hostname(),
        )?;

        if let Some(s) = req.recv_with_timeout(SETTINGS_TIMEOUT)? {
            let sets = HostSettings { settings: s };
            if OSRF_HOST_CONFIG.set(sets).is_err() {
                return Err("Cannot apply host settings more than once".into());
            }

            Ok(())
        } else {
            Err("Settings server returned no response!".into())
        }
    }

    /// Returns the full host settings config as a JsonValue.
    pub fn settings(&self) -> &EgValue {
        &self.settings
    }

    /// Returns the JsonValue at the specified path.
    ///
    /// Panics of the host config has not yet been retrieved.
    ///
    /// E.g. sclient.value("apps/opensrf.settings/unix_config/max_children");
    pub fn get(slashpath: &str) -> EgResult<&EgValue> {
        let hsets = OSRF_HOST_CONFIG
            .get()
            .ok_or_else(|| "Host settings have not been retrieved".to_string())?;

        let mut value = hsets.settings();
        for part in slashpath.split('/') {
            value = &value[part]; // -> JsonValue::Null if key is not found.
        }

        Ok(value)
    }
}
