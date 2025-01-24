//! Global Settings
use std::sync::OnceLock;

static DEFAULT_PREFERRED_MESSAGE_SIZE: u32 = 67108864;
static DEFAULT_EXCEPTIONAL_RECORD_SIZE: u32 = 67108864;

static SETTINGS: OnceLock<Settings> = OnceLock::new();

/// https://www.loc.gov/z3950/agency/asn1.html#Options
#[derive(Debug, Default)]
pub struct InitOptions {
    pub search: bool,
    pub presen: bool,
    pub del_set: bool,
    pub resource_report: bool,
    pub trigger_resource_ctrl: bool,
    pub resource_ctrl: bool,
    pub access_ctrl: bool,
    pub scan: bool,
    pub sort: bool,
    pub extended_services: bool,
    pub level1_segmentation: bool,
    pub level2_segmentation: bool,
    pub concurrent_operations: bool,
    pub named_result_sets: bool,
}

impl InitOptions {
    /// Returns the option values sorted/positioned for building a BitString.
    pub fn as_sorted_values(&self) -> [bool; 15] {
        [
            self.search,
            self.presen,
            self.del_set,
            self.resource_report,
            self.trigger_resource_ctrl,
            self.resource_ctrl,
            self.access_ctrl,
            self.scan,
            self.sort,
            false, // Slot 9 is reserved
            self.extended_services,
            self.level1_segmentation,
            self.level2_segmentation,
            self.concurrent_operations,
            self.named_result_sets,
        ]
    }
}

#[derive(Debug)]
pub struct Settings {
    pub implementation_id: Option<String>,
    pub implementation_name: Option<String>,
    pub implementation_version: Option<String>,
    pub preferred_message_size: u32,
    pub exceptional_record_size: u32,
    pub init_options: InitOptions,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            implementation_id: None,
            implementation_name: None,
            implementation_version: None,
            preferred_message_size: DEFAULT_PREFERRED_MESSAGE_SIZE,
            exceptional_record_size: DEFAULT_EXCEPTIONAL_RECORD_SIZE,
            init_options: InitOptions::default(),
        }
    }
}

impl Settings {
    pub fn global() -> &'static Settings {
        if SETTINGS.get().is_none() {
            SETTINGS.set(Settings::default()).unwrap();
        }

        SETTINGS.get().unwrap()
    }

    /// Take ownership of a Settings instance and store it globally.
    ///
    /// May only be called once globally.
    ///
    /// # Panics
    ///
    /// Panics if called more than once.
    pub fn apply(self) {
        if SETTINGS.set(self).is_err() {
            panic!("Global Settings already applied");
        }
    }
}
