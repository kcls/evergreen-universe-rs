//! Global Implementation Preferences
use std::sync::OnceLock;

// Copied from YAZ
static DEFAULT_PREFERRED_MESSAGE_SIZE: u32 = 67108864;
static DEFAULT_EXCEPTIONAL_RECORD_SIZE: u32 = 67108864;

// Once applied, settings are globally accessible, but cannot change.
static IMPLEMENTATION_PREFS: OnceLock<ImplementationPrefs> = OnceLock::new();

/// Initialization options
///
/// Note the values set to true here are necessarily impacted by what
/// message types this crate supports.
///
/// # References
///
/// * <https://www.loc.gov/z3950/agency/asn1.html#Options>
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
    pub fn as_positioned_values(&self) -> [bool; 15] {
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

/// Implementation preferences specific to each client/server implemenation.
#[derive(Debug)]
pub struct ImplementationPrefs {
    pub implementation_id: Option<String>,
    pub implementation_name: Option<String>,
    pub implementation_version: Option<String>,
    pub preferred_message_size: u32,
    pub exceptional_record_size: u32,
    pub init_options: InitOptions,
}

impl Default for ImplementationPrefs {
    fn default() -> Self {
        ImplementationPrefs {
            implementation_id: None,
            implementation_name: None,
            implementation_version: None,
            preferred_message_size: DEFAULT_PREFERRED_MESSAGE_SIZE,
            exceptional_record_size: DEFAULT_EXCEPTIONAL_RECORD_SIZE,
            init_options: InitOptions::default(),
        }
    }
}

impl ImplementationPrefs {
    /// Returns a reference to the globally applied ImplementationPrefs value.
    pub fn global() -> &'static ImplementationPrefs {
        if IMPLEMENTATION_PREFS.get().is_none() {
            IMPLEMENTATION_PREFS
                .set(ImplementationPrefs::default())
                .unwrap();
        }

        IMPLEMENTATION_PREFS.get().unwrap()
    }

    /// Take ownership of a ImplementationPrefs instance and store it globally.
    ///
    /// May only be called once globally.
    ///
    /// # Panics
    ///
    /// Panics if called more than once.
    pub fn apply(self) {
        if IMPLEMENTATION_PREFS.set(self).is_err() {
            panic!("Global ImplementationPrefs already applied");
        }
    }
}
