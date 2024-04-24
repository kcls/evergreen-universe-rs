use crate as eg;
use eg::Editor;
use eg::EgResult;
use eg::auth;
use eg::date;
use eg::osrf::sclient::HostSettings;
use eg::common::settings::Settings;

/// Returns the auth session duration in seconds for the provided
/// login type and context org units.
pub fn get_auth_duration(
    editor: &mut Editor,
    org_id: i64,
    user_home_ou: i64,
    host_settings: &HostSettings,
    auth_type: &auth::AuthLoginType
) -> EgResult<i64> {
    // First look for an org unit setting.

    let setting_name = match auth_type {
        auth::AuthLoginType::Opac => "auth.opac_timeout",
        auth::AuthLoginType::Staff => "auth.staff_timeout",
        auth::AuthLoginType::Temp => "auth.temp_timeout",
        auth::AuthLoginType::Persist => "auth.persistent_login_interval",
    };

    let mut settings = Settings::new(editor);
    settings.set_org_id(org_id);

    let setting = settings.get_value(setting_name)?;
    if !setting.is_null() {
        // .string() because the value could be numeric.
        return date::interval_to_seconds(&setting.string()?);
    }

    if user_home_ou != org_id {
        // If the provided context org unit has no setting, see if
        // a setting is applied to the user's home org unit.
        settings.set_org_id(user_home_ou);

        let setting = settings.get_value(setting_name)?;
        if !setting.is_null() {
            // .string() because the value could be numeric.
            return date::interval_to_seconds(&setting.string()?);
        }
    }

    // No org unit setting.  Use the default.

    let setkey = format!("apps/open-ils.auth_internal/app_settings/default_timeout/{auth_type}");
    let interval = host_settings.value(&setkey).as_str().unwrap_or("0s");

    date::interval_to_seconds(interval)
}

