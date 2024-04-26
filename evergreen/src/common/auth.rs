use crate as eg;
use eg::auth;
use eg::common::settings::Settings;
use eg::date;
use eg::osrf::sclient::HostSettings;
use eg::Editor;
use eg::EgResult;

/// Returns the auth session duration in seconds for the provided
/// login type and context org unit(s) and host settings.
pub fn get_auth_duration(
    editor: &mut Editor,
    org_id: i64,
    user_home_ou: i64,
    host_settings: &HostSettings,
    auth_type: &auth::AuthLoginType,
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

    let mut interval = settings.get_value(setting_name)?;

    if interval.is_null() && user_home_ou != org_id {
        // If the provided context org unit has no setting, see if
        // a setting is applied to the user's home org unit.
        settings.set_org_id(user_home_ou);
        interval = settings.get_value(setting_name)?;
    }

    if interval.is_null() {
        // No org unit setting.  Use the default.

        let setkey =
            format!("apps/open-ils.auth_internal/app_settings/default_timeout/{auth_type}");

        interval = host_settings.value(&setkey);
    }

    if let Some(num) = interval.as_int() {
        Ok(num)
    } else if let Some(s) = interval.as_str() {
        date::interval_to_seconds(&s)
    } else {
        Ok(0)
    }
}
