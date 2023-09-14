use crate::idl;
use crate::result::EgResult;
use opensrf as osrf;
use std::env;
use std::sync::Arc;

const DEFAULT_IDL_PATH: &str = "/openils/conf/fm_IDL.xml";

#[derive(Clone)]
pub struct Context {
    client: osrf::client::Client,
    config: Arc<osrf::conf::Config>,
    idl: Arc<idl::Parser>,
    host_settings: Option<Arc<osrf::sclient::HostSettings>>,
}

impl Context {
    pub fn client(&self) -> &osrf::client::Client {
        &self.client
    }
    pub fn config(&self) -> &Arc<osrf::conf::Config> {
        &self.config
    }
    pub fn idl(&self) -> &Arc<idl::Parser> {
        &self.idl
    }
    pub fn host_settings(&self) -> Option<&Arc<osrf::sclient::HostSettings>> {
        self.host_settings.as_ref()
    }
}

pub struct InitOptions {
    pub osrf_ops: osrf::init::InitOptions,
    pub skip_host_settings: bool,
}

impl InitOptions {
    pub fn new() -> Self {
        InitOptions {
            osrf_ops: osrf::init::InitOptions::new(),
            skip_host_settings: false,
        }
    }
}

/// Read common command line parameters, parse the core config, apply
/// the primary connection type, and setup logging.
pub fn init() -> EgResult<Context> {
    init_with_options(&InitOptions::new())
}

pub fn init_with_options(options: &InitOptions) -> EgResult<Context> {
    let config = osrf::init::init_with_options(&options.osrf_ops)?;
    let config = config.into_shared();

    let client = osrf::Client::connect(config.clone())
        .or_else(|e| Err(format!("Cannot connect to OpenSRF: {e}")))?;

    // We try to get the IDL path from opensrf.settings, but that will
    // fail if we are not connected to a domain running opensrf.settings
    // (e.g. a public domain).

    let mut idl_file = DEFAULT_IDL_PATH.to_string();
    let mut host_settings: Option<Arc<osrf::sclient::HostSettings>> = None;

    if !options.skip_host_settings {
        if let Ok(s) = osrf::sclient::SettingsClient::get_host_settings(&client, false) {
            if let Some(fname) = s.value("/IDL").as_str() {
                idl_file = fname.to_string();
            }
            host_settings = Some(s.into_shared());
        }
    }

    // Always honor the environment variable if present.
    if let Ok(v) = env::var("EG_IDL_FILE") {
        idl_file = v;
    }

    let idl = idl::Parser::parse_file(&idl_file)
        .or_else(|e| Err(format!("Cannot parse IDL file: {e}")))?;

    client.set_serializer(idl::Parser::as_serializer(&idl));

    Ok(Context {
        client,
        config,
        idl,
        host_settings,
    })
}

/// Create a new connection using pre-compiled context components.  Useful
/// for spawned threads so they can avoid repetitive processing at
/// connect time.
///
/// The only part that must happen in its own thread is the opensrf connect.
pub fn init_from_parts(
    config: Arc<osrf::conf::Config>,
    idl: Arc<idl::Parser>,
    host_settings: Option<Arc<osrf::sclient::HostSettings>>,
) -> EgResult<Context> {
    let client = osrf::Client::connect(config.clone())
        .or_else(|e| Err(format!("Cannot connect to OpenSRF: {e}")))?;

    Ok(Context {
        client,
        config,
        idl,
        host_settings,
    })
}
