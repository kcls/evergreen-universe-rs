use eg::common::auth;
use eg::samples::SampleData;
use eg::Editor;
use eg::EgResult;
use eg::Client;
use evergreen as eg;
use std::time::SystemTime;

pub struct Timer {
    start: Option<SystemTime>,
}

impl Timer {
    pub fn new() -> Timer {
        Timer {
            //start: SystemTime::now(),
            start: None,
        }
    }

    pub fn start(&mut self) {
        self.start = Some(SystemTime::now());
    }

    pub fn stop(&mut self, msg: &str) {
        let start = match self.start {
            Some(s) => s,
            None => {
                eprintln!("Cannot call finish on an un-started Timer");
                return;
            }
        };

        let duration = start.elapsed().unwrap().as_micros();

        // translate micros to millis retaining 3 decimal places.
        let millis = (duration as f64) / 1000.0;

        println!("OK [{:.3} ms]\t{msg}", millis);

        self.start = None;
    }
}

pub struct Tester {
    pub client: Client,
    pub editor: Editor,
    pub samples: SampleData,
    pub timer: Timer,
}

/// Login and augment the Tester's Editor with the authtoken.
pub fn login(tester: &mut Tester) -> EgResult<()> {
    let mut args = auth::InternalLoginArgs::new(eg::samples::AU_STAFF_ID, auth::LoginType::Staff);
    args.org_unit = Some(tester.samples.aou_id);

    let auth_ses = match auth::Session::internal_session_api(&tester.client, &args)? {
        Some(s) => s,
        None => return Err("Login failed".into()),
    };

    tester.editor.set_authtoken(auth_ses.token());

    // Set the 'requestor' object.
    if !tester.editor.checkauth()? {
        return Err("Our authtoken is invalid?".into());
    }

    Ok(())
}
