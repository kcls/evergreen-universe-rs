use eg::common::user;
use eg::osrf::app::ApplicationWorker;
use eg::osrf::message;
use eg::osrf::method::{ParamCount, ParamDataType, StaticMethodDef, StaticParam};
use eg::osrf::session::ServerSession;
use eg::Editor;
use eg::EgResult;
use eg::EgValue;
use evergreen as eg;
use sip2;

// Import our local app module
use crate::app;
use crate::patron::Patron;
use crate::patron::SummaryListOptions;
use crate::patron::SummaryListType;
use crate::session::Config;
use crate::session::Session;

/// List of method definitions we know at compile time.
pub static METHODS: &[StaticMethodDef] = &[StaticMethodDef {
    name: "request",
    desc: "Dispatch a SIP Request",
    param_count: ParamCount::Exactly(2),
    handler: dispatch_sip_request,
    params: &[
        StaticParam {
            name: "Session Key",
            datatype: ParamDataType::String,
            desc: "SIP2 Client Session Key",
        },
        StaticParam {
            name: "sip2::Message",
            datatype: ParamDataType::Object,
            desc: "SIP2 sip2::Message JSON Value",
        },
    ],
}];

pub fn dispatch_sip_request(
    worker: &mut Box<dyn ApplicationWorker>,
    session: &mut ServerSession,
    method: &message::MethodCall,
) -> EgResult<()> {
    message::set_thread_ingress("sip2");

    let worker = app::Sip2Worker::downcast(worker)?;

    let seskey = method.param(0).str()?;

    let sip_msg = sip2::Message::from_json_value(&method.param(1).clone().into_json_value())
        .map_err(|e| format!("Error parsing SIP message: {e}"))?;

    let mut editor = Editor::new(worker.client());

    let msg_code = sip_msg.spec().code;

    if msg_code == "93" {
        let response = handle_login(&mut editor, seskey, sip_msg)?;
        let value = EgValue::from_json_value(response.to_json_value())?;

        return session.respond_complete(value);
    } else if msg_code == "99" {
        let response = handle_sc_status(&mut editor, seskey, sip_msg)?;
        let value = EgValue::from_json_value(response.to_json_value())?;

        return session.respond_complete(value);
    }

    let mut sip_ses = match Session::from_cache(&mut editor, seskey)? {
        Some(s) => s,
        None => {
            if msg_code == "XS" {
                // End-session signal.  May as well handle it gracefully

                let response = sip2::Message::from_code("XT").unwrap();

                let value = EgValue::from_json_value(response.to_json_value())?;
                return session.respond_complete(value);
            }

            return Err(format!("SIP Session not found: {seskey}"))?;
        }
    };

    let response = match msg_code {
        //        "01" => handle_block(&mut sip_ses, sip_msg)?,
        //        "09" => handle_checkin(&mut sip_ses, sip_msg)?,
        "11" => handle_checkout(&mut sip_ses, sip_msg)?,
        //        "15" => handle_hold(&mut sip_ses, sip_msg)?,
        "17" => handle_item_info(&mut sip_ses, sip_msg)?,
        "23" => handle_patron_status(&mut sip_ses, sip_msg)?,
        //        "29" => handle_renew(&mut sip_ses, sip_msg)?,
        "35" => handle_end_patron_session(&mut sip_ses, sip_msg)?,
        //        "37" => handle_payment(&mut sip_ses, sip_msg)?,
        "63" => handle_patron_info(&mut sip_ses, sip_msg)?,
        //        "65" => handle_renew_all(&mut sip_ses, sip_msg)?,
        //        "97" => handle_resend(&mut sip_ses, sip_msg)?,
        "XS" => handle_end_session(&mut sip_ses, sip_msg)?,
        _ => return Err(format!("SIP message {msg_code} not implemented").into()),
    };

    let value = EgValue::from_json_value(response.to_json_value())?;

    session.respond_complete(value)
}

fn handle_login(
    editor: &mut Editor,
    seskey: &str,
    sip_msg: sip2::Message,
) -> EgResult<sip2::Message> {
    // Start with a login-failed response.
    let mut response = sip2::Message::from_ff_values("94", &["0"]).unwrap();

    let sip_username = sip_msg
        .get_field_value("CN")
        .ok_or_else(|| format!("'CN' field required"))?;

    let sip_password = sip_msg
        .get_field_value("CO")
        .ok_or_else(|| format!("'CO' field required"))?;

    let flesh = eg::hash! {
        "flesh": 1,
        "flesh_fields": {
            "sipacc": ["workstation"]
        }
    };

    let query = eg::hash! {
        "sip_username": sip_username,
        "enabled": "t",
    };

    let sip_account = match editor.search_with_ops("sipacc", query, flesh)?.pop() {
        Some(a) => a,
        None => {
            log::warn!("No SIP account for {sip_username}");
            return Ok(response);
        }
    };

    if user::verify_password(editor, sip_account["usr"].int()?, sip_password, "sip2")? {
        let mut session = Session::new(editor, seskey, sip_account)?;
        session.refresh_auth_token()?;
        session.to_cache()?;

        // Set the login succeeded value.
        response.fixed_fields_mut()[0].set_value("1").unwrap();
    } else {
        log::info!("SIP2 login failed for user={sip_username}");
    }

    Ok(response)
}

fn handle_sc_status(
    editor: &mut Editor,
    seskey: &str,
    _sip_msg: sip2::Message,
) -> EgResult<sip2::Message> {
    let mut response = sip2::Message::from_ff_values(
        "98",
        &[
            sip2::util::sip_bool(true),  // online_status
            sip2::util::sip_bool(true),  // checkin_ok
            sip2::util::sip_bool(true),  // checkout_ok
            sip2::util::sip_bool(true),  // acs_renewal_policy
            sip2::util::sip_bool(false), // status_update_ok
            sip2::util::sip_bool(false), // offline_ok
            "999",                       // timeout_period
            "999",                       // retries_allowed
            &sip2::util::sip_date_now(), // transaction date
            "2.00",                      // protocol_version
        ],
    )
    .unwrap();

    if let Some(mut session) = Session::from_cache(editor, seskey)? {
        response.add_field("AO", session.config().institution());
        response.add_field("BX", session.config().supports());

        // The editor on the session will have requestor info.
        let org_id = session.editor().perm_org();

        let org = editor
            .retrieve("aou", org_id)?
            .ok_or_else(|| editor.die_event())?;

        response.add_field("AM", org["name"].str()?);
        response.add_field("AN", org["shortname"].str()?);
    } else {
        // Confirm sc-status-before-login is enabled before continuing.

        let query = eg::hash! {
            "name": "sip.sc_status_before_login_institution",
            "value": {"!=": EgValue::Null},
            "enabled": "t"
        };

        let flag = editor
            .search("cgf", query)?
            .pop()
            .ok_or_else(|| format!("SC Status message requires login"))?;

        response.add_field("AO", flag["value"].str()?);
        response.add_field("BX", Config::default_supports());
    }

    Ok(response)
}

fn handle_item_info(sip_ses: &mut Session, sip_msg: sip2::Message) -> EgResult<sip2::Message> {
    let barcode = sip_msg.get_field_value("AB").unwrap_or("");

    let item = match sip_ses.get_item_details(barcode)? {
        Some(i) => i,

        None => {
            log::info!("{sip_ses} No copy found with barcode: {barcode}");

            return Ok(sip2::Message::from_values(
                "18",
                &[
                    "01",                        // circ status: other/Unknown
                    "01",                        // security marker: other/unknown
                    "01",                        // fee type: other/unknown
                    &sip2::util::sip_date_now(), // transaction date
                ],
                &[("AB", barcode), ("AJ", "")],
            )
            .unwrap());
        }
    };

    let cur_set = sip_ses.config().settings().get("currency");
    let currency = if let Some(cur) = cur_set {
        cur.str()?
    } else {
        "USD"
    };

    let mut resp = sip2::Message::from_values(
        "18",
        &[
            item.circ_status,
            "02", // security marker
            &item.fee_type,
            &sip2::util::sip_date_now(),
        ],
        &[
            ("AB", &item.barcode),
            ("AJ", &item.title),
            ("AP", &item.current_loc),
            ("AQ", &item.permanent_loc),
            ("BG", &item.owning_loc),
            ("CT", &item.destination_loc),
            ("BH", currency),
            ("BV", &format!("{:.2}", item.deposit_amount)),
            ("CF", &format!("{}", item.hold_queue_length)),
            ("CK", &item.media_type),
        ],
    )
    .unwrap();

    resp.maybe_add_field("CM", item.hold_pickup_date.as_deref());
    resp.maybe_add_field("CY", item.hold_patron_barcode.as_deref());
    resp.maybe_add_field("AH", item.due_date.as_deref());

    Ok(resp)
}

fn handle_patron_info(sip_ses: &mut Session, sip_msg: sip2::Message) -> EgResult<sip2::Message> {
    let barcode = sip_msg.get_field_value("AA").unwrap_or("");
    let password_op = sip_msg.get_field_value("AD"); // optional

    let mut start_item = None;
    let mut end_item = None;

    if let Some(s) = sip_msg.get_field_value("BP") {
        if let Ok(v) = s.parse::<usize>() {
            start_item = Some(v);
        }
    }

    if let Some(s) = sip_msg.get_field_value("BQ") {
        if let Ok(v) = s.parse::<usize>() {
            end_item = Some(v);
        }
    }

    // fixed fields are required for correctly formatted messages.
    let summary_ff = &sip_msg.fixed_fields()[2];

    // Position of the "Y" value, of which there should only be 1,
    // indicates which type of extra summary data to include.
    let list_type = match summary_ff.value().find("Y") {
        Some(idx) => match idx {
            0 => SummaryListType::HoldItems,
            1 => SummaryListType::OverdueItems,
            2 => SummaryListType::ChargedItems,
            3 => SummaryListType::FineItems,
            5 => SummaryListType::UnavailHoldItems,
            _ => SummaryListType::Unsupported,
        },
        None => SummaryListType::Unsupported,
    };

    let list_ops = SummaryListOptions {
        list_type: list_type.clone(),
        start_item,
        end_item,
    };

    let patron_op =
        sip_ses.get_patron_details(&barcode, password_op.as_deref(), Some(&list_ops))?;

    let mut resp = patron_response_common(sip_ses, "64", &barcode, patron_op.as_ref())?;

    let patron = match patron_op {
        Some(p) => p,
        None => return Ok(resp),
    };

    resp.maybe_add_field("AQ", patron.home_lib.as_deref());
    resp.maybe_add_field("BF", patron.phone.as_deref());
    resp.maybe_add_field("PB", patron.dob.as_deref());
    resp.maybe_add_field("PA", patron.expire_date.as_deref());
    resp.maybe_add_field("PI", patron.net_access.as_deref());
    resp.maybe_add_field("PC", patron.profile.as_deref());

    if let Some(detail_items) = patron.detail_items {
        let code = match list_type {
            SummaryListType::HoldItems => "AS",
            SummaryListType::OverdueItems => "AT",
            SummaryListType::ChargedItems => "AU",
            SummaryListType::FineItems => "AV",
            SummaryListType::UnavailHoldItems => "CD",
            _ => "",
        };

        detail_items.iter().for_each(|i| resp.add_field(code, i));
    };

    Ok(resp)
}

fn handle_patron_status(sip_ses: &mut Session, sip_msg: sip2::Message) -> EgResult<sip2::Message> {
    let barcode = sip_msg.get_field_value("AA").unwrap_or("");
    let password_op = sip_msg.get_field_value("AD"); // optional

    let patron_op = sip_ses.get_patron_details(&barcode, password_op.as_deref(), None)?;

    patron_response_common(sip_ses, "24", &barcode, patron_op.as_ref())
}

fn patron_response_common(
    sip_ses: &mut Session,
    msg_code: &str,
    barcode: &str,
    patron_op: Option<&Patron>,
) -> EgResult<sip2::Message> {
    let sbool = |v| sip2::util::space_bool(v); // local shorthand
    let sipdate = sip2::util::sip_date_now();

    if patron_op.is_none() {
        log::warn!("Replying to patron lookup for not-found patron");

        let resp = sip2::Message::from_values(
            msg_code,
            &[
                "YYYY          ", // patron status
                "000",            // language
                &sipdate,
                "0000", // holds count
                "0000", // overdue count
                "0000", // out count
                "0000", // fine count
                "0000", // recall count
                "0000", // unavail holds count
            ],
            &[
                ("AO", sip_ses.config().institution()),
                ("AA", barcode),
                ("AE", ""),  // Name
                ("BL", "N"), // valid patron
                ("CQ", "N"), // valid patron password
            ],
        )
        .unwrap();

        return Ok(resp);
    }

    let patron = patron_op.unwrap();

    let summary = format!(
        "{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
        sbool(patron.charge_denied),
        sbool(patron.renew_denied),
        sbool(patron.recall_denied),
        sbool(patron.holds_denied),
        sbool(!patron.card_active),
        " ", // max charged
        sbool(patron.max_overdue),
        " ", // max renewals
        " ", // max claims returned
        " ", // max lost
        sbool(patron.max_fines),
        sbool(patron.max_fines),
        " ", // recall overdue
        sbool(patron.max_fines)
    );

    let cur_set = sip_ses.config().settings().get("currency");
    let currency = if let Some(cur) = cur_set {
        cur.str()?
    } else {
        "USD"
    };

    let mut resp = sip2::Message::from_values(
        msg_code,
        &[
            &summary,
            "000", // language
            &sipdate,
            &sip2::util::sip_count4(patron.holds_count),
            &sip2::util::sip_count4(patron.items_overdue_count),
            &sip2::util::sip_count4(patron.items_out_count),
            &sip2::util::sip_count4(patron.fine_count),
            &sip2::util::sip_count4(patron.recall_count),
            &sip2::util::sip_count4(patron.unavail_holds_count),
        ],
        &[
            ("AO", sip_ses.config().institution()),
            ("AA", barcode),
            ("AE", &patron.name),
            ("BH", currency),
            ("BL", sip2::util::sip_bool(true)), // valid patron
            ("BV", &format!("{:.2}", patron.balance_owed)),
            ("CQ", sip2::util::sip_bool(patron.password_verified)),
            ("XI", &format!("{}", patron.id)),
        ],
    )
    .unwrap();

    resp.maybe_add_field("BD", patron.address.as_deref());
    resp.maybe_add_field("BE", patron.email.as_deref());

    Ok(resp)
}

fn handle_checkout(sip_ses: &mut Session, sip_msg: sip2::Message) -> EgResult<sip2::Message> {
    sip_ses.handle_checkout(&sip_msg)
}

/// This is a no-op.  Just returns the expected response.
fn handle_end_patron_session(
    sip_ses: &mut Session,
    sip_msg: sip2::Message,
) -> EgResult<sip2::Message> {
    let resp = sip2::Message::from_values(
        "36",
        &[sip2::util::sip_bool(true), &sip2::util::sip_date_now()],
        &[
            ("AO", sip_ses.config().institution()),
            ("AA", sip_msg.get_field_value("AA").unwrap_or("")),
        ],
    )
    .unwrap();

    Ok(resp)
}

/// Remove the cached session data and auth data.
///
/// No removal of sip.account here since we never create those.
fn handle_end_session(sip_ses: &mut Session, _sip_msg: sip2::Message) -> EgResult<sip2::Message> {
    sip_ses.editor().clear_auth()?;
    Ok(sip2::Message::from_code("XT").unwrap())
}
