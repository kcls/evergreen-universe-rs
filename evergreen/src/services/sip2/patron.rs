use crate::session::Session;
use eg::date;
use eg::result::EgResult;
use eg::EgValue;
use evergreen as eg;

const EG_NULL: EgValue = EgValue::Null;
const DEFAULT_LIST_ITEM_SIZE: usize = 10;

#[derive(Debug, Clone, PartialEq)]
pub enum Msg64HoldDatatype {
    Barcode,
    Title,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Msg64SummaryDatatype {
    Barcode,
    Title,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AvFormat {
    Legacy,
    SwyerA,
    SwyerB,
    ThreeM,
}

impl From<&str> for AvFormat {
    fn from(s: &str) -> AvFormat {
        match s.to_lowercase().as_str() {
            "eg_legacy" => Self::Legacy,
            "swyer_a" => Self::SwyerA,
            "swyer_b" => Self::SwyerB,
            "3m" => Self::ThreeM,
            _ => Self::Legacy,
        }
    }
}

/// SIP clients can request detail info for specific types of data.
/// These are the options.
#[derive(Debug, Clone)]
pub enum SummaryListType {
    HoldItems,
    UnavailHoldItems,
    ChargedItems,
    OverdueItems,
    FineItems,
    Unsupported,
}

#[derive(Debug, Clone)]
pub struct SummaryListOptions {
    pub list_type: SummaryListType,
    pub start_item: Option<usize>,
    pub end_item: Option<usize>,
}

impl SummaryListOptions {
    pub fn list_type(&self) -> &SummaryListType {
        &self.list_type
    }

    /// Returns zero-based offset from 1-based SIP "start item" value.
    pub fn offset(&self) -> usize {
        if let Some(s) = self.start_item {
            if s > 0 {
                s - 1
            } else {
                0
            }
        } else {
            0
        }
    }

    /// Returns zero-based limit from 1-based SIP "end item" value.
    pub fn limit(&self) -> usize {
        if let Some(e) = self.end_item {
            if e > 0 {
                e - 1
            } else {
                DEFAULT_LIST_ITEM_SIZE
            }
        } else {
            DEFAULT_LIST_ITEM_SIZE
        }
    }
}

#[derive(Debug)]
pub struct Patron {
    pub id: i64,
    pub barcode: String,
    pub charge_denied: bool,
    pub renew_denied: bool,
    pub recall_denied: bool,
    pub holds_denied: bool,
    pub card_lost: bool,
    pub max_overdue: bool,
    pub max_fines: bool,
    pub recall_overdue: bool,
    pub max_bills: bool,
    pub valid: bool,
    pub card_active: bool,
    pub balance_owed: f64,
    pub password_verified: bool,
    pub recall_count: usize,
    pub holds_count: usize,
    pub hold_ids: Vec<i64>,
    pub unavail_hold_ids: Vec<i64>,
    pub unavail_holds_count: usize,
    pub items_overdue_count: usize,
    pub items_overdue_ids: Vec<i64>,
    pub fine_count: usize,
    pub items_out_count: usize,
    pub items_out_ids: Vec<i64>,
    /// May contain holds, checkouts, overdues, or fines depending
    /// on the patron info summary string.
    pub detail_items: Option<Vec<String>>,
    pub name: String,
    pub address: Option<String>,
    pub email: Option<String>,
    pub home_lib: Option<String>,
    pub dob: Option<String>,
    pub expire_date: Option<String>,
    pub net_access: Option<String>,
    pub profile: Option<String>,
    pub phone: Option<String>,
}

impl Patron {
    pub fn new(barcode: &str, name: String) -> Patron {
        Patron {
            id: 0,
            name,
            barcode: barcode.to_string(),
            charge_denied: false,
            renew_denied: false,
            recall_denied: false,
            holds_denied: false,
            card_lost: false,
            max_overdue: false,
            max_fines: false,
            recall_overdue: false,
            max_bills: false,
            valid: false,
            card_active: false,
            balance_owed: 0.0,
            password_verified: false,
            recall_count: 0,
            holds_count: 0,
            unavail_holds_count: 0,
            items_overdue_count: 0,
            items_out_count: 0,
            fine_count: 0,
            hold_ids: Vec::new(),
            unavail_hold_ids: Vec::new(),
            items_overdue_ids: Vec::new(),
            items_out_ids: Vec::new(),
            detail_items: None,
            address: None,
            email: None,
            home_lib: None,
            dob: None,
            expire_date: None,
            net_access: None,
            profile: None,
            phone: None,
        }
    }
}

impl Session {
    pub fn get_patron_details(
        &mut self,
        barcode: &str,
        password_op: Option<&str>,
        summary_list_options: Option<&SummaryListOptions>,
    ) -> EgResult<Option<Patron>> {
        log::info!("{self} SIP patron details for {barcode}");

        let user = match self.get_user(barcode)? {
            Some(u) => u,
            None => {
                log::warn!("{self} No such patron: {barcode}");
                return Ok(None);
            }
        };

        let mut patron = Patron::new(barcode, self.format_user_name(&user));

        patron.id = user.id()?;
        patron.password_verified = self.check_password(patron.id, password_op)?;

        if let Some(summary) = self.editor().retrieve("mous", patron.id)? {
            patron.balance_owed = summary["balance_owed"].float()?;
        }

        if user["billing_address"].is_object() {
            patron.address = Some(self.format_address(&user["billing_address"]));
        } else if user["mailing_address"].is_object() {
            patron.address = Some(self.format_address(&user["mailing_address"]));
        }

        if let Some(email) = user["email"].as_str() {
            if email.len() > 0 {
                patron.email = Some(email.to_string());
            }
        };

        if let Some(sn) = user["home_ou"]["shortname"].as_str() {
            patron.home_lib = Some(sn.to_string());
        }

        // DoB is stored in the database as a YYYY-MM-DD value / no time.
        // SIP wants YYYYMMDD instead.
        if let Some(dob) = user["dob"].as_str() {
            let ymd = dob.replace("-", "");
            patron.dob = Some(ymd);
        }

        if let Some(net) = user["net_access_level"]["name"].as_str() {
            patron.net_access = Some(net.to_string());
        }

        if let Some(profile) = user["profile"]["name"].as_str() {
            patron.profile = Some(profile.to_string());
        }

        let phone = user["day_phone"].as_str().unwrap_or(
            user["evening_phone"]
                .as_str()
                .unwrap_or(user["other_phone"].as_str().unwrap_or("")),
        );

        if phone.len() > 0 {
            patron.phone = Some(phone.to_string());
        }

        if let Some(expire) = user["expire_date"].as_str() {
            if let Ok(date) = date::parse_datetime(expire) {
                patron.expire_date = Some(date.format("%Y%m%d").to_string());
            }
        }

        self.set_patron_privileges(&user, &mut patron)?;
        self.set_patron_summary_items(&mut patron)?;

        if let Some(ops) = summary_list_options {
            self.set_patron_summary_list_items(&mut patron, ops)?;
        }

        self.log_activity(patron.id)?;

        Ok(Some(patron))
    }

    fn log_activity(&mut self, patron_id: i64) -> EgResult<()> {
        let who = self.sip_account()["activity_who"]
            .as_str()
            .unwrap_or("sip2");

        let query = eg::hash! {
            from: [
                "actor.insert_usr_activity",
                patron_id,
                who,
                "verify",
                "sip2", // ingress
            ]
        };

        self.editor().xact_begin()?;

        let resp = self.editor().json_query(query)?;

        if resp.len() > 0 {
            self.editor().commit()?;
            Ok(())
        } else {
            self.editor().rollback()?;
            Err(format!("Patron activity logging returned no response").into())
        }
    }

    /// Caller wants to see specific values of a given type, e.g. list
    /// of holds for a patron.
    fn set_patron_summary_list_items(
        &mut self,
        patron: &mut Patron,
        summary_ops: &SummaryListOptions,
    ) -> EgResult<()> {
        type SL = SummaryListType; // local shorthand
        match summary_ops.list_type() {
            SL::HoldItems => self.add_hold_items(patron, summary_ops, false)?,
            SL::UnavailHoldItems => self.add_hold_items(patron, summary_ops, true)?,
            SL::ChargedItems => self.add_items_out(patron, summary_ops)?,
            SL::OverdueItems => self.add_overdue_items(patron, summary_ops)?,
            SL::FineItems => self.add_fine_items(patron, summary_ops)?,
            SL::Unsupported => {} // NO-OP not necessarily an error.
        }

        Ok(())
    }

    fn add_fine_items(
        &mut self,
        patron: &mut Patron,
        summary_ops: &SummaryListOptions,
    ) -> EgResult<()> {
        let xacts = self.get_patron_xacts(&patron, Some(summary_ops))?;

        let mut fines: Vec<String> = Vec::new();

        for xact in &xacts {
            fines.push(self.add_fine_item(xact)?);
        }

        patron.detail_items = Some(fines);

        Ok(())
    }

    fn add_fine_item(&mut self, xact: &EgValue) -> EgResult<String> {
        let is_circ = xact["xact_type"].as_str().unwrap().eq("circulation");
        let last_btype = xact["last_billing_type"].as_str().unwrap(); // required

        let xact_id = xact.id()?;
        let balance_owed = xact["balance_owed"].float()?;

        let mut title: Option<String> = None;
        let mut author: Option<String> = None;

        let fee_type = if last_btype.eq("Lost Materials") {
            // XXX ugh @ parsing billing type labels
            // TODO: get the btype from the billing row.
            "LOST"
        } else if last_btype.starts_with("Overdue") {
            "FINE"
        } else {
            "FEE"
        };

        if is_circ {
            (title, author) = self.get_circ_title_author(xact_id)?;
        }

        let mut line: String;
        let title = title.as_deref().unwrap_or("");
        let author = author.as_deref().unwrap_or("");

        let av_format = match self.config().settings().get("av_format") {
            Some(f) => f.str()?.into(),
            None => "".into(),
        };

        match av_format {
            AvFormat::Legacy => {
                line = format!("{:.2} {}", balance_owed, last_btype);
                if is_circ {
                    line += &format!(" {} / {}", title, author);
                }
            }

            AvFormat::ThreeM | AvFormat::SwyerA => {
                line = format!("{} ${} \"{}\" ", xact_id, balance_owed, fee_type);

                if is_circ {
                    line += title;
                } else {
                    line += last_btype;
                }
            }

            AvFormat::SwyerB => {
                line = format!(
                    "Charge-Number: {}, Amount-Due: {:.2}, Fine-Type: {}",
                    xact_id, balance_owed, fee_type
                );

                if is_circ {
                    line += &format!(", Title: {}", title);
                } else {
                    line += &format!(", Title: {}", last_btype);
                }
            }
        }

        Ok(line)
    }

    fn get_circ_title_author(&mut self, id: i64) -> EgResult<(Option<String>, Option<String>)> {
        let flesh = eg::hash! {
            flesh: 4,
            flesh_fields: {
                circ: ["target_copy"],
                acp: ["call_number"],
                acn: ["record"],
                bre: ["simple_record"]
            }
        };

        let circ = self.editor().retrieve_with_ops("circ", id, flesh)?.unwrap();

        self.get_copy_title_author(&circ["target_copy"])
    }

    fn add_items_out(
        &mut self,
        patron: &mut Patron,
        summary_ops: &SummaryListOptions,
    ) -> EgResult<()> {
        let all_circ_ids: Vec<&i64> =
            [patron.items_overdue_ids.iter(), patron.items_out_ids.iter()]
                .into_iter()
                .flatten()
                .collect();

        let offset = summary_ops.offset();
        let limit = summary_ops.limit();

        let mut circs: Vec<String> = Vec::new();

        for idx in offset..(offset + limit) {
            if let Some(id) = all_circ_ids.get(idx) {
                circs.push(self.circ_id_to_value(**id)?);
            }
        }

        patron.detail_items = Some(circs);

        Ok(())
    }

    fn add_overdue_items(
        &mut self,
        patron: &mut Patron,
        summary_ops: &SummaryListOptions,
    ) -> EgResult<()> {
        let offset = summary_ops.offset();
        let limit = summary_ops.limit();

        let mut circs: Vec<String> = Vec::new();

        for idx in offset..(offset + limit) {
            if let Some(id) = patron.items_overdue_ids.get(idx) {
                circs.push(self.circ_id_to_value(*id)?);
            }
        }

        patron.detail_items = Some(circs);

        Ok(())
    }

    fn circ_id_to_value(&mut self, id: i64) -> EgResult<String> {
        let mut format = Msg64SummaryDatatype::Barcode;

        if let Some(set) = self.config().settings().get("msg64_summary_datatype") {
            if set.str()?.starts_with("t") {
                format = Msg64SummaryDatatype::Title;
            }
        }

        if format == Msg64SummaryDatatype::Barcode {
            let flesh = eg::hash! {
                flesh: 1,
                flesh_fields: {circ: ["target_copy"]},
            };

            // If we have a circ ID, we have to have a circ.
            let circ = self.editor().retrieve_with_ops("circ", id, flesh)?.unwrap();

            // If we have a circ, we have to have copy barcode.
            let bc = circ["target_copy"]["barcode"].as_str().unwrap();

            return Ok(bc.to_string());
        }

        let (title, _) = self.get_circ_title_author(id)?;

        if let Some(t) = title {
            Ok(t)
        } else {
            Ok(String::new()) // unlikely, but not impossible
        }
    }

    /// Collect details on holds.
    fn add_hold_items(
        &mut self,
        patron: &mut Patron,
        summary_ops: &SummaryListOptions,
        unavail: bool,
    ) -> EgResult<()> {
        let mut format = Msg64HoldDatatype::Barcode;

        if let Some(set) = self.config().settings().get("msg64_hold_datatype") {
            if set.str()?.starts_with("t") {
                format = Msg64HoldDatatype::Title;
            }
        }

        let hold_ids = match unavail {
            true => &patron.unavail_hold_ids,
            false => &patron.hold_ids,
        };

        let mut trimmed_hold_ids = Vec::new();
        let limit = summary_ops.limit();
        let offset = summary_ops.offset();

        for idx in offset..(offset + limit) {
            if let Some(id) = hold_ids.get(idx) {
                trimmed_hold_ids.push(id);
            }
        }

        let mut hold_items: Vec<String> = Vec::new();

        for hold_id in trimmed_hold_ids {
            if let Some(hold) = self.editor().retrieve("ahr", *hold_id)? {
                if format == Msg64HoldDatatype::Barcode {
                    if let Some(copy) = self.find_copy_for_hold(&hold)? {
                        hold_items.push(copy["barcode"].as_str().unwrap().to_string());
                    }
                } else {
                    if let Some(title) = self.find_title_for_hold(&hold)? {
                        hold_items.push(title);
                    }
                }
            }
        }

        patron.detail_items = Some(hold_items);

        Ok(())
    }

    fn find_title_for_hold(&mut self, hold: &EgValue) -> EgResult<Option<String>> {
        let hold_id = hold.id()?;
        let bib_link = match self.editor().retrieve("rhrr", hold_id)? {
            Some(l) => l,
            None => return Ok(None), // shouldn't be happen-able
        };

        let bib_id = bib_link["bib_record"].int()?;
        let search = eg::hash! {
            source: bib_id,
            name: "title",
        };

        let title_fields = self.editor().search("mfde", search)?;

        if let Some(tf) = title_fields.get(0) {
            if let Some(v) = tf["value"].as_str() {
                return Ok(Some(v.to_string()));
            }
        }

        Ok(None)
    }

    fn find_copy_for_hold(&mut self, hold: &EgValue) -> EgResult<Option<EgValue>> {
        if !hold["current_copy"].is_null() {
            // We have a captured copy.  Use it.
            let copy_id = hold["current_copy"].int()?;
            return self.editor().retrieve("acp", copy_id);
        }

        let hold_type = hold["hold_type"].as_str().unwrap(); // required
        let hold_target = hold["target"].int()?;

        if hold_type.eq("C") || hold_type.eq("R") || hold_type.eq("F") {
            // These are all copy-level hold types
            return self.editor().retrieve("acp", hold_target);
        }

        if hold_type.eq("V") {
            // For call number holds, any copy will do.
            return self.get_copy_for_vol(hold_target);
        }

        let mut bre_ids: Vec<i64> = Vec::new();

        if hold_type.eq("M") {
            let search = eg::hash! { metarecord: hold_target };
            let maps = self.editor().search("mmrsm", search)?;
            for map in maps {
                bre_ids.push(map["record"].int()?);
            }
        } else {
            bre_ids.push(hold_target);
        }

        let query = eg::hash! {
            select: {acp: ["id"]},
            from: {acp: "acn"},
            where: {
                "+acp": {deleted: "f"},
                "+acn": {record: bre_ids, deleted: "f"}
            },
            limit: 1
        };

        let copy_id_hashes = self.editor().json_query(query)?;
        if copy_id_hashes.len() > 0 {
            let copy_id = copy_id_hashes[0].int()?;
            return self.editor().retrieve("acp", copy_id);
        }

        Ok(None)
    }

    fn get_copy_for_vol(&mut self, vol_id: i64) -> EgResult<Option<EgValue>> {
        let search = eg::hash! {
            call_number: vol_id,
            deleted: "f",
        };

        let ops = eg::hash! { limit: 1usize };

        let mut copies = self.editor().search_with_ops("acp", search, ops)?;

        Ok(copies.pop())
    }

    fn set_patron_summary_items(&mut self, patron: &mut Patron) -> EgResult<()> {
        self.set_patron_hold_ids(patron, false, None, None)?;
        self.set_patron_hold_ids(patron, true, None, None)?;

        if let Some(summary) = self.editor().retrieve("ocirclist", patron.id)? {
            // overdue and out are packaged as comma-separated ID values.
            let overdue: Vec<i64> = summary["overdue"]
                .as_str()
                .unwrap()
                .split(",")
                .map(|id| id.parse::<i64>().unwrap())
                .filter(|id| id > &0)
                .collect();

            let outs: Vec<i64> = summary["out"]
                .as_str()
                .unwrap()
                .split(",")
                .map(|id| id.parse::<i64>().unwrap())
                .filter(|id| id > &0)
                .collect();

            patron.items_overdue_count = overdue.len();
            patron.items_out_count = outs.len();
            patron.items_overdue_ids = overdue;
            patron.items_out_ids = outs;
        }

        let summaries = self.get_patron_xacts(&patron, None)?;
        patron.fine_count = summaries.len();

        Ok(())
    }

    pub fn get_patron_xacts(
        &mut self,
        patron: &Patron,
        summary_ops: Option<&SummaryListOptions>,
    ) -> EgResult<Vec<EgValue>> {
        let search = eg::hash! {
            usr: patron.id,
            balance_owed: {"<>": 0},
            total_owed: {">": 0},
        };

        let mut ops = eg::hash! {
            order_by: {mbts: "xact_start"}
        };

        if let Some(sum_ops) = summary_ops {
            ops["limit"] = EgValue::from(sum_ops.limit());
            ops["offset"] = EgValue::from(sum_ops.offset());
        }

        self.editor().search_with_ops("mbts", search, ops)
    }

    fn set_patron_hold_ids(
        &mut self,
        patron: &mut Patron,
        unavail: bool,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> EgResult<()> {
        let mut search = eg::hash! {
            "usr": patron.id,
            "fulfillment_time": EG_NULL,
            "cancel_time": EG_NULL,
        };

        if unavail {
            search["-or"] = eg::array! [
              {"current_shelf_lib": EG_NULL},
              {"current_shelf_lib": {"!=": {"+ahr": "pickup_lib"}}}
            ];
        } else if self.config().setting_is_true("msg64_hold_items_available") {
            search["current_shelf_lib"] = eg::hash! {"=": {"+ahr": "pickup_lib"}};
        }

        let mut query = eg::hash! {
            select: {ahr: ["id"]},
            from: "ahr",
            where: {"+ahr": search},
        };

        if let Some(l) = limit {
            query["limit"] = EgValue::from(l);
        }
        if let Some(o) = offset {
            query["offset"] = EgValue::from(o);
        }

        let id_hash_list = self.editor().json_query(query)?;

        for hash in id_hash_list {
            let hold_id = hash.id()?;
            if unavail {
                patron.unavail_hold_ids.push(hold_id);
            } else {
                patron.hold_ids.push(hold_id);
            }
        }

        if unavail {
            patron.unavail_holds_count = patron.unavail_hold_ids.len();
        } else {
            patron.holds_count = patron.hold_ids.len();
        }

        Ok(())
    }

    fn set_patron_privileges(&mut self, user: &EgValue, patron: &mut Patron) -> EgResult<()> {
        let expire_date_str = user["expire_date"].as_str().unwrap(); // required
        let expire_date = date::parse_datetime(&expire_date_str)?;

        if expire_date < eg::date::now() {
            // Patron is expired.  Don't bother checking other penalties, etc.

            patron.charge_denied = true;
            patron.renew_denied = true;
            patron.recall_denied = true;
            patron.holds_denied = true;

            return Ok(());
        }

        if self.config().setting_is_true("patron_status_permit_all") {
            // This setting group allows all patron actions regardless
            // of penalties, fines, etc.
            return Ok(());
        }

        let penalties = self.get_patron_penalties(patron.id)?;

        patron.max_fines = self.penalties_contain(1, &penalties)?; // PATRON_EXCEEDS_FINES
        patron.max_overdue = self.penalties_contain(2, &penalties)?; // PATRON_EXCEEDS_OVERDUE_COUNT
        patron.card_active = user["card"]["active"].boolish();

        let blocked = user["barred"].boolish() || !user["active"].boolish() || !patron.card_active;

        let mut block_tags = String::new();
        for pen in penalties.iter() {
            if let Some(tag) = pen["block_tag"].as_str() {
                block_tags += tag;
            }
        }

        if !blocked && block_tags.len() == 0 {
            // No blocks, etc. left to inspect.  All done.
            return Ok(());
        }

        patron.holds_denied = blocked || block_tags.contains("HOLDS");

        if self.config().setting_is_true("patron_status_permit_loans") {
            // We're going to ignore checkout, renewals blocks for now.
            return Ok(());
        }

        patron.charge_denied = blocked || block_tags.contains("CIRC");
        patron.renew_denied = blocked || block_tags.contains("RENEW");

        // In evergreen, patrons cannot create Recall holds directly, but that
        // doesn't mean they would not have said privilege if the functionality
        // existed.  Base the ability to perform recalls on whether they have
        // checkout and holds privilege, since both would be needed for recalls.
        patron.recall_denied = patron.charge_denied || patron.renew_denied;

        Ok(())
    }

    fn penalties_contain(&self, penalty_id: i64, penalties: &Vec<EgValue>) -> EgResult<bool> {
        for pen in penalties.iter() {
            let pen_id = pen.id()?;
            if pen_id == penalty_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_patron_penalties(&mut self, user_id: i64) -> EgResult<Vec<EgValue>> {
        let ws_org = self.editor().perm_org();

        let search = eg::hash! {
            select: {csp: ["id", "block_list"]},
            from: {ausp: "csp"},
            where: {
                "+ausp": {
                    usr: user_id,
                    "-or": [
                      {stop_date: EG_NULL},
                      {stop_date: {">": "now"}},
                    ],
                    org_unit: {
                        in: {
                            select: {
                                aou: [{
                                    transform: "actor.org_unit_full_path",
                                    column: "id",
                                    result_field: "id",
                                }]
                            },
                            from: "aou",
                            where: {id: ws_org}
                        }
                    }
                }
            }
        };

        self.editor().json_query(search)
    }

    fn get_user(&mut self, barcode: &str) -> EgResult<Option<EgValue>> {
        let search = eg::hash! { barcode: barcode };

        let flesh = eg::hash! {
            flesh: 3,
            flesh_fields: {
                ac: ["usr"],
                au: ["billing_address", "mailing_address", "profile",
                    "stat_cat_entries", "home_ou", "net_access_level"],
                actscecm: ["stat_cat"]
            }
        };

        let mut cards = self.editor().search_with_ops("ac", search, flesh)?;

        if cards.len() == 0 {
            return Ok(None);
        }

        let mut user = cards[0]["usr"].take();
        user["card"] = cards.remove(0);

        Ok(Some(user))
    }

    fn check_password(&mut self, user_id: i64, password_op: Option<&str>) -> EgResult<bool> {
        let password = match password_op {
            Some(p) => p,
            None => return Ok(false),
        };

        log::debug!("{self} verifying password for user ID {user_id}");
        eg::common::user::verify_migrated_password(self.editor(), user_id, password, false)
    }
}
