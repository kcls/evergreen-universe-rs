use crate::editor::Editor;
use crate::event::EgEvent;
use crate::settings::Settings;
use crate::util;
use json::JsonValue;
use std::collections::HashMap;
use std::fmt;

pub struct Circulator {
    editor: Editor,
    settings: Settings,
    exit_early: bool,
    circ_lib: i64,
    events: Vec<EgEvent>,
    copy: Option<JsonValue>,
    circ: Option<JsonValue>,
    patron: Option<JsonValue>,
    is_noncat: bool,
    options: HashMap<String, JsonValue>,
}

impl fmt::Display for Circulator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut patron_barcode = String::from("<unset>");
        let mut copy_barcode = String::from("<unset>");

        if let Some(p) = &self.patron {
            if let Some(bc) = &p["card"]["barcode"].as_str() {
                patron_barcode = bc.to_string();
            }
        }

        if let Some(c) = &self.copy {
            if let Some(bc) = &c["barcode"].as_str() {
                copy_barcode = bc.to_string()
            }
        }

        write!(
            f,
            "Circulator copy={} patron={}",
            copy_barcode, patron_barcode
        )
    }
}

impl Circulator {
    /// Create a new Circulator.
    ///
    ///
    pub fn new(e: Editor, options: HashMap<String, JsonValue>) -> Result<Circulator, String> {
        if e.requestor().is_none() {
            Err(format!("Circulator requires an authenticated requestor"))?;
        }

        let settings = Settings::new(&e);
        let circ_lib = e.requestor_ws_ou();

        Ok(Circulator {
            editor: e,
            settings,
            options,
            circ_lib,
            exit_early: false,
            events: Vec::new(),
            circ: None,
            copy: None,
            patron: None,
            is_noncat: false,
        })
    }

    /// Allows the caller to recover the original editor object after
    /// the circ action has completed.  The caller can use this to
    /// do other stuff with the original editor, make queries, etc.
    /// then finally commit the transaction.
    ///
    /// For code simplicity, replace the value with a cloned Editor
    /// instead of storing the Editor as an Option.
    pub fn take_editor(&mut self) -> Editor {
        let new_e = self.editor.clone();
        std::mem::replace(&mut self.editor, new_e)
    }

    pub fn begin(&mut self) -> Result<(), String> {
        self.editor.xact_begin()
    }

    pub fn commit(&mut self) -> Result<(), String> {
        self.editor.commit()
    }

    pub fn rollback(&mut self) -> Result<(), String> {
        self.editor.rollback()
    }

    /// Returns Result so we can cause early exit on methods.
    pub fn exit_on_event_code(&mut self, code: &str) -> Result<(), String> {
        self.events.push(EgEvent::new(code));
        self.exit_early = true;
        Err(format!("Bailing on event: {code}"))
    }

    /// Search for the copy in question
    fn load_copy(&mut self) -> Result<(), String> {
        let copy_flesh = json::object! {
            flesh: 1,
            flesh_fields: {
                acp: ["call_number", "parts", "floating"],
                acn: ["record"], // TODO do we really need the whole record?
            }
        };

        if let Some(copy_id) = self.options.get("copy_id") {
            let query = json::object! {id: copy_id.clone()};

            if let Some(copy) = self.editor.retrieve_with_ops("acp", query, copy_flesh)? {
                self.copy = Some(copy.to_owned());
            } else {
                self.exit_on_event_code("ASSET_COPY_NOT_FOUND")?;
            }
        } else if let Some(copy_barcode) = self.options.get("copy_barcode") {
            // Non-cataloged items are assumed to not exist.
            if !self.is_noncat {
                let query = json::object! {
                    barcode: copy_barcode.clone(),
                    deleted: "f", // cstore turns false into NULL :\
                };

                if let Some(copy) = self
                    .editor
                    .search_with_ops("acp", query, copy_flesh)?
                    .first()
                {
                    self.copy = Some(copy.to_owned());
                } else {
                    self.exit_on_event_code("ASSET_COPY_NOT_FOUND")?;
                }
            }
        }

        Ok(())
    }

    /// Find the requested patron if possible.
    ///
    /// Also sets a value for self.circ if needed to find the patron.
    fn load_patron(&mut self) -> Result<(), String> {
        if let Some(patron_id) = self.options.get("patron_id") {
            let flesh = json::object! {
                flesh: 1,
                flesh_fields: {
                    au: ["card"]
                }
            };

            if let Some(patron) = self.editor.retrieve_with_ops("au", patron_id, flesh)? {
                self.patron = Some(patron.to_owned());
            } else {
                self.exit_on_event_code("ACTOR_USER_NOT_FOUND")?;
            }
        } else if let Some(patron_barcode) = self.options.get("patron_barcode") {
            let query = json::object! {barcode: patron_barcode.clone()};
            let flesh = json::object! {
                flesh: 1,
                flesh_fields: {
                    "ac": ["usr"]
                }
            };

            if let Some(card) = self.editor.search_with_ops("ac", query, flesh)?.first() {
                let mut card = card.to_owned();

                let mut patron = card["usr"].take();
                card["usr"] = patron["id"].clone(); // de-flesh card->user
                patron["card"] = card; // flesh user->card
            } else {
                self.exit_on_event_code("ACTOR_USER_NOT_FOUND")?;
            }
        } else if let Some(ref copy) = self.copy {
            // See if we can find the circulation / patron related
            // to the provided copy.

            let query = json::object! {
                target_copy: copy["id"].clone(),
                checkin_time: JsonValue::Null,
            };

            let flesh = json::object! {
                flesh: 2,
                flesh_fields: {
                    circ: ["usr"],
                    au: ["card"],
                }
            };

            if let Some(circ) = self.editor.search_with_ops("circ", query, flesh)?.first() {
                // Flesh consistently
                let mut circ = circ.to_owned();
                let patron = circ["usr"].take();

                circ["usr"] = patron["id"].clone();

                self.patron = Some(patron);
                self.circ = Some(circ);
            }
        }

        Ok(())
    }

    pub fn init(&mut self) -> Result<(), String> {
        if let Some(cl) = self.options.get("circ_lib") {
            self.circ_lib = util::json_int(cl)?;
        }

        self.is_noncat = util::json_bool_op(self.options.get("is_noncat"));

        self.load_copy()?;
        self.load_patron()?;

        Ok(())
    }
}
