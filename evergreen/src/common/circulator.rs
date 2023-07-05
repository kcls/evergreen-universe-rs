use crate::editor::Editor;
use crate::event::EgEvent;
use crate::settings::Settings;
use crate::util;
use json::JsonValue;
use std::collections::HashMap;
use std::fmt;

/// Context and shared methods for circulation actions.
///
/// Innards are 'pub' since the impl's are spread across multiple files.
pub struct Circulator {
    pub editor: Editor,
    pub settings: Settings,
    pub exit_early: bool,
    pub circ_lib: i64,
    pub events: Vec<EgEvent>,
    pub copy: Option<JsonValue>,
    pub circ: Option<JsonValue>,
    pub patron: Option<JsonValue>,
    pub transit: Option<JsonValue>,
    pub is_noncat: bool,
    pub options: HashMap<String, JsonValue>,
}

impl fmt::Display for Circulator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut patron_barcode = String::from("null");
        let mut copy_barcode = String::from("null");
        let mut copy_status = -1;

        if let Some(p) = &self.patron {
            if let Some(bc) = &p["card"]["barcode"].as_str() {
                patron_barcode = bc.to_string();
            }
        }

        if let Some(c) = &self.copy {
            if let Some(bc) = &c["barcode"].as_str() {
                copy_barcode = bc.to_string()
            }
            if let Ok(s) = util::json_int(&c["status"]) {
                copy_status = s;
            }
        }

        write!(
            f,
            "Circulator copy={} copy_status={} patron={}",
            copy_barcode, patron_barcode, copy_status
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
            transit: None,
            is_noncat: false,
        })
    }

    /// Unchecked copy access method.
    ///
    /// Panics if copy is unset.
    pub fn copy(&self) -> &JsonValue {
        self.copy.as_ref().unwrap()
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
        self.add_event_code(code);
        self.exit_early = true;
        Err(format!("Bailing on event: {code}"))
    }

    pub fn add_event_code(&mut self, code: &str) {
        self.events.push(EgEvent::new(code));
    }

    /// Search for the copy in question
    fn load_copy(&mut self, maybe_copy_id: Option<JsonValue>) -> Result<(), String> {
        let copy_flesh = json::object! {
            flesh: 1,
            flesh_fields: {
                acp: ["call_number", "parts", "floating"],
                acn: ["record"], // TODO do we really need the whole record?
            }
        };

        // Guessing there's a oneline for this kind of thing.
        let copy_id_op = match maybe_copy_id {
            Some(id) => Some(id),
            None => match self.options.get("copy_id") {
                Some(id2) => Some(id2.clone()),
                None => None,
            }
        };

        if let Some(copy_id) = copy_id_op {
            let query = json::object! {id: copy_id};

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

        self.settings.set_org_id(self.circ_lib);
        self.is_noncat = util::json_bool_op(self.options.get("is_noncat"));

        self.load_copy(None)?;
        self.load_patron()?;

        Ok(())
    }

    /// Update our copy with the values provided.
    ///
    /// * `changes` - a JSON Object with key/value copy attributes to update.
    pub fn update_copy(&mut self, changes: JsonValue) -> Result<&JsonValue, String> {
        let mut copy = match self.copy.take() {
            Some(c) => c,
            None => Err(format!("We have no copy to update"))?,
        };

        for (k, v) in changes.entries() {
            copy[k] = v.to_owned();
        }

        self.editor.update(&copy)?;

        let id = copy["id"].clone();

        // Load the updated copy with the usual fleshing.
        self.load_copy(Some(id))?;

        Ok(self.copy.as_ref().unwrap())
    }
}
