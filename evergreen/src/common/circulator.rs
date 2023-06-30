use crate::editor::Editor;
use crate::event::EgEvent;
use crate::util;
use json::JsonValue;
use std::collections::HashMap;

pub struct Circulator {
    editor: Editor,
    exit_early: bool,
    circ_lib: i64,
    events: Vec<EgEvent>,
    copy: Option<JsonValue>,
    patron: Option<JsonValue>,
    options: HashMap<String, JsonValue>,
}

impl Circulator {

    /// Create a new Circulator.
    ///
    ///
    pub fn new(e: Editor, options: HashMap<String, JsonValue>) -> Result<Circulator, String> {
        if e.requestor().is_none() || !e.in_transaction() {
            Err(format!(
                "Circulator requires an authenticated requestor and a transaction"))?;
        }

        let circ_lib = e.requestor_ws_ou();
        Ok(Circulator {
            editor: e,
            options,
            circ_lib,
            exit_early: false,
            events: Vec::new(),
            copy: None,
            patron: None,
        })
    }

    /// Returns Result so we can cause early exit on methods.
    pub fn exit_on_event_code(&mut self, code: &str) -> Result<(), String> {
        self.events.push(EgEvent::new(code));
        self.exit_early = true;
        Err(format!("Bailing on event: {code}"))
    }

    pub fn init(&mut self) -> Result<(), String> {

        if let Some(cl) = self.options.get("circ_lib") {
            self.circ_lib = util::json_int(cl)?;
        }

        if let Some(copy_barcode) = self.options.get("copy_barcode") {
            let query = json::object! {
                barcode: copy_barcode.clone(),
                deleted: false
            };

            if let Some(copy) = self.editor.search("acp", query)?.first() {
                self.copy = Some(copy.to_owned());
            } else {
                self.exit_on_event_code("ASSET_COPY_NOT_FOUND")?;
            }
        }

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

                // consistent fleshing patron -> card.
                let mut patron = card["usr"].take();
                card["usr"] = patron["id"].clone();
                patron["card"] = card;

            } else {
                self.exit_on_event_code("ACTOR_USER_NOT_FOUND")?;
            }
        }

        Ok(())
    }
}
