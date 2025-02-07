mod server;
mod session;

pub use server::Z39Server;
use crate::message::Message;

/// That which handles [`Message`] requests.
pub trait Z39Worker {
    /// Proces a single [`Message`] and produce a response.
    ///
    /// If an Err(String) is returned, the error is logged and the
    /// remote end is disconnected.
    fn handle_message(&mut self, msg: Message) -> Result<Message, String>; 
}

/// That which produces [`Z39Worker`] instances.  
///
/// This allows [`Z39Session`] to generate worker instances after the
/// worker thread is spawned even though the [`Z39Worker`] itself
/// may not be Send'able.
pub type Z39WorkerGenerator = fn() -> Box<dyn Z39Worker>;
