use super::conf::Config;
use super::session::Session;
use evergreen as eg;
use std::net;
use std::net::TcpListener;
use std::net::TcpStream;
use threadpool::ThreadPool;

pub struct Server {
    ctx: eg::init::Context,
    sip_config: Config,
    sesid: usize,
}

impl Server {
    pub fn new(sip_config: Config, ctx: eg::init::Context) -> Server {
        Server {
            ctx,
            sip_config,
            sesid: 0,
        }
    }

    pub fn serve(&mut self) {
        log::info!("SIP2Meditor server staring up");

        let pool = ThreadPool::new(self.sip_config.max_clients());

        let bind = format!("{}:{}", self.sip_config.sip_address(), self.sip_config.sip_port());

        let listener = TcpListener::bind(bind).expect("Error starting SIP server");

        for stream in listener.incoming() {
            let sesid = self.next_sesid();
            match stream {
                Ok(s) => self.dispatch(&pool, s, sesid),
                Err(e) => log::error!("Error accepting TCP connection {}", e),
            }
        }

        log::info!("SIP2Mediator shutting down; waiting for threads to complete");

        pool.join();
    }

    fn next_sesid(&mut self) -> usize {
        self.sesid += 1;
        self.sesid
    }

    /// Pass the new SIP TCP stream off to a thread for processing.
    fn dispatch(&self, pool: &ThreadPool, stream: TcpStream, sesid: usize) {
        log::info!(
            "Accepting new SIP connection; active={} pending={}",
            pool.active_count(),
            pool.queued_count()
        );

        let threads = pool.active_count() + pool.queued_count();
        let maxcon = self.sip_config.max_clients();

        log::debug!("Working thread count = {threads}");

        if threads >= maxcon {
            log::warn!("Max clients={maxcon} reached.  Rejecting new connections");

            if let Err(e) = stream.shutdown(net::Shutdown::Both) {
                log::error!("Error shutting down SIP TCP connection: {}", e);
            }

            return;
        }

        // Hand the stream off for processing.
        let conf = self.sip_config.clone();
        let idl = self.ctx.idl().clone();
        let osrf_config = self.ctx.config().clone();

        pool.execute(move || Session::run(conf, osrf_config, idl, stream, sesid));
    }
}
