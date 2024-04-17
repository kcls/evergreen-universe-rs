# sip2-mediator-rs

Rust SIP2 Mediator Direct Evergreen API Interface

## About

This is a vairiant of the SIP2 <=> HTTP mediator for Evergreen which 
bypassses the HTTP layer, opting instead to communicate directly
with Evergreen via its API.

Requires Redis.

### See 

* [https://bugs.launchpad.net/evergreen/+bug/1901930](Evergreen_Bug_1901930)
    * This branch must be running on your Evergreen server.
* [https://github.com/berick/SIP2Mediator](https://github.com/berick/SIP2Mediator)
    * The code in this repository effectively replaces the code from 
      the SIP2Mediator repository.

## The Basics (Ubuntu 22.04)

```sh
git clone https://github.com/kcls/evergreen-universe-rs                                  
cd evergreen-universe-rs
make build-sip2mediator-release                                                
sudo make install-sip2mediator-release                                         

# Edit as needed: /usr/local/etc/eg-sip2-mediator.yml

sudo systemctl start eg-sip2-mediator
```

## Additional Configuration

The mediator connects to OpenSRF/Evergreen so it can use the same
Rust-supported environment variables.

For example, create the directory and file: /etc/systemd/system/eg-sip2-mediator.service.d/env.conf

Add to the file:

```conf
[Service]
Environment="OSRF_CONFIG=/openils/conf/opensrf_core.xml"
Environment="OSRF_LOG_FACILITY=LOCAL4"
```

Followed by:

```sh
sudo systemctl daemon-reload
sudo systemctl restart eg-sip2-mediator
```

## Testing

This project comes with a Evergreen-focused end-to-end SIP2 tester.

```sh
cargo run --package sip2mediator --bin sip2-mediator-e2e
```

