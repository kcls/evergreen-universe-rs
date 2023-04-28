# Evergreen Universe / Rust

Rust bindings, libs, and binaries for Evergreen and related projects.

## Included Packages

### MPTC

General purpose threaded server, similar to Perl Net::Server.

### OpenSRF

OpenSRF bindings for communicating with OpenSRF services and creating
new OpenSRF services.

[README](./opensrf/README.md)

### Evergreen

Evergreen bindings for IDL parsing, event handling, cstore editor, and more.

[README](./evergreen/README.md)

### MARC

Library for reading/writing MARC Binary, MARC XML, and MARC Breaker.

[README](./marc/README.md)

### SIP2

SIP2 client library

[README](./sip2/README.md)

### SIP2-Server

SIP2 server custom built for Evergreen.

[README](./sip2-server/README.md)

## Quick Start

### Packages are collected into a single Rust workspace.

The workspace has no default members.  Individual packages must be 
specified at build time or they can all be built with the --all option.

```sh
# Install rust tools 
sudo apt install rust-all

# Checkout the repo
git clone github.com:kcls/evergreen-universe-rs
cd evergreen-universe-rs

# Build all packages
cargo build --all

# Run all tests
cargo test --all

# Build the OpenSRF bits
cargo build --package opensrf

# Build the OpenSRF Router
cargo build --package opensrf --bin opensrf-router

# Run the SIP2 Server
cargo run --package sip2server --bin eg-sip2-server
```


