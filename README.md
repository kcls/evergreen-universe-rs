# Evergreen Universe / Rust

Rust bindings, libs, and binaries for Evergreen and related projects.

## Included Packages

### OpenSRF

OpenSRF bindings for communicating with OpenSRF services and creating
new OpenSRF services.

### Evergreen

Evergreen bindings for IDL parsing, event handling, cstore editor, and more.

### MARC

MARC Binary, XML, and Breaker parsing and creationg library.

### SIP2

SIP2 client library

### SIP2-Server

SIP2 server custom built for Evergreen.

## Quick Start

### Packages are collected into a single Rust workspace.

The workspace has no default members.  Individual packages must be 
specified at build time or they can all be built with the --all option.

```sh
# Install rust tools in home directory.  (No sudo required).
curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"

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


