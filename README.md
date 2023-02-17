# Evergreen Universe / Rust

Rust bindings, libs, and binaries for Evergreen and related projects.

## Quick Start

### Packages are collected into a single Rust workspace.

The workspace has no default members.  Individual packages must be 
specified at build time or they can all be built with the --all option.

```sh
# Install rust tools in home directory.  (No sudo required).
curl https://sh.rustup.rs -sSf | sh -s -- -y
source "$HOME/.cargo/env"

# Just build the OpenSRF bits
cargo build --package opensrf # etc.
```

## opensrf

OpenSRF bindings for communicating with and/or acting as an OpenSRF service.

## evergreen

Evergreen bindings for IDL parsing, event handling, cstore editor, and more.

## marc

MARC Binary, XML, and Breaker parsing and creationg library.

## sip2

SIP2 client library

## sip2-server

Threaded SIP2 server custom built for Evergreen.


