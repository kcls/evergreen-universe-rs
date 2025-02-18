# Evergreen Universe / Rust

Rust bindings, libs, and binaries for 
[Evergreen](https://github.com/evergreen-library-system/Evergreen) 
and related projects.

## Included Packages

### MPTC

General purpose threaded server, similar to Perl Net::Server.

### Evergreen

Evergreen + OpenSRF bindings with OpenSRF server, nascent services, and 
other binaries.

[README](./evergreen/README.md)

### MARCTK

Library for reading/writing MARC Binary, MARC XML, and MARC Breaker.

[README](./marctk/README.md)

### SIP2

SIP2 client library

[README](./sip2/README.md)

### SIP2-Mediator

SIP2 Mediator

[README](./sip2-mediator/README.md)

## Evergreen Rust Primer

### Setup

Actions that communicate via OpenSRF require OpenSRF/Evergreen Redis.

#### Install OpenSRF / Evergreen with Redis

#### Ansible Version

Follow [these ansible instructions](
    https://github.com/berick/evergreen-ansible-installer/tree/working/ubuntu-24.04)

to install on a server/VM.

#### Docker Version

Follow [these instructions](https://github.com/mcoia/eg-docker) to create
a Docker container.

#### Setup Rust

##### Install Prereqs

```sh
sudo apt install git build-essential pkg-config libssl-dev
```

##### Install Rust via Rustup

[https://rustup.rs/](https://rustup.rs/)

##### Checkout Code

```sh
git clone https://github.com/kcls/evergreen-universe-rs                              
```

### Build Everything and Run Tests

#### Makefile Note

Build and install commands are compiled into a Makefile for convenience
and documentation.  See the Makefile for individual `cargo` commands.

#### Build and Test

```sh
cd evergreen-universe-rs

# This will also download and compile dependencies.
make build

# Run unit tests
make test

# To also run the live tests.
# These require a locally running Evergreen instance with
# Concerto data.
cargo test --package evergreen --test live -- --ignored --nocapture

# OPTIONAL: Install compiled binaries to /usr/local/bin/
sudo make install-bin

# OPTIONAL: Install compiled binaries and systemd service files.
sudo make install

```

### Example: Running egsh ("eggshell")

`egsh` is an Evergreen-aware srfsh clone

```sh
cargo run --package evergreen --bin egsh
```

> **_NOTE:_** If binaries are installed, the above command may be shortened to just `egsh` (or `/usr/local/bin/egsh`).

#### Some Commands

```sh
egsh# help

egsh# login admin demo123

# This uses the authtoken stored from last successful use of 'login'
# as the implicit first parameter to the pcrud API call.
egsh# reqauth open-ils.pcrud open-ils.pcrud.retrieve.au 1

egsh# req opensrf.settings opensrf.system.echo {"a b c":123} "12" [1,2,3]

egsh# cstore retrieve actor::user 1

egsh# cstore search aou {"shortname":"BR1"}
```


