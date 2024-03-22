# OpenSRF / Evergreen Rust Primer

Currently assumes Ubuntu 22.04.

## Setup

Actions that communicate via OpenSRF require the OpenSRF/Evergreen
Redis branches be installed and running.

Other actions, e.g. eg-marc-export, which communicate via database 
connection do not require special OpenSRF/Evergreen code.

### Optional: Install OpenSRF / Evergreen with Redis

Follow [these instructions](
    https://github.com/berick/evergreen-ansible-installer/tree/working/ubuntu-22.04-redis)
for installing OpenSRF / Evergreen with Redis.

### Setup Rust

```sh
sudo apt install rust-all 
git clone github.com:kcls/evergreen-universe-rs                                
```

## Build Everything and Run Tests

### Makefile Note

Build and install commands are compiled into a Makefile for convenience
and documentation.  See the Makefile for individual `cargo` commands.

### Build and Test

```sh
cd evergreen-universe-rs

# This will also download and compile dependencies.
make build
make test
```

## Example: Running egsh ("eggshell") -- Requires Redis

egsh is an Evergreen-aware srfsh clone (more or less)

```sh
cargo run --package evergreen --bin egsh
```

### Some Commands

```sh
egsh# help

egsh# login admin demo123

# This uses the authtoken stored from last successful use of 'login'.
egsh# reqauth open-ils.pcrud open-ils.pcrud.retrieve.au 1

egsh# req opensrf.settings opensrf.system.echo {"a b c":123} "12" [1,2,3]

egsh# cstore retrieve actor::user 1

egsh# cstore search aou {"shortname":"BR1"}
```


