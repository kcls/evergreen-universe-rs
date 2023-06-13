# OpenSRF / Evergreen Rust Primer

Currently assumes Ubuntu 22.04.

## Setup

### Install OpenSRF / Evergreen with Redis

Follow [these instructions](
    https://github.com/berick/evergreen-ansible-installer/tree/working/ubuntu-22.04-redis)
for installing OpenSRF / Evergreen with Redis.

### Setup Rust

```sh
sudo apt install rust-all 
git clone github.com:kcls/evergreen-universe-rs                                
```

## Build OpenSRF/Evergreen Rust Code

```sh
cd evergreen-universe-rs

# This will also download and compile dependencies.
make build-opensrf build-evergreen
```

## Run Examples

### Basic OpenSRF Client Example

```sh
cargo run --package opensrf --example client-demo
```

### Running egsh ("eggshell")

egsh is an Evergreen-aware srfsh clone (more or less)

```sh
cargo run --package evergreen --bin egsh
```

#### Some Commands

* egsh# help
* egsh# login admin demo123
* egsh# req opensrf.settings opensrf.system.echo {"a b c":123} "12" [1,2,3]

### Connecting egsh to the localhost database

```sh
cargo run --package evergreen --bin egsh -- --with-database
```
#### Some More Commands

* egsh# idl get aou 1
* egsh# idlf get au 1
* egsh# idlf search aou shortname like "BR%"

