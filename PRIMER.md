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
