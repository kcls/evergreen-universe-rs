# Handy builder / installer for the stuff we want to put into /usr/local/
#
# NOTE if installing via sudo, root may not have 'cargo' in the path,
# so do your 'make foo' first, then 'sudo make install-foo'

TARGET = /usr/local

build: 
	cargo build --all

build-release:
	cargo build --all --release

test:
	cargo test --all

install: install-opensrf install-sip2server

install-release: install-opensrf-release install-sip2server-release

# ---

build-opensrf:
	cargo build --package opensrf

build-opensrf-release:
	cargo build --release --package opensrf

install-opensrf:
	cp ./target/debug/opensrf-router ${TARGET}/bin
	cp ./target/debug/opensrf-websockets ${TARGET}/bin

install-opensrf-release:
	cp ./target/release/opensrf-router ${TARGET}/bin
	cp ./target/release/opensrf-websockets ${TARGET}/bin

# ---

build-sip2server:
	cargo build --package sip2server

build-sip2server-release:
	cargo build --package sip2server --release

install-sip2server:
	cp ./target/debug/eg-sip2-server ${TARGET}/bin

install-sip2server-release:
	cp ./target/release/eg-sip2-server ${TARGET}/bin


