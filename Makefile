# Handy builder / installer for the stuff we want to put into /usr/local/
TARGET = /usr/local

all: 
	cargo build --all

release:
	cargo build --all --release

test:
	cargo test --all

opensrf:
	cargo build --package opensrf

sip2server:
	cargo build --package sip2server

opensrf-release:
	cargo build --release --package opensrf

sip2server-release:
	cargo build --package sip2server --release

# NOTE if installing via sudo, root may not have 'cargo' in the path,
# so do your 'make foo' first, then 'sudo make install-foo'

install-opensrf:
	cp ./target/debug/opensrf-router ${TARGET}/bin
	cp ./target/debug/opensrf-websockets ${TARGET}/bin

install-sip2server:
	cp ./target/debug/eg-sip2-server ${TARGET}/bin

install-sip2server-release:
	cp ./target/release/eg-sip2-server ${TARGET}/bin

install: install-opensrf install-sip2server

install-opensrf-release:
	cp ./target/release/opensrf-router ${TARGET}/bin
	cp ./target/release/opensrf-websockets ${TARGET}/bin

install-release: install-opensrf-release install-sip2server-release

