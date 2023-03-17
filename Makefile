# Handy builder / installer for the stuff we want to put into /usr/local/

all: 
	cargo build --all

release:
	cargo build --all --release

test:
	cargo test --all

opensrf:
	cargo build --package opensrf

opensrf-release:
	cargo build --release --package opensrf

# NOTE if installing via sudo, root may not have 'cargo' in the path,
# so do your 'make foo' first, then 'sudo make install-foo'

install-opensrf:
	cp ./target/debug/opensrf-router /usr/local/bin
	cp ./target/debug/opensrf-websockets /usr/local/bin

install: install-opensrf

install-opensrf-release:
	cp ./target/release/opensrf-router /usr/local/bin
	cp ./target/release/opensrf-websockets /usr/local/bin

install-release: install-opensrf-release



