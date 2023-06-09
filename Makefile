# Build and Install Rust Components and Binaries

TARGET = /usr/local
SYSTEMD_DIR = /lib/systemd/system

build: build-opensrf build-evergreen build-sip2server

build-release: build-opensrf-release build-evergreen-release build-sip2server-release

test:
	cargo test --all

install: install-opensrf install-evergreen install-sip2server

install-release: install-opensrf-release install-evergreen-release install-sip2server-release

# --- OpenSRF ---

build-opensrf:
	cargo build --package opensrf

build-opensrf-release:
	cargo build --release --package opensrf

install-opensrf: install-opensrf-config
	#cp ./target/debug/opensrf-router ${TARGET}/bin
	cp ./target/debug/opensrf-websockets ${TARGET}/bin

install-opensrf-release: install-opensrf-config
	#cp ./target/release/opensrf-router ${TARGET}/bin
	cp ./target/release/opensrf-websockets ${TARGET}/bin

install-opensrf-config:
	#cp ./systemd/opensrf-router.service ${SYSTEMD_DIR}/
	cp ./systemd/opensrf-websockets.service ${SYSTEMD_DIR}/
	systemctl daemon-reload

# --- Evergreen ---

build-evergreen:
	cargo build --package evergreen

build-evergreen-release:
	cargo build --package evergreen --release

install-evergreen: install-evergreen-config
	cp ./target/debug/egsh ${TARGET}/bin
	cp ./target/debug/eg-http-gateway ${TARGET}/bin
	cp ./target/debug/eg-svc-rspub ${TARGET}/bin

install-evergreen-release: install-evergreen-config
	cp ./target/release/egsh ${TARGET}/bin
	cp ./target/release/eg-http-gateway ${TARGET}/bin
	cp ./target/release/eg-svc-rspub ${TARGET}/bin

install-evergreen-config:
	cp ./systemd/eg-http-gateway.service ${SYSTEMD_DIR}/
	cp ./systemd/eg-svc-rspub.service ${SYSTEMD_DIR}/
	systemctl daemon-reload

# --- SIP2 Server ---

build-sip2server:
	cargo build --package sip2server

build-sip2server-release:
	cargo build --package sip2server --release

install-sip2server: install-sip2server-config
	cp ./target/debug/eg-sip2-server ${TARGET}/bin

install-sip2server-release: install-sip2server-config
	cp ./target/release/eg-sip2-server ${TARGET}/bin

install-sip2server-config:
	cp ./sip2-server/conf/eg-sip2-server.example.yml ${TARGET}/etc/
	cp ./systemd/eg-sip2-server.service ${SYSTEMD_DIR}/
	systemctl daemon-reload

