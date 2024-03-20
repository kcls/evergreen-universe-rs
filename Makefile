# Build and Install Rust Components and Binaries

TARGET = /usr/local
SYSTEMD_DIR = /lib/systemd/system

# Number of test threads to run in parallel.
# Inline doc tests are compiler-heavy so having a limit here
# helps ensure things don't get out of hand ram/cpu/disk-wise.
TEST_THREADS = 8
BUILD_THREADS = 8

build: build-evergreen build-sip2server

build-release: build-evergreen-release build-sip2server-release

test:
	cargo test -j ${BUILD_THREADS} --all -- --test-threads=${TEST_THREADS}

test-evergreen:
	cargo test -j ${BUILD_THREADS} --package evergreen -- --test-threads=${TEST_THREADS}

install: install-evergreen install-sip2server

install-release: install-evergreen-release install-sip2server-release

# --- Evergreen ---

build-evergreen:
	cargo build -j ${BUILD_THREADS} --package evergreen

build-evergreen-release:
	cargo build -j ${BUILD_THREADS} --package evergreen --release

install-evergreen: install-evergreen-config
	#cp ./target/debug/eg-router ${TARGET}/bin
	cp ./target/debug/egsh ${TARGET}/bin
	cp ./target/debug/eg-http-gateway ${TARGET}/bin
	cp ./target/debug/eg-websockets ${TARGET}/bin
	cp ./target/debug/eg-service-rs-actor ${TARGET}/bin
	cp ./target/debug/eg-service-rs-circ ${TARGET}/bin

install-evergreen-release: install-evergreen-config
	#cp ./target/release/eg-router ${TARGET}/bin
	cp ./target/release/egsh ${TARGET}/bin
	cp ./target/release/eg-http-gateway ${TARGET}/bin
	cp ./target/release/eg-websockets ${TARGET}/bin
	cp ./target/release/eg-service-rs-actor ${TARGET}/bin
	cp ./target/release/eg-service-rs-circ ${TARGET}/bin

install-evergreen-config:
	#cp ./systemd/eg-router.service ${SYSTEMD_DIR}/
	cp ./systemd/eg-http-gateway.service ${SYSTEMD_DIR}/
	cp ./systemd/eg-websockets.service ${SYSTEMD_DIR}/
	cp ./systemd/eg-service-rs-actor.service ${SYSTEMD_DIR}/
	cp ./systemd/eg-service-rs-circ.service ${SYSTEMD_DIR}/
	systemctl daemon-reload

# --- SIP2 Server ---

build-sip2server:
	cargo build -j ${BUILD_THREADS} --package sip2server

build-sip2server-release:
	cargo build -j ${BUILD_THREADS} --package sip2server --release

install-sip2server: install-sip2server-config
	cp ./target/debug/eg-sip2-server ${TARGET}/bin

install-sip2server-release: install-sip2server-config
	cp ./target/release/eg-sip2-server ${TARGET}/bin

install-sip2server-config:
	cp ./sip2-server/conf/eg-sip2-server.example.yml ${TARGET}/etc/
	cp ./systemd/eg-sip2-server.service ${SYSTEMD_DIR}/
	systemctl daemon-reload

