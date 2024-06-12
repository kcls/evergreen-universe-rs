# Build and Install Rust Components and Binaries

TARGET = /usr/local
SYSTEMD_DIR = /lib/systemd/system

# Number of test threads to run in parallel.
# Inline doc tests are compiler-heavy so having a limit here
# helps ensure things don't get out of hand ram/cpu/disk-wise.
TEST_THREADS = 4
BUILD_THREADS = 4

build: build-evergreen build-sip2mediator

# Removes Cargo artifacts
clean:
	cargo clean

build-release: build-evergreen-release build-sip2mediator-release

test:
	cargo test -j ${BUILD_THREADS} --all -- --test-threads=${TEST_THREADS}

test-evergreen:
	cargo test -j ${BUILD_THREADS} --package evergreen -- --test-threads=${TEST_THREADS}

install: install-evergreen install-sip2mediator

install-bin: install-evergreen-bin install-sip2mediator-bin

install-bin-release: install-evergreen-bin-release install-sip2mediator-bin-release

install-release: install-evergreen-release install-sip2mediator-release

# --- Evergreen ---

build-evergreen:
	cargo build -j ${BUILD_THREADS} --package evergreen

build-evergreen-release:
	cargo build -j ${BUILD_THREADS} --package evergreen --release

install-evergreen: install-evergreen-config install-evergreen-bin

install-evergreen-bin:
	cp ./target/debug/egsh ${TARGET}/bin
	cp ./target/debug/eg-router ${TARGET}/bin
	cp ./target/debug/eg-buswatch ${TARGET}/bin
	cp ./target/debug/eg-http-gateway ${TARGET}/bin
	cp ./target/debug/eg-websockets ${TARGET}/bin
	cp ./target/debug/eg-service-rs-actor ${TARGET}/bin
	cp ./target/debug/eg-service-rs-circ ${TARGET}/bin
	cp ./target/debug/eg-service-rs-sip2 ${TARGET}/bin

install-evergreen-release: install-evergreen-config install-evergreen-bin-release

install-evergreen-bin-release: 
	cp ./target/release/egsh ${TARGET}/bin
	cp ./target/release/eg-router ${TARGET}/bin
	cp ./target/release/eg-buswatch ${TARGET}/bin
	cp ./target/release/eg-http-gateway ${TARGET}/bin
	cp ./target/release/eg-websockets ${TARGET}/bin
	cp ./target/release/eg-service-rs-actor ${TARGET}/bin
	cp ./target/release/eg-service-rs-circ ${TARGET}/bin
	cp ./target/release/eg-service-rs-sip2 ${TARGET}/bin

install-evergreen-config:
	cp ./systemd/eg-router.service ${SYSTEMD_DIR}/
	cp ./systemd/eg-buswatch.service ${SYSTEMD_DIR}/
	cp ./systemd/eg-http-gateway.service ${SYSTEMD_DIR}/
	cp ./systemd/eg-websockets.service ${SYSTEMD_DIR}/
	cp ./systemd/eg-service-rs-actor.service ${SYSTEMD_DIR}/
	cp ./systemd/eg-service-rs-circ.service ${SYSTEMD_DIR}/
	cp ./systemd/eg-service-rs-sip2.service ${SYSTEMD_DIR}/
	systemctl daemon-reload

# --- SIP2 Mediator ---

build-sip2mediator:
	cargo build -j ${BUILD_THREADS} --package sip2mediator

build-sip2mediator-release:
	cargo build -j ${BUILD_THREADS} --package sip2mediator --release

install-sip2mediator: install-sip2mediator-config install-sip2mediator-bin
install-sip2mediator-bin:
	cp ./target/debug/eg-sip2-mediator ${TARGET}/bin

install-sip2mediator-release: install-sip2mediator-config install-sip2mediator-bin-release

install-sip2mediator-bin-release:
	cp ./target/release/eg-sip2-mediator ${TARGET}/bin

install-sip2mediator-config:
	@if [ ! -s ${TARGET}/etc/eg-sip2-mediator.yml ]; \
		then cp ./sip2-mediator/conf/eg-sip2-mediator.yml ${TARGET}/etc/; \
	fi;
	cp ./systemd/eg-sip2-mediator.service ${SYSTEMD_DIR}/ 
	systemctl daemon-reload 

# --- KCLS ---

build-kcls:
	cargo build -j ${BUILD_THREADS} --package kcls

build-kcls-release:
	cargo build -j ${BUILD_THREADS} --package kcls --release

install-kcls: install-kcls-bin

install-kcls-release: install-kcls-bin-release

install-kcls-bin:
	cp ./target/debug/kcls-on-order-audience-repairs ${TARGET}/bin

install-kcls-bin-release:
	cp ./target/release/kcls-on-order-audience-repairs ${TARGET}/bin

