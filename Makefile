# Build and Install Rust Components and Binaries

TARGET = /usr/local
SYSTEMD_DIR = /lib/systemd/system
BIN_DIR = ${TARGET}/bin

# Number of test threads to run in parallel.
# Inline doc tests are compiler-heavy so having a limit here
# helps ensure things don't get out of hand ram/cpu/disk-wise.
TEST_THREADS = 4
BUILD_THREADS = 4

build: build-evergreen build-sip2mediator build-kcls build-kcls-services

# Removes Cargo artifacts
clean:
	cargo clean

build-release: build-evergreen-release build-sip2mediator-release build-kcls-release build-kcls-services-release

test:
	cargo test -j ${BUILD_THREADS} --all -- --test-threads=${TEST_THREADS}

install: install-evergreen install-sip2mediator install-kcls install-kcls-services install-kcls-services-config

install-bin: install-evergreen-bin install-sip2mediator-bin

install-bin-release: install-evergreen-bin-release install-sip2mediator-bin-release

install-release: install-evergreen-release install-sip2mediator-release install-kcls-release install-kcls-services-release install-kcls-services-config

# --- Evergreen ---

build-evergreen: build-evergreen-core build-evergreen-services build-evergreen-bin

build-evergreen-core:
	cargo build -j ${BUILD_THREADS} --package evergreen

build-evergreen-bin: build-evergreen-core
	cargo build -j ${BUILD_THREADS} --package evergreen-bin

build-evergreen-services: build-evergreen-core
	cargo build -j ${BUILD_THREADS} --package eg-service-sip2
	# cargo build -j ${BUILD_THREADS} --package eg-service-addrs  # Moved to kcls-services

build-evergreen-release: build-evergreen-core-release build-evergreen-bin-release build-evergreen-services-release

build-evergreen-core-release:
	cargo build -j ${BUILD_THREADS} --package evergreen --release

build-evergreen-bin-release: build-evergreen-core-release
	cargo build -j ${BUILD_THREADS} --package evergreen-bin --release

build-evergreen-services-release: build-evergreen-core-release
	cargo build -j ${BUILD_THREADS} --package eg-service-sip2 --release
	# cargo build -j ${BUILD_THREADS} --package eg-service-addrs --release  # Moved to kcls-services

install-evergreen: install-evergreen-config install-evergreen-bin

install-evergreen-bin:
	cp ./target/debug/egsh ${BIN_DIR}/
	cp ./target/debug/osrf-router ${BIN_DIR}/
	cp ./target/debug/eg-http-gateway ${BIN_DIR}/
	cp ./target/debug/eg-marc-export ${BIN_DIR}/
	cp ./target/debug/eg-websockets ${BIN_DIR}/
	cp ./target/debug/eg-auth-to-auth-linker ${BIN_DIR}/
	cp ./target/debug/eg-edi-file-fetcher ${BIN_DIR}/

install-evergreen-services:
	cp ./target/debug/eg-service-rs-sip2 ${BIN_DIR}/
	# cp ./target/debug/eg-service-rs-addrs ${BIN_DIR}/  # Moved to kcls-services

install-evergreen-release: install-evergreen-config install-evergreen-bin-release install-evergreen-services-release

install-evergreen-bin-release: 
	cp ./target/release/egsh ${BIN_DIR}/
	cp ./target/release/osrf-router ${BIN_DIR}/
	cp ./target/release/eg-http-gateway ${BIN_DIR}/
	cp ./target/release/eg-marc-export ${BIN_DIR}/
	cp ./target/release/eg-websockets ${BIN_DIR}/
	cp ./target/release/eg-auth-to-auth-linker ${BIN_DIR}/
	cp ./target/release/eg-edi-file-fetcher ${BIN_DIR}/

install-evergreen-services-release:
	cp ./target/release/eg-service-rs-sip2 ${BIN_DIR}/
	# cp ./target/release/eg-service-rs-addrs ${BIN_DIR}/  # Moved to kcls-services

install-evergreen-config:
	cp ./evergreen-bin/systemd/osrf-router.service ${SYSTEMD_DIR}/
	cp ./evergreen-bin/systemd/eg-http-gateway.service ${SYSTEMD_DIR}/
	cp ./evergreen-bin/systemd/eg-websockets.service ${SYSTEMD_DIR}/
	cp ./evergreen-services/systemd/eg-service-rs-sip2.service ${SYSTEMD_DIR}/
	# cp ./evergreen-services/systemd/eg-service-rs-addrs.service ${SYSTEMD_DIR}/  # Moved to kcls-services
	systemctl daemon-reload

# --- SIP2 Mediator ---

build-sip2mediator:
	cargo build -j ${BUILD_THREADS} --package sip2mediator

build-sip2mediator-release:
	cargo build -j ${BUILD_THREADS} --package sip2mediator --release

install-sip2mediator: install-sip2mediator-config install-sip2mediator-bin
install-sip2mediator-bin:
	cp ./target/debug/eg-sip2-mediator ${BIN_DIR}/

install-sip2mediator-release: install-sip2mediator-config install-sip2mediator-bin-release

install-sip2mediator-bin-release:
	cp ./target/release/eg-sip2-mediator ${BIN_DIR}/

install-sip2mediator-config:
	@if [ ! -s ${TARGET}/etc/eg-sip2-mediator.yml ]; \
		then cp ./sip2-mediator/conf/eg-sip2-mediator.yml ${TARGET}/etc/; \
	fi;
	cp ./sip2-mediator/systemd/eg-sip2-mediator.service ${SYSTEMD_DIR}/ 
	systemctl daemon-reload 

# --- KCLS ---

build-kcls:
	cargo build -j ${BUILD_THREADS} --package kcls

# --- KCLS Services ---

build-kcls-services:
	cargo build -j ${BUILD_THREADS} --package eg-service-addrs

build-kcls-services-release:
	cargo build -j ${BUILD_THREADS} --package eg-service-addrs --release

install-kcls-services:
	cp ./target/debug/eg-service-rs-addrs ${BIN_DIR}/

install-kcls-services-release:
	cp ./target/release/eg-service-rs-addrs ${BIN_DIR}/

install-kcls-services-config:
	cp ./kcls-services/systemd/eg-service-rs-addrs.service ${SYSTEMD_DIR}/
	systemctl daemon-reload

build-kcls-release:
	cargo build -j ${BUILD_THREADS} --package kcls --release

install-kcls: install-kcls-bin

install-kcls-release: install-kcls-bin-release

install-kcls-bin:
	cp ./target/debug/kcls-on-order-audience-repairs ${BIN_DIR}/
	cp ./target/debug/kcls-bib-to-auth-linker ${BIN_DIR}/

install-kcls-bin-release:
	cp ./target/release/kcls-on-order-audience-repairs ${BIN_DIR}/
	cp ./target/release/kcls-bib-to-auth-linker ${BIN_DIR}/

