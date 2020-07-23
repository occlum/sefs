Rust_Enclave_Name := libenclave.a
Rust_Enclave_Files := $(wildcard src/*.rs)
Rust_Target_Path := $(CURDIR)/../../../xargo

.PHONY: all

all: $(Rust_Enclave_Name)

$(Rust_Enclave_Name): $(Rust_Enclave_Files)
ifeq ($(XARGO_SGX), 1)
	RUST_TARGET_PATH=$(Rust_Target_Path) xargo build --target x86_64-unknown-linux-sgx --release
	cp ./target/x86_64-unknown-linux-sgx/release/libsefsclienclave.a ../lib/libenclave.a
else
	cargo build --release
	cp ./target/release/libsefsclienclave.a ../lib/libenclave.a
endif
