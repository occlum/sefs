use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=SGX_SDK");
    println!("cargo:rerun-if-env-changed=SGX_MODE");

    let sdk_dir = env::var("SGX_SDK").unwrap_or_else(|_| "/opt/intel/sgxsdk".to_string());
    let is_sim = env::var("SGX_MODE").unwrap_or_else(|_| "HW".to_string());

    println!("cargo:rustc-link-search=native=../lib");
    println!("cargo:rustc-link-lib=static=Enclave_u");

    println!("cargo:rustc-link-search=native={}/lib64", sdk_dir);
    // for sgx_tprotected_fs
    println!("cargo:rustc-link-lib=static=sgx_uprotected_fs");

    match is_sim.as_ref() {
        "SW" | "SIM" => {
            println!("cargo:rustc-link-arg=-Wl,-Bstatic");
            println!("cargo:rustc-link-arg=-lsgx_urts_sim");
            println!("cargo:rustc-link-arg=-Wl,-Bdynamic");
            println!("cargo:rustc-link-arg=-lsgx_uae_service_sim");
            // Needed by sgx_urts_sim static library
            println!("cargo:rustc-link-arg=-lcrypto");
            // Must link with C++ dynamic library
            println!("cargo:rustc-link-arg=-lstdc++");
        }
        "HW" => {
            println!("cargo:rustc-link-lib=dylib=sgx_urts");
            println!("cargo:rustc-link-lib=dylib=sgx_uae_service");
        }
        _ => {
            println!("cargo:rustc-link-lib=dylib=sgx_urts");
            println!("cargo:rustc-link-lib=dylib=sgx_uae_service");
        } // Treat undefined as HW
    }
}
