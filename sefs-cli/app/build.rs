use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=SGX_SDK");
    println!("cargo:rerun-if-env-changed=SGX_MODE");

    let sdk_dir = env::var("SGX_SDK").unwrap_or_else(|_| "/opt/intel/sgxsdk".to_string());
    let is_sim = env::var("SGX_MODE").unwrap_or_else(|_| "HW".to_string());

    println!("cargo:rustc-link-search=native=../lib");
    println!("cargo:rustc-link-lib=static=Enclave_u");

    println!("cargo:rustc-link-search=native={}/lib64", sdk_dir);
    match is_sim.as_ref() {
        "SW" | "SIM" => println!("cargo:rustc-link-lib=static=sgx_urts_sim_with_se_event"),
        "HW" => println!("cargo:rustc-link-lib=dylib=sgx_urts"),
        _ => println!("cargo:rustc-link-lib=dylib=sgx_urts"), // Treat undefined as HW
    }

    // for sgx_tprotected_fs
    match is_sim.as_ref() {
        "SW" | "SIM" => println!("cargo:rustc-link-lib=dylib=sgx_uae_service_sim"),
        "HW" => println!("cargo:rustc-link-lib=dylib=sgx_uae_service"),
        _ => println!("cargo:rustc-link-lib=dylib=sgx_uae_service"), // Treat undefined as HW
    }

    println!("cargo:rustc-link-lib=dylib=sgx_uprotected_fs");
    // Needed by sgx_urts_sim_with_se_event static library
    println!("cargo:rustc-link-lib=dylib=crypto");
    // Must link with C++ dynamic library
    println!("cargo:rustc-link-lib=dylib=stdc++");
}
