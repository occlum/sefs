#![crate_name = "sefsclienclave"]
#![crate_type = "staticlib"]
#![no_std]
#![feature(lang_items)]

#[macro_use]
extern crate log;
//#[macro_use]
//extern crate sgx_tstd;

use self::sgxfs::*;

mod lang;
mod sgxfs;

/// Helper macro to reply error when IO fails
macro_rules! try_io {
    ($expr:expr) => {
        match $expr {
            errno if (errno as i32) == -1 => {
                return errno as i32;
            }
            val => val,
        }
    };
}

#[no_mangle]
pub unsafe extern "C" fn ecall_file_open(
    path: *const u8,
    create: bool,
    _integrity_only: i32,
) -> *mut u8 {
    let integrity_only = if _integrity_only != 0 { true } else { false };
    let mode = match create {
        true => "w+b\0",
        false => "r+b\0",
    };
    let file = if !integrity_only {
        sgx_fopen_auto_key(path, mode.as_ptr())
    } else {
        sgx_fopen_integrity_only(path, mode.as_ptr())
    };
    file
}

#[no_mangle]
pub unsafe extern "C" fn ecall_file_get_mac(
    file: SGX_FILE,
    mac: *mut sgx_aes_gcm_128bit_tag_t,
) -> i32 {
    sgx_fget_mac(file, mac) as i32
}

#[no_mangle]
pub unsafe extern "C" fn ecall_file_close(file: SGX_FILE) -> i32 {
    sgx_fclose(file)
}

#[no_mangle]
pub unsafe extern "C" fn ecall_file_flush(file: SGX_FILE) -> i32 {
    sgx_fflush(file)
}

#[no_mangle]
pub unsafe extern "C" fn ecall_file_read_at(
    file: SGX_FILE,
    offset: usize,
    buf: *mut u8,
    len: usize,
) -> i32 {
    try_io!(sgx_fseek(file, offset as i64, SEEK_SET));
    sgx_fread(buf, 1, len, file) as i32
}

#[no_mangle]
pub unsafe extern "C" fn ecall_file_write_at(
    file: SGX_FILE,
    offset: usize,
    buf: *const u8,
    len: usize,
) -> i32 {
    try_io!(sgx_fseek(file, offset as i64, SEEK_SET));
    sgx_fwrite(buf, 1, len, file) as i32
}

#[no_mangle]
pub unsafe extern "C" fn ecall_file_set_len(file: SGX_FILE, len: usize) -> i32 {
    let current_len = try_io!(sgx_fseek(file, 0, SEEK_END)) as usize;
    if current_len < len {
        static ZEROS: [u8; 0x1000] = [0; 0x1000];
        let mut rest_len = len - current_len;
        while rest_len != 0 {
            let l = rest_len.min(0x1000);
            let ret = try_io!(sgx_fwrite(ZEROS.as_ptr(), 1, l, file)) as i32;
            if ret == -12 {
                warn!("Error 12: \"Cannot allocate memory\". Clear cache and try again.");
                try_io!(sgx_fclear_cache(file));
                try_io!(sgx_fwrite(ZEROS.as_ptr(), 1, l, file));
            } else if ret < 0 {
                return ret;
            }
            rest_len -= l;
        }
        // NOTE: Don't try to write a large slice at once.
        //       It will cause Error 12: "Cannot allocate memory"
    }
    // TODO: how to shrink a file?
    0
}
