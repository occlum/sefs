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
                panic!("failed to call sgx functions");
            }
            val => val,
        }
    };
}

#[no_mangle]
pub unsafe extern "C" fn ecall_file_open(
    error: *mut i32,
    path: *const u8,
    create: bool,
    protect_integrity: bool,
    key: *const SGX_KEY,
) -> *mut u8 {
    let mode = match create {
        true => "w+b\0",
        false => "r+b\0",
    };
    let file = if !key.is_null() {
        sgx_fopen(path, mode.as_ptr(), key)
    } else {
        if protect_integrity {
            sgx_fopen_integrity_only(path, mode.as_ptr())
        } else {
            sgx_fopen_auto_key(path, mode.as_ptr())
        }
    };
    if file.is_null() {
        *error = errno();
    }
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
    let ret = sgx_fflush(file);
    if ret != 0 {
        return sgx_ferror(file);
    }
    0
}

#[no_mangle]
pub unsafe extern "C" fn ecall_file_read_at(
    file: SGX_FILE,
    offset: usize,
    buf: *mut u8,
    len: usize,
) -> usize {
    let file_size = {
        try_io!(sgx_fseek(file, 0, SEEK_END));
        try_io!(sgx_ftell(file)) as usize
    };
    if file_size < offset {
        return 0;
    }
    try_io!(sgx_fseek(file, offset as i64, SEEK_SET));
    sgx_fread(buf, 1, len, file) as usize
}

#[no_mangle]
pub unsafe extern "C" fn ecall_file_write_at(
    file: SGX_FILE,
    offset: usize,
    buf: *const u8,
    len: usize,
) -> usize {
    try_io!(sgx_fseek(file, 0, SEEK_END));
    let file_size = {
        try_io!(sgx_fseek(file, 0, SEEK_END));
        try_io!(sgx_ftell(file)) as usize
    };
    if file_size < offset {
        static ZEROS: [u8; 0x1000] = [0; 0x1000];
        let mut rest_len = offset - file_size;
        while rest_len != 0 {
            let l = rest_len.min(0x1000);
            let len = sgx_fwrite(ZEROS.as_ptr(), 1, l, file);
            assert!(len != 0);
            rest_len -= len;
        }
    }
    try_io!(sgx_fseek(file, offset as i64, SEEK_SET));
    sgx_fwrite(buf, 1, len, file) as usize
}
