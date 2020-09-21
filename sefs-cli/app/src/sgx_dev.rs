use rcore_fs_sefs::dev::SefsMac;
use rcore_fs_sefs::dev::{DevResult, DeviceError, File, Storage};
use sgx_types::*;
use std::fs::{read_dir, remove_file};
use std::io;
use std::mem;
use std::path::*;

pub struct SgxStorage {
    path: PathBuf,
    integrity_only: bool,
}

impl SgxStorage {
    pub fn new(eid: sgx_enclave_id_t, path: impl AsRef<Path>, integrity_only: bool) -> Self {
        unsafe {
            EID = eid;
        }
        SgxStorage {
            path: path.as_ref().to_path_buf(),
            integrity_only: integrity_only,
        }
    }
}

impl Storage for SgxStorage {
    fn open(&self, file_id: &str) -> DevResult<Box<dyn File>> {
        let mut path = self.path.clone();
        path.push(file_id);
        let file = file_open(path.to_str().unwrap(), false, self.integrity_only)?;
        Ok(Box::new(SgxFile { file }))
    }

    fn create(&self, file_id: &str) -> DevResult<Box<dyn File>> {
        let mut path = self.path.clone();
        path.push(file_id);
        let file = file_open(path.to_str().unwrap(), true, self.integrity_only)?;
        Ok(Box::new(SgxFile { file }))
    }

    fn remove(&self, file_id: &str) -> DevResult<()> {
        let mut path = self.path.to_path_buf();
        path.push(file_id);
        remove_file(path)?;
        Ok(())
    }

    fn is_integrity_only(&self) -> bool {
        self.integrity_only
    }

    fn clear(&self) -> DevResult<()> {
        for child in read_dir(&self.path)? {
            let child = child?;
            remove_file(&child.path())?;
        }
        Ok(())
    }
}

pub struct SgxFile {
    file: usize,
}

impl File for SgxFile {
    fn read_at(&self, buf: &mut [u8], offset: usize) -> DevResult<usize> {
        let len = file_read_at(self.file, offset, buf);
        Ok(len)
    }

    fn write_at(&self, buf: &[u8], offset: usize) -> DevResult<usize> {
        let len = file_write_at(self.file, offset, buf);
        if len != buf.len() {
            println!(
                "write_at return len: {} not equal to buf_len: {}",
                len,
                buf.len()
            );
            return Err(DeviceError);
        }
        Ok(len)
    }

    fn set_len(&self, _len: usize) -> DevResult<()> {
        // NOTE: do nothing ?
        Ok(())
    }

    fn flush(&self) -> DevResult<()> {
        match file_flush(self.file) {
            0 => Ok(()),
            _ => {
                println!("failed to flush");
                return Err(DeviceError);
            }
        }
    }

    fn get_file_mac(&self) -> DevResult<SefsMac> {
        let mut mac: sgx_aes_gcm_128bit_tag_t = [0u8; 16];

        file_get_mac(self.file, &mut mac);
        let sefs_mac = SefsMac(mac);
        Ok(sefs_mac)
    }
}

impl Drop for SgxFile {
    fn drop(&mut self) {
        let _ = file_close(self.file);
    }
}

/// Ecall functions to access SgxFile
extern "C" {
    fn ecall_file_open(
        eid: sgx_enclave_id_t,
        retval: *mut size_t,
        error: *mut i32,
        path: *const u8,
        create: uint8_t,
        integrity_only: i32,
    ) -> sgx_status_t;
    fn ecall_file_close(eid: sgx_enclave_id_t, retval: *mut i32, fd: size_t) -> sgx_status_t;
    fn ecall_file_flush(eid: sgx_enclave_id_t, retval: *mut i32, fd: size_t) -> sgx_status_t;
    fn ecall_file_read_at(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        fd: size_t,
        offset: size_t,
        buf: *mut uint8_t,
        len: size_t,
    ) -> sgx_status_t;
    fn ecall_file_write_at(
        eid: sgx_enclave_id_t,
        retval: *mut usize,
        fd: size_t,
        offset: size_t,
        buf: *const uint8_t,
        len: size_t,
    ) -> sgx_status_t;
    fn ecall_file_get_mac(
        eid: sgx_enclave_id_t,
        retvat: *mut i32,
        fd: size_t,
        mac: *mut uint8_t,
        len: size_t,
    ) -> sgx_status_t;
}

/// Must be set when init enclave
static mut EID: sgx_enclave_id_t = 0;

fn file_get_mac(fd: usize, mac: *mut sgx_aes_gcm_128bit_tag_t) -> usize {
    let mut ret_val = 0;
    unsafe {
        let len = mem::size_of::<sgx_aes_gcm_128bit_tag_t>();
        let ret = ecall_file_get_mac(EID, &mut ret_val, fd, mac as *mut u8, len);
        assert_eq!(ret, sgx_status_t::SGX_SUCCESS);
    }
    ret_val as usize
}

fn file_open(path: &str, create: bool, integrity_only: bool) -> DevResult<usize> {
    let cpath = format!("{}\0", path);
    let mut ret_val = 0;
    let mut error = 0;
    unsafe {
        let ret = ecall_file_open(
            EID,
            &mut ret_val,
            &mut error,
            cpath.as_ptr(),
            create as uint8_t,
            integrity_only as i32,
        );
        assert_eq!(ret, sgx_status_t::SGX_SUCCESS);
    }
    if ret_val == 0 {
        let error = io::Error::from_raw_os_error(error);
        println!(
            "failed to open SGX protected file: {}, error: {:?}",
            path, error
        );
        return Err(DeviceError);
    }
    Ok(ret_val)
}

fn file_close(fd: usize) -> i32 {
    let mut ret_val = -1;
    unsafe {
        let ret = ecall_file_close(EID, &mut ret_val, fd);
        assert_eq!(ret, sgx_status_t::SGX_SUCCESS);
    }
    ret_val
}

fn file_flush(fd: usize) -> i32 {
    let mut ret_val = -1;
    unsafe {
        let ret = ecall_file_flush(EID, &mut ret_val, fd);
        assert_eq!(ret, sgx_status_t::SGX_SUCCESS);
    }
    ret_val
}

fn file_read_at(fd: usize, offset: usize, buf: &mut [u8]) -> usize {
    let mut ret_val = 0;
    unsafe {
        let ret = ecall_file_read_at(EID, &mut ret_val, fd, offset, buf.as_mut_ptr(), buf.len());
        assert_eq!(ret, sgx_status_t::SGX_SUCCESS);
    }
    ret_val
}

fn file_write_at(fd: usize, offset: usize, buf: &[u8]) -> usize {
    let mut ret_val = 0;
    unsafe {
        let ret = ecall_file_write_at(EID, &mut ret_val, fd, offset, buf.as_ptr(), buf.len());
        assert_eq!(ret, sgx_status_t::SGX_SUCCESS);
    }
    ret_val
}
