use rcore_fs::dev::{DevError, DevResult, EINVAL};
use rcore_fs_sefs::dev::SefsMac;
use rcore_fs_sefs::dev::{File, Storage};
use sgx_types::*;
use std::fs::{read_dir, remove_file};
use std::io;
use std::mem;
use std::path::*;

pub struct SgxStorage {
    path: PathBuf,
    mode: EncryptMode,
}

pub enum EncryptMode {
    IntegrityOnly,
    EncryptWithIntegrity(sgx_key_128bit_t),
    Encrypt(sgx_key_128bit_t),
    EncryptAutoKey,
}

impl EncryptMode {
    pub fn from_parameters(
        protect_integrity: bool,
        key: &Option<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match (protect_integrity, key) {
            (true, None) => Ok(EncryptMode::IntegrityOnly),
            (true, Some(key_str)) => {
                let key = Self::parse_key(&key_str)?;
                Ok(EncryptMode::EncryptWithIntegrity(key))
            }
            (false, None) => Ok(EncryptMode::EncryptAutoKey),
            (false, Some(key_str)) => {
                let key = Self::parse_key(&key_str)?;
                Ok(EncryptMode::Encrypt(key))
            }
        }
    }

    fn parse_key(key_str: &str) -> Result<sgx_key_128bit_t, Box<dyn std::error::Error>> {
        let bytes_str_vec = {
            let bytes_str_vec: Vec<&str> = key_str.split("-").collect();
            if bytes_str_vec.len() != std::mem::size_of::<sgx_key_128bit_t>() {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "The length or format of Key string is invalid",
                )));
            }
            bytes_str_vec
        };

        let mut key: sgx_key_128bit_t = Default::default();
        for (byte_i, byte_str) in bytes_str_vec.iter().enumerate() {
            key[byte_i] = u8::from_str_radix(byte_str, 16)?;
        }
        Ok(key)
    }
}

impl SgxStorage {
    pub fn new(eid: sgx_enclave_id_t, path: impl AsRef<Path>, mode: EncryptMode) -> Self {
        unsafe {
            EID = eid;
        }
        SgxStorage {
            path: path.as_ref().to_path_buf(),
            mode,
        }
    }
}

impl Storage for SgxStorage {
    fn open(&self, file_id: &str) -> DevResult<Box<dyn File>> {
        let mut path = self.path.clone();
        path.push(file_id);
        let file = file_open(path.to_str().unwrap(), false, &self.mode)?;
        Ok(Box::new(SgxFile { file }))
    }

    fn create(&self, file_id: &str) -> DevResult<Box<dyn File>> {
        let mut path = self.path.clone();
        path.push(file_id);
        let file = file_open(path.to_str().unwrap(), true, &self.mode)?;
        Ok(Box::new(SgxFile { file }))
    }

    fn remove(&self, file_id: &str) -> DevResult<()> {
        let mut path = self.path.to_path_buf();
        path.push(file_id);
        remove_file(path)?;
        Ok(())
    }

    fn protect_integrity(&self) -> bool {
        match self.mode {
            EncryptMode::IntegrityOnly => true,
            EncryptMode::EncryptWithIntegrity(_) => true,
            _ => false,
        }
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
            return Err(DevError(EINVAL));
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
            e => {
                println!("failed to flush");
                return Err(DevError(e));
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
        protect_integrity: uint8_t,
        key: *const sgx_key_128bit_t,
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

fn file_open(path: &str, create: bool, mode: &EncryptMode) -> DevResult<usize> {
    let cpath = format!("{}\0", path);
    let (protect_integrity, key_ptr) = match mode {
        EncryptMode::IntegrityOnly => (true, std::ptr::null()),
        EncryptMode::EncryptWithIntegrity(key) => (true, key as *const sgx_key_128bit_t),
        EncryptMode::Encrypt(key) => (false, key as *const sgx_key_128bit_t),
        EncryptMode::EncryptAutoKey => (false, std::ptr::null()),
    };
    let mut ret_val = 0;
    let mut error = 0;
    unsafe {
        let ret = ecall_file_open(
            EID,
            &mut ret_val,
            &mut error,
            cpath.as_ptr(),
            create as uint8_t,
            protect_integrity as uint8_t,
            key_ptr,
        );
        assert_eq!(ret, sgx_status_t::SGX_SUCCESS);
    }
    if ret_val == 0 {
        let error = io::Error::from_raw_os_error(error);
        println!(
            "failed to open SGX protected file: {}, error: {:?}",
            path, error
        );
        return Err(DevError::from(error));
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
