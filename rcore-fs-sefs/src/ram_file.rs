use alloc::vec::Vec;
use spin::Mutex;
use rcore_fs::dev::{DevError, DevResult, EIO};
use crate::dev::File;
use crate::dev::SefsMac;

pub struct RamFile {
    data: Mutex<Vec<u8>>,
}

impl RamFile {
    pub fn new() -> Self {
        RamFile {
            data: Mutex::new(Vec::new()),
        }
    }
}

impl File for RamFile {
    fn read_at(&self, buf: &mut [u8], offset: usize) -> DevResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let data = self.data.lock();
        let data_len = data.len();
        
        if offset >= data_len {
            return Err(DevError(EIO));
        }
        
        let read_len = core::cmp::min(data_len - offset, buf.len());
        buf[..read_len].copy_from_slice(&data[offset..offset + read_len]);
        Ok(read_len)
    }

    fn write_at(&self, buf: &[u8], offset: usize) -> DevResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut data = self.data.lock();
        let data_len = data.len();
        let write_end = match offset.checked_add(buf.len()) {
            Some(end) => end,
            None => return Err(DevError(EIO)),
        };

        if write_end > data_len {
            return Err(DevError(EIO));
        }
        
        data[offset..write_end].copy_from_slice(buf);
        Ok(buf.len())
    }

    fn set_len(&self, len: usize) -> DevResult<()> {
        let mut data = self.data.lock();
        data.resize(len, 0);
        Ok(())
    }

    fn flush(&self) -> DevResult<()> {
        Ok(())
    }

    fn get_file_mac(&self) -> DevResult<SefsMac> {
        Ok(SefsMac::default())
    }
}
