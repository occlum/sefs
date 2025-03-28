use std::io::{Cursor, Read, Write, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use rcore_fs::dev::{DevError, DevResult, EIO};

use crate::dev::*;

pub struct CursorFile {
    inner: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl CursorFile {
    pub fn new() -> Self {
        CursorFile {
            inner: Arc::new(Mutex::new(Cursor::new(Vec::new()))),
        }
    }
}

impl File for CursorFile {
    fn read_at(&self, buf: &mut [u8], offset: usize) -> DevResult<usize> {
        let mut cursor = self.inner.lock().unwrap();
        cursor.seek(SeekFrom::Start(offset as u64)).map_err(|_| {
            DevError(EIO)
        })?;
        let result = cursor.read(buf).map_err(|_| {
            DevError(EIO)
        })?;
        Ok(result)
    }

    fn write_at(&self, buf: &[u8], offset: usize) -> DevResult<usize> {
        let mut cursor = self.inner.lock().unwrap();
        cursor.seek(SeekFrom::Start(offset as u64)).map_err(|_| {
            DevError(EIO)
        })?;
        let result = cursor.write(buf).map_err(|_| {
            DevError(EIO)
        })?;
        Ok(result)
    }

    fn set_len(&self, len: usize) -> DevResult<()> {
        let mut cursor = self.inner.lock().unwrap();
        cursor.get_mut().resize(len, 0);
        Ok(())
    }

    fn flush(&self) -> DevResult<()> {
        Ok(())
    }

    fn get_file_mac(&self) -> DevResult<SefsMac> {
        Ok(SefsMac::default())
    }
}
