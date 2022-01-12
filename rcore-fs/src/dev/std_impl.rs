#![cfg(any(test, feature = "std"))]

use std::fs::File;
use std::io::{Error, Read, Seek, SeekFrom, Write};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;

impl Device for Mutex<File> {
    fn read_at(&self, offset: usize, buf: &mut [u8]) -> DevResult<usize> {
        let offset = offset as u64;
        let mut file = self.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        let len = file.read(buf)?;
        Ok(len)
    }

    fn write_at(&self, offset: usize, buf: &[u8]) -> DevResult<usize> {
        let offset = offset as u64;
        let mut file = self.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        let len = file.write(buf)?;
        Ok(len)
    }

    fn sync(&self) -> DevResult<()> {
        let file = self.lock().unwrap();
        file.sync_all()?;
        Ok(())
    }
}

pub struct StdTimeProvider;

impl TimeProvider for StdTimeProvider {
    fn current_time(&self) -> Timespec {
        let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        Timespec {
            sec: duration.as_secs() as i64,
            nsec: duration.subsec_nanos() as i64,
        }
    }
}

impl From<Error> for DevError {
    fn from(e: Error) -> Self {
        if let Some(err) = e.raw_os_error() {
            DevError(err)
        } else {
            DevError(EIO)
        }
    }
}
