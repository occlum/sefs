use std::sync::{Arc, Mutex};

use rcore_fs::dev::{DevResult, DevError, EINVAL};
use rcore_fs_sefs::dev::{File, SefsMac, Storage};

enum CacheFileData {
    InMemory{cache: Vec<u8>},
    WritenToInner{inner_file: Box<dyn File>}
}

/// Cache data in memory before it is written to the inner storage
#[derive(Clone)]
pub struct CacheFile {
    data: Arc<Mutex<CacheFileData>>,
}

impl CacheFile {
    fn get_file_size(&self) -> DevResult<usize> {
        match &*self.data.lock().unwrap() {
            CacheFileData::InMemory { cache } => {
                return Ok(cache.len())
            },
            CacheFileData::WritenToInner { inner_file: _ } => {
                return Err(DevError(EINVAL))
            }
        }
    }
}

impl File for CacheFile {
    fn read_at(&self, buf: &mut [u8], offset: usize) -> DevResult<usize> {
        match & *self.data.lock().unwrap() {
            CacheFileData::InMemory { cache } => {
                let data_len = cache.len();
                let len = if offset + buf.len() > data_len {
                    data_len - offset
                } else {
                    buf.len()
                };
                buf[..len].copy_from_slice(&cache[offset..offset + len]);
                return Ok(len)
            },
            CacheFileData::WritenToInner { inner_file } => {
                return inner_file.read_at(buf, offset);
            }
        }
    }
    fn write_at(&self, buf: &[u8], offset: usize) -> DevResult<usize> {
        match & mut *self.data.lock().unwrap() {
            CacheFileData::InMemory { cache } => {
                let data_len = cache.len();
                let len = if offset + buf.len() > data_len {
                    data_len - offset
                } else {
                    buf.len()
                };
                cache[offset..offset + len].copy_from_slice(&buf[..len]);
                return Ok(len);
            },
            CacheFileData::WritenToInner { inner_file } => {
                return inner_file.write_at(buf, offset);
            }
        }
    }
    fn set_len(&self, len: usize) -> DevResult<()> {
        match & mut *self.data.lock().unwrap() {
            CacheFileData::InMemory { cache } => {
                cache.resize(len, 0);
                return Ok(());
            },
            CacheFileData::WritenToInner { inner_file } => {
                return inner_file.set_len(len);
            }
        }
    }
    fn flush(&self) -> DevResult<()> {
        match & mut *self.data.lock().unwrap() {
            CacheFileData::InMemory { cache: _ } => {
                return Ok(());
            },
            CacheFileData::WritenToInner { inner_file } => {
                return inner_file.flush();
            }
        }
    }
    fn get_file_mac(&self) -> DevResult<SefsMac> {
        match &*self.data.lock().unwrap() {
            CacheFileData::InMemory { cache: _ } => {
                return Err(DevError(EINVAL));
            },
            CacheFileData::WritenToInner { inner_file } => {
                return inner_file.get_file_mac();
            }
        }
    }
    
}

impl CacheFile { 
    pub fn new() -> Self { 
        Self { 
            data: Arc::new(Mutex::new(CacheFileData::InMemory { cache: Vec::new() })),
        }
    }
}

#[derive(Clone)]
pub struct CacheStorage {
    cached_metadata: CacheFile,
    inner_storage: Arc<Box<dyn Storage>>,
}

/// CacheStorage wrap the trait `Storage`, and cache the metadata in memory 
/// before it is written to the inner storage
impl CacheStorage {
    pub fn new(inner_storage: Arc<Box<dyn Storage>>) -> Self {
        Self {
            cached_metadata: CacheFile::new(),
            inner_storage,
        }
    }

    pub fn write_cache_to_inner(&self) -> DevResult<()> {
        let _ = self.inner_storage.remove("metadata");
        let metafile = self.inner_storage.create("metadata")?;
        let metafile_size = self.cached_metadata.get_file_size()?;
        metafile.set_len(metafile_size)?;

        let mut offset = 0; 
        let mut buf = vec![0; 1024];
        while offset < metafile_size {
            let read_size = self.cached_metadata.read_at(&mut buf, offset)?;
            metafile.write_at(&buf[..read_size], offset)?;
            offset += read_size;
        }
        // update the inner file status
        let mut inner_file = self.cached_metadata.data.lock().unwrap();
        *inner_file = CacheFileData::WritenToInner { inner_file: metafile };
        Ok(())
    }
}

impl Storage for CacheStorage {
    fn open(&self, file_id: &str) -> DevResult<Box<dyn File>> {
        Ok(self.inner_storage.open(file_id)?)
    }

    fn create(&self, file_id: &str) -> DevResult<Box<dyn File>> {
        let file = if file_id.eq("metadata") {
            Box::new(self.cached_metadata.clone())
        } else {
            self.inner_storage.create(file_id)?
        };
        Ok(file)
    }

    fn remove(&self, file_id: &str) -> DevResult<()> {
        if file_id.eq("metadata") {
            return Ok(());
        }
        self.inner_storage.remove(file_id)
    }

    fn protect_integrity(&self) -> bool {
        self.inner_storage.protect_integrity()
    }

    fn clear(&self) -> DevResult<()> {
        self.inner_storage.clear()
    }
}