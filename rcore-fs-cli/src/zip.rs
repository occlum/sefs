use std::boxed::Box;
use std::error::Error;
use std::fs;
use std::io::{Read, Write};
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::str;
use std::sync::Arc;

use rcore_fs::vfs::{FileType, INode, PATH_MAX};

use crate::thread_pool;

const BUF_SIZE: usize = 0x10000;
const S_IMASK: u32 = 0o777;

pub fn zip_dir(path: &Path, inode: Arc<dyn INode>, thread_pool: &thread_pool::Pool, image_time: Option<i64>) -> Result<(), Box<dyn Error>> {
    let mut entries: Vec<fs::DirEntry> = fs::read_dir(path)?.map(|dir| dir.unwrap()).collect();
    entries.sort_by_key(|entry| entry.file_name());
    let is_incremental = image_time.is_some();
    let mut deleted_files: Vec<String> = Vec::new();
    if is_incremental {
        // at first, we record all the files in the image, 
        // existing files would be remove from the lsit later
        deleted_files = inode.list()?;
        let _ = deleted_files.drain(0..2);
    }
    for entry in entries {
        let name_ = entry.file_name();
        let name = name_.to_str().unwrap();
        let metadata = fs::symlink_metadata(entry.path())?;
        let type_ = metadata.file_type();
        let mode = (metadata.permissions().mode() & S_IMASK) as u16;
        //println!("zip: name: {:?}, mode: {:#o}", entry.path(), mode);
        if is_incremental {
            // if a file still exists, remove it from deleted_files
            // we use a linear search here, because `inode.list()` should have 
            // same order with `entries`. we break at the first match, 
            // it would not cause a large overhead.
            for (index, image_node_name) in deleted_files.iter().enumerate() {
                if image_node_name == name {
                    deleted_files.remove(index);
                    break;
                }
            }
            // skip the file not modified after image is created
            if !type_.is_dir() {
                if let Some(last_modify) = image_time {
                    use std::os::linux::fs::MetadataExt;
                    if metadata.st_ctime() < last_modify {
                        continue;
                    }
                    println!("{} needs to be updated", name);
                }
            }
        }
        if type_.is_file() {
            let inode = if !is_incremental {
                inode.create(name, FileType::File, mode)?
            } else {
                inode.find(name).or(inode.create(name, FileType::File, mode))?
            };
            // copy file content in another thread
            thread_pool.execute(move ||{
                let mut file = fs::File::open(entry.path()).unwrap();
                inode.resize(file.metadata().unwrap().len() as usize).expect(format!("resize {} failed", entry.path().display()).as_str());
                let mut buf = unsafe { Box::<[u8; BUF_SIZE]>::new_uninit().assume_init() };
                let mut offset = 0usize;
                let mut len = BUF_SIZE;
                while len == BUF_SIZE {
                    len = file.read(buf.as_mut()).unwrap();
                    inode.write_at(offset, &buf[..len]).expect(format!("write {} failed", entry.path().display()).as_str());
                    offset += len;
                };
            });
        } else if type_.is_dir() {
            let inode = if !is_incremental {
                inode.create(name, FileType::Dir, mode)?
            } else {
                inode.find(name).or(inode.create(name, FileType::Dir, mode))?
            };
            zip_dir(entry.path().as_path(), inode, thread_pool, image_time)?;
        } else if type_.is_symlink() {
            let target = fs::read_link(entry.path())?;
            let inode = if !is_incremental {
                inode.create(name, FileType::SymLink, mode)?
            } else {
                inode.find(name).or(inode.create(name, FileType::SymLink, mode))?
            };
            let data = target.as_os_str().as_bytes();
            inode.resize(data.len())?;
            inode.write_at(0, data)?;
        }
    }
    // Delete files that are not in the source directory
    for file_name in deleted_files {
        inode.unlink(&file_name).unwrap();
        println!("{} deleted", file_name);
    }
    Ok(())
}

pub fn unzip_dir(path: &Path, inode: Arc<dyn INode>) -> Result<(), Box<dyn Error>> {
    let files = inode.list()?;
    for name in files.iter().skip(2) {
        let inode = inode.lookup(name.as_str())?;
        let mut path = path.to_path_buf();
        path.push(name);
        let info = inode.metadata()?;
        let perms = fs::Permissions::from_mode(info.mode as u32 & S_IMASK);
        match info.type_ {
            FileType::File => {
                let mut file = fs::File::create(&path)?;
                let mut buf = unsafe { Box::<[u8; BUF_SIZE]>::new_uninit().assume_init() };
                let mut offset = 0usize;
                let mut len = BUF_SIZE;
                while len == BUF_SIZE {
                    len = inode.read_at(offset, buf.as_mut())?;
                    file.write_all(&buf[..len])?;
                    offset += len;
                }
                file.set_permissions(perms)?;
            }
            FileType::Dir => {
                fs::create_dir(&path)?;
                unzip_dir(path.as_path(), inode)?;
                fs::set_permissions(&path, perms)?;
            }
            FileType::SymLink => {
                let mut buf: [u8; PATH_MAX] = unsafe { MaybeUninit::uninit().assume_init() };
                let len = inode.read_at(0, buf.as_mut())?;
                std::os::unix::fs::symlink(str::from_utf8(&buf[..len]).unwrap(), path)?;
            }
            _ => panic!("unsupported file type"),
        }
    }
    Ok(())
}
