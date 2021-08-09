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

const BUF_SIZE: usize = 0x10000;
const S_IMASK: u32 = 0o777;

pub fn zip_dir(path: &Path, inode: Arc<dyn INode>) -> Result<(), Box<dyn Error>> {
    let mut entries: Vec<fs::DirEntry> = fs::read_dir(path)?.map(|dir| dir.unwrap()).collect();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name_ = entry.file_name();
        let name = name_.to_str().unwrap();
        let metadata = fs::symlink_metadata(entry.path())?;
        let type_ = metadata.file_type();
        let mode = (metadata.permissions().mode() & S_IMASK) as u16;
        //println!("zip: name: {:?}, mode: {:#o}", entry.path(), mode);
        if type_.is_file() {
            let inode = inode.create(name, FileType::File, mode)?;
            let mut file = fs::File::open(entry.path())?;
            inode.resize(file.metadata()?.len() as usize)?;
            let mut buf = unsafe { Box::<[u8; BUF_SIZE]>::new_uninit().assume_init() };
            let mut offset = 0usize;
            let mut len = BUF_SIZE;
            while len == BUF_SIZE {
                len = file.read(buf.as_mut())?;
                inode.write_at(offset, &buf[..len])?;
                offset += len;
            }
        } else if type_.is_dir() {
            let inode = inode.create(name, FileType::Dir, mode)?;
            zip_dir(entry.path().as_path(), inode)?;
        } else if type_.is_symlink() {
            let target = fs::read_link(entry.path())?;
            let inode = inode.create(name, FileType::SymLink, mode)?;
            let data = target.as_os_str().as_bytes();
            inode.resize(data.len())?;
            inode.write_at(0, data)?;
        }
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
