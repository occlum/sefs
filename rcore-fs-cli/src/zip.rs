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
use std::env;

use rcore_fs::vfs::{FileType, DirEntryData, INode, PATH_MAX};
use rcore_fs::vfs::FsError;

use crate::thread_pool::ThreadPool;

const BUF_SIZE: usize = 0x10000;
const S_IMASK: u32 = 0o777;

struct FileTask {
    inode: Arc<dyn INode>,
    entry: fs::DirEntry,
}

pub fn zip_dir(path: &Path, inode: Arc<dyn INode>) -> Result<(), Box<dyn Error>> {
    let pool = ThreadPool::new(30);
    zip_dir_task(path, inode, &pool)?;
    Ok(())
}

pub fn zip_dir_task(path: &Path, inode: Arc<dyn INode>, pool: &ThreadPool) -> Result<(), Box<dyn Error>> {
    let mut entries: Vec<fs::DirEntry> = fs::read_dir(path)?.map(|dir| dir.unwrap()).collect();
    entries.sort_by_key(|entry| entry.file_name());
    
    let mut dir_entries: Vec<DirEntryData> = Vec::new();
    let mut file_tasks: Vec<FileTask> = Vec::new();

    for entry in entries {
        let name_ = entry.file_name();
        let name = name_.to_str().unwrap();
        let metadata = fs::symlink_metadata(entry.path())?;
        let type_ = metadata.file_type();
        let mode = (metadata.permissions().mode() & S_IMASK) as u16;

        if type_.is_file() {
            let inode = inode.create_for_zip(name, FileType::File, mode)?;
            dir_entries.push(DirEntryData { inode: Arc::clone(&inode), name: String::from(name), file_type: FileType::File });
            file_tasks.push(FileTask { inode, entry });
        } else if type_.is_symlink() {
            let target = fs::read_link(entry.path())?;
            let inode = inode.create_for_zip(name, FileType::SymLink, mode)?;
            dir_entries.push(DirEntryData { inode: Arc::clone(&inode), name: String::from(name), file_type: FileType::SymLink });
            let data = target.as_os_str().as_bytes();
            inode.resize(data.len())?;
            inode.write_at(0, data)?;
        } else if type_.is_dir() {
            let inode = inode.create_for_zip(name, FileType::Dir, mode)?;
            dir_entries.push(DirEntryData { inode: Arc::clone(&inode), name: String::from(name), file_type: FileType::Dir });
            zip_dir_task(entry.path().as_path(), inode, &pool)?;
        }
    }

    if dir_entries.len() > 0 {
        process_sync(inode, dir_entries, file_tasks, &pool);
    }
    Ok(())
}

fn process_sync(dir_inode: Arc<dyn INode>, dir_entries: Vec<DirEntryData>, file_tasks: Vec<FileTask>, pool: &ThreadPool) {
    pool.execute(move || {
        if let Err(e) = dir_inode.write_all_direntry(dir_entries) {
            eprintln!("Failed to write direntry: {}", e);
        }
        if let Err(e) = process_files_task(&file_tasks) {
            eprintln!("Failed to process files: {}", e);
        }
    });
}

fn process_files_task(file_tasks: &[FileTask]) -> Result<(), FsError> {
    for task in file_tasks {
        let mut file = fs::File::open(task.entry.path())?;
        let mut buf = unsafe { Box::<[u8; BUF_SIZE]>::new_uninit().assume_init() };
        let mut offset = 0usize;
        let mut len = BUF_SIZE;
        while len == BUF_SIZE {
            len = file.read(buf.as_mut())?;
            task.inode.write_at(offset, &buf[..len])?;
            offset += len;
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

pub fn inc_zip_dir(root_path: &Path, inc_path: &Path, root_inode: Arc<dyn INode>) -> Result<(), Box<dyn Error>> {
    // Convert to absolute path
    let abs_root = if root_path.is_absolute() {
        root_path.to_path_buf()
    } else {
        env::current_dir()?.join(root_path)
    }.canonicalize()?;

    let abs_inc = if inc_path.is_absolute() {
        inc_path.to_path_buf()
    } else {
        env::current_dir()?.join(inc_path)
    }.canonicalize()?;

    if !abs_inc.starts_with(&abs_root) {
        return Err(format!("{} is not under root {}", 
            abs_inc.display(), abs_root.display()).into());
    }

    let relative_path = abs_inc.strip_prefix(&abs_root)?;
    let mut components = Vec::new();

    for c in relative_path.components() {
        let os_str = c.as_os_str();
        let s = os_str.to_str()
            .ok_or_else(|| format!("Invalid UTF-8 component: {:?}", os_str))?;
        components.push(s.to_string());
    }

    let mut current_inode = root_inode;
    for (i, name) in components.iter().enumerate() {
        let is_last = i == components.len() - 1;
        
        // Process the directory in the middle of the path
        if !is_last {
            current_inode = match current_inode.lookup_follow(name, 1) {
                Ok(inode) => {
                    if inode.metadata()?.type_ != FileType::Dir {
                        return Err(format!("'{}' exists but is not directory", name).into());
                    }
                    inode
                }
                Err(_) => current_inode.create(name, FileType::Dir, 0o755)?,
            };
            continue;
        }

        // Process the final path
        if let Ok(_) = current_inode.lookup_follow(name, 1) {
            if let Err(e) = current_inode.unlink_recursive(name) {
                return Err(Box::new(e));
            }
        }
        
        if !abs_inc.exists() { return Ok(()); }

        let meta = fs::symlink_metadata(&abs_inc)?;
        let mode = (meta.permissions().mode() & S_IMASK) as u16;

        match meta.file_type() {
            ft if ft.is_file() => {
                let inode = current_inode.create(name, FileType::File, mode)?;
                let mut file = fs::File::open(&abs_inc)?;
                let size = file.metadata()?.len() as usize;
                inode.resize(size)?;
                
                let mut buf = vec![0u8; 4096];
                let mut offset = 0;
                while let Ok(len) = file.read(&mut buf) {
                    if len == 0 { break; }
                    inode.write_at(offset, &buf[..len])?;
                    offset += len;
                }
            }
            ft if ft.is_dir() => {
                let inode = current_inode.create(name, FileType::Dir, mode)?;
                for entry in fs::read_dir(&abs_inc)? {
                    let entry = entry?;
                    let path = entry.path();
                    inc_zip_dir(&abs_root, &path, inode.clone())?;
                }
            }
            ft if ft.is_symlink() => {
                let target = fs::read_link(&abs_inc)?;
                let inode = current_inode.create(name, FileType::SymLink, mode)?;
                let data = target.as_os_str().as_bytes();
                inode.resize(data.len())?;
                inode.write_at(0, data)?;
            }
            _ => return Err("Unsupported file type".into()),
        }
    }

    Ok(())
}