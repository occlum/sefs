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
use threadpool::ThreadPool;



use rcore_fs::vfs::{FileType, INode, PATH_MAX};

const BUF_SIZE: usize = 0x10000;
const S_IMASK: u32 = 0o777;

#[derive(Debug)]
struct ZipError(String);

impl std::error::Error for ZipError {}

impl std::fmt::Display for ZipError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// This function creates a thread pool with 16 worker threads to process files and symlinks concurrently,
/// while recursively traversing the directory structure. Directory creation remains sequential
/// to maintain consistency, but file and symlink operations are dispatched to worker threads.
///
/// # Parameters
/// * `path` - Path to the source directory in the host file system
/// * `inode` - Target directory INode in the destination file system
///
/// # Returns
/// * `Result<(), Box<dyn Error>>` - Success or an error
pub fn zip_dir_parallel(path: &Path, inode: Arc<dyn INode>) -> Result<(), Box<dyn Error>> {
    let pool = Arc::new(ThreadPool::new(16));
    let result = zip_dir_recursive(path, inode, &pool);
    pool.join();
    result
}

fn zip_dir_recursive(path: &Path, inode: Arc<dyn INode>, pool: &Arc<ThreadPool>) -> Result<(), Box<dyn Error>> {
    let mut entries: Vec<fs::DirEntry> = fs::read_dir(path)?.map(|dir| dir.unwrap()).collect();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name_ = entry.file_name();
        let name = name_.to_str().unwrap().to_string();
        let metadata = fs::symlink_metadata(entry.path())?;
        let type_ = metadata.file_type();
        let mode = (metadata.permissions().mode() & S_IMASK) as u16;
        let inode = inode.clone();
        let path = entry.path();
        if type_.is_dir() {        
            let dir_inode = inode.create(&name, FileType::Dir, mode)?;
            zip_dir_recursive(&path, dir_inode, &pool)?;
        } else {
            //every file or symlink is handled in a separate thread            
            pool.execute(move || {                
                let inode = Arc::clone(&inode);
                if type_.is_file() {
                    if let Err(e) = handle_file(&inode, &name, mode, &path) {
                        println!("failed!: {}", e);
                     }
                } else if type_.is_symlink() {
                    if let Err(e) = handle_symlink(&inode, &name, mode, &path) {
                        println!("failed!: {}", e);
                    }
                    
                }   
            });
        }
    }

    Ok(())
}

fn handle_file(
    inode: &Arc<dyn INode>,
    name: &str,
    mode: u16,
    path: &Path,
)-> Result<(), Box<dyn Error + Send + Sync>> {

    let file_inode = inode.create(name, FileType::File, mode)?;

    let mut file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    file_inode.resize(metadata.len() as usize)?;

    let mut buf = vec![0u8; BUF_SIZE];
    let mut offset = 0usize;
    let mut len = BUF_SIZE;

    while len == BUF_SIZE {
        len = file.read(&mut buf)?;
        file_inode.write_at(offset, &buf[..len])?;
        offset += len;
    }
    Ok(())
}

fn handle_symlink(
    inode: &Arc<dyn INode>,
    name: &str,
    mode: u16,
    path: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let target = fs::read_link(path)?;
    let symlink_inode = inode.create(name, FileType::SymLink, mode)?;
    let data = target.as_os_str().as_bytes();
    symlink_inode.resize(data.len())?;
    symlink_inode.write_at(0, data)?;
    Ok(())
}

/// The old implementation of zip_dir function, which is not parallelized.
pub fn zip_dir(path: &Path, inode: Arc<dyn INode>) -> Result<(), Box<dyn Error>> {
    let mut entries: Vec<fs::DirEntry> = fs::read_dir(path)?.map(|dir| dir.unwrap()).collect();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let name_ = entry.file_name();
        let name = name_.to_str().unwrap();
        let metadata = fs::symlink_metadata(entry.path())?;
        let type_ = metadata.file_type();
        let mode = (metadata.permissions().mode() & S_IMASK) as u16;
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

/// Updates a file or directory in the destination file system based on a path in the host file system.
/// This function can update a file, symlink or create/update directory structures as needed.
/// It traverses the path components one by one, creating parent directories
/// as needed, and then performs the appropriate operation on the final path component based on its type.
/// 
/// # Parameters
/// * `dir` - Base directory path in the host file system
/// * `path` - Relative path from the base directory to the file or directory to update
/// * `root_inode` - Root INode in the destination file system where updates will be applied
///
/// # Returns
/// * `Result<(), Box<dyn Error>>` - Success or an error
pub fn update_dir(dir: &Path, path: &Path, root_inode: Arc<dyn INode>) -> Result<(), Box<dyn Error>> {
    let full_path = dir.join(path);
    let components: Vec<_> = path.components()
        .filter_map(|comp| comp.as_os_str().to_str())
        .collect();
    let mut current_inode = root_inode;
    for (idx, component) in components.iter().enumerate() {
        if idx == components.len() - 1 {
            // last component
            let metadata = fs::metadata(&full_path)?;
            let mode = (metadata.permissions().mode() & S_IMASK) as u16;
            if metadata.is_dir() {
                match current_inode.find(component) {
                    Ok(_) => (), 
                    Err(_) => {
                        current_inode.create(component, FileType::Dir, mode)?;
                    }
                }
            }
            else if metadata.is_symlink() {
                // symlink-type operation
                if let Ok(_) = current_inode.find(component) {
                    current_inode.unlink(component)?;
                }
                let symlink_inode = current_inode.create(component, FileType::SymLink, mode)?;           
                let target_path = fs::read_link(&full_path)?;
                let target_path_str = target_path.to_string_lossy().into_owned();
                let data = target_path_str.as_bytes();
                
                symlink_inode.resize(data.len())?;
                symlink_inode.write_at(0, data)?;
            }
            else if metadata.is_file(){
                // file-type operation
                if let Ok(_) = current_inode.find(component) {
                    current_inode.unlink(component)?;
                }
                let file_inode = current_inode.create(component, FileType::File, mode)?;
                
                let mut file = fs::File::open(&full_path)?;
                file_inode.resize(file.metadata()?.len() as usize)?;
                
                let mut buf = unsafe { Box::<[u8; BUF_SIZE]>::new_uninit().assume_init() };
                let mut offset = 0usize;
                let mut len = BUF_SIZE;
                while len == BUF_SIZE {
                    len = file.read(buf.as_mut())?;
                    file_inode.write_at(offset, &buf[..len])?;
                    offset += len;
                }
            }
        } 
        else {
            // mkdir
            current_inode = match current_inode.find(component) {
                Ok(inode) => inode,
                Err(_) => current_inode.create(component, FileType::Dir, 0o755)?
            };
        }
    }
    Ok(())
}
