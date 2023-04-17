use crate::dev::DevError;
use alloc::vec;
use alloc::{collections::BTreeMap, string::String, sync::Arc, vec::Vec};
use core::any::{Any, TypeId};
use core::fmt;
use core::result;
use core::str;
use spin::RwLock;

/// Abstract file system object such as file or directory.
pub trait INode: Any + Sync + Send {
    /// Read bytes at `offset` into `buf`, return the number of bytes read.
    fn read_at(&self, offset: usize, buf: &mut [u8]) -> Result<usize>;

    /// Write bytes at `offset` from `buf`, return the number of bytes written.
    fn write_at(&self, offset: usize, buf: &[u8]) -> Result<usize>;

    /// Poll the events, return a bitmap of events.
    fn poll(&self) -> Result<PollStatus> {
        Ok(PollStatus {
            read: true,
            write: true,
            error: false,
        })
    }

    /// Get metadata of the INode
    fn metadata(&self) -> Result<Metadata> {
        Err(FsError::NotSupported)
    }

    /// Set metadata of the INode
    fn set_metadata(&self, _metadata: &Metadata) -> Result<()> {
        Err(FsError::NotSupported)
    }

    /// Manipulate space of the INode
    fn fallocate(&self, _mode: &FallocateMode, _offset: usize, _len: usize) -> Result<()> {
        Err(FsError::NotSupported)
    }

    /// Sync all data and metadata
    fn sync_all(&self) -> Result<()> {
        Err(FsError::NotSupported)
    }

    /// Sync data (not include metadata)
    fn sync_data(&self) -> Result<()> {
        Err(FsError::NotSupported)
    }

    /// Resize the file
    fn resize(&self, _len: usize) -> Result<()> {
        Err(FsError::NotSupported)
    }

    /// Create a new INode in the directory
    fn create(&self, name: &str, type_: FileType, mode: u16) -> Result<Arc<dyn INode>> {
        self.create2(name, type_, mode, 0)
    }

    /// Create a new INode in the directory, with a data field for usages like device file.
    fn create2(
        &self,
        name: &str,
        type_: FileType,
        mode: u16,
        _data: usize,
    ) -> Result<Arc<dyn INode>> {
        self.create(name, type_, mode)
    }

    /// Create a hard link `name` to `other`
    fn link(&self, _name: &str, _other: &Arc<dyn INode>) -> Result<()> {
        Err(FsError::NotSupported)
    }

    /// Delete a hard link `name`
    fn unlink(&self, _name: &str) -> Result<()> {
        Err(FsError::NotSupported)
    }

    /// Move INode `self/old_name` to `target/new_name`.
    /// If `target` equals `self`, do rename.
    fn move_(&self, _old_name: &str, _target: &Arc<dyn INode>, _new_name: &str) -> Result<()> {
        Err(FsError::NotSupported)
    }

    /// Find the INode `name` in the directory
    fn find(&self, _name: &str) -> Result<Arc<dyn INode>> {
        Err(FsError::NotSupported)
    }

    /// Get the name of directory entry
    fn get_entry(&self, _id: usize) -> Result<String> {
        Err(FsError::NotSupported)
    }

    /// Iterate the directory entries
    /// Warn: This function cannot guarantee that iterating an entry only once.
    fn iterate_entries(&self, _ctx: &mut DirentWriterContext) -> Result<usize> {
        Err(FsError::NotSupported)
    }

    /// Control device
    fn io_control(&self, _cmd: u32, _data: usize) -> Result<()> {
        Err(FsError::NotSupported)
    }

    /// Get the file system of the INode
    fn fs(&self) -> Arc<dyn FileSystem> {
        unimplemented!();
    }

    /// This is used to implement dynamics cast.
    /// Simply return self in the implement of the function.
    fn as_any_ref(&self) -> &dyn Any;

    /// Get all directory entries as a Vec
    fn list(&self) -> Result<Vec<String>> {
        let info = self.metadata()?;
        if info.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        Ok((0..)
            .map(|i| self.get_entry(i))
            .take_while(|result| result.is_ok())
            .filter_map(|result| result.ok())
            .collect())
    }

    /// Lookup path from current inode.
    ///
    /// Do not follow symbolic links.
    fn lookup(&self, path: &str) -> Result<Arc<dyn INode>> {
        self.lookup_follow(path, 0)
    }

    /// Lookup path from current inode.
    ///
    /// The current inode must be a directory.
    ///
    /// The length of `path` cannot exceed PATH_MAX.
    /// If `path` ends with `/`, then the returned inode must be a directory inode.
    ///
    /// While looking up the inode, symbolic links will be followed for
    /// at most `max_follows` times, if it is not zero.
    fn lookup_follow(&self, path: &str, max_follows: usize) -> Result<Arc<dyn INode>> {
        if self.metadata()?.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if path.len() > PATH_MAX {
            return Err(FsError::NameTooLong);
        }

        // To handle symlinks
        let mut link_path = String::new();
        let mut follows = 0;

        // Initialize the first inode and the relative path
        let (mut inode, mut relative_path) = if path.starts_with("/") {
            (self.fs().root_inode(), path.trim_start_matches('/'))
        } else {
            (self.find(".")?, path)
        };

        while !relative_path.is_empty() {
            let (next_name, path_remain, must_be_dir) =
                if let Some((prefix, suffix)) = relative_path.split_once('/') {
                    let suffix = suffix.trim_start_matches('/');
                    (prefix, suffix, true)
                } else {
                    (relative_path, "", false)
                };

            // Iterate next inode
            let next_inode = inode.find(next_name)?;
            let next_inode_type = next_inode.metadata()?.type_;

            // If next inode is a symlink, follow symlinks at most `max_follows` times.
            if max_follows > 0 && next_inode_type == FileType::SymLink {
                if follows >= max_follows {
                    return Err(FsError::SymLoop);
                }
                let link_path_remain = {
                    let mut tmp_link_path = {
                        let mut content = [0u8; PATH_MAX];
                        let len = next_inode.read_at(0, &mut content)?;
                        let path_str =
                            str::from_utf8(&content[..len]).map_err(|_| FsError::EntryNotFound)?;
                        if path_str.is_empty() {
                            return Err(FsError::EntryNotFound);
                        }
                        String::from(path_str)
                    };
                    if !path_remain.is_empty() {
                        tmp_link_path += "/";
                        tmp_link_path += path_remain;
                    } else if must_be_dir {
                        tmp_link_path += "/";
                    }
                    tmp_link_path
                };

                // change the inode and relative path according to symlink
                if link_path_remain.starts_with("/") {
                    inode = inode.fs().root_inode();
                }
                link_path.clear();
                link_path.push_str(&link_path_remain.trim_start_matches('/'));
                relative_path = &link_path;
                follows += 1;
            } else {
                // If path ends with `/`, the inode must be a directory
                if must_be_dir && next_inode_type != FileType::Dir {
                    return Err(FsError::NotDir);
                }
                inode = next_inode;
                relative_path = path_remain;
            }
        }

        Ok(inode)
    }

    /// Read all contents into a vector
    fn read_as_vec(&self) -> Result<Vec<u8>> {
        let size = self.metadata()?.size;
        let mut buf = Vec::with_capacity(size);
        buf.spare_capacity_mut();
        unsafe {
            buf.set_len(size);
        }
        self.read_at(0, buf.as_mut_slice())?;
        Ok(buf)
    }

    /// Read elf64 file in to a vector lazily (by only reading the elf header)
    fn read_elf64_lazy_as_vec(&self) -> Result<Vec<u8>> {
        let size = self.metadata()?.size;
        let mut buf = vec![0; size];
        let elf64_hdr_size = 64;
        self.read_at(
            0,
            &mut buf.as_mut_slice()[..core::cmp::min(elf64_hdr_size, size)],
        )?;
        Ok(buf)
    }

    /// Get the extension of this inode
    fn ext(&self) -> Option<&Extension> {
        None
    }
}

impl dyn INode {
    /// Downcast the INode to specific struct
    pub fn downcast_ref<T: INode>(&self) -> Option<&T> {
        self.as_any_ref().downcast_ref::<T>()
    }
}

/// Maximum bytes in a path
///
/// Ref: Linux Kernel include/uapi/linux/limits.h
pub const PATH_MAX: usize = 4096;

pub enum IOCTLError {
    NotValidFD = 9,      // EBADF
    NotValidMemory = 14, // EFAULT
    NotValidParam = 22,  // EINVAL
    NotCharDevice = 25,  // ENOTTY
}

#[derive(Debug, Default)]
pub struct PollStatus {
    pub read: bool,
    pub write: bool,
    pub error: bool,
}

/// Metadata of INode
///
/// Ref: <http://pubs.opengroup.org/onlinepubs/009604499/basedefs/sys/stat.h.html>
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Metadata {
    /// Device ID
    pub dev: usize, // (major << 8) | minor
    /// Inode number
    pub inode: usize,
    /// Size in bytes
    ///
    /// SFS Note: for normal file size is the actuate file size
    /// for directory this is count of dirent.
    pub size: usize,
    /// A file system-specific preferred I/O block size for this object.
    /// In some file system types, this may vary from file to file.
    pub blk_size: usize,
    /// Size in blocks
    pub blocks: usize,
    /// Time of last access
    pub atime: Timespec,
    /// Time of last modification
    pub mtime: Timespec,
    /// Time of last change
    pub ctime: Timespec,
    /// Type of file
    pub type_: FileType,
    /// Permission
    pub mode: u16,
    /// Number of hard links
    ///
    /// SFS Note: different from linux, "." and ".." count in nlinks
    /// this is same as original ucore.
    pub nlinks: usize,
    /// User ID
    pub uid: usize,
    /// Group ID
    pub gid: usize,
    /// Raw device id
    /// e.g. /dev/null: makedev(0x1, 0x3)
    pub rdev: usize, // (major << 8) | minor
}

#[repr(C)]
#[derive(Default, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
pub struct Timespec {
    pub sec: i64,
    pub nsec: i64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FileType {
    File,
    Dir,
    SymLink,
    CharDevice,
    BlockDevice,
    NamedPipe,
    Socket,
}

pub const FS_MAC_SIZE: usize = 16;
pub type FsMac = [u8; FS_MAC_SIZE];

/// Metadata of FileSystem
///
/// Ref: <http://pubs.opengroup.org/onlinepubs/9699919799/>
#[derive(Debug, Default)]
pub struct FsInfo {
    /// File system type
    pub magic: usize,
    /// File system block size
    pub bsize: usize,
    /// Fundamental file system block size
    pub frsize: usize,
    /// Total number of blocks on file system in units of `frsize`
    pub blocks: usize,
    /// Total number of free blocks
    pub bfree: usize,
    /// Number of free blocks available to non-privileged process
    pub bavail: usize,
    /// Total number of file serial numbers
    pub files: usize,
    /// Total number of free file serial numbers
    pub ffree: usize,
    /// Maximum filename length
    pub namemax: usize,
}

/// Operation mode for fallocate
#[derive(Debug)]
pub enum FallocateMode {
    Allocate(AllocFlags),
    PunchHoleKeepSize,
    ZeroRange,
    ZeroRangeKeepSize,
    CollapseRange,
    InsertRange,
}

bitflags! {
    /// Flags for the allocate mode
    pub struct AllocFlags: u32 {
        const KEEP_SIZE = 0x01;
        const UNSHARE_RANGE = 0x40;
    }
}

// Note: IOError/NoMemory always lead to a panic since it's hard to recover from it.
//       We also panic when we can not parse the fs on disk normally
#[derive(Debug, Eq, PartialEq)]
pub enum FsError {
    NotSupported,     // E_UNIMP, or E_INVAL
    NotFile,          // E_ISDIR
    IsDir,            // E_ISDIR, used only in link
    NotDir,           // E_NOTDIR
    EntryNotFound,    // E_NOENT
    EntryExist,       // E_EXIST
    NotSameFs,        // E_XDEV
    InvalidParam,     // E_INVAL
    NoDeviceSpace, // E_NOSPC, but is defined and not used in the original ucore, which uses E_NO_MEM
    DirRemoved,    // E_NOENT, when the current dir was remove by a previous unlink
    DirNotEmpty,   // E_NOTEMPTY
    WrongFs,       // E_INVAL, when we find the content on disk is wrong when opening the device
    DeviceError(i32), // Device error contains the inner error number to report the error of device
    IOCTLError,
    NoDevice,
    Again,          // E_AGAIN, when no data is available, never happens in fs
    SymLoop,        // E_LOOP
    Busy,           // E_BUSY
    WrProtected,    // E_RDOFS
    NoIntegrity,    // E_RDOFS
    PermError,      // E_PERM
    NameTooLong,    // E_NAMETOOLONG
    FileTooBig,     // E_FBIG
    OpNotSupported, // E_OPNOTSUPP
    NotMountPoint,  // E_INVAL
}

impl fmt::Display for FsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<DevError> for FsError {
    fn from(err: DevError) -> Self {
        FsError::DeviceError(err.0)
    }
}

pub type Result<T> = result::Result<T, FsError>;

/// Abstract file system
pub trait FileSystem: Sync + Send {
    /// Sync all data to the storage
    fn sync(&self) -> Result<()>;

    /// Get the root INode of the file system
    fn root_inode(&self) -> Arc<dyn INode>;

    /// Get the root MAC of the file system
    fn root_mac(&self) -> FsMac {
        Default::default()
    }

    /// Get the file system information
    fn info(&self) -> FsInfo;
}

/// An extension is a set of objects that is attached to
/// an inode.
///
/// Each objects of an extension is of different types.
/// In other words, types are used as the keys to get and
/// set the objects in an extension.
pub struct Extension {
    data: RwLock<BTreeMap<TypeId, Arc<dyn AnyExt>>>,
}

impl Extension {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
        }
    }

    /// Get an object of `Arc<T>`.
    pub fn get<T: AnyExt>(&self) -> Option<Arc<T>> {
        self.data
            .read()
            .get(&TypeId::of::<T>())
            .map(|any| unsafe { Arc::from_raw(Arc::into_raw(Arc::clone(any)) as *const T) })
    }

    /// Try to get an object of `Arc<T>`. If no object of the type exists,
    /// put the default value for the type, then return it.
    pub fn get_or_put_default<T: AnyExt + Default>(&self) -> Arc<T> {
        let mut data = self.data.write();
        if let Some(any) = data.get(&TypeId::of::<T>()) {
            return unsafe { Arc::from_raw(Arc::into_raw(Arc::clone(any)) as *const T) };
        }
        let obj: Arc<T> = Arc::new(Default::default());
        data.insert(TypeId::of::<T>(), obj.clone());
        obj
    }

    /// Put an object of `Arc<T>`. If there exists one object of the type,
    /// then the old one is returned.
    pub fn put<T: AnyExt>(&self, obj: Arc<T>) -> Option<Arc<T>> {
        self.data
            .write()
            .insert(TypeId::of::<T>(), obj)
            .map(|any| unsafe { Arc::from_raw(Arc::into_raw(any) as *const T) })
    }

    /// Delete an object of `Arc<T>`. If there exists one object of the type,
    /// then the old one is returned.
    pub fn del<T: AnyExt>(&self) -> Option<Arc<T>> {
        self.data
            .write()
            .remove(&TypeId::of::<T>())
            .map(|any| unsafe { Arc::from_raw(Arc::into_raw(any) as *const T) })
    }
}

impl Clone for Extension {
    fn clone(&self) -> Self {
        Self {
            data: RwLock::new(self.data.read().clone()),
        }
    }
}

impl Default for Extension {
    fn default() -> Self {
        Self::new()
    }
}

/// The struct which implements the AnyExt trait can be used in Extension
pub trait AnyExt: Any + Send + Sync {}

/// DirentWriterContext is a wrapper of DirentWriter with directory position
/// After a successful write, the position increases correspondingly
pub struct DirentWriterContext<'a> {
    pos: usize,
    writer: &'a mut dyn DirentWriter,
}

impl<'a> DirentWriterContext<'a> {
    pub fn new(pos: usize, writer: &'a mut dyn DirentWriter) -> Self {
        Self { pos, writer }
    }

    pub fn write_entry(&mut self, name: &str, ino: u64, type_: FileType) -> Result<()> {
        self.writer.write_entry(name, ino, type_)?;
        self.pos += 1;
        Ok(())
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn written_len(&self) -> usize {
        self.writer.written_len()
    }
}

/// DirentWriter is used to write directory entry, the object which implements it can decide how to format the data
pub trait DirentWriter: Sync + Send {
    fn write_entry(&mut self, name: &str, ino: u64, type_: FileType) -> Result<()>;
    fn written_len(&self) -> usize;
}

impl DirentWriter for Vec<String> {
    fn write_entry(&mut self, name: &str, _ino: u64, _type_: FileType) -> Result<()> {
        self.push(name.into());
        Ok(())
    }
    fn written_len(&self) -> usize {
        self.len()
    }
}

/// Helper macro to write dirent entry of one INode
#[macro_export]
macro_rules! write_inode_entry {
    ($ctx:expr, $name:expr, $inode:expr) => {
        let ctx = $ctx;
        let name = $name;
        let inode = $inode;

        if let Err(e) = ctx.write_entry(
            name,
            inode.metadata()?.inode as u64,
            inode.metadata()?.type_,
        ) {
            if ctx.written_len() == 0 {
                return Err(e);
            } else {
                return Ok(ctx.written_len());
            }
        }
    };
}

pub fn make_rdev(major: usize, minor: usize) -> usize {
    ((major & 0xfff) << 8) | (minor & 0xff)
}
