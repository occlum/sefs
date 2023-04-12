#![cfg_attr(not(any(test, feature = "std")), no_std)]
#![deny(warnings)]
#![feature(get_mut_unchecked)]
#![feature(new_uninit)]

extern crate alloc;
//#[macro_use]
extern crate log;

use alloc::{
    boxed::Box,
    collections::BTreeMap,
    string::{String, ToString},
    sync::{Arc, Weak},
    vec::Vec,
};
use core::any::Any;
use core::sync::atomic::{AtomicUsize, Ordering};
use rcore_fs::dev::{DevError, EIO};
use rcore_fs::vfs::*;
use spin::{RwLock, RwLockWriteGuard};

#[cfg(test)]
mod tests;

/// magic number for unionfs
pub const UNIONFS_MAGIC: usize = 0x2f8d_be2f;

/// Union File System
///
/// It allows files and directories of separate file systems, known as branches,
/// to be transparently overlaid, forming a single coherent file system.
pub struct UnionFS {
    /// Inner file systems
    /// NOTE: the 1st is RW, others are RO
    inners: Vec<Arc<dyn FileSystem>>,
    /// Weak reference to self
    self_ref: Weak<UnionFS>,
    /// Root INode
    root_inode: Option<Arc<UnionINode>>,
    /// Allocate INode ID
    next_inode_id: AtomicUsize,
}

const ROOT_INODE_ID: usize = 2;

/// INode for `UnionFS`
pub struct UnionINode {
    /// INode ID
    id: usize,
    /// Reference to `UnionFS`
    fs: Weak<UnionFS>,
    /// Inner
    inner: RwLock<UnionINodeInner>,
    /// Extensions
    ext: Extension,
}

/// The mutable part of `UnionINode`
struct UnionINodeInner {
    /// Path from root INode with mode
    path_with_mode: PathWithMode,
    /// INodes for each inner file systems
    inners: Vec<VirtualINode>,
    /// Reference to myself
    this: Weak<UnionINode>,
    /// Reference to parent
    parent: Weak<UnionINode>,
    /// Whether uppper directory occludes lower directory
    opaque: bool,
    /// Merged directory entries.
    cached_children: EntriesMap,
}

/// Directory entries
struct EntriesMap {
    /// HashMap of the entries
    map: BTreeMap<String, Option<Entry>>,
    /// Whether the map is merged already
    is_merged: bool,
}

impl EntriesMap {
    fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            is_merged: false,
        }
    }
}

/// Directory entry. It holds the reference to the real INode
enum Entry {
    /// A weak reference to the file/symlink INode with inode_id to re-new the INode
    /// if it is dropped
    File(Weak<UnionINode>, usize),
    /// A Strong reference to the dir INode
    Dir(Arc<UnionINode>),
}

impl Entry {
    fn new(inode: &Arc<UnionINode>) -> Self {
        if inode.metadata().unwrap().type_ == FileType::Dir {
            Self::Dir(Arc::clone(inode))
        } else {
            Self::File(Arc::downgrade(inode), inode.id)
        }
    }

    fn as_inode(&self) -> Option<Arc<UnionINode>> {
        match self {
            Self::Dir(inode) => Some(Arc::clone(inode)),
            Self::File(weak_inode, _) => weak_inode.upgrade(),
        }
    }

    fn id(&self) -> Option<usize> {
        match self {
            Self::File(_, id) => Some(*id),
            _ => None,
        }
    }
}

/// A virtual INode of a path in a FS
#[derive(Clone)]
struct VirtualINode {
    /// The last valid INode in the path.
    last_inode: Arc<dyn INode>,
    /// The distance / depth to the last valid INode.
    ///
    /// This should be 0 if the last INode is the current one,
    /// otherwise the path is not exist in the FS, and this is a virtual INode.
    distance: usize,
}

/// the name of MAC file
const MAC_FILE: &str = ".ufs.mac";
/// the prefix of whiteout file
const WH_PREFIX: &str = ".ufs.wh.";
/// the prefix of opaque file
const OPAQUE_PREFIX: &str = ".ufs.opq.";

impl UnionFS {
    /// Create a `UnionFS` wrapper for file system `fs`
    pub fn new(fs: Vec<Arc<dyn FileSystem>>) -> Result<Arc<Self>> {
        let container_fs = &fs[0];
        match container_fs.root_inode().find(MAC_FILE) {
            Ok(file) => Self::verify_with_mac_file(file, &fs)?,
            Err(FsError::EntryNotFound) => Self::new_mac_file(&fs)?,
            _ => unreachable!(),
        }
        Ok(UnionFS {
            inners: fs,
            self_ref: Weak::default(),
            root_inode: None,
            next_inode_id: AtomicUsize::new(ROOT_INODE_ID + 1),
        }
        .wrap())
    }

    /// Wrap pure `UnionFS` with `Arc<..>`.
    /// Used in constructors.
    fn wrap(self) -> Arc<Self> {
        let mut fs = Arc::new(self);
        unsafe {
            Arc::get_mut_unchecked(&mut fs).self_ref = Arc::downgrade(&fs);
        }
        let root_inode = Self::new_root_inode(&fs);
        unsafe {
            Arc::get_mut_unchecked(&mut fs).root_inode = Some(root_inode);
        }
        fs
    }

    /// Strong type version of `root_inode`
    pub fn root_inode(&self) -> Arc<UnionINode> {
        (*self.root_inode.as_ref().unwrap()).clone()
    }

    /// Verify the MAC(s) in file with the input FS
    fn verify_with_mac_file(file: Arc<dyn INode>, fs: &[Arc<dyn FileSystem>]) -> Result<()> {
        let mut mac_content: FsMac = Default::default();
        let mut offset = 0;
        let mut iterator = fs[1..].iter();
        loop {
            let inner_fs_option = iterator.next();
            let len = file.read_at(offset, &mut mac_content)?;
            if len == 0 {
                if inner_fs_option.is_some() {
                    return Err(FsError::WrongFs);
                }
                // pass the check
                break Ok(());
            }
            assert!(len == FS_MAC_SIZE);
            if inner_fs_option.unwrap().root_mac() != mac_content {
                return Err(FsError::WrongFs);
            }
            offset += FS_MAC_SIZE;
        }
    }

    /// Create a file to record the FS's MAC
    fn new_mac_file(fs: &[Arc<dyn FileSystem>]) -> Result<()> {
        let file = fs[0].root_inode().create(MAC_FILE, FileType::File, 0o777)?;
        let mut offset = 0;
        for inner_fs in fs[1..].iter() {
            let fs_mac = inner_fs.root_mac();
            let len = file.write_at(offset, &fs_mac)?;
            assert!(len == fs_mac.len());
            offset += fs_mac.len();
        }
        Ok(())
    }

    /// Create a new root INode, only use in the constructor
    fn new_root_inode(fs: &Arc<Self>) -> Arc<UnionINode> {
        let inners = fs
            .inners
            .iter()
            .map(|fs| VirtualINode {
                last_inode: fs.root_inode(),
                distance: 0,
            })
            .collect();
        let root_inode = Arc::new(UnionINode {
            id: ROOT_INODE_ID,
            fs: fs.self_ref.clone(),
            inner: RwLock::new(UnionINodeInner {
                inners,
                cached_children: EntriesMap::new(),
                this: Weak::default(),
                parent: Weak::default(),
                path_with_mode: PathWithMode::new(),
                opaque: false,
            }),
            ext: Extension::new(),
        });
        root_inode.inner.write().this = Arc::downgrade(&root_inode);
        root_inode.inner.write().parent = Arc::downgrade(&root_inode);
        root_inode
    }

    /// Create a new INode
    fn create_inode(
        &self,
        inodes: Vec<VirtualINode>,
        path_with_mode: PathWithMode,
        opaque: bool,
        id: Option<usize>,
        ext: Option<Extension>,
    ) -> Arc<UnionINode> {
        Arc::new(UnionINode {
            id: id.unwrap_or_else(|| self.alloc_inode_id()),
            fs: self.self_ref.clone(),
            inner: RwLock::new(UnionINodeInner {
                inners: inodes,
                cached_children: EntriesMap::new(),
                this: Weak::default(),
                parent: Weak::default(),
                path_with_mode,
                opaque,
            }),
            ext: ext.unwrap_or_default(),
        })
    }

    /// Allocate an INode ID
    fn alloc_inode_id(&self) -> usize {
        self.next_inode_id.fetch_add(1, Ordering::SeqCst)
    }
}

impl VirtualINode {
    /// Walk this INode to './name'
    fn walk(&mut self, name: &str) {
        if self.distance == 0 {
            match self.last_inode.find(name) {
                Ok(inode) => self.last_inode = inode,
                Err(_) => self.distance = 1,
            }
        } else {
            match name {
                ".." => self.distance -= 1,
                "." => {}
                _ => self.distance += 1,
            }
        }
    }

    /// Find the next INode at './name'
    pub fn find(&self, name: &str) -> Self {
        let mut inode = self.clone();
        inode.walk(name);
        inode
    }

    /// Whether this is a real INode
    pub fn is_real(&self) -> bool {
        self.distance == 0
    }

    /// Unwrap the last valid INode in the path
    pub fn as_real(&self) -> Option<&Arc<dyn INode>> {
        match self.distance {
            0 => Some(&self.last_inode),
            _ => None,
        }
    }
}

impl UnionINodeInner {
    /// Merge directory entries from several INodes
    fn merge_entries(
        inners: &[VirtualINode],
        opaque: bool,
    ) -> Result<BTreeMap<String, Option<Entry>>> {
        let mut entries = BTreeMap::new();
        // images
        if !opaque {
            for inode in inners[1..].iter().filter_map(|v| v.as_real()) {
                // if the INode in image FS is not a directory,
                // skip to merge the entries in lower FS
                if inode.metadata()?.type_ != FileType::Dir {
                    break;
                }
                for name in inode.list()? {
                    // skip the two special entries
                    if name.is_self() || name.is_parent() {
                        continue;
                    }
                    entries.insert(name, None);
                }
            }
        }
        // container
        if let Some(inode) = inners[0].as_real() {
            for name in inode.list()? {
                // skip the special entries
                if name.starts_with(OPAQUE_PREFIX)
                    || name == MAC_FILE
                    || name.is_self()
                    || name.is_parent()
                {
                    continue;
                }
                if name.starts_with(WH_PREFIX) {
                    // whiteout
                    entries.remove(name.strip_prefix(WH_PREFIX).unwrap());
                } else {
                    entries.insert(name, None);
                }
            }
        }
        Ok(entries)
    }

    /// Get the merged directory entries, the upper INode must to be a directory
    pub fn entries(&mut self) -> &mut BTreeMap<String, Option<Entry>> {
        let cache = &mut self.cached_children.map;
        if !self.cached_children.is_merged {
            let entries = Self::merge_entries(&self.inners, self.opaque).unwrap();
            //debug!("{:?} cached dirents: {:?}", self.path, entries.keys());
            *cache = entries;
            self.cached_children.is_merged = true;
        }
        cache
    }

    /// Determine the upper INode
    pub fn inode(&self) -> &Arc<dyn INode> {
        self.inners
            .iter()
            .filter_map(|v| v.as_real())
            .next()
            .unwrap()
    }

    /// Ensure container INode exists in this `UnionINode` and return it.
    ///
    /// If the INode is not exist, first `mkdir -p` the base path.
    /// Then if it is a file, create a copy of the image file;
    /// If it is a directory, create an empty dir.
    /// If it is a symlink, create a copy of the image symlink.
    pub fn container_inode(&mut self) -> Result<Arc<dyn INode>> {
        let type_ = self.inode().metadata()?.type_;
        if type_ != FileType::File
            && type_ != FileType::Dir
            && type_ != FileType::SymLink
            && type_ != FileType::Socket
        {
            return Err(FsError::NotSupported);
        }
        let VirtualINode {
            mut last_inode,
            distance,
        } = self.inners[0].clone();
        if distance == 0 {
            return Ok(last_inode);
        }

        for (dir_name, mode) in &self.path_with_mode.lastn(distance)[..distance - 1] {
            last_inode = match last_inode.find(dir_name) {
                Ok(inode) => inode,
                // create dirs to the base path
                Err(FsError::EntryNotFound) => last_inode.create(dir_name, FileType::Dir, *mode)?,
                Err(e) => return Err(e),
            };
        }

        let (last_inode_name, mode) = &self.path_with_mode.lastn(1)[0];
        match last_inode.find(last_inode_name) {
            Ok(inode) => {
                last_inode = inode;
            }
            Err(FsError::EntryNotFound) => {
                // create file/dir/symlink in container
                match type_ {
                    FileType::Dir => {
                        last_inode = last_inode.create(last_inode_name, FileType::Dir, *mode)?;
                    }
                    FileType::File => {
                        let last_file_inode =
                            last_inode.create(last_inode_name, FileType::File, *mode)?;
                        // copy it from image to container chunk by chunk
                        const BUF_SIZE: usize = 0x10000;
                        let mut buf = unsafe { Box::<[u8; BUF_SIZE]>::new_uninit().assume_init() };
                        let mut offset = 0usize;
                        let mut len = BUF_SIZE;
                        while len == BUF_SIZE {
                            len = match self.inode().read_at(offset, buf.as_mut()) {
                                Ok(len) => len,
                                Err(e) => {
                                    last_inode.unlink(last_inode_name)?;
                                    return Err(e);
                                }
                            };
                            match last_file_inode.write_at(offset, &buf[..len]) {
                                Ok(len_written) if len_written != len => {
                                    last_inode.unlink(last_inode_name)?;
                                    return Err(FsError::from(DevError(EIO)));
                                }
                                Err(e) => {
                                    last_inode.unlink(last_inode_name)?;
                                    return Err(e);
                                }
                                Ok(_) => {}
                            }
                            offset += len;
                        }
                        last_inode = last_file_inode;
                    }
                    FileType::SymLink | FileType::Socket => {
                        let last_link_inode = last_inode.create(last_inode_name, type_, *mode)?;
                        let data = match self.inode().read_as_vec() {
                            Ok(data) => data,
                            Err(e) => {
                                last_inode.unlink(last_inode_name)?;
                                return Err(e);
                            }
                        };
                        match last_link_inode.write_at(0, &data) {
                            Ok(len_written) if len_written != data.len() => {
                                last_inode.unlink(last_inode_name)?;
                                return Err(FsError::from(DevError(EIO)));
                            }
                            Err(e) => {
                                last_inode.unlink(last_inode_name)?;
                                return Err(e);
                            }
                            Ok(_) => {}
                        }
                        last_inode = last_link_inode;
                    }
                    _ => unreachable!(),
                }
            }
            Err(e) => return Err(e),
        }
        self.inners[0] = VirtualINode {
            last_inode: last_inode.clone(),
            distance: 0,
        };
        Ok(last_inode)
    }

    /// Return container INode if it has
    pub fn maybe_container_inode(&self) -> Option<&Arc<dyn INode>> {
        self.inners[0].as_real()
    }

    /// Whether it has underlying image INodes
    pub fn has_image_inode(&self) -> bool {
        self.inners[1..].iter().any(|v| v.is_real())
    }
}

impl FileSystem for UnionFS {
    fn sync(&self) -> Result<()> {
        for fs in self.inners.iter() {
            fs.sync()?;
        }
        Ok(())
    }

    fn root_inode(&self) -> Arc<dyn INode> {
        self.root_inode()
    }

    fn info(&self) -> FsInfo {
        let mut merged_info: FsInfo = Default::default();
        for (idx, fs) in self.inners.iter().enumerate() {
            // the writable top layer
            if idx == 0 {
                merged_info.bsize = fs.info().bsize;
                merged_info.frsize = fs.info().frsize;
                merged_info.namemax = fs.info().namemax;
                merged_info.bfree = fs.info().bfree;
                merged_info.bavail = fs.info().bavail;
                merged_info.ffree = fs.info().ffree;
            }
            merged_info.blocks = merged_info.blocks.saturating_add(fs.info().blocks);
            merged_info.files = merged_info.files.saturating_add(fs.info().files);
        }
        merged_info.magic = UNIONFS_MAGIC;
        merged_info
    }
}

impl UnionINode {
    /// Helper function to create a child UnionINode, if `id` is provided, use it as
    /// the inode id of the new inode, or just allocate a new one.
    fn new_inode(
        fs: &Weak<UnionFS>,
        parent_guard: &RwLockWriteGuard<UnionINodeInner>,
        name: &str,
        id: Option<usize>,
        ext: Option<Extension>,
    ) -> Arc<UnionINode> {
        let new_inode = {
            let inodes: Vec<_> = parent_guard.inners.iter().map(|x| x.find(name)).collect();
            let mode = inodes
                .iter()
                .find_map(|v| v.as_real())
                .unwrap()
                .metadata()
                .unwrap()
                .mode;
            let path_with_mode = parent_guard.path_with_mode.with_next(name, mode);
            let opaque = {
                let mut opaque = parent_guard.opaque;
                if let Some(inode) = parent_guard.maybe_container_inode() {
                    if inode.find(&name.opaque()).is_ok() {
                        opaque = true;
                    }
                }
                opaque
            };
            let fs = fs.upgrade().unwrap();
            fs.create_inode(inodes, path_with_mode, opaque, id, ext)
        };
        if new_inode.metadata().unwrap().type_ == FileType::Dir {
            new_inode.inner.write().this = Arc::downgrade(&new_inode);
            new_inode.inner.write().parent = Arc::downgrade(&parent_guard.this.upgrade().unwrap());
        }
        new_inode
    }
}

impl INode for UnionINode {
    fn read_at(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        let inner = self.inner.read();
        inner.inode().read_at(offset, buf)
    }

    fn write_at(&self, offset: usize, buf: &[u8]) -> Result<usize> {
        let mut inner = self.inner.write();
        inner.container_inode()?.write_at(offset, buf)
    }

    fn poll(&self) -> Result<PollStatus> {
        let inner = self.inner.read();
        inner.inode().poll()
    }

    fn metadata(&self) -> Result<Metadata> {
        let inner = self.inner.read();
        let mut metadata = inner.inode().metadata()?;
        metadata.inode = self.id;
        Ok(metadata)
    }

    fn set_metadata(&self, metadata: &Metadata) -> Result<()> {
        let mut inner = self.inner.write();
        inner.container_inode()?.set_metadata(metadata)
    }

    fn sync_all(&self) -> Result<()> {
        let inner = self.inner.read();
        if let Some(inode) = inner.maybe_container_inode() {
            inode.sync_all()
        } else {
            Ok(())
        }
    }

    fn sync_data(&self) -> Result<()> {
        let inner = self.inner.read();
        if let Some(inode) = inner.maybe_container_inode() {
            inode.sync_data()
        } else {
            Ok(())
        }
    }

    fn fallocate(&self, mode: &FallocateMode, offset: usize, len: usize) -> Result<()> {
        let mut inner = self.inner.write();
        inner.container_inode()?.fallocate(mode, offset, len)
    }

    fn resize(&self, len: usize) -> Result<()> {
        let mut inner = self.inner.write();
        inner.container_inode()?.resize(len)
    }

    fn create(&self, name: &str, type_: FileType, mode: u16) -> Result<Arc<dyn INode>> {
        if self.metadata()?.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if name.is_reserved() {
            return Err(FsError::InvalidParam);
        }
        if name.is_self() || name.is_parent() {
            return Err(FsError::EntryExist);
        }
        let mut inner = self.inner.write();
        if inner.entries().contains_key(name) {
            return Err(FsError::EntryExist);
        }
        let container_inode = inner.container_inode()?;
        container_inode.create(name, type_, mode)?;
        if container_inode.find(&name.whiteout()).is_ok() {
            match type_ {
                // rename the whiteout file to opaque
                FileType::Dir => {
                    if let Err(e) =
                        container_inode.move_(&name.whiteout(), &container_inode, &name.opaque())
                    {
                        // recover
                        container_inode.unlink(name)?;
                        return Err(e);
                    }
                }
                // unlink the whiteout file
                _ => {
                    if let Err(e) = container_inode.unlink(&name.whiteout()) {
                        // recover
                        container_inode.unlink(name)?;
                        return Err(e);
                    }
                }
            }
        }
        let new_inode = Self::new_inode(&self.fs, &inner, name, None, None);
        inner
            .entries()
            .insert(String::from(name), Some(Entry::new(&new_inode)));
        Ok(new_inode)
    }

    fn link(&self, name: &str, other: &Arc<dyn INode>) -> Result<()> {
        if self.metadata()?.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if name.is_reserved() {
            return Err(FsError::InvalidParam);
        }
        if name.is_self() || name.is_parent() {
            return Err(FsError::EntryExist);
        }
        if self.inner.write().entries().contains_key(name) {
            return Err(FsError::EntryExist);
        }
        let child = other
            .downcast_ref::<UnionINode>()
            .ok_or(FsError::NotSameFs)?;
        if child.metadata()?.type_ == FileType::Dir {
            return Err(FsError::IsDir);
        }
        // ensure 'child' exists in container
        // copy from image on necessary
        let child_inode = child.inner.write().container_inode()?;
        let mut inner = self.inner.write();
        // when we got the lock, the name may have been created by another thread
        if inner.entries().contains_key(name) {
            return Err(FsError::EntryExist);
        }
        let this = inner.container_inode()?;
        this.link(name, &child_inode)?;
        // unlink the whiteout file
        match this.unlink(&name.whiteout()) {
            Ok(_) | Err(FsError::EntryNotFound) => {}
            Err(e) => {
                // recover
                this.unlink(name)?;
                return Err(e);
            }
        }
        // add `name` to entry cache
        inner.entries().insert(String::from(name), None);
        Ok(())
    }

    fn unlink(&self, name: &str) -> Result<()> {
        if self.metadata()?.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if name.is_self() || name.is_parent() {
            return Err(FsError::IsDir);
        }
        let inode = self.find(name)?;
        let inode_type = inode.metadata()?.type_;
        if inode_type == FileType::Dir && inode.list()?.len() > 2 {
            return Err(FsError::DirNotEmpty);
        }
        let mut inner = self.inner.write();
        // when we got the lock, the entry may have been removed by another thread
        if !inner.entries().contains_key(name) {
            return Err(FsError::EntryNotFound);
        }
        // if file is in container, remove directly
        let dir_inode = inner.container_inode()?;
        match dir_inode.find(name) {
            Ok(inode) if inode_type == FileType::Dir => {
                for elem in inode
                    .list()?
                    .iter()
                    .filter(|elem| elem.as_str() != "." && elem.as_str() != "..")
                {
                    inode.unlink(elem)?;
                }
                dir_inode.unlink(name)?;
                if dir_inode.find(&name.opaque()).is_ok() {
                    dir_inode.unlink(&name.opaque())?;
                }
            }
            Ok(_) => dir_inode.unlink(name)?,
            Err(_) => {}
        }
        if inode
            .downcast_ref::<UnionINode>()
            .unwrap()
            .inner
            .read()
            .has_image_inode()
        {
            // add whiteout to container
            dir_inode.create(&name.whiteout(), FileType::File, 0o777)?;
        }
        // remove `name` from entry cache
        inner.entries().remove(name);
        Ok(())
    }

    fn move_(&self, old_name: &str, target: &Arc<dyn INode>, new_name: &str) -> Result<()> {
        if old_name.is_self() || old_name.is_parent() {
            return Err(FsError::IsDir);
        }
        if new_name.is_self() || new_name.is_parent() {
            return Err(FsError::IsDir);
        }
        if new_name.is_reserved() {
            return Err(FsError::InvalidParam);
        }

        let old = self.find(old_name)?;
        let old = old.downcast_ref::<UnionINode>().unwrap();
        let old_inode_type = old.metadata()?.type_;
        // return error when moving a directory from image to container
        // TODO: support the "redirect_dir" feature
        // [Ref](https://www.kernel.org/doc/html/latest/filesystems/overlayfs.html#renaming-directories)
        if old_inode_type == FileType::Dir && old.inner.read().has_image_inode() {
            return Err(FsError::NotSameFs);
        }
        let target = target
            .downcast_ref::<UnionINode>()
            .ok_or(FsError::NotSameFs)?;
        if target.metadata()?.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        // Disallow to make a directory a subdirectory of itself
        // Note: We have already add a check in Occlum to verify if the newpath
        //       contained a path prefix of the oldpath
        // Add the check here to avoid deadlock
        // TODO: How to know if target is a descendant of old ?
        if old.metadata()?.inode == target.metadata()?.inode {
            return Err(FsError::InvalidParam);
        }

        if let Ok(new_inode) = target.find(new_name) {
            if old.metadata()?.inode == new_inode.metadata()?.inode {
                return Ok(());
            }
            let new_inode_type = new_inode.metadata()?.type_;
            // if 'old_name' is a directory,
            // 'new_name' must either not exist, or an empty directory.
            match (old_inode_type, new_inode_type) {
                (FileType::Dir, FileType::Dir) => {
                    if new_inode.list()?.len() > 2 {
                        return Err(FsError::DirNotEmpty);
                    }
                }
                (FileType::Dir, _) => {
                    return Err(FsError::NotDir);
                }
                (_, FileType::Dir) => {
                    return Err(FsError::IsDir);
                }
                _ => {}
            }
            target.unlink(new_name)?;
        }

        // ensure 'old_name' exists in container
        // copy the file from image on necessary
        old.inner.write().container_inode()?;
        // self and target are the same INode
        if self.metadata()?.inode == target.metadata()?.inode {
            let mut self_inner = self.inner.write();
            let self_inode = self_inner.container_inode().unwrap();
            self_inode.move_(old_name, &self_inode, new_name)?;
            if old.inner.read().has_image_inode() {
                match self_inode.create(&old_name.whiteout(), FileType::File, 0o777) {
                    Ok(_) => {}
                    Err(e) => {
                        // recover
                        self_inode.move_(new_name, &self_inode, old_name)?;
                        return Err(e);
                    }
                }
            }
            if self_inode.find(&new_name.whiteout()).is_ok() {
                match old_inode_type {
                    // if is a directory, rename the whiteout to opaque
                    FileType::Dir => {
                        match self_inode.move_(
                            &new_name.whiteout(),
                            &self_inode,
                            &new_name.opaque(),
                        ) {
                            Ok(_) => {}
                            Err(e) => {
                                // recover
                                self_inode.move_(new_name, &self_inode, old_name)?;
                                if old.inner.read().has_image_inode() {
                                    self_inode.unlink(&old_name.whiteout())?;
                                }
                                return Err(e);
                            }
                        }
                    }
                    // if is a file, unlink the whiteout file
                    _ => match self_inode.unlink(&new_name.whiteout()) {
                        Ok(_) => {}
                        Err(e) => {
                            // recover
                            self_inode.move_(new_name, &self_inode, old_name)?;
                            if old.inner.read().has_image_inode() {
                                self_inode.unlink(&old_name.whiteout())?;
                            }
                            return Err(e);
                        }
                    },
                }
            }
            let new_inode = Self::new_inode(
                &self.fs,
                &self_inner,
                new_name,
                Some(old.id),
                Some(old.ext.clone()),
            );
            self_inner.entries().remove(old_name);
            self_inner
                .entries()
                .insert(String::from(new_name), Some(Entry::new(&new_inode)));
        } else {
            // self and target are different INodes
            let (mut self_inner, mut target_inner) = {
                if self.metadata()?.inode < target.metadata()?.inode {
                    let self_inner = self.inner.write();
                    let target_inner = target.inner.write();
                    (self_inner, target_inner)
                } else {
                    let target_inner = target.inner.write();
                    let self_inner = self.inner.write();
                    (self_inner, target_inner)
                }
            };
            let self_inode = self_inner.container_inode().unwrap();
            let target_inode = target_inner.container_inode()?;
            self_inode.move_(old_name, &target_inode, new_name)?;
            if old.inner.read().has_image_inode() {
                match self_inode.create(&old_name.whiteout(), FileType::File, 0o777) {
                    Ok(_) => {}
                    Err(e) => {
                        // recover
                        target_inode.move_(new_name, &self_inode, old_name)?;
                        return Err(e);
                    }
                }
            }
            if target_inode.find(&new_name.whiteout()).is_ok() {
                match old_inode_type {
                    // if is a directory, rename the whiteout to opaque
                    FileType::Dir => match target_inode.move_(
                        &new_name.whiteout(),
                        &target_inode,
                        &new_name.opaque(),
                    ) {
                        Ok(_) => {}
                        Err(e) => {
                            // recover
                            target_inode.move_(new_name, &self_inode, old_name)?;
                            if old.inner.read().has_image_inode() {
                                self_inode.unlink(&old_name.whiteout())?;
                            }
                            return Err(e);
                        }
                    },
                    // if is a file, unlink the whiteout file
                    _ => match target_inode.unlink(&new_name.whiteout()) {
                        Ok(_) => (),
                        Err(e) => {
                            // recover
                            target_inode.move_(new_name, &self_inode, old_name)?;
                            if old.inner.read().has_image_inode() {
                                self_inode.unlink(&old_name.whiteout())?;
                            }
                            return Err(e);
                        }
                    },
                }
            }
            let new_inode = Self::new_inode(
                &self.fs,
                &target_inner,
                new_name,
                Some(old.id),
                Some(old.ext.clone()),
            );
            self_inner.entries().remove(old_name);
            target_inner
                .entries()
                .insert(String::from(new_name), Some(Entry::new(&new_inode)));
        }
        Ok(())
    }

    fn find(&self, name: &str) -> Result<Arc<dyn INode>> {
        if self.metadata()?.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        let mut inner = self.inner.write();

        // Handle the two special entries
        if name.is_self() {
            return Ok(inner.this.upgrade().unwrap());
        } else if name.is_parent() {
            return Ok(inner.parent.upgrade().unwrap());
        }

        let entry_op = inner.entries().get(name);
        if entry_op.is_none() {
            return Err(FsError::EntryNotFound);
        }
        let reused_id = if let Some(entry) = entry_op.unwrap() {
            if let Some(inode) = entry.as_inode() {
                return Ok(inode);
            }
            entry.id()
        } else {
            None
        };
        let new_inode = Self::new_inode(&self.fs, &inner, name, reused_id, None);
        inner
            .entries()
            .insert(String::from(name), Some(Entry::new(&new_inode)));
        Ok(new_inode)
    }

    fn get_entry(&self, id: usize) -> Result<String> {
        if self.metadata()?.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        match id {
            0 => Ok(String::from(".")),
            1 => Ok(String::from("..")),
            i => {
                let mut inner = self.inner.write();
                let entries = inner.entries();
                if let Some(s) = entries.keys().nth(i - 2) {
                    Ok(s.to_string())
                } else {
                    Err(FsError::EntryNotFound)
                }
            }
        }
    }

    fn iterate_entries(&self, mut ctx: &mut DirentWriterContext) -> Result<usize> {
        if self.metadata()?.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        let idx = ctx.pos();
        if idx == 0 {
            let this_inode = self.inner.read().this.upgrade().unwrap();
            rcore_fs::write_inode_entry!(&mut ctx, ".", &this_inode);
        }
        if idx <= 1 {
            let parent_inode = self.inner.read().parent.upgrade().unwrap();
            rcore_fs::write_inode_entry!(&mut ctx, "..", &parent_inode);
        }

        let mut inner = self.inner.write();
        let skipped_children = if idx < 2 { 0 } else { idx - 2 };
        let keys: Vec<_> = inner
            .entries()
            .keys()
            .skip(skipped_children)
            .cloned()
            .collect();
        for name in keys.iter() {
            let entry_op = inner.entries().get(name).unwrap();
            let inode = {
                let (inode_op, reused_id) = if let Some(entry) = entry_op {
                    (entry.as_inode(), entry.id())
                } else {
                    (None, None)
                };
                match inode_op {
                    Some(inode) => inode,
                    None => {
                        let new_inode = Self::new_inode(&self.fs, &inner, name, reused_id, None);
                        inner
                            .entries()
                            .insert(String::from(name), Some(Entry::new(&new_inode)));
                        new_inode
                    }
                }
            };
            rcore_fs::write_inode_entry!(&mut ctx, name, inode);
        }
        Ok(ctx.written_len())
    }

    fn ext(&self) -> Option<&Extension> {
        Some(&self.ext)
    }

    fn io_control(&self, cmd: u32, data: usize) -> Result<()> {
        let inner = self.inner.read();
        inner.inode().io_control(cmd, data)
    }

    fn fs(&self) -> Arc<dyn FileSystem> {
        self.fs.upgrade().unwrap()
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

/// Simple path with access mode
#[derive(Debug, Clone)]
struct PathWithMode(Vec<(String, u16)>);

impl PathWithMode {
    pub fn new() -> Self {
        PathWithMode(Vec::new())
    }

    fn append(&mut self, name: &str, mode: u16) {
        match name {
            "." => {}
            ".." => {
                self.0.pop();
            }
            _ => {
                self.0.push((String::from(name), mode));
            }
        }
    }

    pub fn with_next(&self, name: &str, mode: u16) -> Self {
        let mut next = self.clone();
        next.append(name, mode);
        next
    }

    pub fn lastn(&self, n: usize) -> &[(String, u16)] {
        &self.0[self.0.len() - n..]
    }
}

trait NameExt {
    fn whiteout(&self) -> String;
    fn opaque(&self) -> String;
    fn is_reserved(&self) -> bool;
    fn is_self(&self) -> bool;
    fn is_parent(&self) -> bool;
}

impl NameExt for str {
    fn whiteout(&self) -> String {
        String::from(WH_PREFIX) + self
    }

    fn opaque(&self) -> String {
        String::from(OPAQUE_PREFIX) + self
    }

    fn is_reserved(&self) -> bool {
        self.starts_with(WH_PREFIX) || self.starts_with(OPAQUE_PREFIX) || self == MAC_FILE
    }

    fn is_self(&self) -> bool {
        self == "." || self.is_empty()
    }

    fn is_parent(&self) -> bool {
        self == ".."
    }
}
