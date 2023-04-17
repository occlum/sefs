#![cfg_attr(not(any(test, feature = "std")), no_std)]
#![deny(warnings)]

extern crate alloc;
extern crate log;

use alloc::{
    collections::BTreeMap,
    string::{String, ToString},
    sync::{Arc, Weak},
    vec::Vec,
};
use core::any::Any;
use core::sync::atomic::*;
use rcore_fs::vfs::*;
use spin::{RwLock, RwLockWriteGuard};

/// magic number for ramfs
pub const RAMFS_MAGIC: usize = 0x2f8d_be2c;
const MAX_FNAME_LEN: usize = 255;
const BLKSIZE: usize = 4096;

pub struct RamFS {
    root: Arc<LockedINode>,
    next_inode_id: AtomicUsize,
}

impl FileSystem for RamFS {
    fn sync(&self) -> Result<()> {
        Ok(())
    }

    fn root_inode(&self) -> Arc<dyn INode> {
        Arc::clone(&self.root) as _
    }

    fn info(&self) -> FsInfo {
        FsInfo {
            magic: RAMFS_MAGIC,
            bsize: BLKSIZE,
            frsize: BLKSIZE,
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 0,
            ffree: 0,
            namemax: MAX_FNAME_LEN,
        }
    }
}

impl RamFS {
    pub fn new() -> Arc<Self> {
        let root = Arc::new(LockedINode(RwLock::new(RamFSINode {
            this: Weak::default(),
            parent: Weak::default(),
            children: BTreeMap::new(),
            content: Vec::new(),
            extra: Metadata {
                dev: 0,
                inode: 0,
                size: 0,
                blk_size: 0,
                blocks: 0,
                atime: Timespec { sec: 0, nsec: 0 },
                mtime: Timespec { sec: 0, nsec: 0 },
                ctime: Timespec { sec: 0, nsec: 0 },
                type_: FileType::Dir,
                mode: 0o777,
                nlinks: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
            },
            fs: Weak::default(),
        })));
        let fs = Arc::new(RamFS {
            root,
            next_inode_id: AtomicUsize::new(1),
        });
        let mut root = fs.root.0.write();
        root.parent = Arc::downgrade(&fs.root);
        root.this = Arc::downgrade(&fs.root);
        root.fs = Arc::downgrade(&fs);
        root.extra.inode = fs.alloc_inode_id();
        drop(root);
        fs
    }

    /// Allocate an INode ID
    fn alloc_inode_id(&self) -> usize {
        self.next_inode_id.fetch_add(1, Ordering::SeqCst)
    }
}

struct RamFSINode {
    /// Reference to parent INode
    parent: Weak<LockedINode>,
    /// Reference to myself
    this: Weak<LockedINode>,
    /// Reference to children INodes
    children: BTreeMap<String, Arc<LockedINode>>,
    /// Content of the file
    content: Vec<u8>,
    /// INode metadata
    extra: Metadata,
    /// Reference to FS
    fs: Weak<RamFS>,
}

struct LockedINode(RwLock<RamFSINode>);

impl INode for LockedINode {
    fn read_at(&self, offset: usize, buf: &mut [u8]) -> Result<usize> {
        let file = self.0.read();
        if file.extra.type_ != FileType::File && file.extra.type_ != FileType::SymLink {
            return Err(FsError::NotFile);
        }
        let start = file.content.len().min(offset);
        let end = file.content.len().min(offset + buf.len());
        let src = &file.content[start..end];
        buf[0..src.len()].copy_from_slice(src);
        Ok(src.len())
    }

    fn write_at(&self, offset: usize, buf: &[u8]) -> Result<usize> {
        let mut file = self.0.write();
        if file.extra.type_ != FileType::File && file.extra.type_ != FileType::SymLink {
            return Err(FsError::NotFile);
        }
        let content = &mut file.content;
        if offset + buf.len() > content.len() {
            content.resize(offset + buf.len(), 0);
        }
        let target = &mut content[offset..offset + buf.len()];
        target.copy_from_slice(buf);
        Ok(buf.len())
    }

    fn metadata(&self) -> Result<Metadata> {
        let file = self.0.read();
        let mut metadata = file.extra.clone();
        metadata.size = file.content.len();
        Ok(metadata)
    }

    fn set_metadata(&self, metadata: &Metadata) -> Result<()> {
        let mut file = self.0.write();
        file.extra.atime = metadata.atime;
        file.extra.mtime = metadata.mtime;
        file.extra.ctime = metadata.ctime;
        file.extra.mode = metadata.mode;
        file.extra.uid = metadata.uid;
        file.extra.gid = metadata.gid;
        Ok(())
    }

    fn sync_all(&self) -> Result<()> {
        Ok(())
    }

    fn sync_data(&self) -> Result<()> {
        Ok(())
    }

    fn resize(&self, len: usize) -> Result<()> {
        let mut file = self.0.write();
        if file.extra.type_ != FileType::File && file.extra.type_ != FileType::SymLink {
            return Err(FsError::NotFile);
        }
        file.content.resize(len, 0);
        Ok(())
    }

    fn create2(
        &self,
        name: &str,
        type_: FileType,
        mode: u16,
        data: usize,
    ) -> Result<Arc<dyn INode>> {
        let mut file = self.0.write();
        if file.extra.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if name == "." || name == ".." || name.is_empty() {
            return Err(FsError::EntryExist);
        }
        if file.children.contains_key(name) {
            return Err(FsError::EntryExist);
        }
        let temp_file = Arc::new(LockedINode(RwLock::new(RamFSINode {
            parent: Weak::clone(&file.this),
            this: Weak::default(),
            children: BTreeMap::new(),
            content: Vec::new(),
            extra: Metadata {
                dev: 0,
                inode: file.fs.upgrade().unwrap().alloc_inode_id(),
                size: 0,
                blk_size: 0,
                blocks: 0,
                atime: Timespec { sec: 0, nsec: 0 },
                mtime: Timespec { sec: 0, nsec: 0 },
                ctime: Timespec { sec: 0, nsec: 0 },
                type_,
                mode,
                nlinks: 1,
                uid: 0,
                gid: 0,
                rdev: data,
            },
            fs: Weak::clone(&file.fs),
        })));
        temp_file.0.write().this = Arc::downgrade(&temp_file);
        file.children
            .insert(String::from(name), Arc::clone(&temp_file));
        Ok(temp_file)
    }

    fn link(&self, name: &str, other: &Arc<dyn INode>) -> Result<()> {
        let other = other
            .downcast_ref::<LockedINode>()
            .ok_or(FsError::NotSameFs)?;

        let (mut file, mut other) = write_lock_two_inodes(self, other);
        if file.extra.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if other.extra.type_ == FileType::Dir {
            return Err(FsError::IsDir);
        }
        if name == "." || name == ".." || name.is_empty() {
            return Err(FsError::EntryExist);
        }
        if file.children.contains_key(name) {
            return Err(FsError::EntryExist);
        }

        file.children
            .insert(String::from(name), other.this.upgrade().unwrap());
        other.extra.nlinks += 1;
        Ok(())
    }

    fn unlink(&self, name: &str) -> Result<()> {
        if self.0.read().extra.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if name == "." || name == ".." || name.is_empty() {
            return Err(FsError::IsDir);
        }
        let other = self.find(name)?;
        let other = other
            .downcast_ref::<LockedINode>()
            .ok_or(FsError::NotSameFs)?;
        if !other.0.read().children.is_empty() {
            return Err(FsError::DirNotEmpty);
        }

        let (mut file, mut other) = write_lock_two_inodes(self, other);
        other.extra.nlinks -= 1;
        file.children.remove(name);
        Ok(())
    }

    fn move_(&self, old_name: &str, target: &Arc<dyn INode>, new_name: &str) -> Result<()> {
        if old_name == "." || old_name == ".." || old_name.is_empty() {
            return Err(FsError::IsDir);
        }
        if new_name == "." || new_name == ".." || new_name.is_empty() {
            return Err(FsError::IsDir);
        }
        let inode = self.find(old_name)?;
        let inode = inode
            .downcast_ref::<LockedINode>()
            .ok_or(FsError::NotSameFs)?;
        let target = target
            .downcast_ref::<LockedINode>()
            .ok_or(FsError::NotSameFs)?;
        if target.0.read().extra.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        // Disallow to make a directory a subdirectory of itself
        // Note: We have already add a check in Occlum to verify if the newpath
        //       contained a path prefix of the oldpath
        // Add the check here to avoid deadlock
        // TODO: How to know if target is a descendant of old ?
        if inode.0.read().extra.inode == target.0.read().extra.inode {
            return Err(FsError::InvalidParam);
        }

        if let Some(dest_inode) = target.0.read().children.get(new_name) {
            if inode.0.read().extra.inode == dest_inode.0.read().extra.inode {
                return Ok(());
            }
            let old_type = inode.0.read().extra.type_;
            let dest_type = dest_inode.0.read().extra.type_;
            match (old_type, dest_type) {
                (FileType::Dir, FileType::Dir) => {
                    if !dest_inode.0.read().children.is_empty() {
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
        if self.0.read().extra.inode == target.0.read().extra.inode {
            let mut file = self.0.write();
            file.children.insert(
                String::from(new_name),
                inode.0.read().this.upgrade().unwrap(),
            );
            file.children.remove(old_name);
        } else {
            let (mut file, mut target) = write_lock_two_inodes(self, target);
            target.children.insert(
                String::from(new_name),
                inode.0.read().this.upgrade().unwrap(),
            );
            file.children.remove(old_name);
        }

        Ok(())
    }

    fn find(&self, name: &str) -> Result<Arc<dyn INode>> {
        let file = self.0.read();
        if file.extra.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        //info!("find it: {} {}", name, file.parent.is_none());
        match name {
            "." | "" => Ok(file.this.upgrade().ok_or(FsError::EntryNotFound)?),
            ".." => Ok(file.parent.upgrade().ok_or(FsError::EntryNotFound)?),
            name => {
                let s = file.children.get(name).ok_or(FsError::EntryNotFound)?;
                Ok(Arc::clone(s) as Arc<dyn INode>)
            }
        }
    }

    fn get_entry(&self, id: usize) -> Result<String> {
        let file = self.0.read();
        if file.extra.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }

        match id {
            0 => Ok(String::from(".")),
            1 => Ok(String::from("..")),
            i => {
                if let Some(s) = file.children.keys().nth(i - 2) {
                    Ok(s.to_string())
                } else {
                    Err(FsError::EntryNotFound)
                }
            }
        }
    }

    fn iterate_entries(&self, mut ctx: &mut DirentWriterContext) -> Result<usize> {
        let file = self.0.read();
        if file.extra.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        let idx = ctx.pos();
        // Write the two special entries
        if idx == 0 {
            let this_inode = file.this.upgrade().unwrap();
            rcore_fs::write_inode_entry!(&mut ctx, ".", &this_inode);
        }
        if idx <= 1 {
            let parent_inode = file.parent.upgrade().unwrap();
            rcore_fs::write_inode_entry!(&mut ctx, "..", &parent_inode);
        }
        // Write the normal entries
        let skipped_children = if idx < 2 { 0 } else { idx - 2 };
        for (name, inode) in file.children.iter().skip(skipped_children) {
            rcore_fs::write_inode_entry!(&mut ctx, name, inode);
        }
        Ok(ctx.written_len())
    }

    fn io_control(&self, _cmd: u32, _data: usize) -> Result<()> {
        Err(FsError::NotSupported)
    }

    fn fs(&self) -> Arc<dyn FileSystem> {
        Weak::upgrade(&self.0.read().fs).unwrap()
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}

fn write_lock_two_inodes<'a>(
    this: &'a LockedINode,
    other: &'a LockedINode,
) -> (
    RwLockWriteGuard<'a, RamFSINode>,
    RwLockWriteGuard<'a, RamFSINode>,
) {
    if this.0.read().extra.inode < other.0.read().extra.inode {
        let this = this.0.write();
        let other = other.0.write();
        (this, other)
    } else {
        let other = other.0.write();
        let this = this.0.write();
        (this, other)
    }
}
