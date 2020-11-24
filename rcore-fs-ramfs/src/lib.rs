#![cfg_attr(not(any(test, feature = "std")), no_std)]
#![deny(warnings)]

#[macro_use]
extern crate alloc;
extern crate log;

#[macro_use]
extern crate lazy_static;

use alloc::{
    collections::BTreeMap,
    string::{String, ToString},
    sync::{Arc, Weak},
    vec::Vec,
};
use core::any::Any;
use core::sync::atomic::*;
use rcore_fs::vfs::*;
use spin::RwLock;

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
            bsize: 0,
            frsize: 0,
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 0,
            ffree: 0,
            namemax: 0,
        }
    }
}

impl RamFS {
    pub fn new() -> Arc<Self> {
        let root = Arc::new(LockedINode(RwLock::new(RamFSINode {
            this: Weak::default(),
            parent: Weak::default(),
            children: BTreeMap::new(),
            content: Content::new(),
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
        root.extra.inode =
            Arc::into_raw(root.this.upgrade().unwrap()) as *const RamFSINode as usize;
        drop(root);
        fs
    }

    /// Allocate an INode ID
    fn alloc_inode_id(&self) -> usize {
        self.next_inode_id.fetch_add(1, Ordering::SeqCst)
    }
}

const KB: usize = 1024;
const CHUNK_LEN: usize = 4 * KB;
// ZERO_CHUNK is used to initialize the holes in the file content.
// It is fast to initialize a vector by extending it with the contents of an iterator.
lazy_static! {
    static ref ZERO_CHUNK: Vec<u8> = vec![0; CHUNK_LEN];
}

struct Content {
    data: Vec<Option<Vec<u8>>>,
}

impl Content {
    fn new() -> Self {
        Self { data: Vec::new() }
    }

    fn len(&self) -> usize {
        let cur_chunk_cnt = self.chunk_cnt();
        if cur_chunk_cnt > 0 {
            CHUNK_LEN * (cur_chunk_cnt - 1) + self.data[cur_chunk_cnt - 1].as_ref().unwrap().len()
        } else {
            0
        }
    }

    fn chunk_cnt(&self) -> usize {
        self.data.len()
    }

    fn resize_zero(&mut self, new_len: usize) {
        let cur_chunk_cnt = self.chunk_cnt();
        let new_chunk_cnt = (new_len + CHUNK_LEN - 1) / CHUNK_LEN;
        let tail_chunk_len = {
            let offset = new_len % CHUNK_LEN;
            if offset > 0 {
                offset
            } else if new_len > 0 {
                CHUNK_LEN
            } else {
                0
            }
        };

        if new_chunk_cnt > cur_chunk_cnt {
            // Expand the chunks
            self.data.resize(new_chunk_cnt, None);
            // The chunks between cur_chunk_idx(cur_chunk_cnt - 1) and
            // new_chunk_idx(new_chunk_cnt - 1) are hole, we do not
            // need to initialize the hole until write data into it, so
            // the length of these chunks is zero.
            // Subsequent reads of the data in the hole will return null bytes.
            if cur_chunk_cnt > 0 {
                // Resize current tail chunk, it MUST have been initialized
                self.data[cur_chunk_cnt - 1]
                    .as_mut()
                    .unwrap()
                    .resize(CHUNK_LEN, 0);
            }
            // Initialize the new tail chunk
            self.data[new_chunk_cnt - 1] = Some(Self::new_empty_chunk_with_len(tail_chunk_len));
        } else {
            // Truncate the number of chunks
            self.data.truncate(new_chunk_cnt);
            if new_chunk_cnt > 0 {
                if self.data[new_chunk_cnt - 1].is_none() {
                    // Chunk is a hole, initialize it
                    self.data[new_chunk_cnt - 1] =
                        Some(Self::new_empty_chunk_with_len(tail_chunk_len));
                } else {
                    // Resize new tail chunk
                    self.data[new_chunk_cnt - 1]
                        .as_mut()
                        .unwrap()
                        .resize(tail_chunk_len, 0);
                }
            }
        }
    }

    fn copy_from_slice(&mut self, offset: usize, src: &[u8]) {
        if src.is_empty() {
            return;
        }
        if offset + src.len() > self.len() {
            self.resize_zero(offset + src.len());
        }

        let chunk_idx = offset / CHUNK_LEN;
        let chunk_offset = offset % CHUNK_LEN;
        let copy_len = (CHUNK_LEN - chunk_offset).min(src.len());
        if self.data[chunk_idx].is_none() {
            // Chunk is a hole, initialize it from slice
            self.data[chunk_idx] = Some(Self::new_chunk_from_slice_at_offset(
                &src[..copy_len],
                chunk_offset,
            ))
        } else {
            let target =
                &mut self.data[chunk_idx].as_mut().unwrap()[chunk_offset..chunk_offset + copy_len];
            let buf = &src[..copy_len];
            target.copy_from_slice(buf)
        }

        if copy_len < src.len() {
            let mut chunk_idx = chunk_idx + 1;
            let mut offset = copy_len;
            while src.len() > offset {
                let copy_len = (src.len() - offset).min(CHUNK_LEN);
                if self.data[chunk_idx].is_none() {
                    // Chunk is a hole, initialize it from slice
                    self.data[chunk_idx] = Some(Self::new_chunk_from_slice_at_offset(
                        &src[offset..offset + copy_len],
                        0,
                    ))
                } else {
                    let target = &mut self.data[chunk_idx].as_mut().unwrap()[..copy_len];
                    let buf = &src[offset..offset + copy_len];
                    target.copy_from_slice(buf);
                }
                chunk_idx += 1;
                offset += copy_len;
            }
        }
    }

    fn copy_to_slice(&self, offset: usize, target: &mut [u8]) -> usize {
        let start = self.len().min(offset);
        let len = {
            let end = self.len().min(offset + target.len());
            end - start
        };
        if len == 0 {
            return 0;
        }

        let chunk_idx = start / CHUNK_LEN;
        let chunk_offset = start % CHUNK_LEN;
        let copy_len = (CHUNK_LEN - chunk_offset).min(len);
        if self.data[chunk_idx].is_none() {
            // Chunk is a hole, set null bytes
            for item in target.iter_mut().take(copy_len) {
                *item = 0;
            }
        } else {
            target[..copy_len].copy_from_slice(
                &self.data[chunk_idx].as_ref().unwrap()[chunk_offset..chunk_offset + copy_len],
            );
        }

        if copy_len < len {
            let mut chunk_idx = chunk_idx + 1;
            let mut offset = copy_len;
            while len > offset {
                let copy_len = (len - offset).min(CHUNK_LEN);
                if self.data[chunk_idx].is_none() {
                    // Chunk is a hole, set null bytes
                    for item in target.iter_mut().skip(offset).take(copy_len) {
                        *item = 0;
                    }
                } else {
                    target[offset..offset + copy_len]
                        .copy_from_slice(&self.data[chunk_idx].as_ref().unwrap()[..copy_len]);
                }
                chunk_idx += 1;
                offset += copy_len;
            }
        }
        len
    }

    fn new_empty_chunk_with_len(len: usize) -> Vec<u8> {
        assert!(len <= CHUNK_LEN);
        let mut vec = Vec::with_capacity(CHUNK_LEN);
        vec.extend(&ZERO_CHUNK[..len]);
        vec
    }

    fn new_chunk_from_slice_at_offset(src: &[u8], offset: usize) -> Vec<u8> {
        assert!(src.len() <= CHUNK_LEN);
        let mut vec = Vec::with_capacity(CHUNK_LEN);
        vec.extend(&ZERO_CHUNK[..offset]);
        vec.extend(src);
        vec.extend(&ZERO_CHUNK[offset + src.len()..]);
        vec
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
    content: Content,
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
        let len = file.content.copy_to_slice(offset, buf);
        Ok(len)
    }

    fn write_at(&self, offset: usize, buf: &[u8]) -> Result<usize> {
        let mut file = self.0.write();
        if file.extra.type_ != FileType::File && file.extra.type_ != FileType::SymLink {
            return Err(FsError::NotFile);
        }
        let content = &mut file.content;
        content.copy_from_slice(offset, buf);
        Ok(buf.len())
    }

    fn poll(&self) -> Result<PollStatus> {
        let file = self.0.read();
        if file.extra.type_ != FileType::File {
            return Err(FsError::NotFile);
        }
        Ok(PollStatus {
            read: true,
            write: true,
            error: false,
        })
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
        file.content.resize_zero(len);
        Ok(())
    }

    fn create2(
        &self,
        name: &str,
        type_: FileType,
        mode: u32,
        data: usize,
    ) -> Result<Arc<dyn INode>> {
        let mut file = self.0.write();
        if file.extra.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if name == "." || name == ".." {
            return Err(FsError::EntryExist);
        }
        if file.children.contains_key(name) {
            return Err(FsError::EntryExist);
        }
        let temp_file = Arc::new(LockedINode(RwLock::new(RamFSINode {
            parent: Weak::clone(&file.this),
            this: Weak::default(),
            children: BTreeMap::new(),
            content: Content::new(),
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
                mode: mode as u16,
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

        let (mut file, mut other) = if self.0.read().extra.inode < other.0.read().extra.inode {
            let file = self.0.write();
            let other = other.0.write();
            (file, other)
        } else {
            let other = other.0.write();
            let file = self.0.write();
            (file, other)
        };

        if file.extra.type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if other.extra.type_ == FileType::Dir {
            return Err(FsError::IsDir);
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
        if name == "." || name == ".." {
            return Err(FsError::IsDir);
        }
        let other = self.find(name)?;
        let other = other
            .downcast_ref::<LockedINode>()
            .ok_or(FsError::NotSameFs)?;
        if !other.0.read().children.is_empty() {
            return Err(FsError::DirNotEmpty);
        }

        let (mut file, mut other) = if self.0.read().extra.inode < other.0.read().extra.inode {
            let file = self.0.write();
            let other = other.0.write();
            (file, other)
        } else {
            let other = other.0.write();
            let file = self.0.write();
            (file, other)
        };
        other.extra.nlinks -= 1;
        file.children.remove(name);
        Ok(())
    }

    fn move_(&self, old_name: &str, target: &Arc<dyn INode>, new_name: &str) -> Result<()> {
        if old_name == "." || old_name == ".." {
            return Err(FsError::IsDir);
        }
        if new_name == "." || new_name == ".." {
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
            let (mut file, mut target) = if self.0.read().extra.inode < target.0.read().extra.inode
            {
                let file = self.0.write();
                let target = target.0.write();
                (file, target)
            } else {
                let target = target.0.write();
                let file = self.0.write();
                (file, target)
            };
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
            "." => Ok(file.this.upgrade().ok_or(FsError::EntryNotFound)?),
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
