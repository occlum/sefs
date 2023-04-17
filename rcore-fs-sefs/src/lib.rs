#![cfg_attr(not(any(test, feature = "std")), no_std)]
#![feature(new_uninit)]

#[macro_use]
extern crate alloc;
use alloc::{
    boxed::Box,
    collections::BTreeMap,
    string::{String, ToString},
    sync::{Arc, Weak},
    vec::Vec,
};
use core::any::Any;
use core::fmt::{Debug, Error, Formatter};
use core::mem::MaybeUninit;
use core::ops::Range;

use bitvec::prelude::*;
use rcore_fs::dev::{DevResult, TimeProvider};
use rcore_fs::dirty::Dirty;
use rcore_fs::vfs::{
    self, AllocFlags, DirentWriterContext, FallocateMode, FileSystem, FsError, INode,
};
use spin::{RwLock, RwLockWriteGuard};

use self::dev::*;
pub use self::structs::SEFS_MAGIC;
use self::structs::*;

pub mod dev;
mod structs;

/// Helper methods for `File`
impl dyn File {
    fn read_block(&self, id: BlockId, buf: &mut [u8]) -> DevResult<()> {
        assert!(buf.len() <= BLKSIZE);
        self.read_exact_at(buf, id * BLKSIZE)
    }

    fn write_block(&self, id: BlockId, buf: &[u8]) -> DevResult<()> {
        assert!(buf.len() <= BLKSIZE);
        self.write_all_at(buf, id * BLKSIZE)
    }

    fn read_direntry(&self, id: usize) -> DevResult<DiskEntry> {
        let mut direntry: DiskEntry = unsafe { MaybeUninit::uninit().assume_init() };
        self.read_exact_at(direntry.as_buf_mut(), DIRENT_SIZE * id)?;
        Ok(direntry)
    }

    fn write_direntry(&self, id: usize, direntry: &DiskEntry) -> DevResult<()> {
        self.write_all_at(direntry.as_buf(), DIRENT_SIZE * id)
    }

    /// Load struct `T` from given block in device
    fn load_struct<T: AsBuf>(&self, id: BlockId) -> DevResult<T> {
        let mut s: T = unsafe { MaybeUninit::uninit().assume_init() };
        self.read_block(id, s.as_buf_mut())?;
        Ok(s)
    }
}

/// inode for SEFS
pub struct INodeImpl {
    /// inode number
    id: INodeId,
    /// on-disk inode
    disk_inode: RwLock<Dirty<DiskINode>>,
    /// back file
    file: Box<dyn File>,
    /// Reference to FS
    fs: Arc<SEFS>,
}

impl Debug for INodeImpl {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        write!(
            f,
            "INode {{ id: {}, disk: {:?} }}",
            self.id, self.disk_inode
        )
    }
}

impl INodeImpl {
    /// Only for Dir
    //TODO: Fix the concurrent problem of dentry operations
    fn get_file_inode_and_entry_id(&self, name: &str) -> vfs::Result<(INodeId, usize)> {
        let name = if name.is_empty() { "." } else { name };
        for entry_id in 0..self.disk_inode.read().blocks as usize {
            let entry = self.file.read_direntry(entry_id)?;
            if entry.name.as_ref() == name {
                return Ok((entry.id as INodeId, entry_id));
            }
        }
        Err(FsError::EntryNotFound)
    }

    fn get_file_inode_id(&self, name: &str) -> vfs::Result<INodeId> {
        let name = if name.is_empty() { "." } else { name };
        self.get_file_inode_and_entry_id(name)
            .map(|(inode_id, _)| inode_id)
    }

    fn get_entry_and_entry_id(&self, name: &str) -> vfs::Result<(DiskEntry, usize)> {
        let name = if name.is_empty() { "." } else { name };
        for entry_id in 0..self.disk_inode.read().blocks as usize {
            let entry = self.file.read_direntry(entry_id)?;
            if entry.name.as_ref() == name {
                return Ok((entry, entry_id));
            }
        }
        Err(FsError::EntryNotFound)
    }

    /// Init dir content. Insert 2 init entries.
    /// This do not init nlinks, please modify the nlinks in the invoker.
    fn dirent_init(&self, parent: INodeId) -> vfs::Result<()> {
        self.disk_inode.write().blocks = 2;
        // Insert entries: '.' '..'
        self.file.write_direntry(
            0,
            &DiskEntry {
                id: self.id as u32,
                name: Str256::from("."),
                type_: FileType::Dir,
            },
        )?;
        self.file.write_direntry(
            1,
            &DiskEntry {
                id: parent as u32,
                name: Str256::from(".."),
                type_: FileType::Dir,
            },
        )?;
        Ok(())
    }

    /// Append an entry of file at the end
    fn dirent_append(&self, entry: &DiskEntry) -> vfs::Result<usize> {
        let mut inode = self.disk_inode.write();
        let total = &mut inode.blocks;
        let entry_id = *total as usize;
        self.file.write_direntry(entry_id, entry)?;
        *total += 1;
        Ok(entry_id)
    }

    /// Remove an entry of file and replacing it with the last one, useful for dirent remove
    fn dirent_remove(&self, id: usize) -> vfs::Result<()> {
        let total = self.disk_inode.read().blocks as usize;
        debug_assert!(id < total);
        let last_direntry = self.file.read_direntry(total - 1)?;
        if id != total - 1 {
            self.file.write_direntry(id, &last_direntry)?;
        }
        self.file.set_len((total - 1) * DIRENT_SIZE)?;
        self.disk_inode.write().blocks -= 1;
        Ok(())
    }

    /// Remove an INode entry from Dir entries, and decrease nlinks if success
    fn dirent_inode_remove(&self, inode: Arc<INodeImpl>, entry_id: usize) -> vfs::Result<()> {
        self.dirent_remove(entry_id)?;
        inode.nlinks_dec();
        if inode.disk_inode.read().type_ == FileType::Dir {
            inode.nlinks_dec(); //for .
            self.nlinks_dec(); //for ..
        }
        Ok(())
    }

    fn nlinks_inc(&self) {
        self.disk_inode.write().nlinks += 1;
    }

    fn nlinks_dec(&self) {
        let mut disk_inode = self.disk_inode.write();
        assert!(disk_inode.nlinks > 0);
        disk_inode.nlinks -= 1;
    }

    #[cfg(feature = "create_image")]
    pub fn update_mac(&self) -> vfs::Result<()> {
        if self.fs.device.protect_integrity() {
            self.disk_inode.write().inode_mac = self.file.get_file_mac().unwrap();
            //println!("file_mac {:?}", self.disk_inode.read().inode_mac);
            self.sync_all()?;
        }
        Ok(())
    }

    #[cfg(not(feature = "create_image"))]
    fn check_integrity(&self) {
        if self.fs.device.protect_integrity() {
            let inode_mac = &self.disk_inode.read().inode_mac;
            let file_mac = self.file.get_file_mac().unwrap();
            //info!("inode_mac {:?}, file_mac {:?}", inode_mac, file_mac);
            let not_integrity = inode_mac.0 != file_mac.0;
            assert!(!not_integrity, "FsError::NoIntegrity");
        }
    }

    /// Write the INode's info into metadata file
    fn sync_metadata(&self) -> vfs::Result<()> {
        let mut disk_inode = self.disk_inode.write();
        if disk_inode.dirty() {
            self.fs
                .meta_file
                .write_block(self.id, disk_inode.as_buf())?;
            disk_inode.sync();
        }
        self.fs.meta_file.flush()?;
        Ok(())
    }

    /// Write zeros for the specified range of file.
    fn zero_range(&self, range: &Range<usize>, keep_size: bool) -> vfs::Result<()> {
        let mut inode = self.disk_inode.write();
        let file_size = inode.size as usize;
        let (offset, len) = if keep_size {
            let (start, end) = (range.start.min(file_size), range.end.min(file_size));
            (start, end - start)
        } else {
            (range.start, range.len())
        };
        self.file.write_zeros_at(offset, len).unwrap();

        // May update file size
        if !keep_size && range.end > file_size {
            self.file.set_len(range.end).unwrap();
            inode.size = range.end as u64;
        }
        Ok(())
    }

    /// Insert zeros for the specified range of file.
    ///
    /// The contents of the file starting at the start of range will be shifted
    /// upward, making the file range.len() bytes bigger.
    fn insert_range(&self, range: &Range<usize>) -> vfs::Result<()> {
        let mut inode = self.disk_inode.write();
        let file_size = inode.size as usize;
        if range.start >= file_size {
            return Err(FsError::InvalidParam);
        }
        if range.start % BLKSIZE != 0 || range.len() % BLKSIZE != 0 {
            return Err(FsError::InvalidParam);
        }
        let new_file_size = file_size
            .checked_add(range.len())
            .ok_or(FsError::FileTooBig)?;
        if (new_file_size as isize).is_negative() {
            return Err(FsError::FileTooBig);
        }

        // Move the contents of file forward
        let src_range = Range {
            start: range.start,
            end: file_size,
        };
        let dst_offset = range.end;
        self.copy_range_to(&src_range, dst_offset)?;

        // Insert zeros
        self.file.write_zeros_at(range.start, range.len()).unwrap();

        // Update file size
        self.file.set_len(new_file_size).unwrap();
        inode.size = new_file_size as u64;
        Ok(())
    }

    /// Remove the range of file.
    ///
    /// The contents of the file starting at the end of range will be appended
    /// at the start of the range, making the file range.len() bytes smaller.
    fn collapse_range(&self, range: &Range<usize>) -> vfs::Result<()> {
        let mut inode = self.disk_inode.write();
        let file_size = inode.size as usize;
        if range.end >= file_size {
            return Err(FsError::InvalidParam);
        }
        if range.start % BLKSIZE != 0 || range.len() % BLKSIZE != 0 {
            return Err(FsError::InvalidParam);
        }

        // Move the contents of file backward
        let src_range = Range {
            start: range.end,
            end: file_size,
        };
        let dst_offset = range.start;
        self.copy_range_to(&src_range, dst_offset)?;

        // Update file size
        let new_file_size = file_size - range.len();
        self.file.set_len(new_file_size).unwrap();
        inode.size = new_file_size as u64;
        Ok(())
    }

    /// Copy a range of data to a specified offset of file.
    ///
    /// The source range and the destination range may overlap.
    fn copy_range_to(&self, src_range: &Range<usize>, dst_offset: usize) -> vfs::Result<()> {
        let (mut src_offset, mut dst_offset, offset_step) = if src_range.start < dst_offset {
            let len = src_range.len().min(BLKSIZE);
            let src_offset = (src_range.end - len) as isize;
            let dst_offset = (dst_offset + src_range.len() - len) as isize;
            let offset_step = -(BLKSIZE as isize);
            (src_offset, dst_offset, offset_step)
        } else if src_range.start > dst_offset {
            let src_offset = src_range.start as isize;
            let dst_offset = dst_offset as isize;
            let offset_step = BLKSIZE as isize;
            (src_offset, dst_offset, offset_step)
        } else {
            // src_range.start == dst_offset, do nothing
            return Ok(());
        };

        // Do the range copying
        let mut remaining_size = src_range.len();
        let mut buf: [u8; BLKSIZE] = unsafe { MaybeUninit::uninit().assume_init() };
        while remaining_size > 0 {
            let len = remaining_size.min(BLKSIZE);
            self.file
                .read_exact_at(&mut buf[..len], src_offset as usize)
                .unwrap();
            self.file
                .write_all_at(&buf[..len], dst_offset as usize)
                .unwrap();
            remaining_size -= len;
            src_offset += offset_step;
            dst_offset += offset_step;
        }
        Ok(())
    }
}

impl vfs::INode for INodeImpl {
    fn read_at(&self, offset: usize, buf: &mut [u8]) -> vfs::Result<usize> {
        let DiskINode { type_, size, .. } = **self.disk_inode.read();
        if type_ != FileType::File && type_ != FileType::SymLink && type_ != FileType::Socket {
            return Err(FsError::NotFile);
        }
        let len = {
            let size = size as usize;
            let start = size.min(offset);
            let end = size.min(offset.saturating_add(buf.len()));
            end - start
        };
        let real_len = self.file.read_at(buf, offset)?;
        if real_len < len {
            for item in buf.iter_mut().skip(real_len).take(len - real_len) {
                *item = 0;
            }
        }
        Ok(len)
    }

    fn write_at(&self, offset: usize, buf: &[u8]) -> vfs::Result<usize> {
        let type_ = self.disk_inode.read().type_;
        if type_ != FileType::File && type_ != FileType::SymLink && type_ != FileType::Socket {
            return Err(FsError::NotFile);
        }
        let len = self.file.write_at(buf, offset)?;
        let end_offset = offset + len;
        let mut inode = self.disk_inode.write();
        if end_offset > inode.size as usize {
            self.file.set_len(end_offset)?;
            inode.size = end_offset as u64;
        }
        Ok(len)
    }

    /// the size returned here is logical size(entry num for directory), not the disk space used.
    fn metadata(&self) -> vfs::Result<vfs::Metadata> {
        let disk_inode = self.disk_inode.read();
        Ok(vfs::Metadata {
            dev: 0,
            inode: self.id,
            size: match disk_inode.type_ {
                FileType::File | FileType::SymLink | FileType::Socket => disk_inode.size as usize,
                FileType::Dir => disk_inode.blocks as usize,
                _ => panic!("Unknown file type"),
            },
            mode: disk_inode.mode,
            type_: vfs::FileType::from(disk_inode.type_),
            blocks: disk_inode.blocks as usize,
            atime: disk_inode.atime,
            mtime: disk_inode.mtime,
            ctime: disk_inode.ctime,
            nlinks: disk_inode.nlinks as usize,
            uid: disk_inode.uid as usize,
            gid: disk_inode.gid as usize,
            blk_size: BLKSIZE,
            rdev: 0,
        })
    }

    fn set_metadata(&self, metadata: &vfs::Metadata) -> vfs::Result<()> {
        let mut disk_inode = self.disk_inode.write();
        disk_inode.mode = metadata.mode;
        disk_inode.uid = metadata.uid as u32;
        disk_inode.gid = metadata.gid as u32;
        disk_inode.atime = metadata.atime;
        disk_inode.mtime = metadata.mtime;
        disk_inode.ctime = metadata.ctime;
        Ok(())
    }

    fn sync_all(&self) -> vfs::Result<()> {
        // Sync data
        self.sync_data()?;
        // Sync metadata
        // This sequence ensures the metadata is always valid for the data
        self.sync_metadata()?;
        Ok(())
    }

    fn sync_data(&self) -> vfs::Result<()> {
        self.file.flush()?;
        Ok(())
    }

    fn fallocate(&self, mode: &FallocateMode, offset: usize, len: usize) -> vfs::Result<()> {
        if self.disk_inode.read().type_ != FileType::File {
            return Err(FsError::NotFile);
        }

        let range = {
            if (offset as isize).is_negative() {
                return Err(FsError::FileTooBig);
            }
            let end_offset = offset.checked_add(len).ok_or(FsError::FileTooBig)?;
            if (end_offset as isize).is_negative() {
                return Err(FsError::FileTooBig);
            }
            Range::<usize> {
                start: offset,
                end: end_offset,
            }
        };

        // Handle file space with mode
        //
        // TODO: The Performance issue:
        // The current implementation does not allocate or reserve disk space;
        // it actually writes zeros. This means the performance of fallocate
        // could be terrible in some extreme cases. The collapsing or inserting
        // operation suffers from a similar performance problem as they may copy
        // a huge amount of data.
        //
        // TODO: The robustness issue:
        // The current implementation will panic instead of reporting the
        // not-enough-space error if the underlying disk does not have enough space.
        // Also, for the sub-command of FallocateMode::Allocate with keep_size = true,
        // the success of fallocate does not really mean future writes that fall
        // inside the allocated range are guaranteed to succeed.
        match mode {
            FallocateMode::PunchHoleKeepSize => self.zero_range(&range, true)?,
            FallocateMode::ZeroRange => self.zero_range(&range, false)?,
            FallocateMode::ZeroRangeKeepSize => self.zero_range(&range, true)?,
            FallocateMode::CollapseRange => self.collapse_range(&range)?,
            FallocateMode::InsertRange => self.insert_range(&range)?,
            FallocateMode::Allocate(flags) => {
                if !flags.contains(AllocFlags::KEEP_SIZE) {
                    let mut inode = self.disk_inode.write();
                    let file_size = inode.size as usize;
                    if range.end > file_size {
                        self.file.set_len(range.end).unwrap();
                        inode.size = range.end as u64;
                    }
                }
            }
        }
        Ok(())
    }

    fn resize(&self, len: usize) -> vfs::Result<()> {
        let type_ = self.disk_inode.read().type_;
        if type_ != FileType::File && type_ != FileType::SymLink && type_ != FileType::Socket {
            return Err(FsError::NotFile);
        }
        let mut inode = self.disk_inode.write();
        self.file.set_len(len)?;
        inode.size = len as u64;
        Ok(())
    }

    fn create(
        &self,
        name: &str,
        type_: vfs::FileType,
        mode: u16,
    ) -> vfs::Result<Arc<dyn vfs::INode>> {
        let type_ = match type_ {
            vfs::FileType::File => FileType::File,
            vfs::FileType::Dir => FileType::Dir,
            vfs::FileType::SymLink => FileType::SymLink,
            vfs::FileType::Socket => FileType::Socket,
            _ => return Err(FsError::InvalidParam),
        };
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if info.nlinks == 0 {
            return Err(FsError::DirRemoved);
        }
        if name.len() > MAX_FNAME_LEN {
            return Err(FsError::NameTooLong);
        }

        // Ensure the name is not exist
        if self.get_file_inode_id(name).is_ok() {
            return Err(FsError::EntryExist);
        }

        // Create a new INode
        let inode = self.fs.new_inode(type_, mode)?;
        if type_ == FileType::Dir {
            inode.dirent_init(self.id)?;
        }
        // Insert it into dir entry
        let entry = DiskEntry {
            id: inode.id as u32,
            name: Str256::from(name),
            type_,
        };
        self.dirent_append(&entry)?;
        // Append success, increase nlinks
        inode.nlinks_inc();
        if type_ == FileType::Dir {
            inode.nlinks_inc(); //for .
            self.nlinks_inc(); //for ..
        }
        // Update metadata file to make the INode valid
        self.fs.sync_metadata()?;
        inode.sync_all()?;
        // Sync the dirINode's info into file
        // MUST sync the INode's info first, or the entry maybe invalid
        self.sync_all()?;

        Ok(inode)
    }

    fn unlink(&self, name: &str) -> vfs::Result<()> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if info.nlinks == 0 {
            return Err(FsError::DirRemoved);
        }
        if name == "." || name == ".." || name.is_empty() {
            return Err(FsError::IsDir);
        }
        if name.len() > MAX_FNAME_LEN {
            return Err(FsError::NameTooLong);
        }

        let (inode_id, entry_id) = self.get_file_inode_and_entry_id(name)?;
        let inode = self.fs.get_inode(inode_id)?;

        if inode.disk_inode.read().type_ == FileType::Dir {
            // only . and ..
            assert!(inode.disk_inode.read().blocks >= 2);
            if inode.disk_inode.read().blocks > 2 {
                return Err(FsError::DirNotEmpty);
            }
        }
        // Remove it from dir entries
        self.dirent_inode_remove(inode, entry_id)?;
        // Sync the dirINode's info
        // The real removal of the INode is delayed at INode's drop()
        self.sync_all()?;
        Ok(())
    }

    fn link(&self, name: &str, other: &Arc<dyn INode>) -> vfs::Result<()> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if info.nlinks == 0 {
            return Err(FsError::DirRemoved);
        }
        if name.len() > MAX_FNAME_LEN {
            return Err(FsError::NameTooLong);
        }

        if self.get_file_inode_id(name).is_ok() {
            return Err(FsError::EntryExist);
        }
        let child = other
            .downcast_ref::<INodeImpl>()
            .ok_or(FsError::NotSameFs)?;
        if !Arc::ptr_eq(&self.fs, &child.fs) {
            return Err(FsError::NotSameFs);
        }
        if child.metadata()?.type_ == vfs::FileType::Dir {
            return Err(FsError::IsDir);
        }
        let entry = DiskEntry {
            id: child.id as u32,
            name: Str256::from(name),
            type_: child.disk_inode.read().type_,
        };
        // Insert it into dir entry
        self.dirent_append(&entry)?;
        // Increase nlinks
        child.nlinks_inc();
        child.sync_metadata()?;
        // Now the INode info is valid, we can safely sync the dirINode's info
        self.sync_all()?;
        Ok(())
    }

    fn move_(&self, old_name: &str, target: &Arc<dyn INode>, new_name: &str) -> vfs::Result<()> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if info.nlinks == 0 {
            return Err(FsError::DirRemoved);
        }
        if old_name == "." || old_name == ".." || old_name.is_empty() {
            return Err(FsError::IsDir);
        }
        if new_name == "." || new_name == ".." || new_name.is_empty() {
            return Err(FsError::IsDir);
        }
        if old_name.len() > MAX_FNAME_LEN || new_name.len() > MAX_FNAME_LEN {
            return Err(FsError::NameTooLong);
        }

        let dest = target
            .downcast_ref::<INodeImpl>()
            .ok_or(FsError::NotSameFs)?;
        let dest_info = dest.metadata()?;
        if !Arc::ptr_eq(&self.fs, &dest.fs) {
            return Err(FsError::NotSameFs);
        }
        if dest_info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if dest_info.nlinks == 0 {
            return Err(FsError::DirRemoved);
        }

        // Get the info of the INode to be replaced
        let to_be_replaced_inode_info = if let Ok((dest_inode_id, dest_entry_id)) =
            dest.get_file_inode_and_entry_id(new_name)
        {
            let dest_inode = self.fs.get_inode(dest_inode_id)?;
            let inode = self.find(old_name)?;
            if inode.metadata()?.inode == dest_inode.metadata()?.inode {
                // Same INode, do nothing
                return Ok(());
            }
            let inode_type = inode.metadata()?.type_;
            let dest_type = dest_inode.metadata()?.type_;
            match (inode_type, dest_type) {
                (vfs::FileType::Dir, vfs::FileType::Dir) => {
                    if dest_inode.list()?.len() > 2 {
                        return Err(FsError::DirNotEmpty);
                    }
                }
                (vfs::FileType::Dir, _) => {
                    return Err(FsError::NotDir);
                }
                (_, vfs::FileType::Dir) => {
                    return Err(FsError::IsDir);
                }
                _ => {}
            }
            Some((dest_inode, dest_entry_id))
        } else {
            None
        };

        let (old_entry, entry_id) = self.get_entry_and_entry_id(old_name)?;
        if info.inode == dest_info.inode {
            // Move at same dirINode: just modify name
            let entry = DiskEntry {
                id: old_entry.id as u32,
                name: Str256::from(new_name),
                type_: old_entry.type_,
            };
            self.file.write_direntry(entry_id, &entry)?;
            // Replace the existing inode
            if let Some((replace_inode, replace_entry_id)) = to_be_replaced_inode_info {
                if let Err(e) = self.dirent_inode_remove(replace_inode, replace_entry_id) {
                    // Recover if fail
                    self.file.write_direntry(entry_id, &old_entry)?;
                    return Err(e);
                }
            }
            self.sync_all()?;
        } else {
            // Move between different dirINodes
            let entry = DiskEntry {
                id: old_entry.id as u32,
                name: Str256::from(new_name),
                type_: old_entry.type_,
            };
            let new_entry_id = dest.dirent_append(&entry)?;
            if let Err(e) = self.dirent_remove(entry_id) {
                // Recover if fail
                dest.dirent_remove(new_entry_id)?;
                return Err(e);
            }
            // Replace the existing inode
            if let Some((replace_inode, replace_entry_id)) = to_be_replaced_inode_info {
                if let Err(e) = dest.dirent_inode_remove(replace_inode, replace_entry_id) {
                    // Recover if fail
                    self.dirent_append(&old_entry)?;
                    dest.dirent_remove(new_entry_id)?;
                    return Err(e);
                }
            }
            let inode = self.fs.get_inode(old_entry.id as usize)?;
            if inode.disk_inode.read().type_ == FileType::Dir {
                self.nlinks_dec();
                dest.nlinks_inc();
            }
            // MUST sync self's INode info before dest, or the INode may exist in both dir entries,
            // If one unlinks the entry already, another one's unlink will panic because the nlinks is not correct
            self.sync_all()?;
            dest.sync_all()?;
        }

        Ok(())
    }

    fn find(&self, name: &str) -> vfs::Result<Arc<dyn vfs::INode>> {
        let info = self.metadata()?;
        if info.type_ != vfs::FileType::Dir {
            return Err(FsError::NotDir);
        }
        if name.len() > MAX_FNAME_LEN {
            return Err(FsError::NameTooLong);
        }
        let inode_id = self.get_file_inode_id(name)?;
        Ok(self.fs.get_inode(inode_id)?)
    }

    fn get_entry(&self, id: usize) -> vfs::Result<String> {
        if self.disk_inode.read().type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        if id >= self.disk_inode.read().blocks as usize {
            return Err(FsError::EntryNotFound);
        };
        let entry = self.file.read_direntry(id)?;
        Ok(String::from(entry.name.as_ref()))
    }

    fn iterate_entries(&self, ctx: &mut DirentWriterContext) -> vfs::Result<usize> {
        if self.disk_inode.read().type_ != FileType::Dir {
            return Err(FsError::NotDir);
        }
        let idx = ctx.pos();
        for entry_id in idx..self.disk_inode.read().blocks as usize {
            let entry = self.file.read_direntry(entry_id)?;
            if let Err(e) = ctx.write_entry(
                entry.name.as_ref(),
                entry.id as u64,
                vfs::FileType::from(entry.type_),
            ) {
                if ctx.written_len() == 0 {
                    return Err(e);
                } else {
                    break;
                }
            };
        }
        Ok(ctx.written_len())
    }

    fn io_control(&self, _cmd: u32, _data: usize) -> vfs::Result<()> {
        Err(FsError::NotSupported)
    }

    fn fs(&self) -> Arc<dyn vfs::FileSystem> {
        self.fs.clone()
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }
}
impl Drop for INodeImpl {
    /// Auto sync when drop
    fn drop(&mut self) {
        #[cfg(feature = "create_image")]
        self.update_mac()
            .expect("failed to update mac when dropping the SEFS Inode");

        self.sync_all()
            .expect("failed to sync when dropping the SEFS Inode");
        if self.disk_inode.read().nlinks == 0 {
            // Leave the inode info exists in metadata file, following new inode will override it
            self.disk_inode.write().sync();
            self.fs.free_block(self.id);
            self.fs
                .sync_metadata()
                .expect("failed to update metadata file when freeing the block");
            let disk_filename = &self.disk_inode.read().disk_filename;
            let filename = disk_filename.to_string();
            self.fs
                .device
                .remove(filename.as_str())
                .expect("failed to remove file");
        }
    }
}

/// Simple Encrypted File System
// Be careful with the write lock sequence of super_block, free_map and inodes
// Since free_map and super_block are always used simultaneously,
// use the `write_lock_free_map_and_super_block` to acquire the locks.
pub struct SEFS {
    /// on-disk superblock
    super_block: RwLock<Dirty<SuperBlock>>,
    /// blocks in use are marked 0
    free_map: RwLock<Dirty<BitVec<Lsb0, u8>>>,
    /// inode list
    inodes: RwLock<BTreeMap<INodeId, Weak<INodeImpl>>>,
    /// device
    device: Box<dyn Storage>,
    /// metadata file
    meta_file: Box<dyn File>,
    /// Time provider
    time_provider: &'static dyn TimeProvider,
    /// uuid provider
    uuid_provider: &'static dyn UuidProvider,
    /// Pointer to self, used by INodes
    self_ptr: Weak<SEFS>,
}

impl SEFS {
    /// Load SEFS
    /// The metadata file is used to store the metadate of filesystem, the layout is as follows:
    ///  +-------------------------------------------------------------------------------------------+
    ///  |   block0    |  block1  | block2 | block3 | ... | blockN | blockN+1 | blockN+2 | ... | ... |
    ///  | super_block | free_map | INode2 | INode3 | ... | INodeN | free_map | INodeN+2 | ... | ... |
    ///  +-------------------------------------------------------------------------------------------+
    ///  |                   Group 0                      |              Group 1               | ... |
    ///  +-------------------------------------------------------------------------------------------+
    /// N is the number of bits in a block.
    /// When opening an existing SEFS, load the metadata from the file at first.
    pub fn open(
        device: Box<dyn Storage>,
        time_provider: &'static dyn TimeProvider,
        uuid_provider: &'static dyn UuidProvider,
    ) -> vfs::Result<Arc<Self>> {
        let meta_file = device.open(METAFILE_NAME)?;

        // Load super block
        let super_block = meta_file.load_struct::<SuperBlock>(BLKN_SUPER)?;
        if !super_block.check() {
            return Err(FsError::WrongFs);
        }

        // Load free map
        let mut free_map = BitVec::with_capacity(BLKBITS * super_block.groups as usize);
        unsafe {
            free_map.set_len(BLKBITS * super_block.groups as usize);
        }
        for i in 0..super_block.groups as usize {
            let block_id = Self::get_freemap_block_id_of_group(i);
            meta_file.read_block(
                block_id,
                &mut free_map.as_mut_slice()[BLKSIZE * i..BLKSIZE * (i + 1)],
            )?;
        }

        Ok(SEFS {
            super_block: RwLock::new(Dirty::new(super_block)),
            free_map: RwLock::new(Dirty::new(free_map)),
            inodes: RwLock::new(BTreeMap::new()),
            device,
            meta_file,
            time_provider,
            uuid_provider,
            self_ptr: Weak::default(),
        }
        .wrap())
    }

    /// Create a new SEFS
    pub fn create(
        device: Box<dyn Storage>,
        time_provider: &'static dyn TimeProvider,
        uuid_provider: &'static dyn UuidProvider,
    ) -> vfs::Result<Arc<Self>> {
        let blocks = BLKBITS;

        let super_block = SuperBlock {
            magic: SEFS_MAGIC,
            blocks: blocks as u32,
            unused_blocks: blocks as u32 - 2,
            groups: 1,
        };
        let free_map = {
            let mut bitset = BitVec::with_capacity(BLKBITS);
            bitset.extend(core::iter::repeat(false).take(BLKBITS));
            for i in 2..blocks {
                bitset.set(i, true);
            }
            bitset
        };
        // Clear the existing files in storage
        device.clear()?;
        let meta_file = device.create(METAFILE_NAME)?;
        meta_file.set_len(blocks * BLKSIZE)?;

        let sefs = SEFS {
            super_block: RwLock::new(Dirty::new_dirty(super_block)),
            free_map: RwLock::new(Dirty::new_dirty(free_map)),
            inodes: RwLock::new(BTreeMap::new()),
            device,
            meta_file,
            time_provider,
            uuid_provider,
            self_ptr: Weak::default(),
        }
        .wrap();
        // Init root INode
        let root = sefs.new_inode(FileType::Dir, 0o755)?;
        assert_eq!(root.id, BLKN_ROOT);
        root.dirent_init(BLKN_ROOT)?;
        root.nlinks_inc(); //for .
        root.nlinks_inc(); //for ..(root's parent is itself)
                           // Initilize the metadata file with root INode
        sefs.sync_metadata()?;
        root.sync_all()?;

        Ok(sefs)
    }

    /// Wrap pure SEFS with Arc
    /// Used in constructors
    fn wrap(self) -> Arc<Self> {
        // Create a Arc, make a Weak from it, then put it into the struct.
        // It's a little tricky.
        let fs = Arc::new(self);
        let weak = Arc::downgrade(&fs);
        let ptr = Arc::into_raw(fs) as *mut Self;
        unsafe {
            (*ptr).self_ptr = weak;
        }
        unsafe { Arc::from_raw(ptr) }
    }

    /// Write back super block and free map if dirty
    fn sync_metadata(&self) -> vfs::Result<()> {
        let (mut free_map, mut super_block) = self.write_lock_free_map_and_super_block();
        // Sync super block
        if super_block.dirty() {
            self.meta_file
                .write_all_at(super_block.as_buf(), BLKSIZE * BLKN_SUPER)?;
            super_block.sync();
        }
        // Sync free map
        if free_map.dirty() {
            for i in 0..super_block.groups as usize {
                let slice = &free_map.as_slice()[BLKSIZE * i..BLKSIZE * (i + 1)];
                self.meta_file
                    .write_all_at(slice, BLKSIZE * Self::get_freemap_block_id_of_group(i))?;
            }
            free_map.sync();
        }
        // Flush
        self.meta_file.flush()?;
        Ok(())
    }

    /// Allocate a block, return block id
    fn alloc_block(&self) -> Option<usize> {
        let (mut free_map, mut super_block) = self.write_lock_free_map_and_super_block();
        let id = free_map.alloc().or_else(|| {
            // Allocate a new group
            let new_group_id = super_block.groups as usize;
            super_block.groups += 1;
            super_block.blocks += BLKBITS as u32;
            super_block.unused_blocks += BLKBITS as u32 - 1;
            self.meta_file
                .set_len(super_block.groups as usize * BLKBITS * BLKSIZE)
                .expect("failed to extend meta file");
            free_map.extend(core::iter::repeat(true).take(BLKBITS));
            // Set the bit to false to avoid to allocate it as INode ID
            free_map.set(Self::get_freemap_block_id_of_group(new_group_id), false);
            // Allocate block id again
            free_map.alloc()
        });
        assert!(id.is_some(), "allocate block should always success");
        super_block.unused_blocks -= 1;
        id
    }

    /// Release a block
    fn free_block(&self, block_id: usize) {
        let (mut free_map, mut super_block) = self.write_lock_free_map_and_super_block();
        assert!(!free_map[block_id]);
        free_map.set(block_id, true);
        super_block.unused_blocks += 1;
    }

    /// Helper function to get the write lock on both free_map and super_block
    /// The lock sequence can avoid deadlock
    fn write_lock_free_map_and_super_block(
        &self,
    ) -> (
        RwLockWriteGuard<Dirty<BitVec<Lsb0, u8>>>,
        RwLockWriteGuard<Dirty<SuperBlock>>,
    ) {
        let free_map = self.free_map.write();
        let super_block = self.super_block.write();
        (free_map, super_block)
    }

    /// Create a new INode struct, then insert it to self.inodes
    /// Private used for load or create INode
    fn _new_inode(
        &self,
        id: INodeId,
        disk_inode: Dirty<DiskINode>,
        create: bool,
    ) -> vfs::Result<Arc<INodeImpl>> {
        let filename = disk_inode.disk_filename.to_string();

        let inode = Arc::new(INodeImpl {
            id,
            disk_inode: RwLock::new(disk_inode),
            file: match create {
                true => self.device.create(filename.as_str())?,
                false => self.device.open(filename.as_str())?,
            },
            fs: self.self_ptr.upgrade().unwrap(),
        });
        #[cfg(not(feature = "create_image"))]
        if let false = create {
            inode.check_integrity()
        }
        self.inodes.write().insert(id, Arc::downgrade(&inode));
        Ok(inode)
    }

    /// Get inode by id. Load if not in memory.
    /// ** Must ensure it's a valid INode **
    fn get_inode(&self, id: INodeId) -> vfs::Result<Arc<INodeImpl>> {
        assert!(!self.free_map.read()[id]);

        // In the BTreeSet and not weak.
        if let Some(inode) = self.inodes.read().get(&id) {
            if let Some(inode) = inode.upgrade() {
                return Ok(inode);
            }
        }
        // Load if not in set, or is weak ref.
        let disk_inode = Dirty::new(self.meta_file.load_struct::<DiskINode>(id)?);
        self._new_inode(id, disk_inode, false)
    }

    /// Create a new INode file
    fn new_inode(&self, type_: FileType, mode: u16) -> vfs::Result<Arc<INodeImpl>> {
        let id = self.alloc_block().ok_or(FsError::NoDeviceSpace)?;
        let (time, uuid) = if cfg!(feature = "create_image") && self.device.protect_integrity() {
            (Default::default(), SefsUuid::from(id))
        } else {
            (
                self.time_provider.current_time(),
                self.uuid_provider.generate_uuid(),
            )
        };
        let disk_inode = Dirty::new_dirty(DiskINode {
            size: 0,
            type_,
            mode,
            nlinks: 0,
            blocks: 0,
            uid: 0,
            gid: 0,
            pad: 0,
            atime: time,
            mtime: time,
            ctime: time,
            disk_filename: uuid,
            inode_mac: Default::default(),
        });
        self._new_inode(id, disk_inode, true)
    }

    fn flush_weak_inodes(&self) {
        let mut inodes = self.inodes.write();
        let remove_ids: Vec<_> = inodes
            .iter()
            .filter(|(_, inode)| inode.upgrade().is_none())
            .map(|(&id, _)| id)
            .collect();
        for id in remove_ids.iter() {
            inodes.remove(id);
        }
    }

    fn get_freemap_block_id_of_group(group_id: usize) -> usize {
        BLKBITS * group_id + BLKN_FREEMAP
    }
}

impl vfs::FileSystem for SEFS {
    /// Write back FS if dirty
    fn sync(&self) -> vfs::Result<()> {
        // Sync all INodes
        self.flush_weak_inodes();
        for inode in self.inodes.read().values() {
            if let Some(inode) = inode.upgrade() {
                inode.sync_all()?;
            }
        }
        // Sync metadata
        self.sync_metadata()?;
        Ok(())
    }

    fn root_inode(&self) -> Arc<dyn vfs::INode> {
        self.get_inode(BLKN_ROOT).unwrap()
    }

    fn root_mac(&self) -> vfs::FsMac {
        self.meta_file.get_file_mac().unwrap().0
    }

    fn info(&self) -> vfs::FsInfo {
        let sb = self.super_block.read();
        vfs::FsInfo {
            magic: sb.magic as usize,
            bsize: BLKSIZE,
            frsize: BLKSIZE,
            blocks: sb.blocks as usize,
            bfree: sb.unused_blocks as usize,
            bavail: sb.unused_blocks as usize,
            files: sb.blocks as usize,        // inaccurate
            ffree: sb.unused_blocks as usize, // inaccurate
            namemax: MAX_FNAME_LEN,
        }
    }
}

impl Drop for SEFS {
    /// Auto sync when drop
    fn drop(&mut self) {
        self.sync().expect("Failed to sync when dropping the SEFS");
    }
}

trait BitsetAlloc {
    fn alloc(&mut self) -> Option<usize>;
}

impl BitsetAlloc for BitVec<Lsb0, u8> {
    fn alloc(&mut self) -> Option<usize> {
        // TODO: more efficient
        let id = self.iter().position(|&bit| bit);
        if let Some(id) = id {
            self.set(id, false);
        }
        id
    }
}

impl AsBuf for BitVec<Lsb0, u8> {
    fn as_buf(&self) -> &[u8] {
        self.as_ref()
    }

    fn as_buf_mut(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

impl AsBuf for [u8; BLKSIZE] {}

impl From<FileType> for vfs::FileType {
    fn from(t: FileType) -> Self {
        match t {
            FileType::File => vfs::FileType::File,
            FileType::Dir => vfs::FileType::Dir,
            FileType::SymLink => vfs::FileType::SymLink,
            FileType::Socket => vfs::FileType::Socket,
            _ => panic!("unknown file type"),
        }
    }
}
