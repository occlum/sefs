use crate::*;
use rcore_fs_ramfs::RamFS;

#[test]
fn mount() {
    let rootfs = MountFS::new(RamFS::new());
    let root = rootfs.mountpoint_root_mnode();
    let mnt = root.create("mnt", FileType::Dir, 0o777).unwrap();

    let ramfs = RamFS::new();
    let root1 = ramfs.root_inode();
    root1.create("file", FileType::File, 0o777).unwrap();

    mnt.mount(ramfs).unwrap();
    assert!((mnt as Arc<dyn INode>).find("file").is_ok());
    assert!(root.lookup("mnt/file").is_ok());
}

#[test]
fn remove_busy() {
    let rootfs = MountFS::new(RamFS::new());
    let root = rootfs.mountpoint_root_mnode();
    let mnt = root.create("mnt", FileType::Dir, 0o777).unwrap();
    let ramfs = RamFS::new();
    mnt.mount(ramfs).unwrap();
    assert_eq!(root.unlink("mnt"), Err(FsError::Busy));
}
