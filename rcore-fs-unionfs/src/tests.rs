extern crate std;

use crate::UnionFS;
use alloc::sync::Arc;
use rcore_fs::vfs::*;
use rcore_fs_ramfs::RamFS;
use std::collections::btree_set::BTreeSet;

/// Create a UnionFS for test.
/// Return root INode of (union, container, image).
///
/// container:
/// ├── file1
/// └── file2
/// image:
/// ├── file1
/// ├── file3
/// └── dir
///     ├── file4
///     └── dir2
///         └── file5
fn create_sample() -> Result<(Arc<dyn FileSystem>, Arc<dyn INode>, Arc<dyn INode>)> {
    let container_fs = {
        let fs = RamFS::new();
        let root = fs.root_inode();
        let file1 = root.create("file1", FileType::File, MODE)?;
        let file2 = root.create("file2", FileType::File, MODE)?;
        file1.write_at(0, b"container")?;
        file2.write_at(0, b"container")?;
        fs
    };
    let container_root = container_fs.root_inode();

    let image_fs = {
        let fs = RamFS::new();
        let root = fs.root_inode();
        let file1 = root.create("file1", FileType::File, MODE)?;
        let file3 = root.create("file3", FileType::File, MODE)?;
        let dir = root.create("dir", FileType::Dir, MODE)?;
        let file4 = dir.create("file4", FileType::File, MODE)?;
        let dir2 = dir.create("dir2", FileType::Dir, MODE)?;
        let file5 = dir2.create("file5", FileType::File, MODE)?;
        file1.write_at(0, b"image")?;
        file3.write_at(0, b"image")?;
        file4.write_at(0, b"image")?;
        file5.write_at(0, b"image")?;
        fs
    };
    let image_root = image_fs.root_inode();

    let unionfs = UnionFS::new(vec![container_fs, image_fs])?;

    Ok((unionfs, container_root, image_root))
}

#[test]
fn read_file() -> Result<()> {
    let (fs, _, _) = create_sample()?;
    let root = fs.root_inode();
    assert_eq!(root.lookup("file1")?.read_as_vec()?, b"container");
    assert_eq!(root.lookup("file2")?.read_as_vec()?, b"container");
    assert_eq!(root.lookup("file3")?.read_as_vec()?, b"image");
    assert_eq!(root.lookup("dir/file4")?.read_as_vec()?, b"image");
    Ok(())
}

#[test]
fn write_file() -> Result<()> {
    let (fs, croot, iroot) = create_sample()?;
    let root = fs.root_inode();
    for path in &["file1", "file3", "dir/file4", "/dir/dir2/file5"] {
        const WRITE_DATA: &[u8] = b"I'm writing to container";
        root.lookup(path)?.write_at(0, WRITE_DATA)?;
        assert_eq!(croot.lookup(path)?.read_as_vec()?, WRITE_DATA);
        assert_eq!(iroot.lookup(path)?.read_as_vec()?, b"image");
        assert_eq!(
            croot.lookup(path)?.metadata()?.mode,
            iroot.lookup(path)?.metadata()?.mode
        );
    }
    assert_eq!(
        croot.lookup("dir")?.metadata()?.mode,
        iroot.lookup("dir")?.metadata()?.mode
    );
    assert_eq!(
        croot.lookup("dir/dir2")?.metadata()?.mode,
        iroot.lookup("dir/dir2")?.metadata()?.mode
    );
    Ok(())
}

#[test]
fn get_direntry() -> Result<()> {
    let (fs, _croot, _iroot) = create_sample()?;
    let root = fs.root_inode();
    let entries: BTreeSet<String> = root.list()?.into_iter().collect();
    let expected: BTreeSet<String> = [".", "..", "file1", "file2", "file3", "dir"]
        .iter()
        .map(|&s| String::from(s))
        .collect();
    assert_eq!(entries, expected);
    Ok(())
}

#[test]
fn unlink() -> Result<()> {
    let (fs, croot, iroot) = create_sample()?;
    let root = fs.root_inode();

    root.unlink("file1")?;
    assert!(root.lookup("file1").is_not_found());
    assert!(croot.lookup("file1").is_not_found());
    assert!(croot.lookup(".ufs.wh.file1").is_ok());
    assert!(iroot.lookup("file1").is_ok());

    root.unlink("file2")?;
    assert!(root.lookup("file2").is_not_found());
    assert!(croot.lookup("file2").is_not_found());
    assert!(croot.lookup(".ufs.wh.file2").is_not_found());

    root.unlink("file3")?;
    assert!(root.lookup("file3").is_not_found());
    assert!(croot.lookup(".ufs.wh.file3").is_ok());
    assert!(iroot.lookup("file3").is_ok());

    root.lookup("dir")?.unlink("file4")?;
    assert!(root.lookup("dir/file4").is_not_found());
    assert!(croot.lookup("dir/.ufs.wh.file4").is_ok());
    assert!(iroot.lookup("dir/file4").is_ok());

    root.lookup("dir")?.lookup("dir2")?.unlink("file5")?;
    assert!(root.lookup("dir/dir2/file5").is_not_found());
    assert!(croot.lookup("dir/dir2/.ufs.wh.file5").is_ok());
    assert!(iroot.lookup("dir/dir2/file5").is_ok());

    root.lookup("dir")?.unlink("dir2")?;
    assert!(root.lookup("dir/dir2").is_not_found());
    assert!(croot.lookup("dir/.ufs.wh.dir2").is_ok());
    assert!(iroot.lookup("dir/dir2").is_ok());

    root.unlink("dir")?;
    assert!(root.lookup("dir").is_not_found());
    assert!(croot.lookup(".ufs.wh.dir").is_ok());
    assert!(iroot.lookup("dir").is_ok());

    Ok(())
}

#[test]
fn unlink_then_create() -> Result<()> {
    let (fs, croot, iroot) = create_sample()?;
    let root = fs.root_inode();
    root.unlink("file1")?;
    let file1 = root.create("file1", FileType::File, MODE)?;
    assert_eq!(file1.read_as_vec()?, b"");
    assert!(croot.lookup(".ufs.wh.file1").is_not_found());

    assert!(root.create(".ufs.wh.file1", FileType::File, MODE).is_err());
    assert!(root.create(".ufs.opq.file1", FileType::File, MODE).is_err());
    assert!(root.create(".ufs.mac", FileType::File, MODE).is_err());

    root.unlink("file1")?;
    let file1 = root.create("file1", FileType::Dir, MODE)?;
    assert!(root.lookup("file1").is_ok());
    assert!(root.lookup("file1")?.metadata()?.type_ == FileType::Dir);
    assert!(croot.lookup("file1").is_ok());
    assert!(iroot.lookup("file1").is_ok());
    assert!(iroot.lookup("file1")?.metadata()?.type_ == FileType::File);
    file1.create("file6", FileType::File, MODE)?;
    assert!(root.lookup("file1/file6").is_ok());
    assert!(croot.lookup("file1/file6").is_ok());
    assert!(iroot.lookup("file1/file6").is_err());

    root.lookup("dir")?.unlink("file4")?;
    root.lookup("dir/dir2")?.unlink("file5")?;
    root.lookup("dir")?.unlink("dir2")?;
    root.unlink("dir")?;
    let dir = root.create("dir", FileType::Dir, MODE)?;
    assert!(root.lookup("dir").is_ok());
    assert!(croot.lookup(".ufs.wh.dir").is_not_found());
    assert!(croot.lookup(".ufs.opq.dir").is_ok());
    assert!(iroot.lookup("dir").is_ok());
    assert!(iroot.lookup("dir")?.list()?.len() == 4);
    assert!(root.lookup("dir")?.list()?.len() == 2);

    dir.create("dir2", FileType::Dir, MODE)?;
    assert!(root.lookup("dir/dir2").is_ok());
    assert!(croot.lookup("dir/.ufs.wh.dir2").is_not_found());
    assert!(iroot.lookup("dir/dir2").is_ok());
    assert!(root.lookup("dir/dir2")?.list()?.len() == 2);
    assert!(iroot.lookup("dir/dir2")?.list()?.len() == 3);

    dir.unlink("dir2")?;
    root.unlink("dir")?;
    assert!(root.lookup("dir").is_not_found());
    assert!(croot.lookup(".ufs.wh.dir").is_ok());
    assert!(croot.lookup(".ufs.opq.dir").is_not_found());

    Ok(())
}

#[test]
fn link_container() -> Result<()> {
    let (fs, _, _) = create_sample()?;
    let root = fs.root_inode();

    // create link
    let dir = root.lookup("dir")?;
    let file1 = root.lookup("file1")?;
    dir.link("file1_link", &file1)?;

    // read from new link
    let file1_link = root.lookup("dir/file1_link")?;
    assert_eq!(file1_link.read_as_vec()?, b"container");

    // write then read from another link
    const WRITE_DATA: &[u8] = b"I'm writing to container";
    file1_link.write_at(0, WRITE_DATA)?;
    assert_eq!(file1.read_as_vec()?, WRITE_DATA);
    Ok(())
}

#[test]
fn link_image() -> Result<()> {
    let (fs, _, _) = create_sample()?;
    let root = fs.root_inode();

    // create link
    let dir = root.lookup("dir")?;
    let file3 = root.lookup("file3")?;
    dir.link("file3_link", &file3)?;

    // read from new link
    let file3_link = root.lookup("dir/file3_link")?;
    assert_eq!(file3_link.read_as_vec()?, b"image");

    // write then read from another link
    const WRITE_DATA: &[u8] = b"I'm writing to container";
    file3_link.write_at(0, WRITE_DATA)?;
    assert_eq!(file3.read_as_vec()?, WRITE_DATA);
    Ok(())
}

#[test]
fn move_container() -> Result<()> {
    let (fs, croot, _) = create_sample()?;
    let root = fs.root_inode();

    let dir = root.lookup("dir")?;
    root.move_("file1", &dir, "file1")?;

    assert!(root.lookup("file1").is_not_found());
    assert!(root.lookup("dir/file1").is_ok());
    assert!(croot.lookup("file1").is_not_found());
    assert!(croot.lookup(".ufs.wh.file1").is_ok());
    assert!(croot.lookup("dir/file1").is_ok());
    Ok(())
}

#[test]
fn move_image() -> Result<()> {
    let (fs, croot, iroot) = create_sample()?;
    let root = fs.root_inode();

    let dir = root.lookup("dir")?;
    dir.move_("file4", &root, "file4")?;

    assert!(dir.lookup("file4").is_not_found());
    assert!(root.lookup("file4").is_ok());
    assert!(croot.lookup("dir/.ufs.wh.file4").is_ok());
    assert!(iroot.lookup("dir/file4").is_ok());
    Ok(())
}

const MODE: u16 = 0o777;

trait IsNotFound {
    fn is_not_found(&self) -> bool;
}

impl<T> IsNotFound for Result<T> {
    fn is_not_found(&self) -> bool {
        match self {
            Err(FsError::EntryNotFound) => true,
            _ => false,
        }
    }
}
