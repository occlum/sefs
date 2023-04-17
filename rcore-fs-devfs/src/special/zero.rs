use super::*;

#[derive(Default)]
pub struct ZeroINode;

impl INode for ZeroINode {
    fn read_at(&self, _offset: usize, buf: &mut [u8]) -> Result<usize> {
        // read zeros
        for x in buf.iter_mut() {
            *x = 0;
        }
        Ok(buf.len())
    }

    fn write_at(&self, _offset: usize, buf: &[u8]) -> Result<usize> {
        // write to nothing
        Ok(buf.len())
    }

    fn metadata(&self) -> Result<Metadata> {
        Ok(Metadata {
            dev: 1,
            inode: 1,
            size: 0,
            blk_size: 0,
            blocks: 0,
            atime: Timespec { sec: 0, nsec: 0 },
            mtime: Timespec { sec: 0, nsec: 0 },
            ctime: Timespec { sec: 0, nsec: 0 },
            type_: FileType::CharDevice,
            mode: 0o666,
            nlinks: 1,
            uid: 0,
            gid: 0,
            rdev: make_rdev(1, 5),
        })
    }

    impl_inode!();
}
