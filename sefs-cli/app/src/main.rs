use std::error::Error;
use std::io::{Error as IoError, ErrorKind};
use std::path::PathBuf;
use std::process::exit;

use ctrlc;
use structopt::StructOpt;
use sys_mount;

use rcore_fs::dev::std_impl::StdTimeProvider;
use rcore_fs::vfs::FileSystem;
use rcore_fs_cli::fuse::VfsFuse;
use rcore_fs_cli::zip::{unzip_dir, zip_dir};
use rcore_fs_sefs as sefs;
use rcore_fs_sefs::dev::std_impl::StdUuidProvider;
use rcore_fs_unionfs as unionfs;

mod enclave;
mod sgx_dev;

#[derive(Debug, StructOpt)]
struct Opt {
    /// Command
    #[structopt(subcommand)]
    cmd: Cmd,

    /// Path of enclave library
    #[structopt(parse(from_os_str))]
    enclave: PathBuf,

    /// Image directory
    #[structopt(parse(from_os_str))]
    image: PathBuf,

    /// Container directory
    #[structopt(parse(from_os_str))]
    container: PathBuf,

    /// Target directory
    #[structopt(parse(from_os_str))]
    dir: PathBuf,

    /// Integrity-only mode
    #[structopt(short = "i", long = "integrity-only")]
    integrity_only: bool,
}

#[derive(Debug, StructOpt)]
enum Cmd {
    /// Create a new <image> for <dir>
    #[structopt(name = "zip")]
    Zip,

    /// Unzip data from given <image> to <dir>
    #[structopt(name = "unzip")]
    Unzip,

    /// Mount <image> overlayed with <container> to <dir>
    #[structopt(name = "mount")]
    Mount,
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let opt = Opt::from_args();

    let enclave = match enclave::init_enclave(&opt.enclave.to_str().unwrap()) {
        Ok(r) => {
            println!("[+] Init Enclave Successful {}!", r.geteid());
            r
        }
        Err(x) => {
            println!("[-] Init Enclave Failed!");
            return Err(Box::new(IoError::new(ErrorKind::Other, x.as_str())));
        }
    };

    // open or create
    let create = match opt.cmd {
        Cmd::Mount => false,
        Cmd::Zip => true,
        Cmd::Unzip => false,
    };

    let device = sgx_dev::SgxStorage::new(enclave.geteid(), &opt.image, opt.integrity_only);
    let image_fs = match create {
        true => {
            std::fs::create_dir(&opt.image)?;
            sefs::SEFS::create(Box::new(device), &StdTimeProvider, &StdUuidProvider)?
        }
        false => sefs::SEFS::open(Box::new(device), &StdTimeProvider, &StdUuidProvider)?,
    };
    match opt.cmd {
        Cmd::Mount => {
            let mnt_dir = opt.dir.clone();
            // Ctrl-C handler
            ctrlc::set_handler(move || {
                // Unmount the mount point will cause the "fuse::mount" return,
                // which makes the program exit safely.
                match sys_mount::unmount(&mnt_dir, sys_mount::UnmountFlags::empty()) {
                    Ok(()) => (),
                    Err(why) => {
                        println!("failed to unmount {:?}: {}", &mnt_dir, why);
                        exit(1);
                    }
                }
            })?;
            // Mount as an UnionFS
            if opt.container.is_dir() {
                let union_fs = {
                    let integrity_only = false;
                    let device =
                        sgx_dev::SgxStorage::new(enclave.geteid(), &opt.container, integrity_only);
                    let container_fs =
                        sefs::SEFS::open(Box::new(device), &StdTimeProvider, &StdUuidProvider)?;
                    unionfs::UnionFS::new(vec![container_fs, image_fs])?
                };
                fuse::mount(VfsFuse::new(union_fs), &opt.dir, &[])?
            } else {
                // Mount as an SEFS
                fuse::mount(VfsFuse::new(image_fs), &opt.dir, &[])?
            }
        }
        Cmd::Zip => {
            let root_inode = image_fs.root_inode();
            zip_dir(&opt.dir, root_inode)?;
        }
        Cmd::Unzip => {
            std::fs::create_dir(&opt.dir)?;
            unzip_dir(&opt.dir, image_fs.root_inode())?;
        }
    }
    Ok(())
}
