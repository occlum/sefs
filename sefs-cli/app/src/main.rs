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
    /// Path of the enclave library
    #[structopt(short, long, parse(from_os_str))]
    enclave: PathBuf,
    /// Command
    #[structopt(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, StructOpt)]
enum Cmd {
    /// Create a new <image> for <dir>
    #[structopt(name = "zip")]
    Zip {
        /// Source dirctory
        #[structopt(parse(from_os_str))]
        dir: PathBuf,
        /// Target SEFS image directory
        #[structopt(parse(from_os_str))]
        image: PathBuf,
        /// Integrity-only mode
        #[structopt(short, long)]
        integrity_only: bool,
    },
    /// Unzip data from given <image> to <dir>
    #[structopt(name = "unzip")]
    Unzip {
        /// Source SEFS image directory
        #[structopt(parse(from_os_str))]
        image: PathBuf,
        /// Target unzip directory
        #[structopt(parse(from_os_str))]
        dir: PathBuf,
        /// Integrity-only mode
        #[structopt(short, long)]
        integrity_only: bool,
    },
    /// Mount <image> overlayed with <container> to <dir>
    #[structopt(name = "mount")]
    Mount {
        /// Image SEFS directory
        #[structopt(parse(from_os_str))]
        image: PathBuf,
        /// Container SEFS directory
        #[structopt(parse(from_os_str))]
        container: PathBuf,
        /// Target mount point
        #[structopt(parse(from_os_str))]
        dir: PathBuf,
    },
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

    match opt.cmd {
        Cmd::Mount {
            image,
            container,
            dir,
        } => {
            let image_fs = {
                let device = sgx_dev::SgxStorage::new(enclave.geteid(), &image, true);
                sefs::SEFS::open(Box::new(device), &StdTimeProvider, &StdUuidProvider)?
            };
            let mnt_dir = dir.clone();
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
            if container.is_dir() {
                let union_fs = {
                    let device = sgx_dev::SgxStorage::new(enclave.geteid(), &container, false);
                    let container_fs =
                        sefs::SEFS::open(Box::new(device), &StdTimeProvider, &StdUuidProvider)?;
                    unionfs::UnionFS::new(vec![container_fs, image_fs])?
                };
                fuse::mount(VfsFuse::new(union_fs), &dir, &[])?;
            } else {
                // Mount as an SEFS
                fuse::mount(VfsFuse::new(image_fs), &dir, &[])?;
            }
        }
        Cmd::Zip {
            dir,
            image,
            integrity_only,
        } => {
            let sefs_fs = {
                std::fs::create_dir(&image)?;
                let device = sgx_dev::SgxStorage::new(enclave.geteid(), &image, integrity_only);
                sefs::SEFS::create(Box::new(device), &StdTimeProvider, &StdUuidProvider)?
            };
            zip_dir(&dir, sefs_fs.root_inode())?;
        }
        Cmd::Unzip {
            image,
            dir,
            integrity_only,
        } => {
            let sefs_fs = {
                let device = sgx_dev::SgxStorage::new(enclave.geteid(), &image, integrity_only);
                sefs::SEFS::open(Box::new(device), &StdTimeProvider, &StdUuidProvider)?
            };
            std::fs::create_dir(&dir)?;
            unzip_dir(&dir, sefs_fs.root_inode())?;
        }
    }
    Ok(())
}
