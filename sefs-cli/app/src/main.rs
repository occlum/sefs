use std::error::Error;
use std::ffi::CString;
use std::io::{Error as IoError, ErrorKind, Read};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::exit;

use ctrlc;
use libc;
use structopt::StructOpt;

use rcore_fs::dev::std_impl::StdTimeProvider;
use rcore_fs::vfs::FileSystem;
use rcore_fs_cli::fuse::VfsFuse;
use rcore_fs_cli::zip::{unzip_dir, zip_dir_parallel, update_dir};
use rcore_fs_sefs as sefs;
use rcore_fs_sefs::dev::std_impl::StdUuidProvider;
use rcore_fs_unionfs as unionfs;

mod enclave;
mod sgx_dev;

#[derive(Debug)]
struct MainError(String);

impl std::error::Error for MainError {}

impl std::fmt::Display for MainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

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
        /// Root MAC of the SEFS image
        #[structopt(parse(from_os_str))]
        mac: PathBuf,
        /// Key for encryption
        #[structopt(short, long, parse(from_os_str))]
        key: Option<PathBuf>,
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
        /// Protect the integrity of FS
        #[structopt(short, long)]
        protect_integrity: bool,
        /// Key for decryption
        #[structopt(short, long, parse(from_os_str))]
        key: Option<PathBuf>,
    },
    /// Update data from <dir> to <image>
    #[structopt(name = "update")]
    Update {
        /// Source SEFS image directory
        #[structopt(parse(from_os_str))]
        image: PathBuf,
        /// Target directory
        #[structopt(parse(from_os_str))]
        dir: PathBuf,
        // LogFile directory
        #[structopt(parse(from_os_str))]
        log: PathBuf,
        /// Root MAC of the SEFS image
        #[structopt(parse(from_os_str))]
        mac: PathBuf,
        /// Protect the integrity of FS
        #[structopt(short, long)]
        protect_integrity: bool,
        /// Key for decryption
        #[structopt(short, long, parse(from_os_str))]
        key: Option<PathBuf>,
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
        /// Key for decryption
        #[structopt(short, long, parse(from_os_str))]
        key: Option<PathBuf>,
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
            key,
        } => {
            let key = parse_key(&key)?;
            let image_fs = {
                let mode = sgx_dev::EncryptMode::from_parameters(true, &key)?;
                let device = sgx_dev::SgxStorage::new(enclave.geteid(), &image, mode);
                sefs::SEFS::open(Box::new(device), &StdTimeProvider, &StdUuidProvider)?
            };
            let mnt_dir = dir.clone();
            // Ctrl-C handler
            ctrlc::set_handler(move || {
                // Unmount the mount point will cause the "fuse::mount" return,
                // which makes the program exit safely.
                let mnt_str = CString::new(mnt_dir.as_os_str().as_bytes().to_owned())
                    .expect("invalid mount dir");
                unsafe {
                    match libc::umount(mnt_str.as_ptr()) {
                        0 => (),
                        _err => {
                            let error: Result<(), IoError> = Err(IoError::last_os_error());
                            println!("failed to unmount {:?}: {:?}", &mnt_dir, error);
                            exit(1);
                        }
                    }
                }
            })?;
            // Mount as an UnionFS
            if container.is_dir() {
                let union_fs = {
                    let mode = sgx_dev::EncryptMode::from_parameters(false, &key)?;
                    let device = sgx_dev::SgxStorage::new(enclave.geteid(), &container, mode);
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
            mac,
            key,
        } => {
            let sefs_fs = {
                std::fs::create_dir(&image)?;
                let key = parse_key(&key)?;
                let mode = sgx_dev::EncryptMode::from_parameters(true, &key)?;
                let device = sgx_dev::SgxStorage::new(enclave.geteid(), &image, mode);
                sefs::SEFS::create(Box::new(device), &StdTimeProvider, &StdUuidProvider)?
            };
            //Use thread pool to speed up the zip process
            zip_dir_parallel(&dir, sefs_fs.root_inode())
                .map_err(|e| Box::new(MainError(format!("failed to zip: {}", e))) as Box<dyn Error>)?;
            sefs_fs.sync()?;
            let root_mac_str = {
                let mut s = String::from("");
                for (i, byte) in sefs_fs.root_mac().iter().enumerate() {
                    if i != 0 {
                        s += "-";
                    }
                    s += &format!("{:02x}", byte);
                }
                s
            };
            let f = std::fs::File::create(mac)?;
            f.write_all_at(root_mac_str.as_bytes(), 0)?;
            println!("Generate the SEFS image successfully");
        }
        Cmd::Unzip {
            image,
            dir,
            protect_integrity,
            key,
        } => {
            let sefs_fs = {
                let key = parse_key(&key)?;
                let mode = sgx_dev::EncryptMode::from_parameters(protect_integrity, &key)?;
                let device = sgx_dev::SgxStorage::new(enclave.geteid(), &image, mode);
                sefs::SEFS::open(Box::new(device), &StdTimeProvider, &StdUuidProvider)?
            };
            std::fs::create_dir(&dir)?;
            unzip_dir(&dir, sefs_fs.root_inode())?;
            println!("Decrypt the SEFS image successfully");
        }
        Cmd::Update {
            image,
            dir,
            log,
            mac,
            protect_integrity,
            key,
        } => {
            let sefs_fs = {
                let key = parse_key(&key)?;
                let mode = sgx_dev::EncryptMode::from_parameters(protect_integrity, &key)?;
                let device = sgx_dev::SgxStorage::new(enclave.geteid(), &image, mode);
                sefs::SEFS::open(Box::new(device), &StdTimeProvider, &StdUuidProvider)?
            };        
            let file = std::fs::File::open(&log)?;
            let reader = BufReader::new(file);  
            for line in reader.lines() {
                let path = PathBuf::from(line?.trim_end().to_string());
                update_dir(&dir, &path, sefs_fs.root_inode())?;
                sefs_fs.sync()?;
            }
            let root_mac_str = {
                let mut s = String::from("");
                for (i, byte) in sefs_fs.root_mac().iter().enumerate() {
                    if i != 0 {
                        s += "-";
                    }
                    s += &format!("{:02x}", byte);
                }
                s
            };
            let f = std::fs::File::create(mac)?;
            f.write_all_at(root_mac_str.as_bytes(), 0)?;
            println!("Update the SEFS image successfully");
        }
    }
    Ok(())
}

fn parse_key(key: &Option<PathBuf>) -> Result<Option<String>, Box<dyn Error>> {
    let key = match key {
        None => None,
        Some(key_path) => {
            let mut f = std::fs::File::open(key_path)?;
            let mut key_str = String::new();
            f.read_to_string(&mut key_str)?;
            Some(
                key_str
                    .trim_end_matches(|c| c == '\r' || c == '\n')
                    .to_string(),
            )
        }
    };
    Ok(key)
}
