use async_rdma::{LocalMr, LocalMrReadAccess, LocalMrWriteAccess, Rdma, RdmaBuilder};
use gxhash::gxhash64;
use shared::*;
use std::{alloc::Layout, ffi::OsStr, num::NonZeroU32, os::unix::ffi::OsStrExt, path::{Path, PathBuf}, time::{Duration, SystemTime}};
use anyhow::Result;
use anyhow::anyhow;
use tokio::{
    sync::Mutex,
};
use tracing_subscriber::EnvFilter;
use fuse3::{path::{reply::{FileAttr, ReplyAttr, ReplyCreated, ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit}, PathFilesystem, Session}, raw::{reply::{ReplyData, ReplyOpen, ReplyStatFs, ReplyWrite}, Request}, Errno, FileType};
use fuse3::MountOptions;
use futures_util::{future::join_all, stream::{self, Iter}};
use fuse3::path::reply::DirectoryEntry;
use fuse3::path::reply::DirectoryEntryPlus;
use std::vec::IntoIter;


const TTL: Duration = Duration::new(2, 0);


#[inline]
pub fn path_to_inode(path: &Path) -> Inode {
    Inode(gxhash64(path.as_os_str().as_bytes(), 17))
}

struct NodeConnectionManager {
    pub rdma: Rdma,
    pub addr: String,
}

impl NodeConnectionManager {
    async fn new(
        addr: String,
    ) -> Result<NodeConnectionManager> {
        let rdma = Rdma::connect(addr.clone(), 1, 1, 64_000).await?;


        tracing::debug!("Setup connection maanger for node {}", addr);
        Ok(NodeConnectionManager {
            rdma,
            addr,
        })
    }

    async fn send_request(&self, payload: Op) -> Result<OpResponse> {
        let encoded_payload = bitcode::encode(&payload);

        let layout = Layout::array::<u8>(encoded_payload.len())?;
        let mut send_mr = self.rdma.alloc_local_mr(layout)?;

        // Copy encoded data (no padding, no zeros)
        send_mr.as_mut_slice().copy_from_slice(&encoded_payload);

        self.rdma.send(&send_mr).await?;     // Send
        tracing::debug!("Wrote payload to {}", self.addr);


        let receive_mr = self.rdma.receive().await?;
        let buf = receive_mr.as_slice();

        let deserialized: OpResponse = bitcode::decode(&buf)?;

        Ok(deserialized)
    }
}


struct NodeManager {
    pub nodes: Vec<NodeConnectionManager>,
}

// Blocksize = size
// Size = 1 for directory init, 0 for empty file 

impl PathFilesystem for NodeManager {
    async fn init(&self, _req: Request) -> fuse3::Result<ReplyInit> {
        Ok(ReplyInit {
            max_write: NonZeroU32::new(16 * 1024).unwrap(),
        })
    }

    async fn destroy(&self, _req: Request) {
        tracing::info!("fdfs shutting down");
    }

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> fuse3::Result<ReplyEntry> {
        tracing::info!("lookup called at parent: {:?}, name: {:?}", parent, name);

        let mut path = PathBuf::from(parent);
        path.push(name);

        let attr = self.get_attributes(&PathBuf::from(parent), name, &path).await?;
        
        Ok(ReplyEntry { ttl: TTL, attr  })
        
    }

    // TODO: look into using the file handle in order to avoid re-doing everything with the path
    async fn getattr(&self, _req: Request, path: Option<&OsStr>, _fh: Option<u64>, _flags: u32) -> fuse3::Result<ReplyAttr> {
        let raw = path.ok_or_else(fuse3::Errno::new_not_exist)?;
        let path = PathBuf::from(raw);
        tracing::info!("getattr called at {:?}", path);


        // Special root directory
        if path == Path::new("/") {
            let now = SystemTime::now();
            tracing::info!("Getattr: returning special root directory");
            return Ok(ReplyAttr { ttl: TTL, attr: FileAttr { size: 1, blocks: 1, atime: now, mtime: now, ctime: now,
                kind: FileType::Directory, perm: 0o777, nlink: 2, uid: 0, gid: 0, rdev: 0, blksize: 1 } })
        }


        let parent = path.parent().unwrap();
        let name = path.file_name().unwrap();
        

        let attr = self.get_attributes(&PathBuf::from(parent), name, &path).await?;
        
        Ok(ReplyAttr { ttl: TTL, attr  })
    }

    async fn setattr(&self, _req: Request, path: Option<&OsStr>, _fh: Option<u64>, _set_attr: fuse3::SetAttr) -> fuse3::Result<ReplyAttr> {
        tracing::info!("Setattr call for path {:?}", path);

        // Ignore the attributes set and just return what is already there
        self.getattr(_req, path, _fh, 0).await
    }

    // All Fake Data
    async fn statfs(&self, _req: Request, _path: &OsStr) -> fuse3::Result<ReplyStatFs> {
        let info = self.get_all_node_info().await.unwrap();

        
        Ok(fuse3::path::reply::ReplyStatFs {
            blocks: info.total_size / 4096,      // Total blocks
            bfree: info.free / 4096,       // Free blocks  
            bavail: info.free / 4096,      // Available blocks
            files: 1000000,       // Total inodes
            ffree: 1000000,       // Free inodes
            bsize: 4096,          // Block size
            namelen: 255,         // Max filename length
            frsize: 4096,         // Fragment size
        })
    }

    async fn opendir(&self, _req: Request, _path: &OsStr, flags: u32) -> fuse3::Result<ReplyOpen> {
        // We don't use file handles throughout, just use the path throughout
        Ok(ReplyOpen { fh: 0, flags: flags })
    }
    
    async fn open(&self, _req: Request, _path: &OsStr, flags: u32) -> fuse3::Result<ReplyOpen> {
        Ok(ReplyOpen { fh: 0, flags: flags })
    }

    async fn access(&self, _req: Request, path: &OsStr, _mask: u32) -> fuse3::Result<()> {
        tracing::info!("Access called on path {:?}", path);
        Ok(())
    }

    async fn release(&self, _req: Request, _path: Option<&OsStr>, _fh: u64, _flags: u32, _lock_owner: u64, _flush: bool) -> fuse3::Result<()> {
        Ok(())
    }

    async fn releasedir(&self, _req: Request, _path: &OsStr, _fh: u64, _flags: u32) -> fuse3::Result<()> {
        Ok(())
    }

    async fn create(&self, _req: Request, parent: &OsStr, name: &OsStr, _mode: u32, _flags: u32) -> fuse3::Result<ReplyCreated> {
        tracing::info!("Create called on parent {:?}, name: {:?}", parent, name);

        let mut path = PathBuf::from(parent);
        path.push(name);

        self.create(&path, false).await.unwrap();

        let now = SystemTime::now();
        Ok(ReplyCreated { ttl: TTL,
            attr: FileAttr { size: 0, blocks: 0, atime: now, mtime: now, ctime: now, kind: FileType::RegularFile, perm: 0o777, nlink: 1, uid: 0, gid: 0, rdev: 0, blksize: 0 },
            generation: 0, fh: 0, flags: 0 })
    }

    async fn mkdir(&self, _req: Request, parent: &OsStr, name: &OsStr, _mode: u32, _umask: u32) -> fuse3::Result<ReplyEntry> {
        tracing::info!("mkdir called on parent {:?}, name: {:?}", parent, name);

        let mut path = PathBuf::from(parent);
        path.push(name);

        self.create(&path, true).await.unwrap();

        let now = SystemTime::now();
        Ok(ReplyEntry { ttl: TTL,
            attr: FileAttr { size: 1, blocks: 1, atime: now, mtime: now, ctime: now, kind: FileType::Directory, perm: 0o777, nlink: 2, uid: 0, gid: 0, rdev: 0, blksize: 1 }})
    }

    async fn unlink(&self, _req: Request, parent: &OsStr, name: &OsStr) -> fuse3::Result<()> {
        tracing::info!("unlink called on parent {:?}, name: {:?}", parent, name);

        let mut path = PathBuf::from(parent);
        path.push(name);

        self.delete(&path, false).await.unwrap();

        Ok(())
    }

    async fn rmdir(&self, _req: Request, parent: &OsStr, name: &OsStr) -> fuse3::Result<()> {
        tracing::info!("rmdir called on parent {:?}, name: {:?}", parent, name);

        let mut path = PathBuf::from(parent);
        path.push(name);

        self.delete(&path, true).await.unwrap();

        Ok(())
    }

    async fn read(&self, _req: Request, path: Option<&OsStr>, _fh: u64, offset: u64, size: u32) -> fuse3::Result<ReplyData> {
        tracing::info!("read called for path: {:?} with offset {} and size {}", path, offset, size);
        let path = path.ok_or_else(fuse3::Errno::new_not_exist)?;
        let path = PathBuf::from(path);

        let data = self.read(&path, offset, size as u64).await.unwrap();

        Ok(ReplyData { data: data.into() })
    }

    async fn write(&self, _req: Request, path: Option<&OsStr>, _fh: u64, offset: u64, data: &[u8], _write_flags: u32, _flags: u32) -> fuse3::Result<ReplyWrite> {
        tracing::info!("write called for path: {:?}", path);
        let path = path.ok_or_else(fuse3::Errno::new_not_exist)?;
        let path = PathBuf::from(path);
 
        self.write(&path, offset, data.into()).await.unwrap();

        // TODO: return number of bytes actually written
        Ok(ReplyWrite { written: data.len() as u32 })
    }


    type DirEntryStream<'a>
        = Iter<IntoIter<fuse3::Result<DirectoryEntry>>>
    where
        Self: 'a;

    async fn readdir<'a>(&'a self, _req: Request, path: &'a OsStr, _fh: u64, offset: i64) -> fuse3::Result<ReplyDirectory<Self::DirEntryStream<'a> > > {
        tracing::info!("readdir called for path: {:?}", path);
        let path = PathBuf::from(path);
        let dir_entries = self.list_directory_entries(&path).await.unwrap();

        let mut out: Vec<Result<DirectoryEntry, Errno>> = Vec::with_capacity(dir_entries.len());

        out.push(Ok(
            DirectoryEntry { kind: FileType::Directory, name: ".".into(), offset: 1 }
        ));
        out.push(Ok(
            DirectoryEntry { kind: FileType::Directory, name: "..".into(), offset: 2 }
        ));

        // TODO: As with readdirplus, try to remove that clone on the name
        for (idx, entry) in dir_entries.iter().enumerate() {
            out.push(Ok::<DirectoryEntry, Errno>(
            DirectoryEntry { kind: if entry.is_dir {FileType::Directory} else {FileType::RegularFile}, name: entry.name.clone().into(), offset: (idx + 3) as i64 }));
        }

        let out: Vec<Result<DirectoryEntry, Errno>> = out.into_iter().skip(offset as usize).collect();
        Ok(ReplyDirectory { entries: stream::iter(out) })
    }

    type DirEntryPlusStream<'a>
        = Iter<IntoIter<fuse3::Result<DirectoryEntryPlus>>>
    where
        Self: 'a;

    // TODO: make sure nlink works correctly: just count the number of directories inside +2 
    // Pagination of the directory's entries, but since we HAVE to get all files inside a directory anyways might as well just ignore it and return everything
    async fn readdirplus<'a> (&'a self, _req: Request, parent: &'a OsStr, _fh: u64, offset: u64, _lock_owner: u64) -> fuse3::Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a> > > {
        tracing::info!("readdirplus called for path: {:?}", parent);
        let path = PathBuf::from(parent);
        let dir_entries = self.list_directory_entries(&path).await.unwrap();

        let now = SystemTime::now();
        let mut out: Vec<Result<DirectoryEntryPlus, Errno>> = Vec::with_capacity(dir_entries.len());

        out.push(Ok(
            DirectoryEntryPlus { kind: FileType::Directory, name: ".".into(), offset: 1,
                attr: FileAttr { size: 1, blocks: 1, atime: now, mtime: now, ctime: now, kind: FileType::Directory, perm: 0o777, nlink: 2, uid: 0, gid: 0,
                    rdev: 0, blksize: 1 },
                entry_ttl: TTL, attr_ttl: TTL }
        ));
        out.push(Ok(
            DirectoryEntryPlus { kind: FileType::Directory, name: "..".into(), offset: 2,
                attr: FileAttr { size: 1, blocks: 1, atime: now, mtime: now, ctime: now, kind: FileType::Directory, perm: 0o777, nlink: 2, uid: 0, gid: 0,
                    rdev: 0, blksize: 1 },
                entry_ttl: TTL, attr_ttl: TTL }
        ));

        for (idx, entry) in dir_entries.iter().enumerate() {
            let mut file_path = path.clone();
            file_path.push(entry.name.clone());

            let size = self.get_size(&file_path).await.unwrap();
            let file_type = if entry.is_dir {FileType::Directory} else {FileType::RegularFile};

            let attr = FileAttr { size, blocks: 1, atime: now, mtime: now, ctime: now, kind: file_type, perm: 0o777,
                nlink: if file_type == FileType::Directory {2} else {1}, uid: 0, gid: 0, rdev: 0, blksize: size as u32 };

            // TODO: Try to get rid of this clone on the file name
            out.push(Ok(DirectoryEntryPlus { kind: file_type, name: entry.name.clone().into(), offset: (idx + 3) as i64, attr, entry_ttl: TTL, attr_ttl: TTL }));
        }

        let out: Vec<Result<DirectoryEntryPlus, Errno>> = out.into_iter().skip(offset as usize).collect();
        Ok(ReplyDirectoryPlus { entries: stream::iter(out) })
    }
    
}

impl NodeManager {
    async fn new(nodes_strings: Vec<String>) -> Result<NodeManager> {
        let mut nodes = Vec::with_capacity(nodes_strings.len());
        for node in nodes_strings {
            nodes.push(NodeConnectionManager::new(node).await?); // 100 max connections
        }

        Ok(NodeManager { nodes })
    }

    // TODO: optimize the arguments of this function
    async fn get_attributes(&self, parent: &Path, name: &OsStr, full_path: &Path) -> fuse3::Result<FileAttr> {
        tracing::info!("getattributes called on parent {:?} name {:?}", parent, name);
        
        // TODO: Real error types to see when a directory actually doesnt exist
        let dir_entries = match self.list_directory_entries(parent).await {
            Ok(de) => de,
            Err(e) => {
                tracing::error!("Error listing directory for path {:?}: {}", parent, e);
                return Err(fuse3::Errno::new_not_exist())
            }
        };


        let mut size = 1;
        let mut file_type = FileType::Directory;
        for entry in dir_entries {
            if *entry.name == *name {
                if !entry.is_dir {
                    size = self.get_size(&full_path).await.unwrap();
                    file_type = FileType::RegularFile;
                }

                // Blocksize is also fake, apparantly unix blocks are always 512
                let now = SystemTime::now();
                return Ok(
                    FileAttr { size, blocks: 1, atime: now, mtime: now, ctime: now, kind: file_type, perm: 0o777, nlink: if file_type == FileType::Directory {2} else {1}, uid: 0, gid: 0, rdev: 0, blksize: size as u32 } 
                );
            }
        }

        tracing::warn!("getattributes: could not find file");
        Err(fuse3::Errno::new_not_exist())
 
    }

    #[inline]
    pub fn inode_to_server(&self, inode: Inode) -> &NodeConnectionManager {
        let server_index = inode.0 as usize % self.nodes.len();
        self.nodes.get(server_index).unwrap()
    }

    pub async fn get_all_node_info(&self) -> Result<NodeInfo> {
        let mut futs = Vec::with_capacity(self.nodes.len());

        for node in self.nodes.iter() {
            futs.push(node.send_request(Op::GetNodeStats));
        }

        let results = join_all(futs).await;

        let mut total_size = 0;
        let mut free = 0;

        for res in results {
            let res = match res {
                Ok(r) => r,
                Err(e) => {
                    return Err(anyhow!(e));
                }
            };

            match res {
                OpResponse::NodeStats(s) => {
                    total_size += s.total_size;
                    free += s.free;
                }
                OpResponse::Error(e) => {
                    tracing::error!("Error from node reporting nodeinfo: {}", e);
                    return Err(anyhow!(e));
                }
                _ => unreachable!("node info")
            }
        }

        Ok(NodeInfo { total_size, free })

    }

    // Creates the special root directory
    pub async fn create_special(&self) -> Result<()> {
        let file_inode = path_to_inode(Path::new("/"));
        let file_node = self.inode_to_server(file_inode);

        file_node.send_request(Op::Create { inode: file_inode, is_dir: true }).await?;
        Ok(())
    }

    pub async fn create(&self, path: &Path, is_dir: bool) -> Result<()> {
        let file_inode = path_to_inode(path);
        let file_node = self.inode_to_server(file_inode);

        let parent_inode = path_to_inode(path.parent().unwrap());
        let parent_node = self.inode_to_server(parent_inode);

        let (file_res, parent_res) = tokio::join!(
            file_node.send_request(Op::Create { inode: file_inode, is_dir }),
            parent_node.send_request(
                Op::CreateEntry { inode: parent_inode, entry: Entry { name: path.file_name().unwrap().to_string_lossy().into_owned(), inode: file_inode, is_dir } }
            )
        );

        file_res?;
        parent_res?;
        Ok(())
    }

    // TODO: Recursively delete everything inside if directory = trues
    pub async fn delete(&self, path: &Path, is_dir: bool) -> Result<()> {
        let file_inode = path_to_inode(path);
        let file_node = self.inode_to_server(file_inode);

        let parent_inode = path_to_inode(path.parent().unwrap());
        let parent_node = self.inode_to_server(parent_inode);

        let (file_res, parent_res) = tokio::join!(
            file_node.send_request(Op::Delete { inode: file_inode, is_dir }),
            parent_node.send_request(Op::DeleteEntry { parent_inode, inode: file_inode })
        );

        file_res?;
        parent_res?;
        Ok(())
    }

    pub async fn write(&self, path: &Path, offset: u64, data: Vec<u8>) -> Result<()> {
        let inode = path_to_inode(path);
        let node = self.inode_to_server(inode);

        let res = node.send_request(Op::Write { inode, offset, data }).await?;

        match res {
            OpResponse::WriteOk => Ok(()),
            OpResponse::Error(e) => {
                Err(anyhow!(e))
            }
            _ => unreachable!()
        }
    }

    pub async fn read(&self, path: &Path, offset: u64, size: u64) -> Result<Vec<u8>> {
        let inode = path_to_inode(path);
        let node = self.inode_to_server(inode);

        let res = node.send_request(Op::Read { inode, offset, size }).await?;

        match res {
            OpResponse::ReadData(data) => Ok(data),
            OpResponse::Error(e) => {
                Err(anyhow!(e))
            }
            _ => unreachable!()
        }
    }

    pub async fn get_size(&self, path: &Path) -> Result<u64> {
        let inode = path_to_inode(path);
        let node = self.inode_to_server(inode);

        let res = node.send_request(Op::GetSize { inode }).await?;

        match res {
            OpResponse::SizeData(size) => Ok(size),
            OpResponse::Error(e) => {
                Err(anyhow!(e))
            }
            _ => unreachable!()
        }
    }

    pub async fn list_directory_entries(&self, path: &Path) -> Result<Vec<Entry>> {
        let inode = path_to_inode(path);
        let node = self.inode_to_server(inode);

        let res = node.send_request(Op::ListDirEntries { inode }).await?;

        match res {
            OpResponse::ListDirData(entries) => Ok(entries),
            OpResponse::Error(e) => {
                Err(anyhow!(e))
            }
            _ => unreachable!()
        }
    }
}

#[tokio::main]
async fn main() {
    let matches = clap::App::new("fdfs-client")
        .version("0.0.1")
        .author("TennisBowling <tennisbowling@tennisbowling.com>")
        .setting(clap::AppSettings::ColoredHelp)
        .about("The client for fdfs, the Fast Distributed File System written in Rust")
        .long_version("fdfs version {} by TennisBowling <tennisbowling@tennisbowling.com>")
        .arg(
            clap::Arg::with_name("nodes")
                .short("n")
                .long("nodes")
                .value_name("NODES")
                .help("Comma-separated list of storage node addresses to use")
                .takes_value(true)
                .required(true),
        )
        .arg(
            clap::Arg::with_name("log-level")
                .short("l")
                .long("log-level")
                .value_name("LOG")
                .help("Log level: debug, info, warn, crit")
                .takes_value(true)
                .default_value("debug"),
        )
        .get_matches();
    
    let log_level = matches.value_of("log-level").unwrap();
    let nodes = matches.value_of("nodes").unwrap();
    let nodes = nodes.split(',').collect::<Vec<&str>>().iter().map(|x| x.to_string()).collect();

    let filter_string = format!("{},hyper=info,fuse3=info,async_rdma=info", log_level);

    let filter = EnvFilter::try_new(filter_string).unwrap_or_else(|_| EnvFilter::new(log_level));

    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(filter)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");
    tracing::info!("Starting fdfs client");

    let manager = NodeManager::new(nodes)
        .await
        .unwrap();

    // Create special root directory
    manager.create_special().await.unwrap();


    // Take a look at write back cache
    let options = MountOptions::default().fs_name("fdfs").force_readdir_plus(true).custom_options("noatime").custom_options("nosuid").custom_options("nodev").custom_options("async").to_owned();
    let handle = Session::new(options).mount(manager, "/mnt/fdfs").await.unwrap();

    tokio::signal::ctrl_c().await.unwrap();
    handle.unmount().await.unwrap();

}
