use gxhash::gxhash64;
use shared::*;
use std::{error::Error, ffi::OsStr, num::NonZeroU32, os::unix::ffi::OsStrExt, path::{Path, PathBuf}, sync::atomic::AtomicUsize, time::{Duration, SystemTime}};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};
use tracing_subscriber::EnvFilter;
use fuse3::{path::{self as fuse, reply::{FileAttr, ReplyAttr, ReplyDirectory, ReplyDirectoryPlus, ReplyEntry, ReplyInit}, PathFilesystem, Session}, raw::{reply::{ReplyOpen, ReplyStatFs, ReplyXAttr}, Request}, Errno, FileType};
use fuse3::MountOptions;
use futures_util::stream::{self, Empty, Iter};
use fuse3::path::reply::DirectoryEntry;
use fuse3::path::reply::DirectoryEntryPlus;
use std::vec::IntoIter;


const TTL: Duration = Duration::new(2, 0);

#[inline]
pub fn path_to_inode(path: &Path) -> Inode {
    Inode(gxhash64(path.as_os_str().as_bytes(), 17))
}

struct NodeConnectionManager {
    pub streams: Vec<Mutex<(TcpStream, Vec<u8>)>>,
    pub current: AtomicUsize,
    pub addr: String,
}

impl NodeConnectionManager {
    async fn new(
        addr: String,
        num_connections: u32,
    ) -> Result<NodeConnectionManager, Box<dyn Error>> {
        let mut streams = Vec::with_capacity(num_connections as usize);

        for _ in 0..num_connections {
            let stream = TcpStream::connect(addr.clone()).await?;
            stream.set_nodelay(true)?; // Disable nagle, do not buffer
            streams.push(Mutex::new((stream, Vec::with_capacity(1000000)))); // 1MB buffer
        }

        tracing::debug!("Setup connection maanger for node {}", addr);
        Ok(NodeConnectionManager {
            streams,
            current: AtomicUsize::new(0),
            addr,
        })
    }

    async fn send_request(&self, payload: Op) -> Result<OpResponse, Box<dyn Error>> {
        let stream_id = self
            .current
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % self.streams.len();
        let mut stream_buf = self.streams.get(stream_id).unwrap().lock().await;
        let (stream, buf) = &mut *stream_buf;

        let serialized = bitcode::encode(&payload);
        buf.clear(); // Reuse buffer
        buf.extend_from_slice(&(serialized.len() as u64).to_le_bytes()); // Write size and then the payload
        buf.extend_from_slice(&serialized);
        stream.write_all(&buf).await?;
        tracing::debug!("Wrote payload to {}", self.addr);

        let cap = stream.read_u64_le().await?;
        let mut buf = vec![0u8; cap as usize];
        stream.read_exact(&mut buf).await?;
        let deserialized: OpResponse = bitcode::decode(&buf)?;

        Ok(deserialized)
    }
}

fn normalise_root(path: &OsStr) -> &OsStr {
    if path.is_empty() || path == OsStr::new("/") {
        OsStr::new("/")      // always use canonical form
    } else {
        path
    }
}

struct NodeManager {
    pub nodes: Vec<NodeConnectionManager>,
}

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
        tracing::info!("parent: {:?}, name: {:?}", parent, name);

        let mut path = PathBuf::from(normalise_root(parent));
        path.push(name);

        let attr = self.get_attributes(&PathBuf::from(parent), name, &path).await?;
        
        Ok(ReplyEntry { ttl: TTL, attr  })
        
    }

    // TODO: look into using the file handle in order to avoid re-doing everything with the path
    async fn getattr(&self, _req: Request, path: Option<&OsStr>, _fh: Option<u64>, _flags: u32) -> fuse3::Result<ReplyAttr> {
        let raw = path.ok_or_else(fuse3::Errno::new_not_exist)?;
        let path = PathBuf::from(normalise_root(raw));
        tracing::info!("path is {:?}", path);


        // Special root directory
        if path == Path::new("/") {
            let now = SystemTime::now();
            tracing::info!("Getattr: returning special root directory");
            return Ok(ReplyAttr { ttl: TTL, attr: FileAttr { size: 1, blocks: (1 + 511) / 512, atime: now, mtime: now, ctime: now, crtime: now,
                kind: FileType::Directory, perm: 0o777, nlink: 2, uid: 0, gid: 9, rdev: 0, flags: 0, blksize: 4096 } })
        }


        let parent = path.parent().unwrap();
        let name = path.file_name().unwrap();
        

        let attr = self.get_attributes(&PathBuf::from(parent), name, &path).await?;
        
        Ok(ReplyAttr { ttl: TTL, attr  })
    }

    // All Fake Data
    async fn statfs(&self, _req: Request, _path: &OsStr) -> fuse3::Result<ReplyStatFs> {
        Ok(fuse3::path::reply::ReplyStatFs {
            blocks: 1000000,      // Total blocks
            bfree: 1000000,       // Free blocks  
            bavail: 1000000,      // Available blocks
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

    async fn access(&self, _req: Request, path: &OsStr, _mask: u32) -> fuse3::Result<()> {
        tracing::info!("Access called on path {:?}", path);
        Ok(())
    }

    type DirEntryStream<'a>
        = Iter<IntoIter<fuse3::Result<DirectoryEntry>>>
    where
        Self: 'a;

    async fn readdir<'a>(&'a self, _req: Request, path: &'a OsStr, _fh: u64, offset: i64) -> fuse3::Result<ReplyDirectory<Self::DirEntryStream<'a> > > {
        tracing::error!("READDIR called for path: {:?}", path);
        let path = PathBuf::from(normalise_root(path));
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
        tracing::error!("READDIRPLUS called for path: {:?}", parent);
        let path = PathBuf::from(normalise_root(parent));
        let dir_entries = self.list_directory_entries(&path).await.unwrap();

        let now = SystemTime::now();
        let mut out: Vec<Result<DirectoryEntryPlus, Errno>> = Vec::with_capacity(dir_entries.len());

        out.push(Ok(
            DirectoryEntryPlus { kind: FileType::Directory, name: ".".into(), offset: 1,
                attr: FileAttr { size: 1, blocks: 1, atime: now, mtime: now, ctime: now, crtime: now, kind: FileType::Directory, perm: 0o777, nlink: 2, uid: 0, gid: 0,
                    rdev: 0, flags: 0, blksize: 4096 },
                entry_ttl: TTL, attr_ttl: TTL }
        ));
        out.push(Ok(
            DirectoryEntryPlus { kind: FileType::Directory, name: "..".into(), offset: 2,
                attr: FileAttr { size: 1, blocks: 1, atime: now, mtime: now, ctime: now, crtime: now, kind: FileType::Directory, perm: 0o777, nlink: 2, uid: 0, gid: 0,
                    rdev: 0, flags: 0, blksize: 4096 },
                entry_ttl: TTL, attr_ttl: TTL }
        ));

        for (idx, entry) in dir_entries.iter().enumerate() {
            let mut file_path = path.clone();
            file_path.push(entry.name.clone());

            let size = self.get_size(&file_path).await.unwrap();
            let file_type = if entry.is_dir {FileType::Directory} else {FileType::RegularFile};

            let attr = FileAttr { size, blocks: (size + 511) / 512, atime: now, mtime: now, ctime: now, crtime: now, kind: file_type, perm: 0o777,
                nlink: if file_type == FileType::Directory {2} else {1}, uid: 0, gid: 9, rdev: 0, flags: 0, blksize: 4096 };

            // TODO: Try to get rid of this clone on the file name
            out.push(Ok(DirectoryEntryPlus { kind: file_type, name: entry.name.clone().into(), offset: (idx + 3) as i64, attr, entry_ttl: TTL, attr_ttl: TTL }));
        }

        let out: Vec<Result<DirectoryEntryPlus, Errno>> = out.into_iter().skip(offset as usize).collect();
        Ok(ReplyDirectoryPlus { entries: stream::iter(out) })
    }
    
}

impl NodeManager {
    async fn new(nodes_strings: Vec<String>) -> Result<NodeManager, Box<dyn Error>> {
        let mut nodes = Vec::with_capacity(nodes_strings.len());
        for node in nodes_strings {
            nodes.push(NodeConnectionManager::new(node, 5).await?); // 100 max connections
        }

        Ok(NodeManager { nodes })
    }

    // TODO: optimize the arguments of this function
    async fn get_attributes(&self, parent: &Path, name: &OsStr, full_path: &Path) -> fuse3::Result<FileAttr> {
        let mut size = 1;
        let mut file_type = FileType::Directory;

        // TODO: Real error types to see when a directory actually doesnt exist
        let dir_entries = match self.list_directory_entries(parent).await {
            Ok(de) => de,
            Err(e) => {
                tracing::error!("Error listing directory for path {:?}: {}", parent, e);
                return Err(fuse3::Errno::new_not_exist())
            }
        };


        for entry in dir_entries {
            if *entry.name == *name {
                if !entry.is_dir {
                    size = self.get_size(&full_path).await.unwrap();
                    file_type = FileType::RegularFile;
                }
            }
        }
 

        // Blocksize is also fake, apparantly unix blocks are always 512
        let now = SystemTime::now();
        Ok(
            FileAttr { size, blocks: (size + 511) / 512, atime: now, mtime: now, ctime: now, crtime: now, kind: file_type, perm: 0o777, nlink: if file_type == FileType::Directory {2} else {1}, uid: 0, gid: 9, rdev: 0, flags: 0, blksize: 4096 } 
        )
    }

    #[inline]
    pub fn inode_to_server(&self, inode: Inode) -> &NodeConnectionManager {
        let server_index = inode.0 as usize % self.nodes.len();
        self.nodes.get(server_index).unwrap()
    }

    // Creates the special root directory
    pub async fn create_special(&self) -> Result<(), Box<dyn Error>> {
        let file_inode = path_to_inode(Path::new("/"));
        let file_node = self.inode_to_server(file_inode);

        file_node.send_request(Op::Create { inode: file_inode, is_dir: true }).await?;
        Ok(())
    }

    pub async fn create(&self, path: &Path, is_dir: bool) -> Result<(), Box<dyn Error>> {
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
    pub async fn delete(&self, path: &Path, is_dir: bool) -> Result<(), Box<dyn Error>> {
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

    pub async fn write(&self, path: &Path, offset: u64, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let inode = path_to_inode(path);
        let node = self.inode_to_server(inode);

        let res = node.send_request(Op::Write { inode, offset, data }).await?;

        match res {
            OpResponse::WriteOk => Ok(()),
            OpResponse::Error(e) => {
                Err(e.into())
            }
            _ => unreachable!()
        }
    }

    pub async fn read(&self, path: &Path, offset: u64, size: u64) -> Result<Vec<u8>, Box<dyn Error>> {
        let inode = path_to_inode(path);
        let node = self.inode_to_server(inode);

        let res = node.send_request(Op::Read { inode, offset, size }).await?;

        match res {
            OpResponse::ReadData(data) => Ok(data),
            OpResponse::Error(e) => {
                Err(e.into())
            }
            _ => unreachable!()
        }
    }

    pub async fn get_size(&self, path: &Path) -> Result<u64, Box<dyn Error>> {
        let inode = path_to_inode(path);
        let node = self.inode_to_server(inode);

        let res = node.send_request(Op::GetSize { inode }).await?;

        match res {
            OpResponse::SizeData(size) => Ok(size),
            OpResponse::Error(e) => {
                Err(e.into())
            }
            _ => unreachable!()
        }
    }

    pub async fn list_directory_entries(&self, path: &Path) -> Result<Vec<Entry>, Box<dyn Error>> {
        let inode = path_to_inode(path);
        let node = self.inode_to_server(inode);

        let res = node.send_request(Op::ListDirEntries { inode }).await?;

        match res {
            OpResponse::ListDirData(entries) => Ok(entries),
            OpResponse::Error(e) => {
                Err(e.into())
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

    let filter_string = format!("{},hyper=info,fuse3=info", log_level);

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
    let handle = Session::new(options).mount(manager, "/tmp/fdfs").await.unwrap();

    tokio::signal::ctrl_c().await.unwrap();
    handle.unmount().await.unwrap();

}
