use gxhash::gxhash64;
use shared::*;
use std::{error::Error, os::unix::ffi::OsStrExt, path::Path, sync::atomic::AtomicUsize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};
use tracing_subscriber::EnvFilter;


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

struct NodeManager {
    pub nodes: Vec<NodeConnectionManager>,
}

impl NodeManager {
    async fn new(nodes_strings: Vec<String>) -> Result<NodeManager, Box<dyn Error>> {
        let mut nodes = Vec::with_capacity(nodes_strings.len());
        for node in nodes_strings {
            nodes.push(NodeConnectionManager::new(node, 5).await?); // 100 max connections
        }

        Ok(NodeManager { nodes })
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

    let filter_string = format!("{},hyper=info", log_level);

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
    
    /*
    Comprehensive filesystem manager test:
    - Create nested directory structure
    - Create multiple files with different content
    - Write to all files
    - Read back and verify content
    - Clean up everything
    This covers all of the enumerations of Op with extensive testing */

    // Test data for our files
    let test_files = vec![
        ("/root_dir/file1.txt", "Hello World!"),
        ("/root_dir/file2.txt", "This is test file number 2 with more content."),
        ("/root_dir/subdir1/nested_file1.txt", "Nested file content here."),
        ("/root_dir/subdir1/nested_file2.txt", "Another nested file with different data."),
        ("/root_dir/subdir1/deep/deeper/deepest_file.txt", "Very deep nested file content!"),
        ("/root_dir/subdir2/config.txt", "configuration=test\nvalue=42\nstatus=active"),
        ("/root_dir/subdir2/data.txt", "1,2,3,4,5\na,b,c,d,e\ntest,data,here"),
        ("/root_dir/subdir2/logs/app.log", "2025-01-01 12:00:00 INFO: Application started"),
        ("/root_dir/subdir2/logs/error.log", "2025-01-01 12:01:00 ERROR: Something went wrong"),
        ("/root_dir/temp/temp_file.tmp", "Temporary file content for testing"),
    ];

    // Directories to create (in order)
    let directories = vec![
        "/root_dir",
        "/root_dir/subdir1", 
        "/root_dir/subdir1/deep",
        "/root_dir/subdir1/deep/deeper",
        "/root_dir/subdir2",
        "/root_dir/subdir2/logs",
        "/root_dir/temp",
    ];

    // Create all directories
    tracing::info!("=== Creating directory structure ===");
    for dir_path in &directories {
        tracing::debug!("Creating directory: {}", dir_path);
        let resp = manager.create(Path::new(dir_path), true).await;
        match resp {
            Ok(_) => tracing::info!("Successfully created directory: {}", dir_path),
            Err(e) => {
                tracing::error!("Failed to create directory {}: {:?}", dir_path, e);
                return;
            }
        }
    }

    // Create and write to all files
    tracing::info!("=== Creating and writing to files ===");
    for (file_path, content) in &test_files {
        tracing::debug!("Creating file: {}", file_path);
        let resp = manager.create(Path::new(file_path), false).await;
        match resp {
            Ok(_) => tracing::info!("Successfully created file: {}", file_path),
            Err(e) => {
                tracing::error!("Failed to create file {}: {:?}", file_path, e);
                return;
            }
        }

        tracing::debug!("Writing to file: {}", file_path);
        let resp = manager.write(Path::new(file_path), 0, content.as_bytes().to_vec()).await;
        match resp {
            Ok(_) => tracing::info!("Successfully wrote {} bytes to {}", content.len(), file_path),
            Err(e) => {
                tracing::error!("Failed to write to file {}: {:?}", file_path, e);
                return;
            }
        }
    }

    // List contents of each directory
    tracing::info!("=== Listing directory contents ===");
    for dir_path in &directories {
        tracing::debug!("Listing contents of: {}", dir_path);
        let resp = manager.list_directory_entries(Path::new(dir_path)).await;
        match resp {
            Ok(entries) => {
                tracing::info!("Directory {} contains {} entries: {:?}", dir_path, entries.len(), entries);
            },
            Err(e) => {
                tracing::error!("Failed to list directory {}: {:?}", dir_path, e);
                return;
            }
        }
    }

    // Read back all files and verify content
    tracing::info!("=== Reading and verifying file contents ===");
    let mut total_bytes_read = 0;
    let mut total_bytes_expected = 0;
    
    for (file_path, expected_content) in &test_files {
        tracing::debug!("Getting size of file: {}", file_path);
        let size = match manager.get_size(Path::new(file_path)).await {
            Ok(s) => {
                tracing::info!("File {} has size: {}", file_path, s);
                s
            },
            Err(e) => {
                tracing::error!("Could not get size of file {}: {:?}", file_path, e);
                return;
            }
        };

        tracing::debug!("Reading file: {}", file_path);
        let resp = manager.read(Path::new(file_path), 0, size).await;
        let actual_content = match resp {
            Ok(data) => {
                match String::from_utf8(data) {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!("Failed to decode UTF-8 content from {}: {:?}", file_path, e);
                        return;
                    }
                }
            },
            Err(e) => {
                tracing::error!("Failed to read file {}: {:?}", file_path, e);
                return;
            }
        };

        // Verify content matches
        if actual_content == *expected_content {
            tracing::info!("✓ File {} content verified ({} bytes)", file_path, size);
            total_bytes_read += size;
            total_bytes_expected += expected_content.len();
        } else {
            tracing::error!("✗ File {} content mismatch! Expected: '{}', Got: '{}'", 
                           file_path, expected_content, actual_content);
            return;
        }
    }

    tracing::info!("=== Verification Summary ===");
    tracing::info!("Total files processed: {}", test_files.len());
    tracing::info!("Total bytes read: {}", total_bytes_read);
    tracing::info!("Total bytes expected: {}", total_bytes_expected);
    
    if total_bytes_read == total_bytes_expected as u64 {
        tracing::info!("✓ All file content verification passed!");
    } else {
        tracing::error!("✗ Byte count mismatch: read {} but expected {}", total_bytes_read, total_bytes_expected);
        return;
    }

    // Clean up: Delete all files first
    tracing::info!("=== Cleaning up files ===");
    for (file_path, _) in &test_files {
        tracing::debug!("Deleting file: {}", file_path);
        let resp = manager.delete(Path::new(file_path), false).await;
        match resp {
            Ok(_) => tracing::info!("Successfully deleted file: {}", file_path),
            Err(e) => {
                tracing::error!("Failed to delete file {}: {:?}", file_path, e);
                return;
            }
        }
    }

    // Clean up: Delete directories in reverse order (deepest first)
    tracing::info!("=== Cleaning up directories ===");
    let mut dirs_reversed = directories.clone();
    dirs_reversed.reverse();
    
    for dir_path in &dirs_reversed {
        tracing::debug!("Deleting directory: {}", dir_path);
        let resp = manager.delete(Path::new(dir_path), true).await;
        match resp {
            Ok(_) => tracing::info!("Successfully deleted directory: {}", dir_path),
            Err(e) => {
                tracing::error!("Failed to delete directory {}: {:?}", dir_path, e);
                return;
            }
        }
    }

    tracing::info!("🎉 All filesystem operations completed successfully!");

}
