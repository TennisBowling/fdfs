use gxhash::gxhash64;
use shared::*;
use std::{error::Error, os::unix::ffi::OsStrExt, path::{self, Path}, sync::atomic::AtomicUsize};
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
    let log_level = "debug";
    let filter_string = format!("{},hyper=info", log_level);

    let filter = EnvFilter::try_new(filter_string).unwrap_or_else(|_| EnvFilter::new(log_level));

    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(filter)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");
    tracing::info!("Starting fdfs client");

    let manager = NodeManager::new(vec!["127.0.0.1:10000".to_string()])
        .await
        .unwrap();

    // Create special root directory
    manager.create_special().await.unwrap();
    
    /*
    We're going to
    - Create a directory
    - Create a file
    - Write to the file
    - List files inside the directory
    - Read the size of the file
    - Read the file
    - Delete the file
    - Delete the directory
    This covers all of the enumerations of Op */
    tracing::debug!("Creating directory");
    let resp = manager.create(Path::new("/directory"), true).await;
    tracing::info!("Response: {:?}", resp);

    tracing::debug!("Creating test file inside directory");
    let resp = manager.create(Path::new("/directory/testfile"), false).await;
    tracing::info!("Response: {:?}", resp);

    tracing::debug!("Writing to test file");
    let resp = manager.write(Path::new("/directory/testfile"), 0, "hi ab!".as_bytes().to_vec()).await;
    tracing::info!("Response: {:?}", resp);

    tracing::debug!("Listing files inside the directory");
    let resp = manager.list_directory_entries(Path::new("/directory")).await;
    tracing::info!("Response: {:?}", resp);

    tracing::debug!("Reading size of file");
    let resp = manager.get_size(Path::new("/directory/testfile")).await;
    let size = match resp {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Could not get size: {:?}", e);
            return;
        }
    };
    tracing::info!("Response: {:?}, size: {}", resp, size);

    tracing::debug!("Reading the file with the size obtained");
    let resp = manager.read(Path::new("/directory/testfile"), 0, size).await;
    let str = String::from_utf8(match resp {
        Ok(ref v) => v.to_vec(),
        _ => {
            tracing::error!("wrong thing read");
            return;
        }
    }).unwrap();
    tracing::info!("Response: {:?}, decoded: {}", resp, str);

    tracing::debug!("Deleting the file");
    let resp = manager.delete(Path::new("/directory/testfile"), false).await;
    tracing::info!("Response: {:?}", resp);

    tracing::debug!("Deleting the directory");
    let resp = manager.delete(Path::new("/directory"), true).await;
    tracing::info!("Response: {:?}", resp);

}
