use std::{error::Error, sync::atomic::AtomicUsize, time::Instant};
use gxhash::{gxhash64};
use shared::*;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, stream, sync::Mutex};
use tracing_subscriber::EnvFilter;

pub fn path_to_inode(str: &str) -> Inode {
    Inode(gxhash64(str.as_bytes(), 17))
}

struct NodeConnectionManager {
    pub streams: Vec<Mutex<(TcpStream, Vec<u8>)>>,
    pub current: AtomicUsize,
    pub addr: String,
}

impl NodeConnectionManager {
    async fn new(addr: String, num_connections: u32) -> Result<NodeConnectionManager, Box<dyn Error>> {
        let mut streams = Vec::with_capacity(num_connections as usize);

        for _ in 0..num_connections {
            let stream = TcpStream::connect(addr.clone()).await?;
            stream.set_nodelay(true)?;   // Disable nagle, do not buffer
            streams.push(Mutex::new((stream, Vec::with_capacity(1000000))));     // 1MB buffer

        }

        tracing::debug!("Setup connection maanger for node {}", addr);
        Ok(NodeConnectionManager { streams, current: AtomicUsize::new(0), addr })
    }

    async fn send_request(&self, payload: Op) -> Result<OpResponse, Box<dyn Error>> {
        let stream_id = self.current.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.streams.len();
        let mut stream_buf = self.streams.get(stream_id).unwrap().lock().await;
        let (stream, buf) = &mut *stream_buf;

        let serialized = bitcode::encode(&payload);
        buf.clear();       // Reuse buffer
        buf.extend_from_slice(&(serialized.len() as u64).to_le_bytes());    // Write size and then the payload
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
            nodes.push(NodeConnectionManager::new(node, 1).await?);  // 100 max connections
        }

        Ok(NodeManager { nodes })
    }
    
    pub fn inode_to_server(&self, inode: Inode) -> &NodeConnectionManager {
        let server_index = inode.0 as usize % self.nodes.len();
        self.nodes.get(server_index).unwrap()
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

    let manager = NodeManager::new(vec!["127.0.0.1:10000".to_string()]).await.unwrap();
    
    let inode = path_to_inode("/test/test.txt");
    let server = manager.inode_to_server(inode);
    let resp = server.send_request(Op::Other("hey client!!!!!!!!!".to_string())).await.unwrap();
    tracing::info!("response from storage server: {:?}", resp);

    let start = Instant::now();
    let resp = server.send_request(Op::Other("timing check".to_string())).await.unwrap();
    let end = start.elapsed().as_secs_f32();
    tracing::info!("response from storage server again: {:?}, in {}s", resp, end);
}