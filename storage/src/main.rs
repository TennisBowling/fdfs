use std::{error::Error, io::ErrorKind};
use shared::*;
use tracing_subscriber::EnvFilter;
use tokio::{io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom}, net::{TcpListener, TcpStream}};


async fn handle_write(device: &str, inode: Inode, offset: u64, data: Vec<u8>) -> OpResponse {
    let mut file = match tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("{}ino{}", device, inode.0)).await {   // /mnt/ssd/ino100
            Ok(f) => f,
            Err(e) => {
                return OpResponse::Error(format!("{}", e));
            }
        };

    match file.seek(SeekFrom::Start(offset)).await {
        Ok(_) => {},
        Err(e) => {
            return OpResponse::Error(format!("{}", e));
        }
    }

    match file.write_all(&data).await {
        Ok(_) => {},
        Err(e) => {
            return OpResponse::Error(format!("{}", e));
        }
    }

    OpResponse::WriteOk
}

async fn handle_read(device: &str, inode: Inode, offset: u64, size: u64) -> OpResponse {
    let mut file = match tokio::fs::OpenOptions::new()
        .read(true)
        .open(format!("{}ino{}", device, inode.0)).await {   // /mnt/ssd/ino100
            Ok(f) => f,
            Err(e) => {
                return OpResponse::Error(format!("{}", e));
            }
        };

    match file.seek(SeekFrom::Start(offset)).await {
        Ok(_) => {},
        Err(e) => {
            return OpResponse::Error(format!("{}", e));
        }
    }

    let mut buf = vec![0u8; size as usize];
    match file.read_exact(&mut buf).await {
        Ok(_) => {},
        Err(e) => {
            return OpResponse::Error(format!("{}", e));
        }
    }

    OpResponse::ReadData(buf)
}

async fn handle_stream(device: String, mut stream: TcpStream) -> Result<(), Box<dyn Error>> {
    tracing::debug!("Client connected");

    loop {
        let cap = stream.read_u64_le().await?;
        let mut buf = vec![0u8; cap as usize];
        stream.read_exact(&mut buf).await?;
        let op: Op = bitcode::decode(&mut buf)?;

        tracing::debug!("Decoded op from client");

        match op {
            Op::Write { inode, offset, data } => {
                tracing::debug!("Write op from client");
                let payload = handle_write(&device, inode, offset, data).await;


                let serialized = bitcode::encode(&payload);
                let mut buf = Vec::with_capacity(8 + serialized.len());     // Write size and then the payload
                buf.extend_from_slice(&(serialized.len() as u64).to_le_bytes());
                buf.extend_from_slice(&serialized);
                stream.write_all(&buf).await?;
                tracing::debug!("Wrote write response to client");
            },
            Op::Read { inode, offset, size } => {
                tracing::debug!("Read op from client");
                let payload = handle_read(&device, inode, offset, size).await;

                let serialized = bitcode::encode(&payload);
                let mut buf = Vec::with_capacity(8 + serialized.len());     // Write size and then the payload
                buf.extend_from_slice(&(serialized.len() as u64).to_le_bytes());
                buf.extend_from_slice(&serialized);
                stream.write_all(&buf).await?;
                tracing::debug!("Wrote read response to client");
            },
            Op::Other(str) => {
                tracing::info!("Other message from client: {}", str);
                let payload = OpResponse::Error("hey man - storage server".to_string());
                let serialized = bitcode::encode(&payload);
                let mut buf = Vec::with_capacity(8 + serialized.len());     // Write size and then the payload
                buf.extend_from_slice(&(serialized.len() as u64).to_le_bytes());
                buf.extend_from_slice(&serialized);
                stream.write_all(&buf).await?;
                tracing::debug!("Wrote payload response to client");
            },
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
    tracing::info!("Starting fdfs storage server");

    let listener = TcpListener::bind("0.0.0.0:10000").await.unwrap();

    let device = "./".to_string();

    loop {
        let (stream, _) = listener.accept().await.unwrap();

        let device_clone = device.clone();
        tokio::spawn(async move {

            match handle_stream(device_clone, stream).await {
                Ok(()) => {},
                Err(e) => {
                    if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                        if io_err.kind() == ErrorKind::UnexpectedEof {
                            tracing::warn!("Client disconnected unexpectedly");
                            return;
                        }
                    }

                    tracing::error!("Error handling client: {}", e);
                }
            }
        });
    }

}