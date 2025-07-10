use std::{error::Error, io::ErrorKind};
use shared::*;
use tracing_subscriber::EnvFilter;
use tokio::{fs::File, io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom}, net::{TcpListener, TcpStream}};

async fn handle_create(device: &str, inode: Inode, parent_inode: Inode, name: String, is_dir: bool) -> OpResponse {
    let mut file = match tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(format!("{}dino{}", device, parent_inode.0))
        .await {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Error opening file to write to parent node {:?} inode {:?}: {}", parent_inode, inode, e);
                return OpResponse::Error(format!("{}", e));
            }
        };


    let file_len = file.metadata().await.unwrap().len() as usize;
    let mut buf = vec![0u8; file_len];
    file.read_exact(&mut buf).await.unwrap();
    let mut entries: Vec<Entry> = bitcode::decode(&buf).unwrap();
    
    for entry in entries.iter() {
        if entry.inode.0 == inode.0 {
            return OpResponse::Error("File already exists".to_string());
        }
    }

    // Add the new file to the list of files that the directory contains
    entries.push(Entry { name, inode, is_dir });

    file.set_len(0).await.unwrap();
    file.seek(SeekFrom::Start(0)).await.unwrap();
    file.write_all(&bitcode::encode(&entries)).await.unwrap();

    if is_dir {
        // Just to be careful, create_new will return an error if one already exists and create will truncate
        File::create_new(format!("{}dino{}", device, inode.0)).await.unwrap();
    }
    else {
        File::create_new(format!("{}ino{}", device, inode.0)).await.unwrap();
    }

    OpResponse::CreateOk
}

async fn handle_write(device: &str, inode: Inode, offset: u64, data: Vec<u8>) -> OpResponse {
    let mut file = match tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("{}ino{}", device, inode.0)).await {   // /mnt/ssd/ino100
            Ok(f) => f,
            Err(e) => {
                return OpResponse::Error(format!("Write error: {}", e));
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

async fn handle_delete(device: &str, inode: Inode, parent_inode: Inode, is_dir: bool) -> OpResponse {
    if is_dir {
        tokio::fs::remove_file(format!("{}dino{}", device, inode.0)).await.unwrap();
    }
    else {
        tokio::fs::remove_file(format!("{}ino{}", device, inode.0)).await.unwrap();
    }


    let mut file = match tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(format!("{}dino{}", device, parent_inode.0))
        .await {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Error opening file to write to parent node {:?} the deletion of inode {:?}: {}", parent_inode, inode, e);
                return OpResponse::Error(format!("{}", e));
            }
        };


    let file_len = file.metadata().await.unwrap().len() as usize;
    let mut buf = vec![0u8; file_len];
    file.read_exact(&mut buf).await.unwrap();
    let mut entries: Vec<Entry> = bitcode::decode(&buf).unwrap();
    
    let mut idx = 0;
    for (index, entry) in entries.iter().enumerate() {
        if entry.inode.0 == inode.0 {
            idx = index;
        }
    }

    if idx == 0 {
        return OpResponse::Error("Unable to find entry in directory".to_string());
    }
    
    entries.remove(idx);    // Remove from the entries and then rewrite all the entries to the file

    file.set_len(0).await.unwrap();
    file.seek(SeekFrom::Start(0)).await.unwrap();
    file.write_all(&bitcode::encode(&entries)).await.unwrap();

    OpResponse::DeleteOk
}

async fn handle_getsize(device: &str, inode: Inode) -> OpResponse {
    let file = match tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(format!("{}ino{}", device, inode.0)).await {
            Ok(f) => f,
            Err(e) => {
                return OpResponse::Error(format!("Error opening file to get size: {}", e));
            }
        };


    OpResponse::SizeData(file.metadata().await.unwrap().len())
}

async fn handle_listdirentries(device: &str, inode: Inode) -> OpResponse {
    let mut file = match tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(format!("{}dino{}", device, inode.0))
        .await {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Error opening dir file to get entries {:?}: {}", inode, e);
                return OpResponse::Error(format!("{}", e));
            }
        };

    let file_len = file.metadata().await.unwrap().len() as usize;
    let mut buf = vec![0u8; file_len];
    file.read_exact(&mut buf).await.unwrap();
    let entries: Vec<Entry> = bitcode::decode(&buf).unwrap();

    OpResponse::ListDirData(entries)
}

#[inline]
async fn send_response(stream: &mut TcpStream, payload: OpResponse) -> Result<(), Box<dyn Error>> {
    let serialized = bitcode::encode(&payload);
    let mut buf = Vec::with_capacity(8 + serialized.len());     // Write size and then the payload
    buf.extend_from_slice(&(serialized.len() as u64).to_le_bytes());
    buf.extend_from_slice(&serialized);
    stream.write_all(&buf).await?;

    Ok(())
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
                
                send_response(&mut stream, payload).await?;
                tracing::debug!("Wrote write response to client");
            },
            Op::Read { inode, offset, size } => {
                tracing::debug!("Read op from client");
                let payload = handle_read(&device, inode, offset, size).await;

                send_response(&mut stream, payload).await?;
                tracing::debug!("Wrote read response to client");
            },
            Op::Create { inode, parent_inode, name, is_dir } => {
                tracing::debug!("Create op from client");
                let payload = handle_create(&device, inode, parent_inode, name, is_dir).await;

                send_response(&mut stream, payload).await?;
                tracing::debug!("Wrote create response to client");
            },
            Op::Delete { inode, parent_inode, is_dir } => {
                tracing::debug!("Delete op from client");
                let payload = handle_delete(&device, inode, parent_inode, is_dir).await;

                send_response(&mut stream, payload).await?;
                tracing::debug!("Wrote delete response to client");
            }
            Op::GetSize { inode } => {
                tracing::debug!("GetSize op from client");
                let payload = handle_getsize(&device, inode).await;

                send_response(&mut stream, payload).await?;
                tracing::debug!("Wrote getsize response to client");
            }
            Op::ListDirEntries { inode } => {
                tracing::debug!("ListDirEntries op from client");
                let payload = handle_listdirentries(&device, inode).await;

                send_response(&mut stream, payload).await?;
                tracing::debug!("Wrote listdirentries response to client");
            }
            Op::Other(str) => {
                tracing::info!("Other message from client: {}", str);
                let payload = OpResponse::Error("hey man - storage server".to_string());

                send_response(&mut stream, payload).await?;
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