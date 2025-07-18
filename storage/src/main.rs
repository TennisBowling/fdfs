use std::{error::Error, io::ErrorKind};
use shared::*;
use tracing_subscriber::EnvFilter;
use tokio::{fs::File, io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom}, net::{TcpListener, TcpStream}};


async fn create_empty_entries(mut file: File) {
    let entries: Vec<Entry> = Vec::new();
    file.write_all(&bitcode::encode(&entries)).await.unwrap();
}

async fn handle_createentry(device: &str, inode: Inode, entry: Entry) -> OpResponse {
    let mut file = match tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(format!("{}dino{}", device, inode.0))
        .await {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Error opening file to write to parent node {:?} inode {:?}: {}", inode, entry.inode, e);
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
    entries.push(entry);

    file.set_len(0).await.unwrap();
    file.seek(SeekFrom::Start(0)).await.unwrap();
    file.write_all(&bitcode::encode(&entries)).await.unwrap();

    OpResponse::CreateEntryOk
}

async fn handle_deleteentry(device: &str, parent_inode: Inode, inode: Inode) -> OpResponse {
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
    
    let mut idx: Option<usize> = None;
    for (index, entry) in entries.iter().enumerate() {
        if entry.inode.0 == inode.0 {
            idx = Some(index);
        }
    }

    if idx == None {
        return OpResponse::Error("Unable to find entry in directory".to_string());
    }
    
    entries.remove(idx.unwrap());    // Remove from the entries and then rewrite all the entries to the file

    file.set_len(0).await.unwrap();
    file.seek(SeekFrom::Start(0)).await.unwrap();
    file.write_all(&bitcode::encode(&entries)).await.unwrap();

    OpResponse::DeleteEntryOk
}

async fn handle_create(device: &str, inode: Inode, is_dir: bool) -> OpResponse {
    // Just to be careful, create_new will return an error if one already exists and create will truncate
    if is_dir {
        let file = match File::create_new(format!("{}dino{}", device, inode.0)).await {
            Ok(f) => f,
            Err(e) => {
                return OpResponse::Error(format!("{:?}", e));
            }
        };

        create_empty_entries(file).await;
    }
    else {
        match File::create_new(format!("{}ino{}", device, inode.0)).await {
            Ok(_) => {},
            Err(e) => {
                return OpResponse::Error(format!("{:?}", e));
            }
        };
    }

    OpResponse::CreateOk
}

async fn handle_write(device: &str, inode: Inode, offset: u64, data: Vec<u8>) -> OpResponse {
    let mut file = match tokio::fs::OpenOptions::new()
        .write(true)
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

    let file_size = match file.metadata().await {
        Ok(md) => md.len(),
        Err(e) => {
            return OpResponse::Error(format!("{}", e));
        }
    };

    if offset >= file_size {
        return OpResponse::ReadData(Vec::new());
    }

    let available_to_read = file_size - offset;
    let size = std::cmp::min(size, available_to_read);

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

async fn handle_delete(device: &str, inode: Inode, is_dir: bool) -> OpResponse {
    if is_dir {
        tokio::fs::remove_file(format!("{}dino{}", device, inode.0)).await.unwrap();
    }
    else {
        tokio::fs::remove_file(format!("{}ino{}", device, inode.0)).await.unwrap();
    }

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


        // Should probably trim this down with a macro
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
            Op::CreateEntry { inode, entry } => {
                tracing::debug!("CreateEntry op from client");
                let payload = handle_createentry(&device, inode, entry).await;

                send_response(&mut stream, payload).await?;
                tracing::debug!("Wrote CreateEntry response to client");
            },
            Op::DeleteEntry { inode, parent_inode } => {
                tracing::debug!("DeleteEntry op from client");
                let payload = handle_deleteentry(&device, parent_inode, inode).await;

                send_response(&mut stream, payload).await?;
                tracing::debug!("Wrote DeleteEntry response to client");
            },
            Op::Create { inode, is_dir } => {
                tracing::debug!("Create op from client");
                let payload = handle_create(&device, inode, is_dir).await;

                send_response(&mut stream, payload).await?;
                tracing::debug!("Wrote create response to client");
            },
            Op::Delete { inode, is_dir } => {
                tracing::debug!("Delete op from client");
                let payload = handle_delete(&device, inode, is_dir).await;

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
            Op::GetNodeStats => {
                tracing::debug!("GetNodeStats op from client");

                let stats = fs2::statvfs(&device)?;

                send_response(&mut stream, OpResponse::NodeStats(NodeInfo { total_size: stats.total_space(), free: stats.free_space() })).await?;
                tracing::debug!("Wrote getnodestats response to client");
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

    let matches = clap::App::new("fdfs-server")
        .version("0.0.1")
        .author("TennisBowling <tennisbowling@tennisbowling.com>")
        .setting(clap::AppSettings::ColoredHelp)
        .about("The storage server for fdfs, the Fast Distributed File System written in Rust")
        .long_version("fdfs version {} by TennisBowling <tennisbowling@tennisbowling.com>")
        .arg(
            clap::Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("Port to listen on")
                .takes_value(true)
                .default_value("10000"),
        )
        .arg(
            clap::Arg::with_name("listen-addr")
                .short("addr")
                .long("listen-addr")
                .help("Address to listen for requests on")
                .takes_value(true)
                .default_value("0.0.0.0"),
        )
        .arg(
            clap::Arg::with_name("storage-dir")
                .short("storage")
                .long("storage-dir")
                .help("Directory to store files in to ")
                .takes_value(true)
                .default_value("./"),
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
    
    let listen_addr = matches.value_of("listen-addr").unwrap();
    let port = matches.value_of("port").unwrap();
    let storage_dir = matches.value_of("storage-dir").unwrap();
    let log_level = matches.value_of("log-level").unwrap();


    let filter_string = format!("{},hyper=info", log_level);

    let filter = EnvFilter::try_new(filter_string).unwrap_or_else(|_| EnvFilter::new(log_level));

    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(filter)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Setting default subscriber failed");
    tracing::info!("Starting fdfs storage server");
    

    let listener = TcpListener::bind(format!("{}:{}", listen_addr, port)).await.unwrap();

    let device = storage_dir.to_string();

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