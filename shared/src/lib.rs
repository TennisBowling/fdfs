use bitcode::{Decode, Encode};

#[derive(Encode, Decode, Debug, Copy, Clone)]
pub struct Inode(pub u64);

#[derive(Encode, Decode, Debug)]
pub struct Entry {
    pub name: String,
    pub inode: Inode,
    pub is_dir: bool,
}

// Some thoughts: we store just the file names in the directory but theres no way to go from inode back to filename. should be ok?
#[derive(Encode, Decode, Debug)]
pub enum Op {
    Write {
        inode: Inode,
        offset: u64,
        data: Vec<u8>,
    },
    Read {
        inode: Inode,
        offset: u64,
        size: u64,
    },
    Create {
        inode: Inode,
        parent_inode: Inode,
        name: String,
        is_dir: bool,
    },
    Delete {
        inode: Inode,
        parent_inode: Inode,
        is_dir: bool,
    },
    GetSize {
        inode: Inode,
    },

    // Directory ops
    ListDirEntries {
        inode: Inode,
    },

    Other(String),
}

#[derive(Encode, Decode, Debug)]
pub enum OpResponse {
    WriteOk,
    ReadData(Vec<u8>),
    CreateOk,
    DeleteOk,
    SizeData(u64),
    ListDirData(Vec<Entry>),
    Error(String),
}
