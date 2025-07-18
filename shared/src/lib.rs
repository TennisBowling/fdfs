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
// Theres this problem that the file might not be on the same node as the directory therefore it cant append the entry to the entries in the directory (since its on a different node)
// So Op::Create is being changed to ONLY create the file (and Op::Delete deletes the file) and Op::CreateEntry and Op::DeleteEntry which will be sent to the node containing the dir
// We'll just hash the path of the dir and send the Opcode there to remove the file from the entries and then hash the file path and send delete Delete to there (which has the file) 
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

    CreateEntry {
        inode: Inode,   // The inode of the file's parent inode. Tempted to call parent_inode
        entry: Entry,
    },
    DeleteEntry {
        parent_inode: Inode,
        inode: Inode,
    },

    Create {
        inode: Inode,
        is_dir: bool,
    },
    Delete {
        inode: Inode,
        is_dir: bool,
    },
    GetSize {
        inode: Inode,
    },

    // Directory ops
    ListDirEntries {
        inode: Inode,
    },

    GetNodeStats,

    Other(String),
}

#[derive(Encode, Decode, Debug)]
pub struct NodeInfo {
    pub total_size: u64,
    pub free: u64,
}

#[derive(Encode, Decode, Debug)]
pub enum OpResponse {
    WriteOk,
    ReadData(Vec<u8>),
    CreateEntryOk,
    DeleteEntryOk,
    CreateOk,
    DeleteOk,
    SizeData(u64),
    ListDirData(Vec<Entry>),
    NodeStats(NodeInfo),
    Error(String),
}
