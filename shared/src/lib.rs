use std::error::Error;

use bitcode::{Encode, Decode};
use gxhash::{gxhash64};



#[derive(Encode, Decode, Debug)]
pub struct Inode(pub u64);


#[derive(Encode, Decode, Debug)]
pub enum Op {
    Write { inode: Inode, offset: u64, data: Vec<u8> },
    Read { inode: Inode, offset: u64, size: u64 },
    Other(String)
}


#[derive(Encode, Decode, Debug)]
pub enum OpResponse {
    WriteOk,
    ReadData(Vec<u8>),
    Error(String),
}