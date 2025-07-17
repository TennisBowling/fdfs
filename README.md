# fdfs
A fast distributed file system written in Rust


# Running
### Storage Nodes
`cargo run --package storage -- --port 10000 --address 0.0.0.0`

### Client
`cargo run --package client -- --nodes 127.0.0.1:10000,192.168.10.1:10000`
