use anyhow::{ensure, Result};
use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::server::handler::{RedisConnectionHandler, RedisValue};

pub struct RedisReplicaContext {
    /// master replication ID
    pub replication_id: String,
    /// offset into the circluar replication buffer
    pub replication_offset: usize,
}
impl RedisReplicaContext {
    pub async fn connect<Addr: ToSocketAddrs>(
        server_port: usize,
        master_addr: Addr,
    ) -> Result<Self> {
        let stream = TcpStream::connect(master_addr).await?;
        let mut handler = RedisConnectionHandler::new(stream);

        // --- handshake 1, replica pings master
        let ping_req = RedisValue::Array(vec![RedisValue::BulkString(Bytes::from_static(b"PING"))]);
        handler.write(ping_req).await?;
        let ping_res = handler.read_and_parse().await?;
        log::info!("PING response: {:?}", ping_res);

        // --- handshake 2, replica sends 2 REPLCONF
        let replconf_req = RedisValue::Array(vec![
            RedisValue::BulkString(Bytes::from_static(b"REPLCONF")),
            RedisValue::BulkString(Bytes::from_static(b"listening-port")),
            RedisValue::BulkString(Bytes::from(format!("{}", server_port))),
        ]);
        handler.write(replconf_req).await?;
        let replconf_res = handler.read_and_parse().await?;
        log::info!("REPLCONF response: {:?}", replconf_res);
        ensure!(
            replconf_res == Some(RedisValue::SimpleString(Bytes::from_static(b"OK"))),
            "REPLCONF handshakes expects 'OK' from master"
        );

        let replconf_req = RedisValue::Array(vec![
            RedisValue::BulkString(Bytes::from_static(b"REPLCONF")),
            RedisValue::BulkString(Bytes::from_static(b"capa")),
            RedisValue::BulkString(Bytes::from_static(b"psync2")),
        ]);
        handler.write(replconf_req).await?;
        let replconf_res_2 = handler.read_and_parse().await?;
        log::info!("REPLCONF2 response: {:?}", replconf_res_2);
        ensure!(
            replconf_res_2 == Some(RedisValue::SimpleString(Bytes::from_static(b"OK"))),
            "REPLCONF handshakes expects 'OK' from master"
        );

        Ok(Self {
            replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            replication_offset: 0,
        })
    }
}
