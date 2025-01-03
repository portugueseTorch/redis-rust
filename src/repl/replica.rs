use anyhow::{ensure, Result};
use bytes::Bytes;
use rand::{thread_rng, Rng};
use tokio::net::TcpStream;

use crate::server::handler::{RedisConnectionHandler, RedisValue};

#[derive(Clone, Debug)]
pub struct RedisReplicaContext {
    /// master replication ID
    pub master_replid: String,
    /// offset into the circluar backlog buffer
    pub master_repl_offset: usize,
    /// offset of the replica into circular backlog buffer
    pub slave_repl_offset: usize,
    /// backup repl ID
    pub master_replid2: Option<String>,
    /// backup repl offset
    pub second_repl_offset: Option<usize>,
}
impl RedisReplicaContext {
    pub async fn connect(server_port: usize, master_addr: String) -> Result<Self> {
        let master_addr = master_addr.replace(" ", ":");
        let stream = TcpStream::connect(master_addr).await?;
        let mut handler = RedisConnectionHandler::new(stream);

        // --- handshake 1, replica pings master
        let ping_req = RedisValue::Array(vec![RedisValue::BulkString(Bytes::from_static(b"PING"))]);
        handler.write(ping_req).await?;
        let ping_res = handler.read_and_parse().await?;
        ensure!(
            ping_res == Some(RedisValue::SimpleString(Bytes::from_static(b"PONG"))),
            "PING handshake expects 'PONG' from master"
        );

        // --- handshake 2, replica sends 2 REPLCONF
        let replconf_req = RedisValue::Array(vec![
            RedisValue::BulkString(Bytes::from_static(b"REPLCONF")),
            RedisValue::BulkString(Bytes::from_static(b"listening-port")),
            RedisValue::BulkString(Bytes::from(format!("{}", server_port))),
        ]);
        handler.write(replconf_req).await?;
        let replconf_res = handler.read_and_parse().await?;
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
        ensure!(
            replconf_res_2 == Some(RedisValue::SimpleString(Bytes::from_static(b"OK"))),
            "REPLCONF handshakes expects 'OK' from master"
        );

        // --- handshake 3, replica sends PSYNC
        let psync_req = RedisValue::Array(vec![
            RedisValue::BulkString(Bytes::from_static(b"PSYNC")),
            RedisValue::BulkString(Bytes::from_static(b"?")),
            RedisValue::BulkString(Bytes::from_static(b"-1")),
        ]);
        handler.write(psync_req).await?;
        handler
            .read_and_parse()
            .await
            .expect("Failure reading result from initial PSYNC");
        let file_data = handler
            .read_rdb_file()
            .await
            .expect("Failure reading RDB file");
        log::info!("File data: {:?}", file_data);

        Ok(Self {
            master_replid: gen_uuid(),
            master_repl_offset: 0,
            slave_repl_offset: 0,
            master_replid2: None,
            second_repl_offset: None,
        })
    }
}

pub fn gen_uuid() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = thread_rng();

    (0..40)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}
