use anyhow::{bail, ensure, Result};
use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::server::serde::tokenize;

use super::serde::{RESPRaw, RESPToken};

pub struct RedisConnectionHandler {
    stream: TcpStream,
    buffer: BytesMut,
}

/// Fundamental type returned by the parser, ready to be consumed by the executor
pub type RESPResult = Result<Option<RedisValue>>;

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub enum RedisValue {
    SimpleString(Bytes),
    BulkString(Bytes),
    Array(Vec<RedisValue>),
    NullBulkString,
    SimpleError(Bytes),
}

impl RedisValue {
    fn from_token(tok: RESPRaw, buf: &Bytes) -> RedisValue {
        match tok {
            RESPRaw::SimpleString(str) => RedisValue::SimpleString(str.as_bytes(&buf)),
            RESPRaw::BulkString(bulk_str) => RedisValue::BulkString(bulk_str.as_bytes(&buf)),
            RESPRaw::NullBulkString(_) => RedisValue::NullBulkString,
            RESPRaw::Array(arr) => RedisValue::Array(
                arr.into_iter()
                    .map(|m| RedisValue::from_token(m, buf))
                    .collect(),
            ),
        }
    }
}

impl RedisConnectionHandler {
    pub fn new(stream: TcpStream) -> Self {
        log::info!(
            "New handler spawned for {}",
            stream.peer_addr().unwrap().ip()
        );

        Self {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }

    pub async fn parse_request(&mut self) -> RESPResult {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }

        match tokenize(&self.buffer, 0)? {
            Some(tok) => {
                let req_data = self.buffer.split_to(tok.1);
                let parsed_req = RedisValue::from_token(tok.0, &req_data.freeze());

                // check request was an array of bulk strings
                match &parsed_req {
                    RedisValue::Array(arr) => {
                        for item in arr.iter() {
                            ensure!(
                                matches!(item, RedisValue::BulkString(_)),
                                "Request should be an array of bulk strings"
                            )
                        }
                        Ok(Some(parsed_req))
                    }
                    _ => bail!("Request should be an array of bulk strings"),
                }
            }
            None => Ok(None),
        }
    }

    pub async fn write(&mut self, response: RedisValue) -> Result<()> {
        let serialized_data = response.serialize()?;

        self.stream.write(serialized_data.as_bytes()).await.unwrap();
        Ok(())
    }
}
