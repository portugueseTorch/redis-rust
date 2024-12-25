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
pub type RESPResult = Result<Option<RESPValue>>;

#[derive(PartialEq, Clone, Debug)]
pub enum RESPValue {
    SimpleString(Bytes),
    BulkString(Bytes),
    Array(Vec<RESPValue>),
    Null,
    NullBulkString,
    SimpleError(Bytes),
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

    /// Reads the data from self.stream in self.buffer, zero-copy-parsing the data
    /// and returning a Result<Option<RESPValue>, RESPError>
    /// If reading or parsing fails, an error is returned
    /// If 0 bytes are read from the stream, Ok(None) is returned
    pub async fn parse_request(&mut self) -> RESPResult {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }

        match tokenize(&self.buffer, 0)? {
            Some(tok) => {
                let req_data = self.buffer.split_to(tok.1);
                let parsed_req = RESPValue::from_token(tok.0, &req_data.freeze());

                // check request was an array of bulk strings
                match &parsed_req {
                    RESPValue::Array(arr) => {
                        for item in arr.iter() {
                            ensure!(
                                matches!(item, RESPValue::BulkString(_)),
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

    pub async fn write(&mut self, response: RESPValue) -> Result<()> {
        let serialized_data = response.serialize()?;

        self.stream.write(serialized_data.as_bytes()).await.unwrap();
        Ok(())
    }
}

impl RESPValue {
    fn from_token(tok: RESPRaw, buf: &Bytes) -> RESPValue {
        match tok {
            RESPRaw::SimpleString(str) => RESPValue::SimpleString(str.as_bytes(&buf)),
            RESPRaw::BulkString(bulk_str) => RESPValue::BulkString(bulk_str.as_bytes(&buf)),
            RESPRaw::NullBulkString(_) => RESPValue::NullBulkString,
            RESPRaw::Array(arr) => RESPValue::Array(
                arr.into_iter()
                    .map(|m| RESPValue::from_token(m, buf))
                    .collect(),
            ),
        }
    }
}
