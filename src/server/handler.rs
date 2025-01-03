use core::str;

use anyhow::{bail, ensure, Result};
use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::server::serde::{get_next_word, tokenize};

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
        Self {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }

    fn _parse(&mut self, token: Option<RESPToken>) -> RESPResult {
        token.map_or(Ok(None), |tok| {
            let req_data = self.buffer.split_to(tok.1);
            Ok(Some(RedisValue::from_token(tok.0, &req_data.freeze())))
        })
    }

    pub async fn read_rdb_file(&mut self) -> Result<Vec<u8>> {
        // --- read stream data into the buffer
        let bytes_read = self
            .stream
            .read_buf(&mut self.buffer)
            .await
            .expect("Failure reading from stream");
        if bytes_read == 0 {
            return Ok(vec![]);
        }

        // --- ensure correct format
        ensure!(self.buffer[0] == b'$', "Invalid format for FULLSYNC data");

        // --- parse file size
        let (tok, file_offset) = get_next_word(&self.buffer, 1).unwrap();
        let raw_file_size = tok.as_slice(&self.buffer);
        let file_size: usize = str::from_utf8(raw_file_size)?.parse()?;
        let _ = self.buffer.split_to(file_offset).freeze();

        // --- ensure file size is correct/all data is present
        ensure!(
            self.buffer.len() == file_size,
            "Expected RDB file for FULLSYNC to be {}, but got {}",
            file_size,
            self.buffer.len()
        );
        let file_data = self.buffer.split_to(file_size - 1).freeze();

        // --- flush out buffer
        self.buffer.clear();

        Ok(file_data.to_vec())
    }

    /// Reads from self.buffer and parses the message to a RedisValue
    pub async fn read_and_parse(&mut self) -> RESPResult {
        let bytes_read = self
            .stream
            .read_buf(&mut self.buffer)
            .await
            .expect("Failure reading from stream");
        if bytes_read == 0 {
            return Ok(None);
        }

        log::info!("Parsing: {:?}", &self.buffer);
        let token = tokenize(&self.buffer, 0).expect("Failure parsing request");
        self._parse(token)
    }

    pub async fn write(&mut self, response: RedisValue) -> Result<usize> {
        let serialized_data = response.serialize()?;
        let bytes = self.stream.write(serialized_data.as_bytes()).await?;

        Ok(bytes)
    }

    pub async fn write_raw(&mut self, data: &[u8]) -> Result<usize> {
        let bytes = self.stream.write(data).await?;

        Ok(bytes)
    }

    pub async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await?;

        Ok(())
    }
}
