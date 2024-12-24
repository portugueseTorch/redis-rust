use core::str;

use anyhow::{bail, Result};
use bytes::Bytes;

use super::handler::RESPValue;

impl RESPValue {
    pub fn get_cmd_and_args(self) -> (Bytes, Vec<RESPValue>) {
        let request = match self {
            RESPValue::Array(arr) => arr,
            _ => panic!("Incoming array should be an array"),
        };

        let cmd = request.first().unwrap().clone().unpack_bulk_str().unwrap();
        let args = request.into_iter().skip(1).collect();

        (cmd, args)
    }

    fn unpack_bulk_str(self) -> Result<Bytes> {
        match self {
            RESPValue::BulkString(b) => Ok(b),
            _ => bail!("Should be a bulk string"),
        }
    }
}

pub fn ping() -> RESPValue {
    RESPValue::SimpleString(Bytes::from_static(b"PONG"))
}

pub fn echo(args: &Vec<RESPValue>) -> RESPValue {
    args.first().unwrap().clone()
}
