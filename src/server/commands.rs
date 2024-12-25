use core::str;
use std::collections::HashMap;

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

pub fn set(args: &Vec<RESPValue>, store: &mut HashMap<Bytes, Bytes>) -> RESPValue {
    let key = args
        .get(0)
        .expect("No key specified for SET command")
        .clone()
        .unpack_bulk_str()
        .unwrap();
    let value = args
        .get(1)
        .expect("No value specified for SET command")
        .clone()
        .unpack_bulk_str()
        .unwrap();

    store.insert(key, value);

    RESPValue::SimpleString(Bytes::from_static(b"OK"))
}

pub fn get(args: &Vec<RESPValue>, store: &HashMap<Bytes, Bytes>) -> RESPValue {
    let key = args
        .get(0)
        .expect("No key specified for SET command")
        .clone()
        .unpack_bulk_str()
        .unwrap();
    let value = store.get(&key);

    match value {
        Some(val) => RESPValue::BulkString(val.clone()),
        None => RESPValue::Null,
    }
}
