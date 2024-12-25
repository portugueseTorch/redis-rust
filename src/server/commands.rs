use core::str;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

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

pub fn set(
    args: &Vec<RESPValue>,
    store: &mut HashMap<Bytes, (Bytes, Option<SystemTime>)>,
) -> RESPValue {
    let key = get_argument(0, args).unwrap();
    let value = get_argument(1, args).unwrap();
    let cmd_arg = args.get(2);

    let timeout_stamp = cmd_arg.map(|cmd| {
        let cmd_as_str = str::from_utf8(&cmd.clone().unpack_bulk_str().unwrap())
            .unwrap()
            .to_lowercase();

        match cmd_as_str.as_str() {
            "px" => {
                let timeout_value_raw =
                    get_argument(3, args).expect("PX cmd arg should provide a timeout stamp");
                let timeout_value: u64 =
                    str::from_utf8(&timeout_value_raw).unwrap().parse().unwrap();

                SystemTime::now() + Duration::from_millis(timeout_value)
            }
            _ => panic!("Invalid command argument for SET: '{}'", cmd_as_str),
        }
    });

    store.insert(key, (value, timeout_stamp));
    RESPValue::SimpleString(Bytes::from_static(b"OK"))
}

pub fn get(
    args: &Vec<RESPValue>,
    store: &HashMap<Bytes, (Bytes, Option<SystemTime>)>,
) -> RESPValue {
    let key = get_argument(0, args).unwrap();
    let value = store.get(&key);

    match value {
        Some((val, expiry)) => match expiry {
            Some(e) => {
                if *e < SystemTime::now() {
                    RESPValue::NullBulkString
                } else {
                    RESPValue::BulkString(val.clone())
                }
            }
            None => RESPValue::BulkString(val.clone()),
        },
        None => RESPValue::NullBulkString,
    }
}

fn get_argument(pos: usize, args: &Vec<RESPValue>) -> Result<Bytes> {
    args.get(pos)
        .expect("No key specified for SET command")
        .clone()
        .unpack_bulk_str()
}
