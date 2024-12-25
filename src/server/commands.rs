use core::str;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{bail, Result};
use bytes::Bytes;

use crate::{RedisDatabaseConfig, RedisStore};

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

fn get_argument(pos: usize, args: &Vec<RESPValue>) -> Result<Bytes> {
    args.get(pos)
        .expect("No key specified for SET command")
        .clone()
        .unpack_bulk_str()
}

pub fn ping() -> RESPValue {
    RESPValue::SimpleString(Bytes::from_static(b"PONG"))
}

pub fn echo(args: &Vec<RESPValue>) -> RESPValue {
    args.first().unwrap().clone()
}

//    let mut store_lock = store.lock().await;
//    store_lock.insert(key, (value, timeout_stamp));

pub async fn set(args: &Vec<RESPValue>, store: &mut RedisStore) -> RESPValue {
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

    store.lock().await.insert(key, (value, timeout_stamp));
    RESPValue::SimpleString(Bytes::from_static(b"OK"))
}

pub async fn get(args: &Vec<RESPValue>, store: &RedisStore) -> RESPValue {
    let key = get_argument(0, args).unwrap();

    let result = {
        let store = store.lock().await;
        store.get(&key).cloned()
    };

    match result {
        Some((_, Some(expiry))) if expiry < SystemTime::now() => RESPValue::NullBulkString,
        Some((val, _)) => RESPValue::BulkString(val),
        None => RESPValue::NullBulkString,
    }
}

pub fn config(args: &Vec<RESPValue>, rdb_conf: Option<&Arc<RedisDatabaseConfig>>) -> RESPValue {
    let sub_cmd = get_argument(0, args).unwrap();
    let sub_cmd = str::from_utf8(&sub_cmd).unwrap().to_uppercase();

    match sub_cmd.as_str() {
        "GET" => {
            if rdb_conf.is_none() {
                return RESPValue::SimpleError(Bytes::from_static(b"No config object exists"));
            }

            let mut resp: Vec<RESPValue> = Vec::new();
            let config = rdb_conf.unwrap();

            for arg in args.iter().skip(1) {
                let raw_key = arg.clone().unpack_bulk_str().unwrap();
                let key = String::from(str::from_utf8(&raw_key).unwrap());

                match config.0.get(&key) {
                    Some(val) => {
                        let key_as_bytes = Bytes::copy_from_slice(key.as_bytes());
                        let val_as_bytes = Bytes::copy_from_slice(val.clone().as_bytes());

                        resp.extend([
                            RESPValue::BulkString(key_as_bytes),
                            RESPValue::BulkString(val_as_bytes),
                        ])
                    }
                    None => {}
                }
            }
            RESPValue::Array(resp)
        }
        _ => RESPValue::SimpleError(Bytes::from(format!(
            "Invalid sub command for 'CONFIG': '{}'",
            sub_cmd
        ))),
    }
}
