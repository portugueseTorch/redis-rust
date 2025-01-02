use core::str;
use std::{
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Result};
use bytes::Bytes;

use super::{handler::RedisValue, server::RedisServer};

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

impl RedisValue {
    pub fn get_cmd_and_args(self) -> (Bytes, Vec<RedisValue>) {
        let request = match self {
            RedisValue::Array(arr) => arr,
            _ => panic!("Incoming array should be an array"),
        };

        let cmd = request.first().unwrap().clone().unpack_bulk_str().unwrap();
        let args = request.into_iter().skip(1).collect();

        (cmd, args)
    }

    fn unpack_bulk_str(&self) -> Result<Bytes> {
        match self {
            RedisValue::BulkString(b) => Ok(b.clone()),
            _ => bail!("Should be a bulk string"),
        }
    }
}

fn get_argument(pos: usize, args: &Vec<RedisValue>) -> &RedisValue {
    args.get(pos).expect("No key specified for SET command")
}

pub fn ping() -> RedisValue {
    RedisValue::SimpleString(Bytes::from_static(b"PONG"))
}

pub fn echo(args: &Vec<RedisValue>) -> RedisValue {
    args.first().unwrap().clone()
}

pub async fn set(args: &Vec<RedisValue>, server: &RedisServer) -> RedisValue {
    let key = get_argument(0, args).clone();
    let value = get_argument(1, args).clone();

    let mut main_store = server.main_store.lock().await;
    let mut expire_store = server.expire_store.lock().await;

    if let Some(cmd_arg) = args.get(2) {
        let cmd_as_str = str::from_utf8(&cmd_arg.clone().unpack_bulk_str().unwrap())
            .unwrap()
            .to_uppercase();
        let timeout = match cmd_as_str.as_str() {
            "PX" => {
                let timeout_value_raw = get_argument(3, args);
                let timeout_value: u64 =
                    str::from_utf8(&timeout_value_raw.unpack_bulk_str().unwrap())
                        .unwrap()
                        .parse()
                        .unwrap();
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    + timeout_value
            }
            _ => panic!("Invalid command argument for SET: '{}'", cmd_as_str),
        };
        expire_store.insert(key.clone(), timeout);
    }
    main_store.insert(key, value);

    RedisValue::SimpleString(Bytes::from_static(b"OK"))
}

pub async fn get(args: &Vec<RedisValue>, server: &RedisServer) -> RedisValue {
    let key = get_argument(0, args);

    let mut main_store = server.main_store.lock().await;
    let mut expire_store = server.expire_store.lock().await;

    match main_store.get(&key) {
        Some(val) => {
            if let Some(timestamp) = expire_store.get(key) {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;

                if timestamp < &now {
                    main_store.remove(key);
                    expire_store.remove(key);
                    return RedisValue::NullBulkString;
                }
            }
            val.clone()
        }
        None => RedisValue::NullBulkString,
    }
}

pub async fn keys(args: &Vec<RedisValue>, server: &RedisServer) -> RedisValue {
    let _pattern = str::from_utf8(&get_argument(0, args).unpack_bulk_str().unwrap()).unwrap();
    let main_store_lock = server.main_store.lock().await;
    let expire_store_lock = server.expire_store.lock().await;

    let mut res = vec![];

    for key in main_store_lock.keys() {
        // --- if expired, skip it
        let expire_key = expire_store_lock.get(key);
        if expire_key.is_some_and(|&k| k < now()) {
            continue;
        }

        res.push(key.clone());
    }

    RedisValue::Array(res)
}

pub fn config(args: &Vec<RedisValue>, server: &RedisServer) -> RedisValue {
    let sub_cmd = str::from_utf8(&get_argument(0, args).unpack_bulk_str().unwrap())
        .unwrap()
        .to_uppercase();

    match sub_cmd.as_str() {
        "GET" => {
            if server.config.is_none() {
                return RedisValue::SimpleError(Bytes::from_static(b"No config object exists"));
            }

            let mut resp: Vec<RedisValue> = Vec::new();
            let config = server.config.as_ref().unwrap();

            for arg in args.iter().skip(1) {
                let raw_key = arg.clone().unpack_bulk_str().unwrap();
                let key = String::from(str::from_utf8(&raw_key).unwrap());

                match key.as_str() {
                    "dir" => resp.extend([
                        RedisValue::BulkString(Bytes::from(key)),
                        RedisValue::BulkString(Bytes::from(config.dir.clone())),
                    ]),
                    "dbfilename" => resp.extend([
                        RedisValue::BulkString(Bytes::from(key)),
                        RedisValue::BulkString(Bytes::from(config.dbfilename.clone())),
                    ]),
                    _ => continue,
                }
            }
            RedisValue::Array(resp)
        }
        _ => RedisValue::SimpleError(Bytes::from(format!(
            "Invalid sub command for 'CONFIG': '{}'",
            sub_cmd
        ))),
    }
}

pub fn info(_args: &Vec<RedisValue>, server: &RedisServer) -> RedisValue {
    let role = server
        .replica_context
        .as_ref()
        .map_or("master", |_| "slave");

    // --- format info args
    let role = format_info("role", &role);
    let repl_id = format_info("master_replid", &server.replication_id);
    let repl_offset = format_info("master_repl_offset", &server.replication_offset);
    let info_data = vec![role, repl_id, repl_offset].join("\r\n");

    RedisValue::BulkString(Bytes::from(info_data))
}

fn format_info<V: Display>(key: &str, value: &V) -> String {
    format!("{}:{}", key, value)
}
