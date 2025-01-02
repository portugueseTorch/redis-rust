use core::str;
use std::{
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Result};
use bytes::Bytes;
use tokio::{fs::File, io::AsyncReadExt};

use crate::repl::ServerContext;

use super::{
    handler::{RedisConnectionHandler, RedisValue},
    server::RedisServer,
};

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub struct CommandContext<'a> {
    pub args: &'a Vec<RedisValue>,
    pub server: &'a RedisServer,
    pub handler: &'a mut RedisConnectionHandler,
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

pub async fn ping(ctx: &mut CommandContext<'_>) -> Result<usize> {
    let res = RedisValue::SimpleString(Bytes::from_static(b"PONG"));
    let bytes = ctx.handler.write(res).await?;

    Ok(bytes)
}

pub async fn echo(ctx: &mut CommandContext<'_>) -> Result<usize> {
    let res = ctx.args.first().unwrap().clone();
    let bytes = ctx.handler.write(res).await?;

    Ok(bytes)
}

pub async fn set(ctx: &mut CommandContext<'_>) -> Result<usize> {
    let key = get_argument(0, ctx.args).clone();
    let value = get_argument(1, ctx.args).clone();

    let mut main_store = ctx.server.main_store.lock().await;
    let mut expire_store = ctx.server.expire_store.lock().await;

    if let Some(cmd_arg) = ctx.args.get(2) {
        let cmd_as_str = str::from_utf8(&cmd_arg.clone().unpack_bulk_str().unwrap())
            .unwrap()
            .to_uppercase();
        let timeout = match cmd_as_str.as_str() {
            "PX" => {
                let timeout_value_raw = get_argument(3, ctx.args);
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

    let res = RedisValue::SimpleString(Bytes::from_static(b"OK"));
    let bytes = ctx.handler.write(res).await?;

    Ok(bytes)
}

pub async fn get(ctx: &mut CommandContext<'_>) -> Result<usize> {
    let key = get_argument(0, ctx.args);

    let mut main_store = ctx.server.main_store.lock().await;
    let mut expire_store = ctx.server.expire_store.lock().await;

    let res = match main_store.get(&key) {
        Some(val) => {
            let timestamp = expire_store.get(key).unwrap_or(&u64::MAX);

            if *timestamp < now() {
                main_store.remove(key);
                expire_store.remove(key);
                RedisValue::NullBulkString
            } else {
                val.clone()
            }
        }
        None => RedisValue::NullBulkString,
    };
    let bytes = ctx.handler.write(res).await?;

    Ok(bytes)
}

pub async fn keys(ctx: &mut CommandContext<'_>) -> Result<usize> {
    let _pattern = str::from_utf8(&get_argument(0, ctx.args).unpack_bulk_str().unwrap()).unwrap();
    let main_store_lock = ctx.server.main_store.lock().await;
    let expire_store_lock = ctx.server.expire_store.lock().await;

    let mut res = vec![];

    for key in main_store_lock.keys() {
        // --- if expired, skip it
        let expire_key = expire_store_lock.get(key);
        if expire_key.is_some_and(|&k| k < now()) {
            continue;
        }

        res.push(key.clone());
    }

    let res = RedisValue::Array(res);
    let bytes = ctx.handler.write(res).await?;

    Ok(bytes)
}

pub async fn config(ctx: &mut CommandContext<'_>) -> Result<usize> {
    let sub_cmd = str::from_utf8(&get_argument(0, ctx.args).unpack_bulk_str().unwrap())
        .unwrap()
        .to_uppercase();

    let res = match sub_cmd.as_str() {
        "GET" => {
            if ctx.server.config.is_none() {
                RedisValue::SimpleError(Bytes::from_static(b"No config object exists"))
            } else {
                let mut resp: Vec<RedisValue> = Vec::new();
                let config = ctx.server.config.as_ref().unwrap();

                for arg in ctx.args.iter().skip(1) {
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
        }
        _ => RedisValue::SimpleError(Bytes::from(format!(
            "Invalid sub command for 'CONFIG': '{}'",
            sub_cmd
        ))),
    };
    let bytes = ctx.handler.write(res).await?;

    Ok(bytes)
}

pub async fn info(ctx: &mut CommandContext<'_>) -> Result<usize> {
    let info_data = match &ctx.server.server_context {
        ServerContext::Master(master) => {
            let role = format_info("role", &"master");
            let repl_id = format_info("master_replid", &master.master_replid);
            let repl_offset = format_info("master_repl_offset", &master.master_repl_offset);
            vec![role, repl_id, repl_offset].join("\r\n")
        }
        ServerContext::Replica(replica) => {
            let role = format_info("role", &"slave");
            let master_replid = format_info("master_replid", &replica.master_replid);
            let master_repl_offset = format_info("master_repl_offset", &replica.master_repl_offset);
            let slave_repl_offset = format_info("slave_repl_offset", &replica.slave_repl_offset);
            let master_replid2 = format_info(
                "master_replid2",
                &replica.master_replid2.as_ref().unwrap_or(&"".to_string()),
            );
            let second_repl_offset = format_info(
                "second_repl_offset",
                &replica.second_repl_offset.map_or(-1, |m| m as i32),
            );

            vec![
                role,
                master_replid,
                master_repl_offset,
                slave_repl_offset,
                master_replid2,
                second_repl_offset,
            ]
            .join("\r\n")
        }
    };

    let res = RedisValue::BulkString(Bytes::from(info_data));
    let bytes = ctx.handler.write(res).await?;

    Ok(bytes)
}

pub async fn replconf(ctx: &mut CommandContext<'_>) -> Result<usize> {
    let res = RedisValue::SimpleString(Bytes::from_static(b"OK"));
    let bytes = ctx.handler.write(res).await?;

    Ok(bytes)
}

pub async fn psync(ctx: &mut CommandContext<'_>) -> Result<usize> {
    let res = RedisValue::SimpleString(Bytes::from(format!(
        "+FULLRESYNC {} 0\r\n",
        ctx.server.server_context.get_master_replid()
    )));
    ctx.handler.write(res).await?;

    // --- send rdb dump over the wire for fullsync
    let mut file = File::open("empty.rdb").await?;
    let mut buf = vec![];
    file.read_to_end(&mut buf).await?;

    let bytes_len = ctx
        .handler
        .write_raw(format!("${}\r\n", buf.len()).as_bytes())
        .await?;
    let bytes_content = ctx.handler.write_raw(&buf).await?;

    Ok(bytes_len + bytes_content)
}

fn format_info<V: Display>(key: &str, value: &V) -> String {
    format!("{}:{}", key, value)
}
