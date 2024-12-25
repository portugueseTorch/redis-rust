#![allow(unused_imports)]
use core::str;
use std::{
    collections::HashMap,
    env,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::SystemTime,
};

use bytes::Bytes;
use server::{
    commands::{config, echo, get, ping, set},
    handler::{RESPValue, RedisConnectionHandler},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

mod server;

pub type RedisStore = Arc<Mutex<HashMap<Bytes, (Bytes, Option<SystemTime>)>>>;

pub struct RedisDatabaseConfig(pub HashMap<String, String>);
impl RedisDatabaseConfig {
    pub fn from_env_args(args: Vec<String>) -> Option<Arc<Self>> {
        let dir = args
            .windows(2)
            .find(|f| f[0] == "--dir")
            .as_ref()
            .map(|m| m[1].clone());
        let dbfilename = args
            .windows(2)
            .find(|f| f[0] == "--dbfilename")
            .as_ref()
            .map(|m| m[1].clone());

        match (dir, dbfilename) {
            (Some(dir), Some(dbfilename)) => Some(Arc::new(RedisDatabaseConfig(HashMap::from([
                (String::from("dir"), dir),
                (String::from("dbfilename"), dbfilename),
            ])))),
            _ => None,
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    log::info!("TCP server running on 127.0.0.1:6379");

    // --- config
    let args: Vec<String> = env::args().collect();
    let config = RedisDatabaseConfig::from_env_args(args);

    // --- session data
    let store: RedisStore = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((stream, _)) => {
                let store = Arc::clone(&store);
                let config = config.as_ref().map(|m| Arc::clone(m));

                tokio::spawn(async move { handle_connection(stream, store, config).await });
            }
            Err(e) => log::error!("{}", e),
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    mut store: RedisStore,
    rdb_conf: Option<Arc<RedisDatabaseConfig>>,
) {
    let mut handler = RedisConnectionHandler::new(stream);

    loop {
        let parsed_data = handler.parse_request().await.unwrap();

        let response = match parsed_data {
            Some(value) => {
                let (cmd, args) = value.get_cmd_and_args();
                let cmd_as_str = str::from_utf8(&cmd).unwrap();

                match cmd_as_str.to_uppercase().as_str() {
                    "PING" => ping(),
                    "ECHO" => echo(&args),
                    "SET" => set(&args, &mut store).await,
                    "GET" => get(&args, &store).await,
                    "CONFIG" => config(&args, rdb_conf.as_ref()),
                    _ => RESPValue::SimpleError(Bytes::from(format!(
                        "Invalid command: '{}'",
                        cmd_as_str
                    ))),
                }
            }
            None => {
                log::info!("Closing connection...");
                break;
            }
        };

        handler.write(response).await.unwrap()
    }
}
