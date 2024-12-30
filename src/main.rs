#![allow(unused_imports)]
use core::str;
use std::{
    borrow::BorrowMut,
    collections::HashMap,
    env,
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::SystemTime,
};

use bytes::Bytes;
use server::{
    commands::{config, echo, get, keys, ping, set},
    handler::{RedisConnectionHandler, RedisValue},
    server::RedisServer,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
};

mod server;

#[tokio::main]
async fn main() {
    env_logger::init();

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    log::info!("TCP server running on 127.0.0.1:6379");

    let args: Vec<String> = env::args().collect();
    let redis_server = RedisServer::init(args).expect("Failure initializing server");

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((stream, _)) => {
                let redis_server = Arc::clone(&redis_server);
                tokio::spawn(async move { handle_connection(stream, redis_server).await });
            }
            Err(e) => log::error!("{}", e),
        }
    }
}

async fn handle_connection(stream: TcpStream, redis_server: Arc<RedisServer>) {
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
                    "SET" => set(&args, &redis_server).await,
                    "GET" => get(&args, &redis_server).await,
                    "KEYS" => keys(&args, &redis_server),
                    "CONFIG" => config(&args, &redis_server),
                    _ => RedisValue::SimpleError(Bytes::from(format!(
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
