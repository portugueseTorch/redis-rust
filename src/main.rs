#![allow(unused_imports)]
use core::str;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    time::SystemTime,
};

use bytes::Bytes;
use server::{
    commands::{echo, get, ping, set},
    handler::RedisConnectionHandler,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

mod server;

#[tokio::main]
async fn main() {
    env_logger::init();

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    log::info!("TCP server running on 127.0.0.1:6379");

    loop {
        let stream = listener.accept().await;

        match stream {
            Ok((stream, _)) => {
                tokio::spawn(async move { handle_connection(stream).await });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_connection(stream: TcpStream) {
    let mut handler = RedisConnectionHandler::new(stream);
    let mut store: HashMap<Bytes, (Bytes, Option<SystemTime>)> = HashMap::new();

    loop {
        let parsed_data = handler.parse_request().await.unwrap();

        let response = match parsed_data {
            Some(value) => {
                let (cmd, args) = value.get_cmd_and_args();
                let cmd_as_str = str::from_utf8(&cmd).unwrap().to_lowercase();

                match cmd_as_str.as_str() {
                    "ping" => ping(),
                    "echo" => echo(&args),
                    "set" => set(&args, &mut store),
                    "get" => get(&args, &store),
                    _ => panic!("Invalid command found: {}", 4),
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