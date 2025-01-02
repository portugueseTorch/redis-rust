#[allow(unused_imports)]
use core::str;
use std::sync::Arc;

use bytes::Bytes;
use clap::Parser;
use server::{
    commands::{config, echo, get, info, keys, ping, set},
    handler::{RedisConnectionHandler, RedisValue},
    server::RedisServer,
};
use tokio::net::TcpStream;

mod replica;
mod server;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(long)]
    pub dir: Option<String>,
    #[arg(long)]
    pub dbfilename: Option<String>,
    #[arg(long)]
    pub port: Option<usize>,
    #[arg(long)]
    pub replicaof: Option<String>,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Args::parse();
    let redis_server = RedisServer::init(args)
        .await
        .expect("Failure initializing server");

    loop {
        let stream = redis_server.listener.accept().await;

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
        let parsed_data = handler.read_and_parse().await.unwrap();
        let parsed_request = match &parsed_data {
            None => None,
            Some(RedisValue::Array(arr)) => {
                for item in arr.iter() {
                    if !matches!(item, RedisValue::BulkString(_)) {
                        log::error!("Invalid request format, closing connection...");
                        return;
                    }
                }
                parsed_data
            }
            _ => {
                log::error!("Invalid request format. closing connection...");
                return;
            }
        };

        let response = match parsed_request {
            Some(value) => {
                let (cmd, args) = value.get_cmd_and_args();
                let cmd_as_str = str::from_utf8(&cmd).unwrap();

                match cmd_as_str.to_uppercase().as_str() {
                    "PING" => ping(),
                    "ECHO" => echo(&args),
                    "INFO" => info(&args, &redis_server),
                    "SET" => set(&args, &redis_server).await,
                    "GET" => get(&args, &redis_server).await,
                    "KEYS" => keys(&args, &redis_server).await,
                    "REPLCONF" => RedisValue::SimpleString(Bytes::from_static(b"OK")),
                    "CONFIG" => config(&args, &redis_server),
                    _ => RedisValue::SimpleError(Bytes::from(format!(
                        "Invalid command: '{}'",
                        cmd_as_str
                    ))),
                }
            }
            None => {
                break;
            }
        };

        handler.write(response).await.unwrap();
    }

    log::info!("Closing connection...");
}
