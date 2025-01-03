#[allow(unused_imports)]
use core::str;
use std::sync::Arc;

use bytes::Bytes;
use clap::Parser;
use server::{
    commands::{config, echo, get, info, keys, ping, psync, replconf, set, CommandContext},
    handler::{RedisConnectionHandler, RedisValue},
    server::RedisServer,
};
use tokio::net::TcpStream;

mod repl;
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

        match parsed_request {
            Some(value) => {
                let (cmd, args) = value.get_cmd_and_args();
                let cmd_as_str = str::from_utf8(&cmd).unwrap();
                let mut ctx = CommandContext {
                    args: &args,
                    server: &redis_server,
                    handler: &mut handler,
                };

                match cmd_as_str.to_uppercase().as_str() {
                    "PING" => ping(&mut ctx).await.unwrap(),
                    "ECHO" => echo(&mut ctx).await.unwrap(),
                    "INFO" => info(&mut ctx).await.unwrap(),
                    "SET" => set(&mut ctx).await.unwrap(),
                    "GET" => get(&mut ctx).await.unwrap(),
                    "KEYS" => keys(&mut ctx).await.unwrap(),
                    "REPLCONF" => replconf(&mut ctx).await.unwrap(),
                    "PSYNC" => psync(&mut ctx).await.unwrap(),
                    "CONFIG" => config(&mut ctx).await.unwrap(),
                    _ => {
                        let res = RedisValue::SimpleError(Bytes::from(format!(
                            "Invalid command: '{}'",
                            cmd_as_str
                        )));
                        handler.write(res).await.unwrap()
                    }
                }
            }
            None => {
                break;
            }
        };
    }

    log::info!("Closing connection...");
}
