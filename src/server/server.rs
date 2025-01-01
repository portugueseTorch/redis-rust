use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Read},
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use bytes::Bytes;
use tokio::{net::TcpListener, sync::Mutex};

use crate::Args;

use super::handler::RedisValue;

const LEN_ENCODING_MASK: u8 = 0b11000000;
const LEN_DECODING_MASK: u8 = 0b00111111;

pub type RedisMainStore = Arc<Mutex<HashMap<RedisValue, RedisValue>>>;
pub type RedisExpireStore = Arc<Mutex<HashMap<RedisValue, u64>>>;
pub struct RedisServerConfig {
    pub dir: String,
    pub dbfilename: String,
}

type RedisServerAux = (
    RedisMainStore,
    RedisExpireStore,
    Option<Arc<RedisServerConfig>>,
);

pub struct RedisServer {
    pub config: Option<Arc<RedisServerConfig>>,
    pub main_store: RedisMainStore,
    pub expire_store: RedisExpireStore,
    /// listener for the client connection
    pub listener: TcpListener,
    /// listener for the master connection - is some only for replicas
    pub master_listener: Option<i32>,
    /// master replication ID
    pub replication_id: String,
    /// offset into the circluar replication buffer
    pub replication_offset: usize,
}
impl RedisServer {
    pub async fn init(args: Args) -> anyhow::Result<Arc<Self>> {
        let dir = args.dir;
        let dbfilename = args.dbfilename;
        let port = args.port.unwrap_or(6379);
        let replicaof = args.replicaof;

        // --- set up client listener
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
            .await
            .unwrap();

        // --- start connection with master, if replica
        let master_listener: Result<Option<i32>> = if let Some(master_addr) = replicaof {
            let master_addr: Vec<&str> = master_addr.split(" ").collect();
            let _master_addr = master_addr.join(":");

            Ok(Some(42))
        } else {
            Ok(None)
        };
        let master_listener = master_listener.unwrap();

        // --- init stores or load state from rdb file
        let (main_store, expire_store, config): RedisServerAux = match (dir, dbfilename) {
            (Some(dir), Some(dbfilename)) => RedisServer::from_rdbfile(&dir, &dbfilename)?,
            _ => (
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                None,
            ),
        };

        log::info!(
            "Redis {}server running on 127.0.0.1:{}",
            master_listener.as_ref().map_or("", |_| "replica "),
            port
        );

        Ok(Arc::new(Self {
            main_store,
            expire_store,
            config,
            listener,
            master_listener,
            //
            replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            replication_offset: 0,
        }))
    }

    fn from_rdbfile(dir: &str, dbfilename: &str) -> anyhow::Result<RedisServerAux> {
        // --- redis config
        let config = RedisServerConfig {
            dir: dir.to_string(),
            dbfilename: dbfilename.to_string(),
        };

        // --- open file and read contents into buf
        let path = Path::new(&dir).join(&dbfilename);
        let rdbfile = File::open(path);
        if rdbfile.is_err() {
            return Ok((
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Some(Arc::new(config)),
            ));
        }
        let mut buf: Vec<u8> = vec![];
        let mut reader = BufReader::new(rdbfile.unwrap());
        reader.read_to_end(&mut buf)?;

        let fb_pos = buf.iter().position(|&b| b == 0xfb).unwrap();
        let (main_store_size, next_pos) = parse_length_encoding(&buf, fb_pos + 1);
        let (expire_store_size, mut next_pos) = parse_length_encoding(&buf, next_pos);

        let mut main_store = HashMap::with_capacity(main_store_size);
        let mut expire_store = HashMap::with_capacity(expire_store_size);

        let mut parsing_complete = false;
        while next_pos < buf.len() && buf[next_pos] != 0xfe {
            match buf[next_pos] {
                0xfc => {
                    next_pos += 1;

                    let expire_time_in_ms = u64::from_le_bytes(
                        buf[next_pos..next_pos + 8]
                            .try_into()
                            .expect("Should be a slice of length 8"),
                    );
                    next_pos += 8;

                    // --- type of the value, for now support only string encoding
                    if buf[next_pos] != 0 {
                        log::error!("Invalid encoding for value: {:x?}", buf[next_pos]);
                        break;
                    }
                    next_pos += 1;

                    let (key, next) = parse_rdb_string(&buf, next_pos)?;
                    let (val, next) = parse_rdb_string(&buf, next)?;
                    next_pos = next;

                    // --- if the key has expired already, skip persisting this
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    if expire_time_in_ms < now {
                        continue;
                    }

                    main_store.insert(key.clone(), val);
                    expire_store.insert(key, expire_time_in_ms);
                    next_pos = next
                }
                0xff => {
                    parsing_complete = true;
                    break;
                }
                _ => {
                    // --- type of the value, for now support only string encoding
                    if buf[next_pos] != 0 {
                        log::error!("Invalid encoding for value: {:x?}", buf[next_pos]);
                        break;
                    }
                    next_pos += 1;

                    let (key, next) = parse_rdb_string(&buf, next_pos)?;
                    let (val, next) = parse_rdb_string(&buf, next)?;

                    main_store.insert(key.clone(), val);
                    next_pos = next
                }
            }
        }

        if !parsing_complete {
            log::error!("Error while parsing rdbfile. Defaulting to empty stores...");
            return Ok((
                Arc::new(Mutex::new(HashMap::new())),
                Arc::new(Mutex::new(HashMap::new())),
                Some(Arc::new(config)),
            ));
        }

        Ok((
            Arc::new(Mutex::new(main_store)),
            Arc::new(Mutex::new(expire_store)),
            Some(Arc::new(config)),
        ))
    }
}

fn parse_rdb_string(buf: &Vec<u8>, pos: usize) -> Result<(RedisValue, usize)> {
    let (str_len, next_pos) = parse_length_encoding(buf, pos);

    if next_pos + str_len > buf.len() {
        return Err(anyhow::anyhow!(
            "Buffer overflow when parsing string: needed {} bytes but got {}",
            str_len,
            buf.len() - next_pos
        ));
    }
    let raw_str = &buf[next_pos..next_pos + str_len];
    let parsed = RedisValue::BulkString(Bytes::copy_from_slice(raw_str));
    Ok((parsed, next_pos + str_len))
}

fn parse_length_encoding(buf: &Vec<u8>, pos: usize) -> (usize, usize) {
    let enconding_byte = *buf.get(pos).unwrap();
    match enconding_byte & LEN_ENCODING_MASK {
        // --- one byte length
        0b00000000 => ((enconding_byte & LEN_DECODING_MASK) as usize, pos + 1),
        // --- 14 bit length
        0b01000000 => unimplemented!("14 bit length encoding not implemented yet"),
        // --- 4 byte length
        0b10000000 => (
            usize::from_le_bytes(
                buf[pos + 1..pos + 5]
                    .try_into()
                    .expect("Should be a 4 byte slice"),
            ),
            pos + 5,
        ),
        // --- special encoding
        0b11000000 => unimplemented!("Special encoding length not implemented yet"),
        _ => panic!(
            "Unexpected length encoding: '{:08b}'",
            buf.get(pos).unwrap()
        ),
    }
}
