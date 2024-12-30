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
use tokio::sync::Mutex;

use super::handler::RedisValue;

const LEN_ENCODING_MASK: u8 = 0b11000000;
const LEN_DECODING_MASK: u8 = 0b00111111;

pub type RedisMainStore = Arc<Mutex<HashMap<RedisValue, RedisValue>>>;
pub type RedisExpireStore = Arc<Mutex<HashMap<RedisValue, u64>>>;
pub struct RedisDatabaseConfig {
    pub dir: String,
    pub dbfilename: String,
}

pub struct RedisServer {
    pub config: Option<Arc<RedisDatabaseConfig>>,
    pub main_store: RedisMainStore,
    pub expire_store: RedisExpireStore,
}
impl RedisServer {
    pub fn init(args: Vec<String>) -> anyhow::Result<Arc<Self>> {
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

        let server = match (dir, dbfilename) {
            (Some(dir), Some(dbfilename)) => {
                let path = Path::new(&dir).join(&dbfilename);
                RedisServer::from_rdbfile(&path)?
            }
            _ => Self {
                config: None,
                main_store: Arc::new(Mutex::new(HashMap::new())),
                expire_store: Arc::new(Mutex::new(HashMap::new())),
            },
        };

        Ok(Arc::new(server))
    }

    fn from_rdbfile(file_path: &Path) -> anyhow::Result<Self> {
        // --- open file and read contents into buf
        let rdbfile = File::open(file_path)?;
        let mut buf: Vec<u8> = vec![];
        let mut reader = BufReader::new(rdbfile);
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
            return Ok(Self {
                config: None,
                main_store: Arc::new(Mutex::new(HashMap::new())),
                expire_store: Arc::new(Mutex::new(HashMap::new())),
            });
        }

        Ok(Self {
            config: None,
            main_store: Arc::new(Mutex::new(main_store)),
            expire_store: Arc::new(Mutex::new(expire_store)),
        })
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
