use std::{collections::HashMap, sync::Arc, time::SystemTime};

use tokio::sync::Mutex;

use super::handler::RedisValue;

pub type RedisMainStore = Arc<Mutex<HashMap<RedisValue, RedisValue>>>;
pub type RedisExpireStore = Arc<Mutex<HashMap<RedisValue, SystemTime>>>;

pub struct RedisDatabaseConfig {
    pub dir: String,
    pub dbfilename: String,
}
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
            (Some(dir), Some(dbfilename)) => {
                Some(Arc::new(RedisDatabaseConfig { dir, dbfilename }))
            }
            _ => None,
        }
    }
}

pub struct RedisServer {
    pub config: Option<Arc<RedisDatabaseConfig>>,
    pub main_store: RedisMainStore,
    pub expire_store: RedisExpireStore,
}
impl RedisServer {
    pub fn init(args: Vec<String>) -> Arc<Self> {
        let config = RedisDatabaseConfig::from_env_args(args);
        let main_store = Arc::new(Mutex::new(HashMap::new()));
        let expire_store = Arc::new(Mutex::new(HashMap::new()));

        Arc::new(Self {
            config,
            main_store,
            expire_store,
        })
    }
}
