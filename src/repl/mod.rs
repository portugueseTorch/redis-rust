use anyhow::Result;
use master::RedisMasterContext;
use replica::RedisReplicaContext;

pub mod master;
pub mod replica;

#[derive(Clone, Debug)]
pub enum ServerContext {
    Master(RedisMasterContext),
    Replica(RedisReplicaContext),
}
impl ServerContext {
    pub async fn new(replica_of: Option<String>, port: usize) -> Result<Self> {
        let server_context = match replica_of {
            None => Self::Master(RedisMasterContext::new()),
            Some(master_addr) => {
                Self::Replica(RedisReplicaContext::connect(port, master_addr).await?)
            }
        };

        Ok(server_context)
    }

    pub fn is_master(&self) -> bool {
        matches!(self, Self::Master(_))
    }

    pub fn get_master_replid(&self) -> &str {
        match self {
            Self::Master(ctx) => &ctx.master_replid,
            Self::Replica(ctx) => &ctx.master_replid,
        }
    }
}
