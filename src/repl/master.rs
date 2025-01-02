use super::replica::gen_uuid;

#[derive(Clone, Debug)]
pub struct RedisMasterContext {
    /// master replication ID
    pub master_replid: String,
    /// offset into the circluar replication buffer
    pub master_repl_offset: usize,
}
impl RedisMasterContext {
    pub fn new() -> Self {
        Self {
            master_replid: gen_uuid(),
            master_repl_offset: 0,
        }
    }
}
