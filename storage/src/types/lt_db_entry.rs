use ton_api::ton::ton_node::blockidext::BlockIdExt;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LtDbEntry {
    block_id_ext: BlockIdExt,
    lt: u64,
    unix_time: u32,
}

impl LtDbEntry {
    pub const fn with_values(block_id_ext: BlockIdExt, lt: u64, unix_time: u32) -> Self {
        Self { block_id_ext, lt, unix_time }
    }

    pub const fn block_id_ext(&self) -> &BlockIdExt {
        &self.block_id_ext
    }

    pub const fn lt(&self) -> u64 {
        self.lt
    }

    pub const fn unix_time(&self) -> u32 {
        self.unix_time
    }
}
