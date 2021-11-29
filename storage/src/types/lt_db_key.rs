use crate::{db::traits::DbKey, traits::Serializable};
use std::io::Write;
use ton_block::ShardIdent;
use ton_types::Result;

pub struct LtDbKey(Vec<u8>);

impl LtDbKey {
    pub fn with_values(shard_id: &ShardIdent, index: u32) -> Result<Self> {
        let mut key = shard_id.to_vec()?;
        key.write_all(&index.to_le_bytes())?;

        Ok(Self(key))
    }
}

impl DbKey for LtDbKey {
    fn key_name(&self) -> &'static str {
        "LtDbKey"
    }

    fn key(&self) -> &[u8] {
        self.0.as_slice()
    }
}
