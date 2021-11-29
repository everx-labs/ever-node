use crate::{db::traits::DbKey, traits::Serializable};
use ton_block::ShardIdent;
use ton_types::Result;

pub struct ShardIdentKey(Vec<u8>);

impl ShardIdentKey {
    pub fn new(shard_ident: &ShardIdent) -> Result<Self> {
        let mut key = Vec::with_capacity(std::mem::size_of::<ShardIdent>());
        shard_ident.serialize(&mut key)?;

        Ok(Self(key))
    }
}

impl DbKey for ShardIdentKey {
    fn key_name(&self) -> &'static str {
        "ShardIdentKey"
    }

    fn as_string(&self) -> String {
        ShardIdent::from_slice(self.key())
            .map(|shard_ident| format!("{}", shard_ident))
            .unwrap_or_else(|_err| hex::encode(self.key()))
    }

    fn key(&self) -> &[u8] {
        self.0.as_slice()
    }
}
