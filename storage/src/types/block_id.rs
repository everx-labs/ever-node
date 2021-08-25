use crate::db::traits::DbKey;
use ton_block::BlockIdExt;

impl DbKey for BlockIdExt {
    fn key_name(&self) -> &'static str {
        "BlockId"
    }
    fn as_string(&self) -> String {
        format!("{}", self)
    }
    fn key(&self) -> &[u8] {
        self.root_hash().as_slice()
    }
}
