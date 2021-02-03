use crate::db::traits::DbKey;
use strum_macros::AsRefStr;

#[derive(Debug, AsRefStr)]
pub enum StatusKey {
    // TODO: Reserved for DynamicBocDb
}

impl DbKey for StatusKey {
    fn key_name(&self) -> &'static str {
        "StatusKey"
    }

    fn as_string(&self) -> String {
        self.as_ref().to_string()
    }

    fn key(&self) -> &[u8] {
        self.as_ref().as_bytes()
    }
}
