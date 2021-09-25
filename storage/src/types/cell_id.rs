use crate::db::traits::DbKey;
use std::fmt::{Display, Formatter, Debug};
use ton_types::types::UInt256;

#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CellId {
    hash: UInt256,
}

impl CellId {
    pub const fn new(hash: UInt256) -> Self {
        Self { hash }
    }
}

impl Display for CellId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:#x}", self.hash))
    }
}

impl Debug for CellId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("CellId[{:#x}]", self.hash))
    }
}

impl DbKey for CellId {
    fn key_name(&self) -> &'static str {
        "CellId"
    }

    fn key(&self) -> &[u8] {
        self.hash.as_slice()
    }
}

impl From<UInt256> for CellId {
    fn from(value: UInt256) -> Self {
        CellId::new(value)
    }
}

// impl Into<UInt256> for CellId {
//     fn into(self) -> UInt256 {
//         self.hash
//     }
// }
