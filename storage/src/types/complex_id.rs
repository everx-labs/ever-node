use crate::db::db::traits::DbKey;
use fnv::FnvHasher;
use std::{fmt::{Display, Formatter}, hash::{Hash, Hasher}, marker::PhantomData};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ComplexId<A: Hash, B: Hash> {
    hash: [u8; 8],
    phantom_a: PhantomData<A>,
    phantom_b: PhantomData<B>,
}

impl<A: Hash, B: Hash> ComplexId<A, B> {
    pub fn new(a: &A, b: &B) -> Self {
        let mut hasher = FnvHasher::default();
        a.hash(&mut hasher);
        b.hash(&mut hasher);
        Self {
            hash: hasher.finish().to_le_bytes(),
            phantom_a: PhantomData::default(),
            phantom_b: PhantomData::default(),
        }
    }
}

impl<A: Hash, B: Hash> Display for ComplexId<A, B> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", hex::encode(&self.hash)))
    }
}

impl<A: Hash, B: Hash> DbKey for ComplexId<A, B> {
    fn key_name(&self) -> &'static str {
        "ComplexId"
    }

    fn key(&self) -> &[u8] {
        &self.hash[..]
    }
}
