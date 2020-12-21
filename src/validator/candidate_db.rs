use catchain::{PublicKey, BlockHash};
use ton_types::{UInt256, Result};
use validator_session::ValidatorBlockCandidate;
use std::sync::{Arc};
use std::collections::HashMap;
use std::fmt::{Formatter, Display};

#[derive(PartialEq, Eq, Hash)]
struct CandidateDbKey {
    source: UInt256,
    root_hash: BlockHash,
    file_hash: BlockHash,
    collated_data_hash: BlockHash
}

impl Display for CandidateDbKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "source {}, rh {}, fh {}, collated_data_hash {}",
            self.source.to_hex_string(), self.root_hash, self.file_hash, self.collated_data_hash)
    }
}

pub struct CandidateDb {
    db: HashMap<CandidateDbKey, Arc<ValidatorBlockCandidate>>
}

impl CandidateDb {
    pub fn new() -> Self {
        return CandidateDb{ db: HashMap::new() };
    }

    pub async fn save(&mut self, candidate: ValidatorBlockCandidate) -> Result<()> {
        let pub_key = candidate.public_key.pub_key()?;

        let key = CandidateDbKey {
            source: UInt256::from(pub_key),
            root_hash: candidate.id.root_hash.clone(),
            file_hash: candidate.id.file_hash.clone(),
            collated_data_hash: catchain::utils::get_hash (candidate.collated_data.data())
        };

        self.db.insert(key, Arc::new(candidate).clone());
        Ok(())
    }

    pub async fn load(&self, source: PublicKey,
            root_hash: BlockHash,
            file_hash: BlockHash,
            collated_data_hash: BlockHash
    ) -> Result<Arc<ValidatorBlockCandidate>> {
        let pub_key = source.pub_key()?;
        let key = CandidateDbKey { source: UInt256::from(pub_key), root_hash, file_hash, collated_data_hash };
        match self.db.get(&key) {
            Some(x) => Ok(x.clone()),
            None => Err(failure::err_msg(format!("Cannot find candidate for {}", key)))
        }
    }
}

impl Drop for CandidateDb {
    fn drop(&mut self) {}
}
