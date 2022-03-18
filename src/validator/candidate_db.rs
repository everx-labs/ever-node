/*
* Copyright (C) 2019-2021 TON Labs. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific TON DEV software governing permissions and
* limitations under the License.
*/

use ever_crypto::Ed25519KeyOption;
use catchain::{BlockHash, CatchainFactory};
use storage::{
    db_impl_single,
    db::traits::{DbKey, KvcWriteable},
    traits::Serializable
};
use std::{fmt::{Formatter, Display}, io::{Read, Write}, sync::Arc};
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{error, Result};
use validator_session::{ValidatorBlockCandidate, ValidatorBlockId};

#[derive(PartialEq, Eq, Hash)]
pub struct CandidateDbKey {
    root_hash: BlockHash,
}

impl Display for CandidateDbKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_string())
    }
}

impl DbKey for CandidateDbKey {
    fn key_name(&self) -> &'static str { "CandidateDbKey" }
    fn as_string(&self) -> String {
        hex::encode(self.root_hash.as_slice())
    }
    fn key(&self) -> &[u8] {
        self.root_hash.as_slice()
    }
}

// This wrapper structure has no separated meanging, and created only for library compatibility issues.
pub struct ValidatorBlockCandidateWrapper {
    candidate: ValidatorBlockCandidate
}

impl Serializable for ValidatorBlockCandidateWrapper {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        let candidate = ton_api::ton::db::candidate::Candidate {
            source : self.candidate.public_key.into_public_key_tl()?,
            id : BlockIdExt::with_params(
                ShardIdent::default(), 0, // Shard & seqno: not needed
                self.candidate.id.root_hash.clone(),
                self.candidate.id.file_hash.clone()
            ).into(),
            data : self.candidate.data.data().clone(),
            collated_data : self.candidate.collated_data.data().clone()
        };
        let mut serializer = ton_api::Serializer::new(writer);
        serializer.write_bare(&candidate)
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> where Self: Sized {
        match ton_api::Deserializer::new(reader).read_bare() {
            Ok(ton_api::ton::db::candidate::Candidate { source, id, data, collated_data }) => {
                let candidate = ValidatorBlockCandidate {
                    public_key: Ed25519KeyOption::from_public_key_tl(&source)?,
                    id: ValidatorBlockId { root_hash: id.root_hash.into(), file_hash: id.file_hash.into() },
                    collated_file_hash: catchain::utils::get_hash(&data),
                    data: CatchainFactory::create_block_payload(data),
                    collated_data: CatchainFactory::create_block_payload(collated_data)
                };
                Ok(ValidatorBlockCandidateWrapper { candidate } )
            }
            Err(e) => Err(e)
        }
    }
}

db_impl_single!(CandidateDb, KvcWriteable, CandidateDbKey);

impl CandidateDb {
    pub fn save(&self, candidate: ValidatorBlockCandidate) -> Result<()> {
        let key = CandidateDbKey {
            root_hash: candidate.id.root_hash.clone()
        };
        self.put(&key, &ValidatorBlockCandidateWrapper { candidate }.to_vec()?)
    }
    pub fn load(
        &self,
        root_hash: &BlockHash,
    ) -> Result<Arc<ValidatorBlockCandidate>> {
        let key = CandidateDbKey {
            root_hash: root_hash.clone(),
        };
        match self.try_get(&key) {
            Ok(Some(db_slice)) => {
                let value = ValidatorBlockCandidateWrapper::from_slice(db_slice.as_ref())?;
                Ok(Arc::new(value.candidate))
            }
            Ok(None) => Err(error!("Cannot find candidate for {}", key)),
            Err(e) => Err(error!("Operational problem encountered: {}", e))
        }
    }
}
