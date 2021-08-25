use adnl::common::KeyOption;
use catchain::{PublicKey, BlockHash, CatchainFactory};
use storage::{db_impl_serializable, db::traits::{DbKey, KvcWriteable}, traits::Serializable};
use std::{fmt::{Formatter, Display}, io::{Read, Write}, sync::Arc};
use ton_block::{BlockIdExt, ShardIdent};
use ton_types::{UInt256, Result};
use validator_session::{ValidatorBlockCandidate, ValidatorBlockId};

#[derive(PartialEq, Eq, Hash)]
pub struct CandidateDbKey {
    source: UInt256,
    root_hash: BlockHash,
    file_hash: BlockHash,
    collated_data_hash: BlockHash,
}

impl Display for CandidateDbKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "source {}, rh {}, fh {}, collated_data_hash {}",
            self.source.to_hex_string(), self.root_hash, self.file_hash, self.collated_data_hash)
    }
}

impl DbKey for CandidateDbKey {
    fn key_name(&self) -> &'static str { "CandidateDbKey" }
    fn as_string(&self) -> String {
        let mut key = self.source.to_hex_string() + " " + &self.root_hash.to_hex_string().to_string() + " ";
        key += &self.file_hash.to_hex_string().to_string();
        key += " ";
        key += &self.collated_data_hash.to_hex_string().to_string();
        return key;
    }
    fn key(&self) -> &[u8] {
        self.root_hash.key()
    }
}

// This wrapper structure has no separated meanging, and created only for library compatibility issues.
pub struct ValidatorBlockCandidateWrapper {
    candidate: ValidatorBlockCandidate
}

impl Serializable for ValidatorBlockCandidateWrapper {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut serializer = ton_api::Serializer::new(writer);

        serializer.write_bare(&ton_api::ton::db::candidate::Candidate {
            source : self.candidate.public_key.into_tl_public_key()?,
            id : BlockIdExt::with_params(
                ShardIdent::default(), 0, // Shard & seqno: not needed
                self.candidate.id.root_hash.clone(),
                self.candidate.id.file_hash.clone()
            ).into(),
            data : self.candidate.data.data().clone(),
            collated_data : self.candidate.collated_data.data().clone()
        })?;

        return Ok(());
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self> where Self: Sized {
        match ton_api::Deserializer::new(reader).read_bare() {
            Err(e) => Err(e),
            Ok(ton_api::ton::db::candidate::Candidate { source, id, data, collated_data }) =>
                Ok(ValidatorBlockCandidateWrapper {candidate : ValidatorBlockCandidate {
                    public_key: Arc::new(KeyOption::from_tl_public_key(&source)?),
                    id: ValidatorBlockId { root_hash: id.root_hash.into(), file_hash: id.file_hash.into() },
                    collated_file_hash: catchain::utils::get_hash(&data),
                    data: CatchainFactory::create_block_payload(data),
                    collated_data: CatchainFactory::create_block_payload(collated_data)
                }})
        }
    }
}

db_impl_serializable!(CandidateDbImpl, KvcWriteable, CandidateDbKey, ValidatorBlockCandidateWrapper);

pub struct CandidateDb {
    path : String,
    db : Option<CandidateDbImpl>
}

impl CandidateDb {
    pub fn new(pth : String) -> Self {
        return CandidateDb{
            path: pth,
            db: None
        };
    }

    pub fn start(&mut self) -> Result<()> {
        match &self.db {
            None => {
                self.db = Some(CandidateDbImpl::with_path(&self.path));
                Ok(())
            },
            Some(_) => {
                Err(failure::err_msg(format!("CandidateDB ({}) is already started", self.path)))
            }
        }
    }

    pub fn destroy(&mut self) -> Result<()> {
        if let Some(db) = &mut self.db {
            db.destroy()?;
            self.db = None
        }
        Ok(())
    }

    fn get_db(&self) -> Result<&CandidateDbImpl> {
        match &self.db {
            Some(x) => Ok(&x),
            None => Err(failure::err_msg(
                format!("Accessing CandidateDB ({}), which is not opened!", self.path)
            ))
        }
    }

    pub async fn save(&self, candidate: ValidatorBlockCandidate) -> Result<()> {
        let pub_key = candidate.public_key.pub_key()?;
        let key = CandidateDbKey {
            source: UInt256::from(pub_key),
            root_hash: candidate.id.root_hash.clone(),
            file_hash: candidate.id.file_hash.clone(),
            collated_data_hash: catchain::utils::get_hash (candidate.collated_data.data())
        };

        return self.get_db()?.put_value(&key, ValidatorBlockCandidateWrapper{candidate});
    }

    pub async fn load(&self, source: PublicKey,
            root_hash: BlockHash,
            file_hash: BlockHash,
            collated_data_hash: BlockHash
    ) -> Result<Arc<ValidatorBlockCandidate>> {
        let pub_key = source.pub_key()?;
        let key = CandidateDbKey { source: UInt256::from(pub_key), root_hash, file_hash, collated_data_hash };

        match self.get_db()?.try_get_value(&key) {
            Ok(Some(value)) =>
                Ok(Arc::new(value.candidate)),
            Ok(None) => Err(failure::err_msg(format!("Cannot find candidate for {}", key))),
            Err(e) => Err(failure::err_msg(format!("Operational problem encountered: {}", e))),
        }
    }
}

pub struct UnitKey {
}

impl DbKey for UnitKey {
    fn key_name(&self) -> &'static str { "LastRotationBlockDbKey_UnitKey" }
    fn as_string(&self) -> String {
        return "()".as_string();
    }
    fn key(&self) -> &[u8] { &[] }
}

db_impl_serializable!(LastRotationBlockDbImpl, KvcWriteable, UnitKey, BlockIdExt);

pub struct LastRotationBlockDb {
    db: LastRotationBlockDbImpl
}

impl LastRotationBlockDb {
    pub fn new(path : String) -> Self {
        return LastRotationBlockDb {
            db: LastRotationBlockDbImpl::with_path(&path)
        };
    }

    pub fn get_last_rotation_block_id(&mut self) -> Result<Option<BlockIdExt>> {
        self.db.try_get_value(&UnitKey{})
    }

    pub fn set_last_rotation_block_id(&mut self, info: &BlockIdExt) -> Result<()> {
        self.db.put_value(&UnitKey{}, info)
    }

    pub fn clear_last_rotation_block_id(&mut self) -> Result<()> {
        self.db.delete(&UnitKey{})
    }
}
