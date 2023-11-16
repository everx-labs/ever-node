use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    sync::Arc
};
use dashmap::DashMap;
use ton_types::{error, fail, Result, UInt256};
use ton_block::{ValidatorSet, ShardIdent, ValidatorDescr};
use crate::validator::validator_utils::validatorset_to_string;

/// 1) shard, cc_seqno -> session_id
///    added around session start
/// implement: function "add_session_info (shard, shard_cc_seqno, session_id)" -- into MessageCache
///    --- adds into current master_cc session
///
/// previous sessions for (shard,cc):
///    a) get prev block(s)
///    b) find block(s) gen. (shard(), gen_catchain_seqno())
///    c) get session ids for (shard(), gen_catchain_seqno())
///    d) if not found: warning "cannot find session info for ..."
///
/// implement: function "get_session_info_for_block (block_id_ext)" -- into RempBlockParser
/// implement: function "get_session_info (shard, shard_cc_seqno)" -- into MessageCache
///    --- searches through all master_cc sessions
/// implement: general function, finding previous sessions for (prev_blocks)
///    --- traverse backwards by blocks (walking)
///    --- when session change, record that fact functions to be called from remp start?


#[derive(Clone, PartialEq, Eq)]
pub struct GeneralSessionInfo {
    pub shard: ShardIdent,
    pub opts_hash: UInt256,
    pub catchain_seqno: u32,
    pub key_seqno: u32,
    pub max_vertical_seqno: u32,
}

impl std::fmt::Display for GeneralSessionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}, cc {}", self.shard, self.catchain_seqno)
    }
}

impl GeneralSessionInfo {
    pub fn with_shard_cc_seqno(&self, shard: ShardIdent, new_cc_seqno: u32) -> Self {
        Self { catchain_seqno: new_cc_seqno, shard, ..self.clone() }
    }
}

#[derive(Clone)]
pub struct SessionValidatorsInfo {
    general_info: Arc<GeneralSessionInfo>,
    prev_block_seqno: Option<u32>,
    pub session_id: UInt256,
    pub validator_set: ValidatorSet,
}

impl SessionValidatorsInfo {
    pub fn new (general_info: Arc<GeneralSessionInfo>, session_id: UInt256, validator_set: ValidatorSet, prev_block_seqno: Option<u32>) -> Self {
        SessionValidatorsInfo { general_info, session_id, validator_set, prev_block_seqno }
    }

    pub fn shard (&self) -> ShardIdent {
        return self.general_info.shard.clone()
    }
}

impl Display for SessionValidatorsInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let set_string = validatorset_to_string(&self.validator_set);
        write!(f, "general_info: {}, session_id: {:x}, val. set: {}, prev block: {:?}",
               self.general_info, self.session_id, set_string, self.prev_block_seqno
        )
    }
}

impl PartialEq<Self> for SessionValidatorsInfo {
    fn eq(&self, other: &Self) -> bool {
        return self.general_info == other.general_info &&
            self.session_id == other.session_id &&
            self.validator_set == other.validator_set
    }
}

impl Eq for SessionValidatorsInfo {}

#[derive(PartialEq, Eq)]
pub struct SessionValidatorsList {
    sessions: HashMap<ShardIdent, Arc<SessionValidatorsInfo>>
}

impl SessionValidatorsList {
    pub fn new() -> Self {
        Self { sessions: HashMap::default() }
    }

    pub fn add_session(&mut self, session: Arc<SessionValidatorsInfo>) -> Result<()> {
        match self.sessions.insert(session.shard().clone(), session.clone()) {
            None => Ok(()),
            Some(x) if x == session => Ok(()),
            Some(x) => {
                fail!("SessionValidatorList: different session for shard {}: {} replaced by {}",
                    session.shard(), x, session
                )
            }
        }
    }

    pub fn get_session_for_shard(&self, shard: &ShardIdent) -> Option<Arc<SessionValidatorsInfo>> {
        self.sessions.get(shard).cloned()
    }

    pub fn get_shards_sorted(&self) -> Vec<ShardIdent> {
        let mut shards = self.sessions.keys().cloned().collect::<Vec<ShardIdent>>();
        shards.sort();
        shards
    }

    pub fn get_validator_list (&self) -> Result<Vec<ValidatorDescr>> {
        let mut prev_validator_list = Vec::new();

        for shard in self.get_shards_sorted().iter() {
            for val in self.sessions.get(shard).ok_or_else(
                || error!("Cannot retrieve shard {} from SessionValidatorList", shard)
            )?.validator_set.list() {
                if !prev_validator_list.contains(val) {
                    prev_validator_list.push(val.clone());
                }
            }
        }

        Ok(prev_validator_list)
    }

    pub fn get_maximal_cc_seqno (&self) -> Option<u32> {
        self.sessions.iter().map(|(_shard,info)| info.general_info.catchain_seqno).max()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }
}

impl Display for SessionValidatorsList {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SessionValidatorsList: [{}]", self.sessions.iter().map(
            |(shard,session)| format!(" {}: {} ", shard, session)
        ).collect::<String>())
    }
}

pub struct SessionValidatorsCache {
    session_info: DashMap<(ShardIdent, u32), Arc<SessionValidatorsInfo>>,
    session_prev_list: DashMap<(ShardIdent, u32), Arc<SessionValidatorsList>>
}

impl SessionValidatorsCache {
    pub fn new() -> Self {
        SessionValidatorsCache { session_info: DashMap::new(), session_prev_list: DashMap::new() }
    }

    pub fn add_info(&self, cc_seqno: u32, session_info: SessionValidatorsInfo) -> Result<()> {
        let session_info = Arc::new(session_info);
        if let Some(prev_info) = self.session_info.insert((session_info.shard().clone(), cc_seqno), session_info.clone()) {
            if prev_info != session_info {
                log::error!(target: "remp", "SessionValidatorsInfo for ({}, cc_seqno {}) changed: '{}' to '{}'", session_info.shard(), cc_seqno, session_info, prev_info);
            }
        }

        Ok(())
    }

    pub fn get_info(&self, shard: &ShardIdent, cc_seqno: u32) -> Option<Arc<SessionValidatorsInfo>> {
        self.session_info.get(&(shard.clone(), cc_seqno)).map(|si| si.value().clone())
    }

    pub fn add_prev_list(&self, shard: &ShardIdent, cc_seqno: u32, session_prev_list: Arc<SessionValidatorsList>) -> Result<()> {
        if let Some(old_list) = self.session_prev_list.insert((shard.clone(), cc_seqno), session_prev_list.clone()) {
            if old_list != session_prev_list {
                log::error!(target: "remp", "SessionValidatorsInfo for ({}, cc_seqno {}) changed: '{}' to '{}'", shard, cc_seqno, old_list, session_prev_list);
            }
        }

        Ok(())
    }

    pub fn get_prev_list(&self, shard: &ShardIdent, cc_seqno: u32) -> Option<Arc<SessionValidatorsList>> {
        self.session_prev_list.get(&(shard.clone(), cc_seqno)).map(|sl| sl.value().clone())
    }
}
