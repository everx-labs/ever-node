use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use ton_types::{error, fail, Result, UInt256};
use ton_block::{ValidatorSet, ValidatorDescr, ShardIdent};

#[derive(Clone)]
pub struct SessionInfo {
    shard: ShardIdent,
    session_id: UInt256,
    set: ValidatorSet,
}

impl SessionInfo {
    pub fn new (shard: ShardIdent, session_id: UInt256, set: ValidatorSet) -> Self {
        SessionInfo { shard, session_id, set }
    }
}

impl Display for SessionInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let set_string : String = self.set.list().iter().map(|x|
            format!("{:?} ", x.adnl_addr.as_ref().map(|y| format!("{:x}", y)))
        ).collect();
        write!(f, "session_id: {:x}, shard: {}, val. set: {}", self.session_id, self.shard, set_string)
    }
}

pub struct SessionHistory {
    info: HashMap <UInt256, SessionInfo>,
    prevs: HashMap <UInt256, Vec<UInt256>>,
    session_id: HashMap <ShardIdent, UInt256>
}

#[allow(dead_code)]
impl SessionHistory {
    pub fn new () -> Self {
        SessionHistory { info: HashMap::new(), prevs: HashMap::new(), session_id: HashMap::new() }
    }

    fn add_session (&mut self, nfo: SessionInfo) {
        //self.session_id.insert(nfo.shard.clone(), nfo.session_id.clone());
        self.info.insert(nfo.session_id.clone(), nfo);
    }

    pub fn set_as_actual_session(&mut self, session_id: UInt256) -> Result<()> {
        match self.info.get(&session_id) {
            Some(info) => {
                self.session_id.insert(info.shard.clone(), session_id);
                Ok(())
            },
            None =>
                fail!("Cannot set {:x} as actual session: the session is not registered.",
                    session_id
                )
        }
    }

    pub fn get_session_info (&self, id: &UInt256) -> Result<&SessionInfo> {
        Ok (self.info.get(id).ok_or_else(
            || error!("INTERNAL ERROR: session {} is missing", id)
        )?)
    }

    pub fn get_session_info_by_shard (&self, shard: &ShardIdent) -> Result<Option<&SessionInfo>> {
        match self.session_id.get(shard) {
            Some(x) => Ok(Some(self.get_session_info(x)?)),
            None => Ok(None)
        }
    }

    pub fn get_catchain_seqno(&self, session: UInt256) -> Result<u32> {
        Ok(self.get_session_info(&session)?.set.catchain_seqno())
    }

    fn list_prev(sessions_list: Vec<UInt256>) -> String {
        sessions_list.iter().map(|x| format!("{:x} ",x)).collect()
    }

    fn append_unique <T> (list: &mut Vec<T>, uniqs: &mut HashSet<T>, new_val_set: &[T])
        where T: Eq + Hash + Clone
    {
        for p in new_val_set.iter() {
            if !uniqs.contains(p) {
                list.push(p.clone());
                uniqs.insert(p.clone());
            }
        }
    }

    fn set_prev(&mut self, new_session: UInt256, olds: Vec<UInt256>) -> Result<()> {
        let mut old_sessions = Vec::new();
        let mut old_sessions_hash : HashSet<UInt256> = HashSet::new();
        Self::append_unique(&mut old_sessions, &mut old_sessions_hash, &olds);

        for ns in old_sessions.iter() {
            if *ns == new_session {
                fail!("Cannot set previous sessions for {:x}: circular dependence, prevs {}",
                    new_session,
                    Self::list_prev(old_sessions)
                );
            }
        };

        if let Some(old_olds) = self.prevs.insert(new_session.clone(), old_sessions.clone()) {
            fail!("Re-initialization of previous sessions for {:x}: {} replaced with {}",
                new_session, Self::list_prev(old_olds), Self::list_prev(old_sessions)
            );
        }

        Ok(())
    }

    pub fn split_session (&mut self, old_session: UInt256, new_sessions: Vec<SessionInfo>) -> Result<()> {
        for ns in new_sessions.into_iter() {
            self.add_session(ns.clone());
            self.set_prev(ns.session_id, vec![old_session.clone()])?;
        }
        Ok(())
    }

    pub fn merge_sessions (&mut self, old1: UInt256, old2: UInt256, new_session: SessionInfo) -> Result<()> {
        self.add_session(new_session.clone());
        self.set_prev(new_session.session_id, vec![old1, old2])
    }

    pub fn transfer_session (&mut self, old: UInt256, new_session: SessionInfo) -> Result<()> {
        self.add_session(new_session.clone());
        self.set_prev(new_session.session_id, vec![old])
    }

    pub fn new_session_after(&mut self, new: SessionInfo, old: Vec<ShardIdent>) -> Result<()> {
        if let Some(x) = self.info.get(&new.session_id) {
            log::trace!(target: "validator_manager", "New session {:x}/{}: session already added: old info {}",
                new.session_id, new.shard, x
            );
            if x.set.catchain_seqno() == new.set.catchain_seqno() && x.set.list() == new.set.list() {
                return Ok(())
            }
            else {
                fail!("New session {:x}/{} add: session info do not match: {} vs. {}", new.session_id, new.shard, new, x)
            }
        }

        log::trace!(target: "validator_manager",
            "New session {:x}/{} after {:?}",
            new.session_id, new.shard, old
        );

        self.add_session(new.clone());
        let mut old_sessions = Vec::new();
        for x in old {
            match self.session_id.get(&x) {
                None => (),
                Some(old_session_id) if *old_session_id == new.session_id =>
                    fail!("Shard {}: trying to add circular dependence for session {:x}", x, old_session_id),
                Some(old_session_id) => old_sessions.push(old_session_id.clone())
            }
        }

        self.set_prev(new.session_id, old_sessions)
    }

    pub fn get_curr_validator_list(&self, session: &UInt256) -> Result<Vec<ValidatorDescr>> {
        let mut list = Vec::new();
        let mut list_hash = HashSet::new();
        Self::append_unique (&mut list, &mut list_hash, self.get_session_info(session)?.set.list());
        Ok(list)
    }

    pub fn get_prev_validator_list(&self, session: &UInt256) -> Result<Vec<ValidatorDescr>> {
        let prevs = self.prevs.get(session).ok_or_else(|| error!("INTERNAL ERROR: session {} is missing", session))?;
        let mut list = Vec::new();
        let mut list_hash = HashSet::new();
        for p in prevs.iter() {
            Self::append_unique (&mut list, &mut list_hash, self.get_session_info(p)?.set.list());
        }
        Ok(list)
    }

    pub fn get_prev_sessions(&self, session: &UInt256) -> Result<Vec<UInt256>> {
        let prevs = self.prevs.get(session).ok_or_else(|| error!("INTERNAL ERROR: session {} is missing", session))?;
        Ok(prevs.clone())
    }

    pub fn garbage_collect (&mut self, current_sessions: HashSet<UInt256>) {
        let mut to_remove: HashSet<UInt256> = HashSet::new();

        for s in self.info.keys() {
            to_remove.insert(s.clone());
            if !self.prevs.contains_key(s) {
                log::error!(target: "validator_manager", "Non-synchronous info and prevs: info {} not found in prevs", s);
            }
        }

        for s in self.prevs.keys() {
            if !self.info.contains_key(s) {
                log::error!(target: "validator", "Non-synchronous set and prevs: prevs {} not found in set", s);
                to_remove.insert(s.clone());
            }
        }

        for s in current_sessions.iter() {
            to_remove.remove(s);
            if let Some(prev_sessions) = self.prevs.get(s) {
                for p in prev_sessions.iter() {
                    to_remove.remove(p);
                }
            }
        }

        self.session_id.retain(|_k,session_id| !to_remove.contains(session_id));
        for r in to_remove.iter() {
            self.prevs.remove(r);
            self.info.remove(r);
        }
    }
}
