use std::fmt::{self, Display, Formatter};
use ton_block::{
    Message, EnqueuedMsg, MsgEnvelope, AccountIdPrefixFull, OutMsgQueueKey,
    Deserializable, Grams, ShardIdent, ConfigParams, AddSub,
};
use ton_executor::{CalcMsgFwdFees};
use ton_types::{error, fail, Result, AccountId, Cell, SliceData, UInt256};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MsgEnqueueStuff {
    enq: EnqueuedMsg,
    env: MsgEnvelope,
    msg: Message,
    created_lt: u64,
    enqueued_lt: u64,
    src_prefix: AccountIdPrefixFull,
    dst_prefix: AccountIdPrefixFull,
    cur_prefix: AccountIdPrefixFull,
    next_prefix: AccountIdPrefixFull,
}

impl MsgEnqueueStuff {
    pub fn construct_from(slice: &mut SliceData, created_lt: u64) -> Result<Self> {
        let enq = EnqueuedMsg::construct_from(slice)?;
        Self::from_enqueue_and_lt(enq, created_lt)
    }
    pub fn from_enqueue_and_lt(enq: EnqueuedMsg, created_lt: u64) -> Result<Self> {
        let enq = Self::from_enqueue(enq)?;
        if enq.created_lt != created_lt {
            fail!("Wrong LT when unpack EnqueuedMsg with key {:x}", enq.out_msg_key())
        }
        Ok(enq)
    }
    pub fn from_enqueue(enq: EnqueuedMsg) -> Result<Self> {
        let env = enq.read_out_msg()?;
        let msg = env.read_message()?;
        let enqueued_lt = enq.enqueued_lt;
        let created_lt = msg.lt().ok_or_else(|| error!("wrong message type {:x}", env.message_cell().repr_hash()))?;
        let src_prefix = AccountIdPrefixFull::prefix(&msg.src().unwrap_or_default())?;
        let dst_prefix = AccountIdPrefixFull::prefix(&msg.dst().unwrap_or_default())?;

        let cur_prefix  = src_prefix.interpolate_addr_intermediate(&dst_prefix, env.cur_addr())?;
        let next_prefix = src_prefix.interpolate_addr_intermediate(&dst_prefix, env.next_addr())?;

        Ok(Self{
            enq,
            env,
            msg,
            created_lt,
            enqueued_lt,
            src_prefix,
            dst_prefix,
            cur_prefix,
            next_prefix,
        })
    }
    pub fn next_hop(&self, shard: &ShardIdent, enqueued_lt: u64, config: &ConfigParams) -> Result<(MsgEnqueueStuff, Grams)> {
        let fwd_prices = config.fwd_prices(self.msg.is_masterchain())?;
        let mut fwd_fee_remaining = self.fwd_fee_remaining().clone();
        let transit_fee = fwd_prices.next_fee(&fwd_fee_remaining);
        fwd_fee_remaining.sub(&transit_fee)?;

        let route_info = self.next_prefix.perform_hypercube_routing(&self.dst_prefix, shard, &Default::default())?;
        let cur_prefix  = self.next_prefix.interpolate_addr_intermediate(&self.dst_prefix, &route_info.0)?;
        let next_prefix = self.next_prefix.interpolate_addr_intermediate(&self.dst_prefix, &route_info.1)?;
        let env = MsgEnvelope::with_routing(self.message_cell().clone(), fwd_fee_remaining, route_info.0, route_info.1);
        let enq = Self {
            enq: EnqueuedMsg::with_param(enqueued_lt, &env)?,
            env,
            msg: self.msg.clone(),
            created_lt: self.created_lt,
            enqueued_lt,
            src_prefix: self.src_prefix.clone(),
            dst_prefix: self.dst_prefix.clone(),
            cur_prefix,
            next_prefix,
        };
        Ok((enq, transit_fee))
    }
    /// create enqeue for message
    /// create envelope message
    /// all fee from message
    pub fn new(msg: &Message, shard: &ShardIdent) -> Result<Self> {
        let header = msg.int_header().ok_or_else(|| error!("message must be internal"))?;
        let fwd_fee = &header.fwd_fee;
        let created_lt = header.created_lt;
        let enqueued_lt = header.created_lt;
        let src_prefix = AccountIdPrefixFull::prefix(&msg.src().unwrap_or_default())?;
        let dst_prefix = AccountIdPrefixFull::prefix(&header.dst)?;
        let env = if !shard.contains_full_prefix(&dst_prefix) {
            MsgEnvelope::hypercube_routing(msg, shard, fwd_fee.clone())?
        } else {
            MsgEnvelope::with_message_and_fee(msg, fwd_fee.clone())?
        };
        let cur_prefix  = src_prefix.interpolate_addr_intermediate(&dst_prefix, env.cur_addr())?;
        let next_prefix = src_prefix.interpolate_addr_intermediate(&dst_prefix, env.next_addr())?;
        let msg = msg.clone();
        let enq = EnqueuedMsg::with_param(enqueued_lt, &env)?;
        Ok(Self{
            enq,
            env,
            msg,
            created_lt,
            enqueued_lt,
            src_prefix,
            dst_prefix,
            cur_prefix,
            next_prefix,
        })
    }

    pub fn same_workchain(&self) -> bool {
        if let (Some(src), Some(dst)) = (self.msg.src(), self.msg.dst()) {
            return src.get_workchain_id() == dst.get_workchain_id()
        }
        false
    }
    pub fn enqueued(&self) -> &EnqueuedMsg {
        &self.enq
    }
    pub fn envelope_hash(&self) -> UInt256 {
        self.enq.out_msg_cell().repr_hash()
    }
    pub fn envelope(&self) -> &MsgEnvelope {
        &self.env
    }
    pub fn message_hash(&self) -> UInt256 {
        self.env.message_cell().repr_hash()
    }
    pub fn message_cell(&self) -> &Cell {
        self.env.message_cell()
    }
    pub fn message(&self) -> &Message {
        &self.msg
    }
    pub fn withdraw(self) -> (EnqueuedMsg, MsgEnvelope, Message) {
        (self.enq, self.env, self.msg)
    }
    pub fn out_msg_key(&self) -> OutMsgQueueKey {
        OutMsgQueueKey::with_account_prefix(&self.next_prefix, self.message_hash())
    }
    pub fn dst_account_id(&self) -> Result<AccountId> {
        self.msg.int_dst_account_id().ok_or_else(|| error!("internal message with hash {:x} \
            has wrong destination address", self.message_hash()))
    }
    pub fn fwd_fee_remaining(&self) -> &Grams {
        self.env.fwd_fee_remaining()
    }
    pub fn created_lt(&self) -> u64 { self.created_lt }
    pub fn enqueued_lt(&self) -> u64 { self.enqueued_lt }
    pub fn src_prefix(&self) -> &AccountIdPrefixFull { &self.src_prefix }
    pub fn dst_prefix(&self) -> &AccountIdPrefixFull { &self.dst_prefix }
    pub fn cur_prefix(&self) -> &AccountIdPrefixFull { &self.cur_prefix }
    pub fn next_prefix(&self) -> &AccountIdPrefixFull { &self.next_prefix }
}

impl Display for MsgEnqueueStuff {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f, 
            "message with (lt,hash)=({},{}), enqueued_lt={}", 
            self.created_lt, 
            self.message_hash().to_hex_string(),
            self.enqueued_lt
        )?;
        if f.alternate() {
            writeln!(f)?;
            writeln!(f, "src: {}", self.src_prefix)?;
            writeln!(f, "dst: {}", self.dst_prefix)?;
            writeln!(f, "cur: {}", self.cur_prefix)?;
            writeln!(f, "nxt: {}", self.next_prefix)?;
        }
        Ok(())
    }
}
