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

use std::fmt::{self, Display, Formatter};
use ton_block::{
    GlobalCapabilities,
    Message, EnqueuedMsg, MsgEnvelope, AccountIdPrefixFull, IntermediateAddress, OutMsgQueueKey,
    Serializable, Deserializable, Grams, ShardIdent, AddSub,
};
use ton_executor::{BlockchainConfig, CalcMsgFwdFees};
use ton_types::{error, fail, Result, AccountId, Cell, SliceData, UInt256};


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MsgEnvelopeStuff {
    env: MsgEnvelope,
    msg: Message,
    src_prefix: AccountIdPrefixFull,
    dst_prefix: AccountIdPrefixFull,
    cur_prefix: AccountIdPrefixFull,
    next_prefix: AccountIdPrefixFull,
}

impl MsgEnvelopeStuff {
    pub fn from_envelope(env: MsgEnvelope) -> Result<Self> {
        let msg = env.read_message()?;
        let src = msg.src_ref().ok_or_else(|| error!("source address of message {:x} is invalid", env.message_hash()))?;
        let src_prefix = AccountIdPrefixFull::prefix(src)?;
        let dst = msg.dst_ref().ok_or_else(|| error!("destination address of message {:x} is invalid", env.message_hash()))?;
        let dst_prefix = AccountIdPrefixFull::prefix(dst)?;

        let cur_prefix  = src_prefix.interpolate_addr_intermediate(&dst_prefix, env.cur_addr())?;
        let next_prefix = src_prefix.interpolate_addr_intermediate(&dst_prefix, env.next_addr())?;
        Ok(Self{
            env,
            msg,
            src_prefix,
            dst_prefix,
            cur_prefix,
            next_prefix,
        })
    }
    pub fn new(msg: Message, shard: &ShardIdent, fwd_fee: Grams, use_hypercube: bool) -> Result<Self> {
        let msg_cell = msg.serialize()?;
        let src = msg.src_ref().ok_or_else(|| error!("source address of message {:x} is invalid", msg_cell.repr_hash()))?;
        let src_prefix = AccountIdPrefixFull::prefix(src)?;
        let dst = msg.dst_ref().ok_or_else(|| error!("destination address of message {:x} is invalid", msg_cell.repr_hash()))?;
        let dst_prefix = AccountIdPrefixFull::prefix(dst)?;
        let (cur_addr, next_addr) = perform_hypercube_routing(&src_prefix, &dst_prefix, shard, use_hypercube)?;
        let env = MsgEnvelope::with_routing(
            msg_cell,
            fwd_fee,
            cur_addr,
            next_addr
        );
        let cur_prefix  = src_prefix.interpolate_addr_intermediate(&dst_prefix, env.cur_addr())?;
        let next_prefix = src_prefix.interpolate_addr_intermediate(&dst_prefix, env.next_addr())?;
        Ok(Self{
            env,
            msg,
            src_prefix,
            dst_prefix,
            cur_prefix,
            next_prefix,
        })
    }
    pub fn inner(&self) -> &MsgEnvelope { &self.env }
    pub fn message(&self) -> &Message { &self.msg }
    pub fn message_hash(&self) -> UInt256 { self.env.message_hash() }
    pub fn message_cell(&self) -> Cell { self.env.message_cell() }
    pub fn dst_prefix(&self) -> &AccountIdPrefixFull { &self.dst_prefix }
    pub fn cur_prefix(&self) -> &AccountIdPrefixFull { &self.cur_prefix }
    pub fn next_prefix(&self) -> &AccountIdPrefixFull { &self.next_prefix }
    pub fn fwd_fee_remaining(&self) -> &Grams { self.env.fwd_fee_remaining() }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MsgEnqueueStuff {
    enq: EnqueuedMsg,
    env: MsgEnvelopeStuff,
    created_lt: u64,
    enqueued_lt: u64,
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
        let env = MsgEnvelopeStuff::from_envelope(enq.read_out_msg()?)?;
        let enqueued_lt = enq.enqueued_lt;
        let created_lt = env.message().lt().ok_or_else(|| error!("wrong message type {:x}", env.message_hash()))?;

        Ok(Self{
            enq,
            env,
            created_lt,
            enqueued_lt,
        })
    }
    #[allow(dead_code)]
    pub fn from_envelope(env: MsgEnvelopeStuff, enqueued_lt: u64) -> Result<Self> {
        let created_lt = enqueued_lt;
        let enq = EnqueuedMsg::with_param(enqueued_lt, env.inner())?;
        Ok(Self{
            enq,
            env,
            created_lt,
            enqueued_lt,
        })
    }
    pub fn next_hop(&self, shard: &ShardIdent, enqueued_lt: u64, config: &BlockchainConfig) -> Result<(MsgEnqueueStuff, Grams)> {
        let fwd_prices = config.get_fwd_prices(self.message().is_masterchain());
        let mut fwd_fee_remaining = self.fwd_fee_remaining().clone();
        let transit_fee = fwd_prices.next_fee_checked(&fwd_fee_remaining)?;
        fwd_fee_remaining.sub(&transit_fee)?;

        let use_hypercube = !config.has_capability(GlobalCapabilities::CapOffHypercube);
        let (cur_addr, next_addr) = perform_hypercube_routing(&self.env.next_prefix, &self.env.dst_prefix, shard, use_hypercube)?;
        let cur_prefix  = self.env.next_prefix.interpolate_addr_intermediate(&self.env.dst_prefix, &cur_addr)?;
        let next_prefix = self.env.next_prefix.interpolate_addr_intermediate(&self.env.dst_prefix, &next_addr)?;
        let msg = self.message().clone();
        let env = MsgEnvelope::with_routing(self.message_cell().clone(), fwd_fee_remaining, cur_addr, next_addr);
        let env = MsgEnvelopeStuff {
            env,
            msg,
            src_prefix: self.env.src_prefix.clone(),
            dst_prefix: self.env.dst_prefix.clone(),
            cur_prefix,
            next_prefix,
        };
        let enq = Self {
            enq: EnqueuedMsg::with_param(enqueued_lt, env.inner())?,
            env,
            created_lt: self.created_lt,
            enqueued_lt,
        };
        Ok((enq, transit_fee))
    }
    /// create enqeue for message
    /// create envelope message
    /// all fee from message
    pub fn new(msg: Message, shard: &ShardIdent, fwd_fee: Grams, use_hypercube: bool) -> Result<Self> {
        let created_lt = msg.lt().unwrap_or_default();
        let enqueued_lt = created_lt;
        let env = MsgEnvelopeStuff::new(msg, shard, fwd_fee, use_hypercube)?;
        let enq = EnqueuedMsg::with_param(enqueued_lt, env.inner())?;
        Ok(Self{
            env,
            enq,
            created_lt,
            enqueued_lt,
        })
    }

    pub fn same_workchain(&self) -> bool {
        if let (Some(src), Some(dst)) = (self.message().src_workchain_id(), self.message().dst_workchain_id()) {
            return src == dst
        }
        false
    }
    pub fn enqueued(&self) -> &EnqueuedMsg {
        &self.enq
    }
    pub fn envelope_hash(&self) -> UInt256 {
        self.enq.out_msg_cell().repr_hash()
    }
    // pub fn envelope(&self) -> &MsgEnvelope {
    //     self.env.inner()
    // }
    pub fn envelope_cell(&self) -> Cell {
        self.enq.out_msg_cell()
    }
    pub fn message_hash(&self) -> UInt256 {
        self.env.message_hash()
    }
    pub fn message_cell(&self) -> Cell {
        self.env.message_cell()
    }
    pub fn message(&self) -> &Message {
        &self.env.msg
    }
    pub fn out_msg_key(&self) -> OutMsgQueueKey {
        OutMsgQueueKey::with_account_prefix(&self.next_prefix(), self.message_hash())
    }
    pub fn dst_account_id(&self) -> Result<AccountId> {
        self.message().int_dst_account_id().ok_or_else(|| error!("internal message with hash {:x} \
            has wrong destination address", self.message_hash()))
    }
    pub fn created_lt(&self) -> u64 { self.created_lt }
    pub fn enqueued_lt(&self) -> u64 { self.enqueued_lt }
// Unused 
//    pub fn src_prefix(&self) -> &AccountIdPrefixFull { self.env.src_prefix() }
    pub fn dst_prefix(&self) -> &AccountIdPrefixFull { self.env.dst_prefix() }
    pub fn cur_prefix(&self) -> &AccountIdPrefixFull { self.env.cur_prefix() }
    pub fn next_prefix(&self) -> &AccountIdPrefixFull { self.env.next_prefix() }
    pub fn fwd_fee_remaining(&self) -> &Grams { self.env.fwd_fee_remaining() }
}

impl Display for MsgEnqueueStuff {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f, 
            "message with (lt,hash)=({},{:x}), enqueued_lt={}", 
            self.created_lt, 
            self.message_hash(),
            self.enqueued_lt
        )?;
        if f.alternate() {
            writeln!(f)?;
            writeln!(f, "src: {}", self.env.src_prefix)?;
            writeln!(f, "dst: {}", self.env.dst_prefix)?;
            writeln!(f, "cur: {}", self.env.cur_prefix)?;
            writeln!(f, "nxt: {}", self.env.next_prefix)?;
        }
        Ok(())
    }
}

/// Returns count of the first bits matched in both addresses
pub fn count_matching_bits(this: &AccountIdPrefixFull, other: &AccountIdPrefixFull) -> u8 {
    if this.workchain_id != other.workchain_id {
        (this.workchain_id ^ other.workchain_id).leading_zeros() as u8
    } else if this.prefix != other.prefix {
        32 + (this.prefix ^ other.prefix).leading_zeros() as u8
    } else {
        96
    }
}

/// Performs Hypercube Routing from src to dest address.
/// Result: (transit_addr_dest_bits, nh_addr_dest_bits)
pub fn perform_hypercube_routing(
    src: &AccountIdPrefixFull,
    dest: &AccountIdPrefixFull,
    cur_shard: &ShardIdent,
    use_hypercube: bool,
) -> Result<(IntermediateAddress, IntermediateAddress)> {
    if use_hypercube {
        let transit = src.interpolate_addr_intermediate(dest, &IntermediateAddress::default())?;
        if !cur_shard.contains_full_prefix(&transit) {
            fail!("Shard {} must fully contain transit prefix {}", cur_shard, transit)
        }

        if cur_shard.contains_full_prefix(&dest) {
            // If destination is in this shard, set cur:=next_hop:=dest
            return Ok((IntermediateAddress::full_dest(), IntermediateAddress::full_dest()))
        }

        if transit.is_masterchain() || dest.is_masterchain() {
            // Route messages to/from masterchain directly
            return Ok((IntermediateAddress::default(), IntermediateAddress::full_dest()))
        }

        if transit.workchain_id != dest.workchain_id {
            return Ok((IntermediateAddress::default(), IntermediateAddress::use_dest_bits(32)?))
        }

        let prefix = cur_shard.shard_prefix_with_tag();
        let x = prefix & (prefix - 1);
        let y = prefix | (prefix - 1);
        let t = transit.prefix;
        let q = dest.prefix ^ t;
        // Top i bits match, next 4 bits differ:
        let mut i = q.leading_zeros() as u8 & 0xFC;
        let mut m = u64::max_value() >> i;
        while i < 60 {
            m >>= 4;
            let h = t ^ (q & !m);
            i += 4;
            if h < x || h > y {
                let cur_prefix = IntermediateAddress::use_dest_bits(28 + i)?;
                let next_prefix = IntermediateAddress::use_dest_bits(32 + i)?;
                return Ok((cur_prefix, next_prefix))
            }
        }
        fail!("cannot perform hypercube routing from {} to {} via {}", src, dest, cur_shard)
    } else if cur_shard.contains_full_prefix(&dest) {
        Ok((IntermediateAddress::full_dest(), IntermediateAddress::full_dest()))
    } else {
        Ok((IntermediateAddress::default(), IntermediateAddress::full_dest()))
    }
}
