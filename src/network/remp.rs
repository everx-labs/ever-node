/*
* Copyright (C) 2019-2023 EverX. All Rights Reserved.
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

#[cfg(feature = "telemetry")]
use crate::validator::telemetry::RempCoreTelemetry;

use adnl::{common::{AdnlPeers, Subscriber, TaggedByteSlice}, node::AdnlNode};
use std::{
    cmp::min, collections::HashMap, sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::{Duration, Instant},
};
use ton_api::{
    ton::ton_node::{
        RempMessage, RempReceipt, RempMessageStatus, RempMessageStatusCompact, 
        RempReceiptCompact, RempCombinedReceipt
    },
    serialize_boxed, deserialize_boxed,
};
#[cfg(feature = "telemetry")]
use ton_api::tag_from_boxed_type;
use ton_block::BlockIdExt;
use ton_types::{error, fail, KeyId, Result, UInt256};

#[async_trait::async_trait]
pub trait RempMessagesSubscriber: Sync + Send {
    async fn new_remp_message(&self, message: RempMessage, source: &Arc<KeyId>) -> Result<()>;
}
#[async_trait::async_trait]
pub trait RempReceiptsSubscriber: Sync + Send {
    async fn new_remp_receipt(&self, receipt: RempReceipt, source: &Arc<KeyId>) -> Result<()>;
}

#[derive(Debug)]
struct ReceiptStuff {
    pub to: Arc<KeyId>,
    pub receipt: RempReceipt,
    // pub sign: Vec<u8>,
    pub self_adnl_id: Arc<KeyId>,
}

pub struct RempNode {
    adnl: Arc<AdnlNode>,
    local_key: Arc<KeyId>,
    messages_subscriber: tokio::sync::OnceCell<Arc<dyn RempMessagesSubscriber>>,
    receipts_subscriber: tokio::sync::OnceCell<Arc<dyn RempReceiptsSubscriber>>,
    receipts_sender: tokio::sync::OnceCell<tokio::sync::mpsc::UnboundedSender<ReceiptStuff>>,
    receipts_in_channel: Arc<AtomicU64>,
    #[cfg(feature = "telemetry")]
    tag_message: u32,
    //#[cfg(feature = "telemetry")]
    //tag_receipt: u32,
    #[cfg(feature = "telemetry")]
    telemetry: tokio::sync::OnceCell<Arc<RempCoreTelemetry>>,
}

impl RempNode {
    pub fn new(adnl: Arc<AdnlNode>, key_tag: usize) -> Result<Self> {
        let local_key = adnl.key_by_tag(key_tag)?.id().clone();
        Ok(Self {
            adnl,
            local_key,
            messages_subscriber: Default::default(),
            receipts_subscriber: Default::default(),
            receipts_sender: Default::default(),
            receipts_in_channel: Arc::new(AtomicU64::new(0)),
            #[cfg(feature = "telemetry")]
            tag_message: tag_from_boxed_type::<RempMessage>(),
            //#[cfg(feature = "telemetry")]
            //tag_receipt: tag_from_boxed_type::<RempReceipt>(),
            #[cfg(feature = "telemetry")]
            telemetry: Default::default(),
        })
    }

    pub fn start(&self) -> Result<()> {
        let (receipts_sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        self.receipts_sender.set(receipts_sender).map_err(|_| error!("Can't set receipts_sender"))?;
        start_receipts_worker(
            Arc::new(SendCombinedReceiptByAdnl::new(self.adnl.clone())),
            receiver,
            self.receipts_in_channel.clone(),
            #[cfg(feature = "telemetry")]
            self.telemetry.get().ok_or_else(|| error!("telemetry is not set"))?.clone()
        );
        Ok(())
    }

    /* send via query
    pub async fn send_message(&self, to: Arc<KeyId>, message: RempMessage) -> Result<()> {
        let id = message.id().clone();
        let peers = AdnlPeers::with_keys(self.local_key.clone(), to);
        let query = TaggedTlObject {
            object: TLObject::new(message),
            #[cfg(feature = "telemetry")]
            tag: self.tag_message
        };
        match self.adnl.clone().query(&query, &peers, None).await {
            Err(e) => fail!("Error while sending RempMessage {:x} via query: {}", id, e),
            Ok(None) => fail!("sending RempMessage {:x} via query: timeout", id),
            Ok(Some(_)) => Ok(())
        }
    }*/

    pub fn send_message(&self, to: Arc<KeyId>, message: &RempMessage) -> Result<()> {
        let id = message.id().clone();
        let peers = AdnlPeers::with_keys(self.local_key.clone(), to);
        let tagged_data = TaggedByteSlice {
            object: &serialize_boxed(message)?,
            #[cfg(feature = "telemetry")]
            tag: self.tag_message
        };
        if let Err(e) = self.adnl.send_custom(&tagged_data, &peers) {
            fail!("Error while sending RempMessage {:x} via message: {}", id, e);
        }
        Ok(())
    }

    /*pub async fn send_receipt(&self, to: Arc<KeyId>, id: &UInt256, receipt: RempSignedReceipt) -> Result<()> {
        let peers = AdnlPeers::with_keys(self.local_key.clone(), to);
        let query = TaggedTlObject {
            object: TLObject::new(receipt),
            #[cfg(feature = "telemetry")]
            tag: self.tag_receipt
        };
        match self.adnl.clone().query(&query, &peers, None).await {
            Err(e) => fail!("Error while sending RempSignedReceipt {:x}: {}", id, e),
            Ok(None) => fail!("sending RempSignedReceipt {:x}: timeout", id),
            Ok(Some(_)) => Ok(())
        }
    }*/

    pub async fn combine_and_send_receipt(
        &self, 
        to: Arc<KeyId>, 
        receipt: RempReceipt,
        //sign: Vec<u8>,
        self_adnl_id: Arc<KeyId>, 
    ) -> Result<()> {
        let message_id = receipt.message_id().clone();
        self.receipts_sender
            .get().ok_or_else(|| error!("receipts_sender is not set"))?
            .send(ReceiptStuff{to, receipt, self_adnl_id})?;
        let in_channel = self.receipts_in_channel.fetch_add(1, Ordering::Relaxed) + 1;
        log::debug!(
            "combine_and_send_receipt: {:x}, channel's load: {}", message_id, in_channel
        );
        #[cfg(feature = "telemetry")]
        self.telemetry
            .get().ok_or_else(|| error!("telemetry is not set"))?
            .receipts_queue_in(in_channel);
        Ok(())
    }

    pub fn set_messages_subscriber(&self, subscriber: Arc<dyn RempMessagesSubscriber>) -> Result<()> {
        self.messages_subscriber.set(subscriber).map_err(|_| error!("Can't set remp messages subscriber"))?;
        Ok(())
    }

    pub fn set_receipts_subscriber(&self, subscriber: Arc<dyn RempReceiptsSubscriber>) -> Result<()> {
        self.receipts_subscriber.set(subscriber).map_err(|_| error!("Can't set remp receipts subscriber"))?;
        Ok(())
    }

    #[cfg(feature = "telemetry")]
    pub fn set_telemetry(&self, telemetry: Arc<RempCoreTelemetry>) -> Result<()> {
        self.telemetry.set(telemetry).map_err(|_| error!("Can't set telemetry"))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Subscriber for RempNode {
    /*async fn try_consume_query(
        &self, 
        object: TLObject, 
        peers: &AdnlPeers
    ) -> Result<QueryResult> {
        let object = match object.downcast::<RempMessage>() {
            Ok(query) => {
                let subscriber = self.messages_subscriber.get()
                    .ok_or_else(|| error!("messages_subscriber is not set"))?;
                subscriber.new_remp_message(query, peers.other()).await?;
                return QueryResult::consume_boxed(
                    TLObject::new(RempReceived::TonNode_RempReceived),
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(object) => object
        };
        match object.downcast::<RempSignedReceipt>() {
            Ok(query) => {
                let subscriber = self.receipts_subscriber.get()
                    .ok_or_else(|| error!("receipts_subscriber is not set"))?;
                subscriber.new_remp_receipt(query, peers.other()).await?;
                QueryResult::consume_boxed(
                    TLObject::new(RempReceived::TonNode_RempReceived),
                    #[cfg(feature = "telemetry")]
                    None
                )
            },
            Err(object) => {
                Ok(QueryResult::Rejected(object))
            }
        }
    }*/

    async fn try_consume_custom(&self, data: &[u8], peers: &AdnlPeers) -> Result<bool> {
        match deserialize_boxed(data) {
            Ok(object) => {
                let object = match object.downcast::<RempCombinedReceipt>() {
                    Ok(combined_receipt) => {
                        let subscriber = self.receipts_subscriber.get()
                            .ok_or_else(|| error!("receipts_subscriber is not set"))?;
                        for r in expand_combined_receipt(&combined_receipt)? {
                            subscriber.new_remp_receipt(r, peers.other()).await?;
                        }
                        return Ok(true);
                    }
                    Err(object) => object
                };
                match object.downcast::<RempMessage>() {
                    Ok(query) => {
                        let subscriber = self.messages_subscriber.get()
                            .ok_or_else(|| error!("messages_subscriber is not set"))?;
                        subscriber.new_remp_message(query, peers.other()).await?;
                        return Ok(true);
                    }
                    Err(_) => return Ok(false)
                };
            },
            Err(_) => {
                return Ok(false);
            }
        }
    }
}

const RECEIPTS_SEND_PERIOD_MS: u64 = 1000;
const PACKAGE_MAX_LOAD: usize = 768;
const MESSAGE_UPDATES_TIMEOUTS_MS: [u64; 4] = [0, 1000, 2000, 5000 ];
const CLEANUP_MSG_AFTER_SEC: u64 = 30;
const LONG_ITERATION_WARNING_MS: u128 = 50;
const BUILD_PACKET_PERIOD_MS: u128 = 100;

#[async_trait::async_trait]
trait SendCombinedReceipt: Send + Sync {
    async fn send_combined_receipt(
        &self,
        to: &Arc<KeyId>,
        receipt: &[u8],
        self_adnl_id: Arc<KeyId>,
    ) -> Result<()>;
}

struct SendCombinedReceiptByAdnl {
    adnl: Arc<AdnlNode>,
    #[cfg(feature = "telemetry")]
    adnl_tag: u32,
}

impl SendCombinedReceiptByAdnl {
    pub fn new(adnl: Arc<AdnlNode>) -> Self {
        Self {
            adnl,
            #[cfg(feature = "telemetry")]
            adnl_tag: tag_from_boxed_type::<RempCombinedReceipt>(),
        }
    }
}

#[async_trait::async_trait]
impl SendCombinedReceipt for SendCombinedReceiptByAdnl {
    async fn send_combined_receipt(
        &self,
        to: &Arc<KeyId>,
        receipt: &[u8],
        self_adnl_id: Arc<KeyId>,
    ) -> Result<()> {
        let tagged_data = TaggedByteSlice {
            object: &receipt,
            #[cfg(feature = "telemetry")]
            tag: self.adnl_tag
        };
        let peers = AdnlPeers::with_keys(self_adnl_id, to.clone());
        if let Err(e) = self.adnl.send_custom(&tagged_data, &peers) {
            log::error!("send_combined_receipt: {:?}", e);
        }
        Ok(())
    }
}

struct LastReceipt {
    pub receipt: Option<RempReceipt>,
    //pub sign: Option<Vec<u8>>,
    pub sent_receipts: u32,
    pub updated_at: Instant,
    pub self_adnl_id: Arc<KeyId>,
}

fn start_receipts_worker(
    sender: Arc<dyn SendCombinedReceipt>,
    mut receiver: tokio::sync::mpsc::UnboundedReceiver<ReceiptStuff>,
    receipts_in_channel: Arc<AtomicU64>,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<RempCoreTelemetry>,
) {
    tokio::spawn(async move {
        loop {
            match receipts_worker(
                sender.clone(),
                &mut receiver,
                receipts_in_channel.clone(),
                #[cfg(feature = "telemetry")]
                telemetry.clone(),
            ).await {
                Ok(_) => log::error!("ReceiptsSender::worker has stopped unexpected"),
                Err(e) => log::error!("CRITICAL! ReceiptsSender::worker has stopped with error: {:?}", e),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

async fn receipts_worker(
    sender: Arc<dyn SendCombinedReceipt>,
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<ReceiptStuff>,
    receipts_in_channel: Arc<AtomicU64>,
    #[cfg(feature = "telemetry")]
    telemetry: Arc<RempCoreTelemetry>,
) -> Result<()> {

    let mut receipts: HashMap<Arc<KeyId>, (Instant, HashMap<UInt256, LastReceipt>)> = HashMap::new();
    let mut prev_packet_built_iter = Instant::now();
    #[cfg(feature = "telemetry")]
    let mut pending_receipts = 0;

    loop {
        match tokio::time::timeout(
            Duration::from_millis(RECEIPTS_SEND_PERIOD_MS),
            receiver.recv()
        ).await {
            Err(tokio::time::error::Elapsed{..}) => {
                log::debug!("ReceiptsSender::worker: New iteration - timeout");
            }
            Ok(None) => {
                fail!("ReceiptsSender::worker: receiver.recv() returned None");
            }
            Ok(Some(rs)) => {
                receipts_in_channel.fetch_sub(1, Ordering::Relaxed);
                log::debug!("ReceiptsSender::worker: New iteration - receipt for {:x}", rs.receipt.message_id());
                if add_receipt_to_map(&mut receipts, rs.to, rs.receipt, rs.self_adnl_id)? {
                    #[cfg(feature = "telemetry")] {
                        pending_receipts += 1;
                        telemetry.pending_receipts(pending_receipts);
                    }
                }

                #[cfg(feature = "telemetry")]
                telemetry.receipts_queue_out();
            }
        };

        if prev_packet_built_iter.elapsed().as_millis() >= BUILD_PACKET_PERIOD_MS {
            let now = Instant::now();
            let mut empty_nodes = vec![];
            let mut total_packets = 0;
            for (node_id, (node_updated_at, node_receipts)) in receipts.iter_mut() {
                let mut packets = 0;
                let now = Instant::now();
                loop {
                    let build_result = build_combined_receipt(node_id, node_receipts)?;
                    #[cfg(feature = "telemetry")] {
                        pending_receipts -= build_result.cleaned_up;
                        telemetry.pending_receipts(pending_receipts);
                    }

                    if let (Some(receipt), Some(data), Some(adnl_id)) =
                        (build_result.receipt.as_ref(), build_result.receipt_data.as_ref(), build_result.self_adnl_id.as_ref())
                    {
                        let timeout = node_updated_at.elapsed().as_millis() as u64 > RECEIPTS_SEND_PERIOD_MS;
                        if build_result.is_full_packet || timeout {
                            let now = Instant::now();
                            sender.send_combined_receipt(&node_id, &data, adnl_id.clone()).await?;
                            log::debug!(
                                "ReceiptsSender::worker: sent packet (because of {})  {} bytes  {} receipts  to {}  TIME {}ms", 
                                if timeout { "timeout" } else { "full packet" },
                                data.len(), receipt.receipts().len(), 
                                &node_id, now.elapsed().as_millis()
                            );
                            #[cfg(feature = "telemetry")]
                            telemetry.combined_receipt_sent(
                                data.len() as u64,
                                receipt.receipts().len() as u64
                            );

                            *node_updated_at = Instant::now();

                            mark_as_sent(node_receipts, &receipt)?;

                            packets += 1;

                            if node_receipts.is_empty() {
                                empty_nodes.push(node_id.clone());
                            }
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                }
                if packets > 1 {
                    log::debug!("ReceiptsSender::worker: send {} packets to {} TIME {}ms", 
                        packets, &node_id, now.elapsed().as_millis());
                }
                total_packets += packets;
            }

            for node_id in &empty_nodes {
                receipts.remove(node_id);
                log::debug!("ReceiptsSender::worker: clean up empty node {}", node_id);
            }

            prev_packet_built_iter = Instant::now();
            let proc_time = now.elapsed();
            #[cfg(feature = "telemetry")]
            telemetry.receipts_queue_processing(&proc_time);
            if proc_time.as_millis() > LONG_ITERATION_WARNING_MS {
                log::warn!("ReceiptsSender::worker: long iteration {}ms (sent {} packets)",
                    now.elapsed().as_millis(), total_packets);
            }
        }
    }
}

#[derive(Default)]
struct BuildCombinedReceiptResult {
    receipt: Option<RempCombinedReceipt>,
    receipt_data: Option<Vec<u8>>,
    is_full_packet: bool,
    cleaned_up: u64,
    self_adnl_id: Option<Arc<KeyId>>,
}

fn build_combined_receipt(
    node_id: &Arc<KeyId>,
    node_receipts: &mut HashMap<UInt256, LastReceipt>
) -> Result<BuildCombinedReceiptResult> {

    let mut result = BuildCombinedReceiptResult::default();
    let mut to_cleanup = vec!();
    let mut to_packet = vec!();
    for (id, last_receipt) in node_receipts.iter() {
        let elapsed = last_receipt.updated_at.elapsed();
        if elapsed.as_secs() > CLEANUP_MSG_AFTER_SEC {
            to_cleanup.push(id.clone());
        } else if last_receipt.receipt.is_some() {
            if is_update_timeout(&elapsed, last_receipt.sent_receipts) {
                log::trace!(
                    "build_combined_receipt for {}: include receipt for {:x}, updated since {}ms, sent {}",
                    node_id, id, elapsed.as_millis(), last_receipt.sent_receipts
                );
                to_packet.push(last_receipt);
                let (receipt, receipt_data) = build_combined_receipt_from(&to_packet)?;
                if receipt_data.len() > PACKAGE_MAX_LOAD {
                    result.is_full_packet = true;
                    if result.receipt.is_none() {
                        log::warn!(
                            "build_combined_receipt for {}: receipt becomes too long ({})\
                            after only one receipt was included.", 
                            node_id, receipt_data.len()
                        );
                        result.receipt = Some(receipt);
                        result.receipt_data = Some(receipt_data);
                        result.self_adnl_id = Some(last_receipt.self_adnl_id.clone());
                    }
                    break;
                }
                result.receipt = Some(receipt);
                result.receipt_data = Some(receipt_data);
                result.self_adnl_id = Some(last_receipt.self_adnl_id.clone());
            }
        }
    }

    for id in to_cleanup.iter() {
        node_receipts.remove(id);
        log::trace!("build_combined_receipt for {}: {:x} was removed because of timeout",
            node_id, id);
    }
    result.cleaned_up = to_cleanup.len() as u64;

    if let (Some(receipt), Some(data)) = (result.receipt.as_ref(), result.receipt_data.as_ref()) {
        log::trace!(
            "build_combined_receipt for {}:  {} bytes  {} receipts  {}, cleaned up: {}",
            node_id,
            data.len(), 
            receipt.receipts().len(), 
            if result.is_full_packet {"FULL"} else {"not full"}, 
            to_cleanup.len()
        );
    } else {
        log::trace!(
            "build_combined_receipt for {}: 0 receipts, cleaned up: {}",
            node_id, to_cleanup.len()
        );
    }

    Ok(result)
}

fn expand_combined_receipt(combined_receipt: &RempCombinedReceipt) -> Result<Vec<RempReceipt>> {

    let mut result = Vec::new();

    let get_block_id = |index| -> Result<BlockIdExt> {
        combined_receipt.ids().get(index as usize).cloned().ok_or_else(|| error!("Wrong block id index"))
    };

    for cr in combined_receipt.receipts().iter() {
        let status = match cr.receipt() {
            RempMessageStatusCompact::TonNode_RempAcceptedCompact(acc) => {
                RempMessageStatus::TonNode_RempAccepted(
                    ton_api::ton::ton_node::rempmessagestatus::RempAccepted {
                        level: (acc.level).try_into()?,
                        block_id: get_block_id(acc.block_id_index)?,
                        master_id: get_block_id(acc.master_id_index)?,
                    }
                )
            },
            RempMessageStatusCompact::TonNode_RempDuplicateCompact(d) => {
                RempMessageStatus::TonNode_RempDuplicate(
                    ton_api::ton::ton_node::rempmessagestatus::RempDuplicate {
                        block_id: get_block_id(d.block_id_index)?
                    }
                )
            },
            RempMessageStatusCompact::TonNode_RempIgnoredCompact(ign) => {
                RempMessageStatus::TonNode_RempIgnored(
                    ton_api::ton::ton_node::rempmessagestatus::RempIgnored {
                        level: (ign.level).try_into()?,
                        block_id: get_block_id(ign.block_id_index)?,
                    }
                )
            },
            RempMessageStatusCompact::TonNode_RempNewCompact => {
                RempMessageStatus::TonNode_RempNew
            },
            RempMessageStatusCompact::TonNode_RempRejectedCompact(rj) => {
                RempMessageStatus::TonNode_RempRejected(
                    ton_api::ton::ton_node::rempmessagestatus::RempRejected {
                        level: (rj.level).try_into()?,
                        block_id: get_block_id(rj.block_id_index)?,
                        error: rj.error.clone(),
                    }
                )
            },
            RempMessageStatusCompact::TonNode_RempSentToValidatorsCompact(stv) => {
                RempMessageStatus::TonNode_RempSentToValidators(
                    ton_api::ton::ton_node::rempmessagestatus::RempSentToValidators {
                        sent_to: stv.sent_to as i32,
                        total_validators: stv.total_validators as i32
                    }
                )
            },
            RempMessageStatusCompact::TonNode_RempTimeoutCompact => {
                RempMessageStatus::TonNode_RempTimeout
            },
        };

        let full_receipt = RempReceipt::TonNode_RempReceipt(
            ton_api::ton::ton_node::rempreceipt::RempReceipt {
                message_id: cr.message_id().clone(),
                status,
                timestamp: *cr.timestamp(),
                source_id: combined_receipt.source_id().clone(),
            }
        );
        // result.push(
        //     RempSignedReceipt::TonNode_RempSignedReceipt(
        //         ton_api::ton::ton_node::rempsignedreceipt::RempSignedReceipt {
        //             receipt: ton_api::ton::bytes(serialize_boxed(&full_receipt)?),
        //             signature: cr.signature().clone(),
        //         }
        //     )
        // );
        result.push(full_receipt);
    }
    Ok(result)
}

fn is_update_timeout(elapsed: &Duration, sent_receipts: u32) -> bool {
    let index = min(sent_receipts as usize, MESSAGE_UPDATES_TIMEOUTS_MS.len() - 1);
    elapsed.as_millis() as u64 >= MESSAGE_UPDATES_TIMEOUTS_MS[index]
}

fn build_combined_receipt_from(receipts: &[&LastReceipt]) -> Result<(RempCombinedReceipt, Vec<u8>)> {
    let mut ids = vec!();
    let mut compact_receipts = vec!();
    let mut source_id = UInt256::default();

    let mut get_block_id_index = |id: &BlockIdExt| {
        if let Some(index) = ids.iter().position(|e| e == id) {
            index as u8
        } else {
            ids.push(id.clone());
            (ids.len() - 1) as u8
        }
    };

    for receipt in receipts {
        if let Some(r) = receipt.receipt.as_ref() {
            let compact_status = match r.status() {
                RempMessageStatus::TonNode_RempAccepted(acc) => {
                    RempMessageStatusCompact::TonNode_RempAcceptedCompact(
                        ton_api::ton::ton_node::rempmessagestatuscompact::RempAcceptedCompact {
                            level: (&acc.level).into(),
                            block_id_index: get_block_id_index(&acc.block_id),
                            master_id_index: get_block_id_index(&acc.master_id),
                        }
                    )
                },
                RempMessageStatus::TonNode_RempDuplicate(d) => {
                    RempMessageStatusCompact::TonNode_RempDuplicateCompact(
                        ton_api::ton::ton_node::rempmessagestatuscompact::RempDuplicateCompact {
                            block_id_index: get_block_id_index(&d.block_id)
                        }
                    )
                },
                RempMessageStatus::TonNode_RempIgnored(ign) => {
                    RempMessageStatusCompact::TonNode_RempIgnoredCompact(
                        ton_api::ton::ton_node::rempmessagestatuscompact::RempIgnoredCompact {
                            level: (&ign.level).into(),
                            block_id_index: get_block_id_index(&ign.block_id),
                        }
                    )
                },
                RempMessageStatus::TonNode_RempNew => {
                    RempMessageStatusCompact::TonNode_RempNewCompact
                },
                RempMessageStatus::TonNode_RempRejected(rj) => {
                    RempMessageStatusCompact::TonNode_RempRejectedCompact(
                        ton_api::ton::ton_node::rempmessagestatuscompact::RempRejectedCompact {
                            level: (&rj.level).into(),
                            block_id_index: get_block_id_index(&rj.block_id),
                            error: rj.error.clone(),
                        }
                    )
                },
                RempMessageStatus::TonNode_RempSentToValidators(stv) => {
                    RempMessageStatusCompact::TonNode_RempSentToValidatorsCompact(
                        ton_api::ton::ton_node::rempmessagestatuscompact::RempSentToValidatorsCompact{
                            sent_to: stv.sent_to as u8,
                            total_validators: stv.total_validators as u8
                        }
                    )
                },
                RempMessageStatus::TonNode_RempTimeout => {
                    RempMessageStatusCompact::TonNode_RempTimeoutCompact
                },
            };

            if source_id == UInt256::default() {
                source_id = r.source_id().clone();
            } else if source_id != *r.source_id() {
                fail!("INTERNAL ERROR: source_id {:x} != r.source_id {:x} ", source_id , r.source_id());
            }

            // compact_receipts.push(RempSignedReceiptCompact::TonNode_RempSignedReceiptCompact (
            //     ton_api::ton::ton_node::rempsignedreceiptcompact::RempSignedReceiptCompact {
            //         message_id: r.message_id().clone(),
            //         receipt: compact_status,
            //         timestamp: r.timestamp().clone(),
            //         signature: ton_api::ton::int512(sign.as_slice().try_into()?),
            //     }
            // ));
            compact_receipts.push(RempReceiptCompact::TonNode_RempReceiptCompact (
                ton_api::ton::ton_node::rempreceiptcompact::RempReceiptCompact {
                    message_id: r.message_id().clone(),
                    receipt: compact_status,
                    timestamp: r.timestamp().clone(),
                }
            ));
        }
    }

    let rcr = RempCombinedReceipt::TonNode_RempCombinedReceipt(
        ton_api::ton::ton_node::rempcombinedreceipt::RempCombinedReceipt {
            source_id,
            ids: ids.into(),
            receipts: compact_receipts.into(),
        }
    );
    let data = serialize_boxed(&rcr)?;

    Ok((rcr, data))
}

fn add_receipt_to_map(
    receipts: &mut HashMap<Arc<KeyId>, (Instant, HashMap<UInt256, LastReceipt>)>,
    to: Arc<KeyId>,
    receipt: RempReceipt,
    // sign: Vec<u8>,
    self_adnl_id: Arc<KeyId>,
) -> Result<bool> {
    let id = receipt.message_id().clone();
    let added_new = if let Some((_, node)) = receipts.get_mut(&to) {
        if let Some(msg) = node.get_mut(receipt.message_id()) {
            msg.receipt = Some(receipt);
            // msg.sign = Some(sign.as_slice().try_into()?);
            msg.self_adnl_id = self_adnl_id;
            log::trace!("add_receipt_to_map for msg {:x} to {}: replaced prev receipt", id, to);
            false
        } else {
            node.insert(
                receipt.message_id().clone(),
                LastReceipt {
                    receipt: Some(receipt),
                    // sign: Some(sign.as_slice().try_into()?),
                    sent_receipts: 0,
                    updated_at: Instant::now(),
                    self_adnl_id,
                }
            );
            log::trace!("add_receipt_to_map for msg {:x} to {}: added new receipt", id, to);
            true
        }
    } else {
        let mut node = HashMap::new();
        node.insert(
            receipt.message_id().clone(),
            LastReceipt {
                receipt: Some(receipt),
                // sign: Some(sign.as_slice().try_into()?),
                sent_receipts: 0,
                updated_at: Instant::now(),
                self_adnl_id,
            }
        );
        log::trace!("add_receipt_to_map for msg {:x}: added new node {}", id, to);
        receipts.insert(to, (Instant::now(), node));
        true
    };
    Ok(added_new)
}

fn mark_as_sent(
    node_receipts: &mut HashMap<UInt256, LastReceipt>,
    combined_receipt: &RempCombinedReceipt,
) -> Result<()> {
    for receipt in combined_receipt.receipts().iter() {
        if let Some(r) = node_receipts.get_mut(receipt.message_id()) {
            r.receipt = None;
            // r.sign = None;
            r.updated_at = Instant::now();
            r.sent_receipts += 1;
        }
    }
    Ok(())
}

