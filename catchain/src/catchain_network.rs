#![allow(dead_code, unused_variables, unused_imports)]

use adnl::{
    common::{serialize, KeyId},
    node::AdnlNode,
};
use tokio::task::spawn_blocking;

use crate::CatchainNode;
use crate::{AdnlNodePtr, CatchainOverlayListenerPtr, CatchainOverlayPtr, OverlayFullId};
use crate::{BlockPayload, CatchainOverlay, ExternalQueryResponseCallback, PublicKeyHash};
use rldp::RldpNode;
use std::sync::Mutex;
use std::{io::Cursor, sync::Arc};
use ton_api::ton::{self, rpc::overlay::Query as OverlayQuery};
use ton_api::{Deserializer, Serializer};
use ton_types::Result;

pub struct CatchainNetwork {
    runtime_handle: tokio::runtime::Handle,
    rldp: Arc<RldpNode>,
    query: OverlayQuery,
}
use overlay::OverlayShortId;

impl CatchainNetwork {
    pub fn new(
        runtime_handle: &tokio::runtime::Handle,
        adnl: &Arc<AdnlNode>,
        overlay_id: &OverlayFullId,
    ) -> Result<Self> {
        let rldp = RldpNode::with_adnl_node(adnl.clone(), Vec::new())?;
        let query = OverlayQuery {
            overlay: ton::int256(overlay_id.as_slice().clone()),
        };

        Ok(CatchainNetwork {
            runtime_handle: runtime_handle.clone(),
            rldp: rldp,
            query: query,
        })
    }

    pub fn create(
        runtime_handle: &tokio::runtime::Handle,
        adnl: &AdnlNodePtr,
        _local_id: &PublicKeyHash,
        overlay_full_id: &OverlayFullId,
        _nodes: &Vec<CatchainNode>,
        _listener: CatchainOverlayListenerPtr,
    ) -> CatchainOverlayPtr {
        //TODO: use all incoming parameters
        Arc::new(Mutex::new(
            match Self::new(runtime_handle, adnl, overlay_full_id) {
                Ok(body) => body,
                Err(err) => {
                    error!("CatchainNetwork::create: {:?}", err);
                    panic!("ADNL overlay can't be created");
                }
            },
        ))
    }

    async fn send_data_query(
        rldp: &Arc<RldpNode>,
        query: &OverlayQuery,
        src: &Arc<KeyId>,
        dst_id: &Arc<KeyId>,
        request: &BlockPayload,
        _attempts: u32,
    ) -> Result<Vec<u8>> {
        let mut full_request = serialize(query)?;
        CatchainNetwork::serialize_append(&mut full_request, request)?;
        let data = Arc::new(full_request);
        unimplemented!(); //due to RLDP API changes
                          //let ret = rldp
                          //    .query(dst_id, src, &data, Some(10 * 1024 * 1024))
                          //    .await?;
                          //let ret = ret.ok_or(failure::err_msg("Answer operation is None!"))?;
                          //Ok(ret)
    }

    fn serialize_append(buf: &mut Vec<u8>, message: &BlockPayload) -> Result<()> {
        Serializer::new(buf).write_bare(message)?;
        Ok(())
    }

    fn send(&self, receiver_id: &PublicKeyHash, sender_id: &PublicKeyHash, message: &BlockPayload) {
        println!("<send_query>");
        let msg = message.clone();
        let rldp = self.rldp.clone();
        let query = self.query.clone();
        let sender = sender_id.clone();
        let receiver = receiver_id.clone();
        self.runtime_handle.spawn(async move {
            let res =
                CatchainNetwork::send_data_query(&rldp, &query, &sender, &receiver, &msg, 1).await;
            println!(
                "send_message (receiver_id: {}, query: {}) result: {:?}",
                receiver,
                base64::encode(&query.overlay.0),
                res
            );
        });
    }
}

impl CatchainOverlay for CatchainNetwork {
    /// Send message
    fn send_message(
        &mut self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        message: &BlockPayload,
    ) {
        println!("<send_message>");
        self.send(receiver_id, sender_id, message);
        println!("</send_message>");
    }

    /// Send message to multiple sources
    fn send_message_multicast(
        &mut self,
        receiver_ids: &[PublicKeyHash],
        sender_id: &PublicKeyHash,
        message: &BlockPayload,
    ) {
        println!("<send_message_multicast>");
        for receiver_id in receiver_ids.iter() {
            self.send(receiver_id, sender_id, message);
        }
        info!("</send_message_multicast>");
    }

    /// Send query
    fn send_query(
        &mut self,
        receiver_id: &PublicKeyHash,
        sender_id: &PublicKeyHash,
        _name: &str,
        timeout: std::time::Duration,
        message: &BlockPayload,
        response_callback: ExternalQueryResponseCallback,
    ) {
        println!("<send_query>");
        let dst_id = receiver_id.clone();
        let src_id = sender_id.clone();
        let msg = message.clone();
        let rldp = self.rldp.clone();
        let query = self.query.clone();
        let timeout = timeout.clone();
        self.runtime_handle.spawn(async move {
            /*     if let Err(_) = tokio::time::timeout(timeout, async move {
                let answer = CatchainNetwork::send_data_query(&rldp, &query, &src_id, &dst_id, &msg, 1).await;

                match answer {
                    Err(e) => {
                        println!("error send data query (receiver_id: {}): {}", &dst_id, e);
                    },
                    Ok(payload) => {
                        println!("send_message (receiver_id: {}, query: {}) result: {:?}",
                            &dst_id,
                            &query.overlay,
                            payload);

                        let ret_msg = Deserializer::new(&mut Cursor::new(payload)).read_bare().unwrap();
                        response_callback(Ok(ret_msg));
                    },
                }
            }).await {
                println!("did not receive value within timeout`s value (send_query)");
            }*/
            println!("async test");
        });
    }

    fn send_broadcast_fec_ex(
        &mut self,
        _sender_id: &PublicKeyHash,
        _send_as: &PublicKeyHash,
        _payload: BlockPayload,
    ) {
        warn!("CatchainNetwork: send broadcast_fec_ex is not implemented");
    }
}
