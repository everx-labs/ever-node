use crate::{
    network::remp::{RempNode, RempMessagesSubscriber, RempReceiptsSubscriber, ReceiptStuff},
    test_helper::{get_adnl_config, init_test_log}, validator::telemetry::RempCoreTelemetry
};

use adnl::node::AdnlNode;
use std::{
    collections::LinkedList, 
    sync::{Arc, atomic::{AtomicU32, AtomicU64, Ordering}, Mutex}, time::Duration
};
use ton_api::{
    IntoBoxed,
    ton::ton_node::{RempReceipt, RempCombinedReceipt, RempMessageStatus},
};
use ton_types::{fail, KeyId, Result, UInt256};

const KEY_TAG: usize = 0;

async fn init_remp_node(ip: &str) -> Result<(Arc<AdnlNode>, Arc<RempNode>, Arc<RempCoreTelemetry>)> {
    let config = get_adnl_config("target/remp", ip, vec![KEY_TAG], true).await.unwrap();
    let node = AdnlNode::with_config(config).await.unwrap();
    let remp = Arc::new(RempNode::new(node.clone(), KEY_TAG)?);
    AdnlNode::start(
        &node,
        vec![remp.clone()]
    ).await.unwrap();
    #[cfg(feature = "telemetry")]
    let t = Arc::new(RempCoreTelemetry::new(1));
    remp.set_telemetry(t.clone())?;
    remp.start()?;
    Ok((node, remp, t))
}

struct TestRempSubscriber {
    pub got_messages: Arc<AtomicU32>,
    pub got_receipts: Arc<AtomicU32>,
}

#[async_trait::async_trait]
impl RempMessagesSubscriber for TestRempSubscriber {
    async fn new_remp_message(&self, message: ton_api::ton::ton_node::RempMessage, _source: &Arc<KeyId>) -> Result<()> {
        println!("{}", message.message()[0]);
        self.got_messages.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}
#[async_trait::async_trait]
impl RempReceiptsSubscriber for TestRempSubscriber {
    async fn new_remp_receipt(&self, _receipt: RempReceipt, _source: &Arc<KeyId>) -> Result<()> {
        self.got_receipts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn test_remp_client_compact_protocol() -> Result<()> {

    init_test_log();

    let (node1, remp1, telemetry1) = init_remp_node("127.0.0.1:4191").await?;
    let (node2, remp2, telemetry2) = init_remp_node("127.0.0.1:4192").await?;
    let peer1 = node2.add_peer(
        node2.key_by_tag(KEY_TAG).unwrap().id(),
        node1.ip_address(),
        &node1.key_by_tag(KEY_TAG).unwrap()
    ).unwrap().unwrap();
    let peer2 = node1.add_peer(
        node1.key_by_tag(KEY_TAG).unwrap().id(),
        node2.ip_address(),
        &node2.key_by_tag(KEY_TAG).unwrap()
    ).unwrap().unwrap();
    let s1 = Arc::new(TestRempSubscriber {
        got_messages: Arc::new(AtomicU32::new(0)),
        got_receipts: Arc::new(AtomicU32::new(0)),
    });
    remp1.set_messages_subscriber(s1.clone())?;
    remp1.set_receipts_subscriber(s1.clone())?;
    let s2 = Arc::new(TestRempSubscriber {
        got_messages: Arc::new(AtomicU32::new(0)),
        got_receipts: Arc::new(AtomicU32::new(0)),
    });
    remp2.set_messages_subscriber(s2.clone())?;
    remp2.set_receipts_subscriber(s2.clone())?;

    let messages = 10000_u32;
    for n in 0..messages {

        let mut id = [0; 32];
        for i in 0..32 {
            id[i] = ((n % 30) >> (8 * (i % 4))) as u8;
        }

        let r = ton_api::ton::ton_node::RempReceipt::TonNode_RempReceipt(
            ton_api::ton::ton_node::rempreceipt::RempReceipt {
                message_id: UInt256::from(&id),
                status: RempMessageStatus::TonNode_RempRejected(
                    ton_api::ton::ton_node::rempmessagestatus::RempRejected {
                        level: 2.try_into()?,
                        block_id: ton_block::BlockIdExt::default(),
                        error: "Error test".to_owned(),
                    }
                ),
                timestamp: 123,
                source_id: UInt256::default(),
            }
        );

        remp1.combine_and_send_receipt(peer2.clone(), r.clone(), node1.key_by_tag(KEY_TAG).unwrap().id().clone()).await?;
        remp2.combine_and_send_receipt(peer1.clone(), r.clone(), node2.key_by_tag(KEY_TAG).unwrap().id().clone()).await?;
    }

    tokio::time::sleep(Duration::from_millis(5000)).await;

    assert!(s1.got_receipts.load(Ordering::Relaxed) > 0);
    assert!(s2.got_receipts.load(Ordering::Relaxed) > 0);

    log::info!("\n1{}", telemetry1.report());
    log::info!("\n\n2{}", telemetry2.report());

    node1.stop().await;
    node2.stop().await;

    Ok(())
}

struct TestSendCombinedReceipt {
    pub sent_receipts: Mutex<LinkedList<RempCombinedReceipt>>
}

#[async_trait::async_trait]
impl super::SendCombinedReceipt for TestSendCombinedReceipt {
    async fn send_combined_receipt(
        &self,
        _to: &Arc<KeyId>,
        receipt: &[u8],
        _self_adnl_id: Arc<KeyId>,
    ) -> Result<()> {
        
        let receipt = ton_api::deserialize_boxed(receipt)?
            .downcast::<RempCombinedReceipt>()
            .or_else(|_| fail!("Can't deserialise RempReceipt from TLObject"))?;

        self.sent_receipts.lock().unwrap().push_back(receipt);
        Ok(())
    }
}

#[tokio::test]
async fn test_remp_receipts_send_worker() -> Result<()> {

    init_test_log();

    let sender = Arc::new(TestSendCombinedReceipt { 
        sent_receipts: Mutex::new(LinkedList::new())
    });

    let (receipts_sender, receiver) = tokio::sync::mpsc::unbounded_channel();
    let receipts_in_channel = Arc::new(AtomicU64::new(0));
    #[cfg(feature = "telemetry")]
    let telemetry = Arc::new(RempCoreTelemetry::new(10));
    super::start_receipts_worker(
        sender.clone(),
        receiver,
        receipts_in_channel.clone(),
        telemetry.clone()
    );

    let receipts_count = 10_000 as usize;
    let nodes_count = 3 as usize;
    let uniq_messages_count = (receipts_count / 20) as usize;
    let source_id = UInt256::from([0x11_u8; 32]);


    for n in 1..=receipts_count {

        let mut id = [0; 32];
        for i in 0..32 {
            id[i] = ((n % uniq_messages_count) >> (8 * (i % 4))) as u8;
        }

        let to = KeyId::from_data([(n % nodes_count) as u8; 32]);
        //let sign = vec![0_u8; 64];
        let receipt = ton_api::ton::ton_node::rempreceipt::RempReceipt {
            message_id: UInt256::from(&id),
            status : RempMessageStatus::TonNode_RempNew,
            timestamp: 0,
            source_id : source_id.clone()
        }.into_boxed();

        let self_adnl_id = KeyId::from_data([0; 32]);
        receipts_sender.send(ReceiptStuff{to, receipt, self_adnl_id})?;

        if n % 100 == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    log::info!("sent {} receipts", receipts_count);

    tokio::time::sleep(Duration::from_secs(5)).await;

    assert!(sender.sent_receipts.lock().unwrap().len() >= nodes_count as usize);
    log::info!("sent packages {}", sender.sent_receipts.lock().unwrap().len());
    log::info!("sent receipts {}", sender.sent_receipts.lock().unwrap().iter().map(|p| p.receipts().len()).sum::<usize>());

    log::info!("{}", telemetry.report());

    Ok(())
}