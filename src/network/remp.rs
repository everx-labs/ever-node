
use adnl::{
    common::{QueryResult, AdnlPeers, Subscriber, TaggedTlObject}, 
    node::AdnlNode
};
use ever_crypto::KeyId;
use ton_api::{
    ton::{
        ton_node::{RempMessage, RempReceipt, RempSignedReceipt, RempReceived},
        TLObject
    },
    tag_from_boxed_type
};
use ton_types::{Result, fail, error, UInt256};
use std::sync::Arc;

#[async_trait::async_trait]
pub trait RempMessagesSubscriber: Sync + Send {
    async fn new_remp_message(&self, message: RempMessage, source: &Arc<KeyId>) -> Result<()>;
}
#[async_trait::async_trait]
pub trait RempReceiptsSubscriber: Sync + Send {
    async fn new_remp_receipt(&self, receipt: RempSignedReceipt, source: &Arc<KeyId>) -> Result<()>;
}

pub struct RempNode {
    adnl: Arc<AdnlNode>,
    local_key: Arc<KeyId>,
    messages_subscriber: tokio::sync::OnceCell<Arc<dyn RempMessagesSubscriber>>,
    receipts_subscriber: tokio::sync::OnceCell<Arc<dyn RempReceiptsSubscriber>>,
    #[cfg(feature = "telemetry")]
    tag_message: u32,
    #[cfg(feature = "telemetry")]
    tag_receipt: u32
}

impl RempNode {
    pub fn new(adnl: Arc<AdnlNode>, key_tag: usize) -> Result<Self> {
        let local_key = adnl.key_by_tag(key_tag)?.id().clone();
        Ok(Self {
            adnl,
            local_key,
            messages_subscriber: Default::default(),
            receipts_subscriber: Default::default(),
            #[cfg(feature = "telemetry")]
            tag_message: tag_from_boxed_type::<RempMessage>(),
            #[cfg(feature = "telemetry")]
            tag_receipt: tag_from_boxed_type::<RempReceipt>()
        })
    }

    pub async fn send_message(&self, to: Arc<KeyId>, message: RempMessage) -> Result<()> {
        let id = message.id().clone();
        let peers = AdnlPeers::with_keys(self.local_key.clone(), to);
        let query = TaggedTlObject {
            object: TLObject::new(message),
            #[cfg(feature = "telemetry")]
            tag: self.tag_message
        };
        match self.adnl.clone().query(&query, &peers, None).await {
            Err(e) => fail!("Error while sending RempMessage {:x}: {}", id, e),
            Ok(None) => fail!("sending RempMessage {:x}: timeout", id),
            Ok(Some(_)) => Ok(())
        }
    }

    pub async fn send_receipt(&self, to: Arc<KeyId>, id: &UInt256, receipt: RempSignedReceipt) -> Result<()> {
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
    }

    pub fn set_messages_subscriber(&self, subscriber: Arc<dyn RempMessagesSubscriber>) -> Result<()> {
        self.messages_subscriber.set(subscriber).map_err(|_| error!("Can't set remp messages subscriber"))?;
        Ok(())
    }

    pub fn set_receipts_subscriber(&self, subscriber: Arc<dyn RempReceiptsSubscriber>) -> Result<()> {
        self.receipts_subscriber.set(subscriber).map_err(|_| error!("Can't set remp receipts subscriber"))?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Subscriber for RempNode {
    async fn try_consume_query(
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
    }
}

