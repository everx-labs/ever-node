use crate::{
    engine_traits::{EngineOperations, RempCoreInterface}, ext_messages::create_ext_message,
    network::remp::RempMessagesSubscriber,
};

use std::{sync::Arc, sync::Weak};
use ton_api::ton::ton_node::RempMessage;
use ton_types::{error, fail, KeyId, Result, UInt256};
use crate::engine_traits::RempDuplicateStatus;

#[derive(Default)]
pub struct RempService {
    engine: tokio::sync::OnceCell<Weak<dyn EngineOperations>>,
    remp_core: tokio::sync::OnceCell<Weak<dyn RempCoreInterface>>,
}

impl RempService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_engine(&self, engine: Arc<dyn EngineOperations>) -> Result<()> {
        self.engine.set(Arc::downgrade(&engine)).map_err(|_| error!("Attempt to set engine twice"))
    }

    pub fn set_remp_core_interface(&self, remp_core: Arc<dyn RempCoreInterface>) -> Result<()> {
        self.remp_core.set(Arc::downgrade(&remp_core)).map_err(|_| error!("Attempt to set remp_core twice"))
    }

    fn get_core_interface(&self) -> Result<Arc<dyn RempCoreInterface>> {
        self.remp_core.get()
            .ok_or_else(|| error!("remp_core was not set"))?
            .upgrade().ok_or_else(|| error!("remp_core weak reference is null"))
    }

    pub fn check_remp_duplicate(&self, id: &UInt256) -> Result<RempDuplicateStatus> {
        self.get_core_interface()?.check_remp_duplicate(id)
    }

    async fn new_remp_message(&self, message: RempMessage, source: &Arc<KeyId>) -> Result<()> {
        // TODO send error receipt in case of any error
        let engine = self.engine
            .get().ok_or_else(|| error!("engine was not set"))?
            .upgrade().ok_or_else(|| error!("engine weak reference is null"))?;

        let remp_core = self.get_core_interface()?;

        #[cfg(feature = "telemetry")]
        engine.remp_core_telemetry().message_from_fullnode();

        if !engine.check_sync().await? {
            fail!("Can't process REMP message because validator is out of sync");
        }

        log::trace!(target: "remp", "Message: {:?}", message.message());

        // deserialise message
        let id = message.id().clone();
        let (real_id, message) = create_ext_message(&message.message())?;
        if real_id != id {
            fail!("Given message id {:x} is not equal calculated one {:x}", id, real_id);
        }

        log::trace!(target: "remp", "Point 0. Incoming REMP message {:x} received from {}: {:?}",
            id, source, message
        );

        // push into remp catchain
        remp_core.process_incoming_message(id, message, source.clone()).await?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl RempMessagesSubscriber for RempService {
    async fn new_remp_message(&self, message: RempMessage, source: &Arc<KeyId>) -> Result<()> {
        // TODO send error receipt in case of any error

        let id = message.id().clone();
        log::trace!(target: "remp", "Point 0. Processing incoming REMP message {:x}", id);
        match self.new_remp_message(message, source).await {
            Ok(_) => log::trace!(target: "remp", "Point 0. Processed incoming REMP message {:x}", id),
            Err(e) => log::error!(target: "remp", "Point 0. Error processing incoming REMP message {:x}: {}", id, e)
        }
        Ok(())
    }
}
