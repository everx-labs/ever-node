/*
* Copyright (C) 2019-2024 EverX. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific EVERX DEV software governing permissions and
* limitations under the License.
*/

use crate::{
    engine_traits::{EngineOperations, RempCoreInterface, RempDuplicateStatus},
    network::remp::RempMessagesSubscriber
};

use std::sync::{Arc, Weak};
use ever_block::{error, fail, KeyId, Result, UInt256};

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

    async fn process_incoming_message(&self, message: &ton_api::ton::ton_node::RempMessage, source: &Arc<KeyId>) -> Result<()> {
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

        // push into remp catchain
        remp_core.process_incoming_message(message, source.clone()).await
    }
}

#[async_trait::async_trait]
impl RempMessagesSubscriber for RempService {
    async fn new_remp_message(&self, message: ton_api::ton::ton_node::RempMessage, source: &Arc<KeyId>) -> Result<()> {
        // TODO send error receipt in case of any error

        let id = message.id().clone();
        log::trace!(target: "remp", "Point 0. Processing incoming REMP message {:x}", id);
        match self.process_incoming_message(&message, source).await {
            Ok(_) => log::trace!(target: "remp", "Point 0. Processed incoming REMP message {:x}", id),
            Err(e) => log::error!(target: "remp", "Point 0. Error processing incoming REMP message {:x}: {}", id, e)
        }
        Ok(())
    }
}
