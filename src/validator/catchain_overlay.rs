use crate::engine_traits::PrivateOverlayOperations;
use overlay::PrivateOverlayShortId;
use std::sync::Arc;
use ton_types::{Result, UInt256};
use validator_session::{PublicKeyHash, CatchainOverlayPtr, CatchainNode};

pub(crate) struct CatchainOverlayManagerImpl {
    network: Arc<dyn PrivateOverlayOperations>,
    validator_list_id: UInt256
}

impl CatchainOverlayManagerImpl {
    pub fn new(network: Arc<dyn PrivateOverlayOperations>, validator_list_id: UInt256) -> Self {
        Self {
            network,
            validator_list_id
        }
    }
}

impl catchain::CatchainOverlayManager for CatchainOverlayManagerImpl {

    fn start_overlay(
        &self,
        _local_id: &PublicKeyHash,
        overlay_short_id: &Arc<PrivateOverlayShortId>,
        nodes: &Vec<CatchainNode>,
        listener: catchain::CatchainOverlayListenerPtr,
        replay_listener: catchain::CatchainOverlayLogReplayListenerPtr,
    ) -> Result<CatchainOverlayPtr> {
        self.network.create_catchain_client(
            self.validator_list_id.clone(), overlay_short_id, nodes, listener, replay_listener
        )
    }

    /// Stop existing overlay
    fn stop_overlay(
        &self,
        overlay_short_id: &Arc<PrivateOverlayShortId>,
        _overlay: &CatchainOverlayPtr,
    ) {
        let engine_network = self.network.clone();
        engine_network.stop_catchain_client(overlay_short_id);
    }

}