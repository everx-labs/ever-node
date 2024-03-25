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

use crate::network::node_network::NodeNetwork;

use adnl::{common::{Query, TaggedTlObject, Wait}, node::{AdnlNode, AddressCache}};
use adnl::DhtNode;
use adnl::{OverlayShortId, OverlayNode};
use rand::Rng;
use std::{
    cmp::min,
    sync::{Arc, atomic::{AtomicBool, AtomicU32, AtomicI32, AtomicU64, AtomicI64, Ordering}},
    time::{Duration, Instant}
};
use ton_api::ton::{TLObject, rpc::ton_node::GetCapabilities, ton_node::Capabilities};
#[cfg(feature = "telemetry")]
use ton_api::tag_from_boxed_type;
use ton_types::{error, fail, KeyId, KeyOption, Result};

#[derive(Debug)]
pub struct Neighbour {
    id : Arc<KeyId>,
    last_ping: AtomicU64,
    proto_version: AtomicI32,
    capabilities: AtomicI64,
    roundtrip_adnl: AtomicU64,
    roundtrip_rldp: AtomicU64,
    all_attempts: AtomicU64,
    fail_attempts: AtomicU64,
    fines_points: AtomicU32,
    active_check: AtomicBool,
    unreliability: AtomicI32
}

pub struct Neighbours {
    peers: NeighboursCache,
    reserve: ReserveNeighbours, ///< Replaced peers saved stats
    all_peers: lockfree::set::Set<Arc<KeyId>>,
    overlay_id: Arc<OverlayShortId>,
    overlay: Arc<OverlayNode>,
    dht: Arc<DhtNode>,
    fail_attempts: AtomicU64,
    all_attempts: AtomicU64,
    start: Instant,
    cancellation_token: Arc<tokio_util::sync::CancellationToken>,
    #[cfg(feature = "telemetry")]
    tag_get_capabilities: u32
}

const CAPABILITY_COMPATIBLE: i64 = 0x01;
const VERSION_COMPATIBLE: i32 = 2;

pub const PROTOCOL_CAPABILITIES: i64 = CAPABILITY_COMPATIBLE;
pub const PROTOCOL_VERSION: i32 = VERSION_COMPATIBLE;
pub const BETTER_REPLACE_UNRELIABILITY: i32 = 5;
pub const FAIL_UNRELIABILITY: i32 = 10;

const FINES_POINTS_COUNT: u32 = 100;

pub const UPDATE_FLAG_SUCCESS: u8 = 1;
pub const UPDATE_FLAG_IS_RDPL:u8 = 2;
pub const UPDATE_FLAG_IS_REGISTER:u8 = 4;
pub const UPDATE_FLAG_IS_REG_IN_COMMON_STAT:u8 = 8;

impl Neighbour {

    pub fn new(id : Arc<KeyId>, default_rldp_roundtrip: u32) ->  Self {
        Self {
            id,
            last_ping: AtomicU64::new(0),
            proto_version: AtomicI32::new(0),
            capabilities: AtomicI64::new(0),
            roundtrip_adnl: AtomicU64::new(0),
            roundtrip_rldp: AtomicU64::new(default_rldp_roundtrip as u64),
            all_attempts: AtomicU64::new(0),
            fail_attempts: AtomicU64::new(0),
            fines_points: AtomicU32::new(0),
            active_check: AtomicBool::new(false),
            //roundtrip_relax_at: 0,
            //roundtrip_weight: 0.0,
            unreliability: AtomicI32::new(0),
        }
    }

    /// Corrected unreliability
    pub fn effective_unreliability(&self) -> i32 {
        let mut unr = self.unreliability.load(Ordering::Relaxed);
        let version = self.proto_version.load(Ordering::Relaxed);
        let capabilities = self.capabilities.load(Ordering::Relaxed);
        if version < PROTOCOL_VERSION {
            unr += 4;
        } else if (version == PROTOCOL_VERSION) && (capabilities < PROTOCOL_CAPABILITIES) {
            unr += 2;
        }
        return unr;
    }

    pub fn is_good(&self) -> bool {
        self.effective_unreliability() <= BETTER_REPLACE_UNRELIABILITY
    }

    pub fn update_proto_version(&self, q: &Capabilities) {
        self.proto_version.store(q.version().clone(), Ordering::Relaxed);
        self.capabilities.store(q.capabilities().clone(), Ordering::Relaxed);
    }

    pub fn id(&self) -> &Arc<KeyId> {
        &self.id
    }

    pub fn query_success(&self, roundtrip: u64, is_rldp: bool) {
        loop {
            let old_un = self.unreliability.load(Ordering::Relaxed);
            if old_un > 0 {
                let new_un = old_un - 1;
                if self.unreliability.compare_exchange(
                    old_un,
                    new_un,
                    Ordering::Relaxed,
                    Ordering::Relaxed
                ).is_err() {
                    continue;
                } else {
   //                 log::trace!("query_success (key_id {}) new value: {}", self.id, new_un);
                }
            }
            break;
        }
        if is_rldp {
            self.update_roundtrip_rldp(roundtrip)
        } else {
            self.update_roundtrip_adnl(roundtrip)
        }
    }

    pub fn query_failed(&self, roundtrip: u64, is_rldp: bool) {
        let _un = self.unreliability.fetch_add(1, Ordering::Relaxed) + 1;
        let labels = [("neighbour", self.id.to_string())];
        metrics::increment_counter!("neighbours_failed", &labels);
//        log::trace!("query_failed (key_id {}, overlay: ) new value: {}", self.id, un);
        if is_rldp {
            self.update_roundtrip_rldp(roundtrip)
        } else {
            self.update_roundtrip_adnl(roundtrip)
        }
    }

// Unused
//    pub fn capabilities(&self) -> i64 {
//        self.capabilities.load(Ordering::Relaxed)
//    }

    pub fn roundtrip_adnl(&self) -> Option<u64> {
        Self::roundtrip(&self.roundtrip_adnl)
    }

    pub fn roundtrip_rldp(&self) -> Option<u64> {
        Self::roundtrip(&self.roundtrip_rldp)
    }

    pub fn update_roundtrip_adnl(&self, roundtrip: u64) {
        Self::set_roundtrip(&self.roundtrip_adnl, roundtrip)
    }

    pub fn update_roundtrip_rldp(&self, roundtrip: u64) {
        Self::set_roundtrip(&self.roundtrip_rldp, roundtrip)
    }

    fn last_ping(&self) -> u64 {
        self.last_ping.load(Ordering::Relaxed)
    }

    fn roundtrip(storage: &AtomicU64) -> Option<u64> {
        let roundtrip = storage.load(Ordering::Relaxed);
        if roundtrip == 0 {
            None
        } else {
            Some(roundtrip)
        }
    }

    fn set_last_ping(&self, elapsed: u64) {
        self.last_ping.store(elapsed, Ordering::Relaxed)
    }

    fn set_roundtrip(storage: &AtomicU64, roundtrip: u64) {
        let roundtrip_old = storage.load(Ordering::Relaxed);
        let roundtrip = if roundtrip_old > 0 {
            (roundtrip_old + roundtrip) / 2
        } else {
            roundtrip
        };
    //    log::trace!("roundtrip new value: {}", roundtrip);
        storage.store(roundtrip, Ordering::Relaxed);
    }

}

pub const MAX_NEIGHBOURS: usize = 16;
const PING_DELAY_MS: u64 = 1000; // Neighbour ping recommended minimum delay
const RESERVE_PING_DELAY_MS: u64 = 30000; // Reserved neighbour ping recommended minimum delay

impl Neighbours {

    const DEFAULT_RLDP_ROUNDTRIP_MS: u32 = 2000;
    const MAX_PINGS: usize = 6;
    const TIMEOUT_PING_MAX_MS: u64 = 1000;
    const TIMEOUT_RELOAD_MAX_SEC: u64 = 30;
    const TIMEOUT_RELOAD_MIN_SEC: u64 = 10;

    const TIMEOUT_PING_MAX: Duration = Duration::from_millis(Self::TIMEOUT_PING_MAX_MS);
    const TIMEOUT_PING_MIN: Duration = Duration::from_millis(10);
    const TIMEOUT_RANDOM_PEERS: Duration = Duration::from_millis(1000);

    pub fn new(
        start_peers: &Vec<Arc<KeyId>>,
        dht: &Arc<DhtNode>,
        overlay: &Arc<OverlayNode>,
        overlay_id: Arc<OverlayShortId>,
        default_rldp_roundtrip: &Option<u32>,
        cancellation_token: Arc<tokio_util::sync::CancellationToken>
    ) -> Result<Self> {
        let default_rldp_roundtrip = default_rldp_roundtrip.unwrap_or(
            Self::DEFAULT_RLDP_ROUNDTRIP_MS
        );
        let ret = Neighbours {
            peers: NeighboursCache::new(start_peers, default_rldp_roundtrip)?,
            reserve: ReserveNeighbours::new(),
            all_peers: lockfree::set::Set::new(),
            overlay: overlay.clone(),
            dht: dht.clone(),
            overlay_id,
            fail_attempts: AtomicU64::new(0),
            all_attempts: AtomicU64::new(0),
            start: Instant::now(),
            cancellation_token,
            #[cfg(feature = "telemetry")]
            tag_get_capabilities: tag_from_boxed_type::<GetCapabilities>()
        };
        Ok(ret)
    }

    pub fn count(&self) -> usize {
        self.peers.count()
    }

    pub fn new_neighbour(&self, peer: Arc<KeyId>) -> Arc<Neighbour> {
        Arc::new(Neighbour::new(peer, self.peers.cache.default_rldp_roundtrip))
    }

    pub fn add(&self, peer: Arc<KeyId>) -> Result<bool> {
        if self.count() >= MAX_NEIGHBOURS {
            return Ok(false);
        }
        let reserve_peer = self.reserve.find_in_reserve(&peer);
        self.reserve.on_active_added(&reserve_peer);
        self.peers.insert_ex(peer, false, reserve_peer)
    }

    pub fn contains(&self, peer: &Arc<KeyId>) -> bool {
        self.peers.contains(peer)
    }

    pub fn contains_overlay_peer(&self, id: &Arc<KeyId>) -> bool {
        self.all_peers.contains(id)
    }

    pub fn add_overlay_peer(&self, id: Arc<KeyId>) -> bool {
        self.all_peers.insert(id).is_ok()
    }

    pub fn remove_overlay_peer(&self, id: &Arc<KeyId>) {
        self.all_peers.remove(id);
    }

    pub fn all_peers(&self) -> &lockfree::set::Set<Arc<KeyId>> {
        &self.all_peers
    }

    pub fn peer(&self, peer: &Arc<KeyId>) -> Option<Arc<Neighbour>> {
        self.peers.get(peer)
    }

    pub fn got_neighbours(&self, overlay_peers: AddressCache) -> Result<()> {
        log::trace!("got_neighbours");
        log::trace!("neighbours reserve before: {}", self.reserve.descr());
        let mut ex = false;
        let mut rng = rand::thread_rng();
        let mut is_delete_peer = false;

        let (mut iter, mut current) = overlay_peers.first();
        while let Some(elem) = current {
            if self.contains(&elem) {
                current = overlay_peers.next(&mut iter);
                continue;
            }
            let count = self.peers.count();
            let reserve_peer = self.reserve.find_in_reserve(&elem);
            // If peer exists in reserve and it is known to be bad unreliability, skipping
            if reserve_peer.as_ref().map_or(false, |v| !v.is_good()) {
                current = overlay_peers.next(&mut iter);
                continue;
            }

            if count == MAX_NEIGHBOURS {
                let mut cur_worst: Option<Arc<Neighbour>> = None;
                let mut cur_rand:  Option<Arc<Neighbour>> = None;
                let mut cnt: u32 = 0;
                let mut u:i32 = 0;

                // Iteration by active peers to find both worst unreliability peer 
                // and random peer to replace
                for current in self.peers.get_iter() {
                    let un = current.effective_unreliability();
                    if un > u {
                        u = un;
                        cur_worst = Some(current.clone());
                    }
                    if cnt == 0 || rng.gen_range(0, cnt) == 0 {
                        cur_rand = Some(current.clone());
                    }
                    cnt += 1;
                }
                let mut deleted_peer = cur_rand;

                if u > BETTER_REPLACE_UNRELIABILITY {
                    deleted_peer = cur_worst;
                    is_delete_peer = true;
                } else {
                   ex = true;
                }

                let deleted_peer = deleted_peer.ok_or_else(
                    || error!("Internal error: deleted peer is not set!")
                )?;
                log::trace!(
                    "deleted_peer: {} max un: {} is_delete_peer: {}", 
                    deleted_peer.id(), u, is_delete_peer.to_string()
                );
                if is_delete_peer {
                    self.reserve.on_active_replaced(&reserve_peer, deleted_peer.clone());
                }
                self.peers.replace(deleted_peer.id(), elem.clone(), reserve_peer, is_delete_peer)?;

                if is_delete_peer {
                    self.overlay.delete_public_peer(deleted_peer.id(), &self.overlay_id)?;
                    self.remove_overlay_peer(deleted_peer.id());
                    is_delete_peer = false;
                }
            } else {
                self.peers.insert(elem.clone(), reserve_peer)?;
            }

            if ex {
                break;
            }
            current = overlay_peers.next(&mut iter);
        }
        log::trace!("neighbours reserve after: {}", self.reserve.descr());
        log::trace!("/got_neighbours");
        Ok(())
    }

    pub fn start_reload(self: Arc<Self>) {
        NodeNetwork::spawn_background_task(
            self.cancellation_token.clone(),
            async move {
                loop {
                    let sleep_time = rand::thread_rng().gen_range(
                        Self::TIMEOUT_RELOAD_MIN_SEC,
                        Self::TIMEOUT_RELOAD_MAX_SEC
                    );
                    tokio::time::sleep(Duration::from_secs(sleep_time)).await;
                    if let Err(e) = self.reload_neighbours(&self.overlay_id).await {
                        log::warn!("reload neighbours err: {:?}", e);
                    }
                }
            }
        )
    }

    pub fn start_ping(self: Arc<Self>) {
        NodeNetwork::spawn_background_task(
            self.cancellation_token.clone(),
            async move {
                self.ping_neighbours().await;
            }
        )
    }

    pub async fn reload_neighbours(&self, overlay_id: &Arc<OverlayShortId>) -> Result<()> {
        log::trace!("start reload_neighbours (overlay: {})", overlay_id);
        let neighbours_cache = AddressCache::with_limit((MAX_NEIGHBOURS * 2 + 1) as u32);
        self.overlay.get_cached_random_peers(&neighbours_cache, overlay_id, (MAX_NEIGHBOURS * 2) as u32)?;
        self.got_neighbours(neighbours_cache)?;
        log::trace!("finish reload_neighbours (overlay: {})", overlay_id);
        Ok(())
    }

    pub fn start_rnd_peers_process(self: Arc<Self>) {
        NodeNetwork::spawn_background_task(
            self.cancellation_token.clone(),
            async move {
                log::trace!("Wait random peers in overlay {}...", self.overlay_id);
                loop {
                    for peer in self.peers.get_iter() {
                        match self.overlay.get_random_peers(
                            &peer.id(),
                            &self.overlay_id,
                            None
                        ).await {
                            Ok(Some(peers)) => {
                                let mut new_peers = Vec::new();
                                for peer in peers.iter() {
                                    let result: Result<Arc<dyn KeyOption>> = (&peer.id).try_into();
                                    match result {
                                        Ok(key) => if !self.contains_overlay_peer(key.id()) {
                                            new_peers.push(key.id().clone());
                                        },
                                        Err(e) => log::warn!("Bad peer key: {}", e)
                                    }
                                }
                                if !new_peers.is_empty() {
                                    self.clone().add_new_peers(new_peers);
                                }
                            },
                            Err(e) => log::warn!(
                                "Get random peers in overlay {} error: {}",
                                self.overlay_id, e
                            ),
                            _ => {},
                        }
                    }
                    tokio::time::sleep(Self::TIMEOUT_RANDOM_PEERS).await;
                }
            }
        )
    }

    pub async fn stop(&self) {
        self.cancellation_token.cancel();
    }

    fn add_new_peers(self: Arc<Self>, peers: Vec<Arc<KeyId>>) {
        let this = self.clone();
        tokio::spawn(async move {
            for peer in peers.iter() {
                log::trace!("add_new_peers: searching IP for peer {}...", peer);
                match DhtNode::find_address_in_network(&this.dht, peer, None).await {
                    Ok(Some((ip, _))) => {
                        log::info!("add_new_peers: peer {}, IP {}", peer, ip);
                        if !this.add_overlay_peer(peer.clone()) {
                            log::debug!("add_new_peers already present");
                        }
                    }
                    Ok(None) => log::warn!("add_new_peers: peer {}, IP not found", peer),
                    Err(e) => log::warn!("add_new_peers: peer {}, IP search error {}", peer, e)
                }
            }
        });
    }

    pub fn choose_neighbour(&self) -> Result<Option<Arc<Neighbour>>> {
        let count = self.peers.count();
        if count == 0 {
            return Ok(None)
        }

        let mut rng = rand::thread_rng();
        let mut best: Option<Arc<Neighbour>> = None;
        let mut sum = 0;
        let node_stat = self.fail_attempts.load(Ordering::Relaxed) as f64 /
            self.all_attempts.load(Ordering::Relaxed) as f64;

        log::trace!("Select neighbour for overlay {}", self.overlay_id);
        for neighbour in self.peers.get_iter() {
            let unr = neighbour.effective_unreliability();
            let roundtrip_rldp = neighbour.roundtrip_rldp.load(Ordering::Relaxed);
            let roundtrip_adnl = neighbour.roundtrip_adnl.load(Ordering::Relaxed);
            let peer_stat = neighbour.fail_attempts.load(Ordering::Relaxed) as f64 /
                neighbour.all_attempts.load(Ordering::Relaxed) as f64;
            let fines_points = neighbour.fines_points.load(Ordering::Relaxed);

            if count == 1 {
                return Ok(Some(neighbour.clone()))
            }
            let labels = [("neighbour", neighbour.id().to_string())];
            metrics::gauge!("neighbour_unr", unr as f64, &labels);
            log::trace!(
                "Neighbour {}, unr {}, rt ADNL {}, rt RLDP {} (all stat: {:.4}, peer stat: {:.4}/{}))",
                neighbour.id(), unr,
                roundtrip_adnl,
                roundtrip_rldp,
                node_stat,
                peer_stat,
                fines_points
            );
            if unr <= FAIL_UNRELIABILITY {
                if node_stat + (node_stat * 0.2 as f64) < peer_stat {
                    if fines_points > 0 {
                        let _ = neighbour.fines_points.fetch_update(
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                            |x| if x > 0 {
                                Some(x - 1)
                            } else {
                                None
                            }
                        );
                        continue;
                    }
                    neighbour.active_check.store(true, Ordering::Relaxed);
                }

                let w = (1 << (FAIL_UNRELIABILITY - unr)) as i64;
                sum += w;

                if rng.gen_range(0, sum) < w {
                    best = Some(neighbour.clone());
                }
            }
        }

        if let Some(best) = &best {
            log::trace!("Selected neighbour {}", best.id);
        } else {
            log::trace!("Selected neighbour None");
        }
        Ok(best)
    }

    pub fn update_neighbour_stats(
        &self,
        neighbour: &Arc<Neighbour>,
        roundtrip: u64,
        update_flag: u8,
    ) {
        log::trace!("update_neighbour_stats");
        if update_flag & UPDATE_FLAG_SUCCESS > 0 {
            neighbour.query_success(roundtrip, update_flag & UPDATE_FLAG_IS_RDPL > 0);
        } else {
            neighbour.query_failed(roundtrip, update_flag & UPDATE_FLAG_IS_RDPL > 0);
        }
        if update_flag & UPDATE_FLAG_IS_REGISTER > 0 {
            neighbour.all_attempts.fetch_add(1, Ordering::Relaxed);
            if update_flag & UPDATE_FLAG_IS_REG_IN_COMMON_STAT > 0 {
                self.all_attempts.fetch_add(1, Ordering::Relaxed);
            }
            if !(update_flag & UPDATE_FLAG_SUCCESS >0) {
                neighbour.fail_attempts.fetch_add(1, Ordering::Relaxed);
                if update_flag & UPDATE_FLAG_IS_REG_IN_COMMON_STAT > 0 {
                    self.fail_attempts.fetch_add(1, Ordering::Relaxed);
                }
            }
            if neighbour.active_check.load(Ordering::Relaxed) {
                if !(update_flag & UPDATE_FLAG_SUCCESS > 0) {
                    neighbour.fines_points.fetch_add(FINES_POINTS_COUNT, Ordering::Relaxed);
                }
                neighbour.active_check.store(false, Ordering::Relaxed);
            }
        };
        log::trace!("/update_neighbour_stats");
    }

    pub fn got_neighbour_capabilities(
        &self,
        peer: &Arc<Neighbour>,
        _roundtrip: u64,
        capabilities: &Capabilities
    ) {
        peer.update_proto_version(capabilities);
    }

    async fn ping_neighbours(self: &Arc<Self>) {
        let (wait, mut queue_reader) = Wait::new();
        loop {
            let peers = self.peers.count();
            let max_count = min(peers, Self::MAX_PINGS);
            if max_count == 0 {
                log::trace!("No peers in overlay {}", self.overlay_id);
                tokio::time::sleep(Self::TIMEOUT_PING_MAX).await;
                continue
            }
            log::trace!("neighbours: overlay {} count {}", self.overlay_id, peers);

            // Ping active list with priority and reserve nodes otherwise
            let mut ping_reserve = false;
            let (mut next_ping, mut ping_idx) = self.peers.next_for_ping(&self.start);
            next_ping = next_ping.or_else(
                || {
                    ping_reserve = true;
                    let (rv, idx) = self.reserve.next_for_ping(&self.start);
                    ping_idx = idx;
                    rv
                }
            );
            let peer = match next_ping {
                Some(peer) => peer,
                None => {
                    log::trace!("next_for_ping: None");
                    tokio::time::sleep(Self::TIMEOUT_PING_MIN).await;
                    continue
                }
            };
            let last = self.start.elapsed().as_millis() as u64 - peer.last_ping();
            if last < Self::TIMEOUT_PING_MAX_MS {
                tokio::time::sleep(
                    Duration::from_millis(Self::TIMEOUT_PING_MAX_MS - last)
                ).await;
            } else {
                tokio::time::sleep(Self::TIMEOUT_PING_MIN).await;
            }
            let self_cloned = self.clone();
            let wait_cloned = wait.clone();
            let mut count = wait.request();
            peer.set_last_ping(self.start.elapsed().as_millis() as u64);
            tokio::spawn(
                async move {
                    if let Err(e) = self_cloned.update_capabilities(peer, ping_reserve).await {
                        log::warn!("{}; ping_idx #{}", e, ping_idx)
                    }
                    wait_cloned.respond(Some(()));
                }
            );
            while count >= max_count {
                wait.wait(&mut queue_reader, false).await;
                count -= 1;
            }
        }
    }

    async fn update_capabilities(
        self: Arc<Self>, 
        peer: Arc<Neighbour>, 
        ping_reserve: bool
    ) -> Result<()> {
        let now = Instant::now();
        peer.set_last_ping(self.start.elapsed().as_millis() as u64);

        let query = TaggedTlObject {
            object: TLObject::new(GetCapabilities),
            #[cfg(feature = "telemetry")]
            tag: self.tag_get_capabilities
        };
        let timeout = Some(AdnlNode::calc_timeout(peer.roundtrip_adnl()));
        match self.overlay.query(&peer.id, &query, &self.overlay_id, timeout).await {
            Ok(Some(answer)) => {
                let caps: Capabilities = Query::parse(answer, &query.object)?;
                let status = if !ping_reserve { "active" } else { "reserve" };
                log::trace!(
                    "Got capabilities from {} {} {}: {:?}", 
                    status, peer.id, self.overlay_id, caps
                );
                let roundtrip = now.elapsed().as_millis() as u64;
                self.update_neighbour_stats(
                    &peer, 
                    roundtrip, 
                    if !ping_reserve {
                        UPDATE_FLAG_IS_REG_IN_COMMON_STAT | UPDATE_FLAG_SUCCESS
                    } else {
                        UPDATE_FLAG_SUCCESS
                    }
                );
                self.got_neighbour_capabilities(&peer, roundtrip, &caps);
                Ok(())
            },
            _ => {
                // We are not registering failed ping here.
                // Successful ping will improve unreliability.
                // Failed ping will NOT modify unreliability.
                //
                if peer.is_good() {
                    let roundtrip = now.elapsed().as_millis() as u64;
                    self.update_neighbour_stats(
                        &peer, 
                        roundtrip, 
                        if !ping_reserve {
                            UPDATE_FLAG_IS_REG_IN_COMMON_STAT
                        } else {
                            0u8
                        }
                    );
                }
                let status = if !ping_reserve { "active" } else { "reserve" };
                fail!("Capabilities were not received from {} {} (unr {}) {}",
                    status, peer.id, peer.effective_unreliability(), self.overlay_id)
            }
        }
    }

}

#[derive(Clone)]
pub struct NeighboursCache {
    cache: Arc<NeighboursCacheCore>
}

impl NeighboursCache {
    pub fn new(start_peers: &Vec<Arc<KeyId>>, default_rldp_roundtrip: u32) -> Result<Self> {
        let cache = NeighboursCacheCore::new(start_peers, default_rldp_roundtrip)?;
        Ok(NeighboursCache {cache: Arc::new(cache)})
    }
    pub fn contains(&self, peer: &Arc<KeyId>) -> bool {
        self.cache.contains(peer)
    }
    pub fn insert(
        &self,
        peer: Arc<KeyId>,
        existing_in_reserve: Option<Arc<Neighbour>>
    ) -> Result<bool> {
        self.cache.insert(peer, existing_in_reserve)
    }
    pub fn count(&self) -> usize {
        self.cache.count()
    }
    pub fn get(&self, peer: &Arc<KeyId>) -> Option<Arc<Neighbour>> {
        self.cache.get(peer)
    }
    pub fn next_for_ping(&self, start: &Instant) -> (Option<Arc<Neighbour>>, u64) {
        self.cache.next_for_ping(start)
    }
    pub fn replace(
        &self,
        old: &Arc<KeyId>,
        new: Arc<KeyId>,
        existing_in_reserve: Option<Arc<Neighbour>>,
        bad_peer: bool
    ) -> Result<bool> {
        self.cache.replace(old, new, existing_in_reserve, bad_peer)
    }
    fn insert_ex(
        &self,
        peer: Arc<KeyId>,
        silent_insert: bool,
        existing_in_reserve: Option<Arc<Neighbour>>
    ) -> Result<bool> {
        self.cache.insert_ex(peer, silent_insert, existing_in_reserve)
    }
    pub fn get_iter(&self) -> NeighboursCacheIterator {
        NeighboursCacheIterator::new(self.cache.clone())
    }
}

struct NeighboursCacheCore {
    count: AtomicU32,
    next: AtomicU32,
    indices: lockfree::map::Map<u32, Arc<KeyId>>,
    values: lockfree::map::Map<Arc<KeyId>, Arc<Neighbour>>,
    default_rldp_roundtrip: u32
}

impl NeighboursCacheCore {
    
    pub fn new(start_peers: &Vec<Arc<KeyId>>, default_rldp_roundtrip: u32) -> Result<Self> {
        let instance = NeighboursCacheCore {
            count: AtomicU32::new(0),
            next: AtomicU32::new(0),
            indices: lockfree::map::Map::new(),
            values: lockfree::map::Map::new(),
            default_rldp_roundtrip
        };

        let mut index = 0;
        for peer in start_peers.iter() {
            if index < MAX_NEIGHBOURS {
                instance.insert(peer.clone(), None)?;
                index = index + 1;
            }
        }

        Ok(instance)
    }

    pub fn contains(&self, peer: &Arc<KeyId>) -> bool {
        self.values.get(peer).is_some()
    }

    pub fn insert(
        &self,
        peer: Arc<KeyId>,
        existing_in_reserve: Option<Arc<Neighbour>>
    ) -> Result<bool> {
        let status = self.insert_ex(peer, false, existing_in_reserve)?;
        Ok(status)
    }

    pub fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed) as usize
    }

    pub fn get(&self, peer: &Arc<KeyId>) -> Option<Arc<Neighbour>> {
        let result = if let Some (result) = &self.values.get(peer) {
            Some(result.val().clone())
        } else {
            None
        };

        result
    }

    pub fn next_for_ping(&self, start: &Instant) -> (Option<Arc<Neighbour>>, u64) {
        let mut next = self.next.load(Ordering::Relaxed);
        let count = self.count.load(Ordering::Relaxed);
        let started_from = next;
        let mut ret: Option<Arc<Neighbour>> = None;
        let mut ping_idx: u64 = 0;
        loop {
            let key_id = if let Some(key_id) = self.indices.get(&next) {
                key_id
            } else {
                return (None, 0);
            };
            if let Some(neighbour) = self.values.get(key_id.val()) {
                let cur_idx: u64 = next.into();
                next = if next >= count - 1 {
                    0   // ping cyclically
                } else {
                    next + 1
                };
                self.next.store(next, Ordering::Relaxed);
                let neighbour = neighbour.val();
                if start.elapsed().as_millis() as u64 - neighbour.last_ping() < PING_DELAY_MS {
                    // Pinged recently
                    if next == started_from {
                        break
                    } else {
                        continue
                    }
                }
                ret.replace(neighbour.clone());
                ping_idx = cur_idx;
            } else {
                // Value has been updated. Repeat step
                continue
            }
            break
        }
        (ret, ping_idx)
    }

    fn insert_ex(
        &self,
        peer: Arc<KeyId>,
        silent_insert: bool,
        existing_in_reserve: Option<Arc<Neighbour>>
    ) -> Result<bool> {
        let count = self.count.load(Ordering::Relaxed);
        if !silent_insert && (count >= MAX_NEIGHBOURS as u32) {
            fail!("NeighboursCache overflow!");
        }

        let mut is_overflow = false;
        let mut index = 0;
        let insertion = self.values.insert_with(peer.clone(), |_key, prev_gen_val, updated_pair |
            if updated_pair.is_some() {
                lockfree::map::Preview::Discard
            } else if prev_gen_val.is_some() {
                lockfree::map::Preview::Keep
            } else {
                if !silent_insert {
                    index = self.count.fetch_add(1, Ordering::Relaxed);
                    if index >= MAX_NEIGHBOURS as u32 {
                        self.count.fetch_sub(1, Ordering::Relaxed);
                        is_overflow = true;
                    }
                }

                if is_overflow {
                    lockfree::map::Preview::Discard
                } else {
                    lockfree::map::Preview::New(existing_in_reserve.clone().unwrap_or_else(
                        ||Arc::new(Neighbour::new(peer.clone(), self.default_rldp_roundtrip))))
                }
            }
        );

        if is_overflow {
            fail!("NeighboursCache overflow!");
        }

        let status = match insertion {
            lockfree::map::Insertion::Created => true,
            lockfree::map::Insertion::Failed(_) => false,
            lockfree::map::Insertion::Updated(_) => {
                fail!("neighbours: unreachable Insertion::Updated")
            },
        };

        if status && !silent_insert {
            self.indices.insert(index, peer);
        }

        Ok(status)
    }

    pub fn replace(
        &self,
        old: &Arc<KeyId>,
        new: Arc<KeyId>,
        existing_in_reserve: Option<Arc<Neighbour>>,
        bad_peer: bool
    ) -> Result<bool> {
        let new_unr = existing_in_reserve.as_ref().map_or(0, |v| v.effective_unreliability());
        log::info!("started replace (old: {}, new: {}) {}, new_unr: {}",
            &old, &new, if bad_peer { "as a bad peer" } else { "for rotation" }, new_unr);
        let index = if let Some(index) = self.get_index(old) {
            index
        } else {
            fail!("replaced neighbour not found!")
        };
        log::info!("replace func use index: {} (old: {}, new: {})", &index, &old, &new);
        let status_insert = self.insert_ex(new.clone(), true, existing_in_reserve)?;

        if status_insert {
            self.indices.insert(index, new);
            self.values.remove(old);
        }
        log::info!("finish replace (old: {})", &old);
        Ok(status_insert)
    }

    fn get_index(&self, peer: &Arc<KeyId>) -> Option<u32> {
        for index in self.indices.iter() {
            if index.1.cmp(peer) == std::cmp::Ordering::Equal {
                return Some(index.0.clone())
            }
        }
        None
    }
    
}

pub struct NeighboursCacheIterator {
    current: i32,
    parent: Arc<NeighboursCacheCore>
}

impl NeighboursCacheIterator {
    fn new(parent: Arc<NeighboursCacheCore>) -> Self {
        NeighboursCacheIterator {
            current: -1,
            parent: parent,
        }
    }
}

impl Iterator for NeighboursCacheIterator {
    type Item = Arc<Neighbour>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result = None;

        let current = self.current + 1;
        for _ in 0..5 {
            let key_id = if let Some(key_id) = &self.parent.indices.get(&(current as u32)) {
                key_id.val().clone()
            } else {
                return None;
            };

            if let Some(neighbour) = &self.parent.values.get(&key_id) {
                self.current = current;
                result = Some(neighbour.val().clone());
                break;
            } else {
                // Value has been updated. Repeat step
                continue;
            }
        }

        result
    }
}

/// Neighbours, replaced in main active list, are stored in reserve list
struct ReserveNeighbours {
    last_ping: AtomicU64,
    reserve: lockfree::map::Map<Arc<KeyId>, Arc<Neighbour>>
}

impl ReserveNeighbours {
    
    pub fn new() -> Self {
        ReserveNeighbours {
            last_ping: AtomicU64::new(0),
            reserve: lockfree::map::Map::new(),
        }
    }

    /// Save peer stat in reserve when peer is replaced in active list.
    /// Remove its replacement from reserve (if it was in reserve ('reserve_peer'))
    pub fn on_active_replaced(&self, reserve_peer: &Option<Arc<Neighbour>>, peer: Arc<Neighbour>) {
        if reserve_peer.is_some() {
            log::trace!("reserve del: {}", &reserve_peer.as_ref().unwrap().id());
            self.reserve.remove(reserve_peer.as_ref().unwrap().id());
        }
        log::trace!("reserve ins: {}, unr {}", &peer.id(), peer.effective_unreliability());
        self.reserve.insert(peer.id().clone(), peer);
    }

    pub fn on_active_added(&self, reserve_peer: &Option<Arc<Neighbour>>) {
        if reserve_peer.is_some() {
            log::trace!("reserve del2: {}", &reserve_peer.as_ref().unwrap().id());
            self.reserve.remove(reserve_peer.as_ref().unwrap().id());
        }
    }

    fn last_ping(&self) -> u64 {
        self.last_ping.load(Ordering::Relaxed)
    }

    fn set_last_ping(&self, elapsed: u64) {
        self.last_ping.store(elapsed, Ordering::Relaxed)
    }

    /// Description
    pub fn descr(&self) -> String {
        let mut rv: String = String::new();
        for guard in self.reserve.iter() {
            rv.push_str(&format!("{}: unr {}; ",
                guard.key(), guard.val().effective_unreliability()));
        }
        rv
    }

    /// Get next (bad) peer from reserve list, not ping'ed in last PING_DELAY_MS
    pub fn next_for_ping(&self, start: &Instant) -> (Option<Arc<Neighbour>>, u64) {
        let mut rv: Option<Arc<Neighbour>> = None;
        let mut idx: u64 = 0;
        let now = start.elapsed().as_millis() as u64;
        if now - self.last_ping() > RESERVE_PING_DELAY_MS {
            self.set_last_ping(now);
            let mut cur_idx: u64 = 0;
            for neighbour in self.reserve.iter() {
                cur_idx += 1;
                let neighbour = neighbour.val();
                if neighbour.is_good() { continue }
                if rv.as_ref().is_some() && rv.as_ref().unwrap().last_ping() < neighbour.last_ping() {
                    continue
                }
                rv.replace(neighbour.clone());
                idx = cur_idx;
            }
        }
        return (rv, if idx > 0 { idx - 1 } else { 0 } );
    }

    /// Find old peer stats by its KeyId if this peer already was in active list and was replaced
    pub fn find_in_reserve(&self, peer_id: &Arc<KeyId>) -> Option<Arc<Neighbour>> {
        if let Some(neighbour) = self.reserve.get(peer_id) {
            return Some(neighbour.val().clone());
        }
        return None;
    }
    
}
