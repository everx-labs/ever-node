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

use adnl::{common::{Query, TaggedTlObject, Wait}, node::{AdnlNode, AddressCache}};
use crate::engine::STATSD;
use ever_crypto::{Ed25519KeyOption, KeyId};
use dht::DhtNode;
use overlay::{OverlayShortId, OverlayNode};
use rand::{Rng};
use std::{
    cmp::Ordering, time::{Duration, Instant}, sync::Arc,
    sync::atomic::{
        self, AtomicBool, AtomicU32, AtomicI32, AtomicU64, AtomicI64
    }
};
use ton_types::{error, fail, Result};
use ton_api::{
    tag_from_boxed_type,
    ton::{
       TLObject, rpc::ton_node::GetCapabilities, ton_node::Capabilities
    }
};

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
    all_peers: lockfree::set::Set<Arc<KeyId>>,
    overlay_id: Arc<OverlayShortId>,
    overlay: Arc<OverlayNode>,
    dht: Arc<DhtNode>,
    fail_attempts: AtomicU64,
    all_attempts: AtomicU64,
    start: Instant,
    #[cfg(feature = "telemetry")]
    tag_get_capabilities: u32
}

const CAPABILITY_COMPATIBLE: i64 = 0x01;
const VERSION_COMPATIBLE: i32 = 2;

pub const PROTOCOL_CAPABILITIES: i64 = CAPABILITY_COMPATIBLE;
pub const PROTOCOL_VERSION: i32 = VERSION_COMPATIBLE;
pub const STOP_UNRELIABILITY: i32 = 5;
pub const FAIL_UNRELIABILITY: i32 = 10;

const FINES_POINTS_COUNT: u32 = 100;

impl Neighbour {

    pub fn new(id : Arc<KeyId>, default_rldp_roundtrip: u32) ->  Self {
        Self {
            id: id,
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

    pub fn update_proto_version(&self, q: &Capabilities) {
        self.proto_version.store(q.version().clone(), atomic::Ordering::Relaxed);
        self.capabilities.store(q.capabilities().clone(), atomic::Ordering::Relaxed);
    }

    pub fn id(&self) -> &Arc<KeyId> {
        &self.id
    }
    
    pub fn query_success(&self, roundtrip: u64, is_rldp: bool) {
        loop {
            let old_un = self.unreliability.load(atomic::Ordering::Relaxed);
            if old_un > 0 { 
                let new_un = old_un - 1;
                if self.unreliability.compare_exchange(
                    old_un, 
                    new_un, 
                    atomic::Ordering::Relaxed,
                    atomic::Ordering::Relaxed
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
        let _un = self.unreliability.fetch_add(1, atomic::Ordering::Relaxed) + 1;
        let metric = format!("neghbour.{}.failed", self.id);
        STATSD.incr(&metric);
//        log::trace!("query_failed (key_id {}, overlay: ) new value: {}", self.id, un);
        if is_rldp {
            self.update_roundtrip_rldp(roundtrip)
        } else {
            self.update_roundtrip_adnl(roundtrip)
        }
    }
    
// Unused
//    pub fn capabilities(&self) -> i64 {
//        self.capabilities.load(atomic::Ordering::Relaxed)
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
        self.last_ping.load(atomic::Ordering::Relaxed)
    }

    fn roundtrip(storage: &AtomicU64) -> Option<u64> {
        let roundtrip = storage.load(atomic::Ordering::Relaxed);
        if roundtrip == 0 {
            None
        } else {
            Some(roundtrip)
        }
    }

    fn set_last_ping(&self, elapsed: u64) {
        self.last_ping.store(elapsed, atomic::Ordering::Relaxed)
    }

    fn set_roundtrip(storage: &AtomicU64, roundtrip: u64) {
        let roundtrip_old = storage.load(atomic::Ordering::Relaxed);
        let roundtrip = if roundtrip_old > 0 {
            (roundtrip_old + roundtrip) / 2
        } else {
            roundtrip
        };
    //    log::trace!("roundtrip new value: {}", roundtrip);
        storage.store(roundtrip, atomic::Ordering::Relaxed);
    }

}

pub const MAX_NEIGHBOURS: usize = 16;

impl Neighbours {

    const TIMEOUT_PING_MAX: u64 = 1000;         // Milliseconds
    const TIMEOUT_PING_MIN: u64 = 10;           // Milliseconds
    const DEFAULT_RLDP_ROUNDTRIP: u32 = 2000;   // Milliseconds

    pub fn new(
        start_peers: &Vec<Arc<KeyId>>,
        dht: &Arc<DhtNode>,
        overlay: &Arc<OverlayNode>,
        overlay_id: Arc<OverlayShortId>,
        default_rldp_roundtrip: &Option<u32>
    ) -> Result<Self> {
        let default_rldp_roundtrip = default_rldp_roundtrip.unwrap_or(Self::DEFAULT_RLDP_ROUNDTRIP);
        let ret = Neighbours {
            peers: NeighboursCache::new(start_peers, default_rldp_roundtrip)?,
            all_peers: lockfree::set::Set::new(),
            overlay: overlay.clone(),
            dht: dht.clone(),
            overlay_id,
            fail_attempts: AtomicU64::new(0),
            all_attempts: AtomicU64::new(0),
            start: Instant::now(),
            #[cfg(feature = "telemetry")]
            tag_get_capabilities: tag_from_boxed_type::<GetCapabilities>()
        };
        Ok(ret)
    }

    pub fn count(&self) -> usize {
        self.peers.count()
    }

    pub fn add(&self, peer: Arc<KeyId>) -> Result<bool> {
        if self.count() >= MAX_NEIGHBOURS {
            return Ok(false);
        }
        self.peers.insert_ex(peer, false)
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

    pub fn got_neighbours(&self, peers: AddressCache) -> Result<()> {
        log::trace!("got_neighbours");
        let mut ex = false;
        let mut rng = rand::thread_rng();
        let mut is_delete_peer = false;

        let (mut iter, mut current) = peers.first();
        while let Some(elem) = current {
            if self.contains(&elem) {
                current = peers.next(&mut iter);
                continue;
            }
            let count = self.peers.count();

            if count == MAX_NEIGHBOURS {
                let mut a: Option<Arc<KeyId>> = None;
                let mut b: Option<Arc<KeyId>> = None;
                let mut cnt: u32 = 0;
                let mut u:i32 = 0;

                for current in self.peers.get_iter() {
                    let un = current.unreliability.load(atomic::Ordering::Relaxed);
                    if un > u {
                        u = un;
                        a = Some(current.id.clone());
                    }
                    if cnt == 0 || rng.gen_range(0, cnt) == 0 {
                        b = Some(current.id.clone());
                    } 
                    cnt += 1;
                }
                let mut deleted_peer = b;

                if u > STOP_UNRELIABILITY {
                    deleted_peer = a;
                    is_delete_peer = true;
                } else {
                   ex = true;
                }
                let deleted_peer = deleted_peer.ok_or_else(|| error!("Internal error: deleted peer is not set!"))?;
                self.peers.replace(&deleted_peer, elem.clone())?;

                if is_delete_peer {
                    self.overlay.delete_public_peer(&deleted_peer, &self.overlay_id)?;
                    self.remove_overlay_peer(&deleted_peer);
                    is_delete_peer = false;
                }
            } else {
                self.peers.insert(elem.clone())?;
            }

            if ex {
                break;
            }
            current = peers.next(&mut iter);
        }

        log::trace!("/got_neighbours");
        Ok(())
    }

    pub fn start_reload(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                let sleep_time = rand::thread_rng().gen_range(10, 30);
                tokio::time::sleep(Duration::from_secs(sleep_time)).await;
                if let Err(e) = self.reload_neighbours(&self.overlay_id).await {
                    log::warn!("reload neighbours err: {:?}", e);
                }
            }
        });
    }

    pub fn start_ping(self: Arc<Self>) {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.ping_neighbours().await {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    log::error!("ping_neighbours: {}", e)
                }
            }
        });
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
        let _handler = tokio::spawn(async move {

            let receiver = self.overlay.clone();
            let id = self.overlay_id.clone();
            log::trace!("wait random peers...");
            loop {
                let this = self.clone();
                tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                for peer in this.peers.get_iter() {
                    match receiver.get_random_peers(&peer.id(), &id, None).await {
                        Ok(Some(peers)) => {
                            let mut new_peers = Vec::new();

                            for peer in peers.iter() {
                                match Ed25519KeyOption::from_public_key_tl(&peer.id) {
                                    Ok(peer_key) => {
                                        if !this.contains_overlay_peer(peer_key.id()) {
                                            new_peers.push(peer_key.id().clone());
                                        }
                                    }, 
                                    Err(e) => log::warn!("{}",e)
                                }
                            }
                            if new_peers.len() != 0 {
                                this.clone().add_new_peers(new_peers);
                            }
                        },
                        Err(e) => { log::warn!("call get_random_peers is error: {}", e);},
                        _ => {},
                    }
                }
            }
        });
    }

    fn add_new_peers(self: Arc<Self>, peers: Vec<Arc<KeyId>>) {
        let this = self.clone();
        tokio::spawn(async move {
            for peer in peers.iter() {
                log::trace!("add_new_peers: start find address: peer {}", peer);
                match DhtNode::find_address(&this.dht, peer).await {
                    Ok(Some((ip, _))) => {
                        log::info!("add_new_peers: addr peer {}", ip);
                        if !this.add_overlay_peer(peer.clone()) {
                            log::debug!("add_new_peers already present");
                        }
                    }
                    Ok(None) => {
                        log::warn!("add_new_peers: find address - not found");
                    }
                    Err(e) => {
                        log::warn!("add_new_peers: find address error - {}", e);
                    }
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
        let node_stat = self.fail_attempts.load(atomic::Ordering::Relaxed) as f64 / 
            self.all_attempts.load(atomic::Ordering::Relaxed) as f64;

        log::trace!("Select neighbour for overlay {}", self.overlay_id);
        for neighbour in self.peers.get_iter() {
            let mut unr = neighbour.unreliability.load(atomic::Ordering::Relaxed);
            let version = neighbour.proto_version.load(atomic::Ordering::Relaxed);
            let capabilities = neighbour.capabilities.load(atomic::Ordering::Relaxed);
            let roundtrip_rldp = neighbour.roundtrip_rldp.load(atomic::Ordering::Relaxed);
            let roundtrip_adnl = neighbour.roundtrip_adnl.load(atomic::Ordering::Relaxed);
            let peer_stat = neighbour.fail_attempts.load(atomic::Ordering::Relaxed) as f64 /
                neighbour.all_attempts.load(atomic::Ordering::Relaxed) as f64;
            let fines_points = neighbour.fines_points.load(atomic::Ordering::Relaxed);

            if count == 1 {
                return Ok(Some(neighbour.clone()))
            }
            if version < PROTOCOL_VERSION {
                unr += 4;
            } else if (version == PROTOCOL_VERSION) && (capabilities < PROTOCOL_CAPABILITIES) {
                unr += 2;
            }
            let stat_name = format!("neighbour.unr.{}", neighbour.id());
            STATSD.gauge(&stat_name, unr as f64);
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
                            atomic::Ordering::Relaxed, atomic::Ordering::Relaxed, |x| if x > 0 {
                                Some(x - 1) 
                            } else {
                                None 
                            }
                        );
                        continue;
                    }
                    neighbour.active_check.store(true, atomic::Ordering::Relaxed);
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
        peer: &Arc<KeyId>,
        roundtrip: u64,
        success: bool,
        is_rldp: bool,
        is_register: bool
    ) -> Result<()> {
        log::trace!("update_neighbour_stats");
        let it = &self.peers.get(peer);
        if let Some(neighbour) = it {
            if success {
                neighbour.query_success(roundtrip, is_rldp);
            } else {
                neighbour.query_failed(roundtrip, is_rldp);
            }
            if is_register {
                neighbour.all_attempts.fetch_add(1, atomic::Ordering::Relaxed);
                self.all_attempts.fetch_add(1, atomic::Ordering::Relaxed);
                if !success {
                    neighbour.fail_attempts.fetch_add(1, atomic::Ordering::Relaxed);
                    self.fail_attempts.fetch_add(1, atomic::Ordering::Relaxed);
                }
                if neighbour.active_check.load(atomic::Ordering::Relaxed) {
                    if !success {
                        neighbour.fines_points.fetch_add(FINES_POINTS_COUNT, atomic::Ordering::Relaxed);
                    }
                    neighbour.active_check.store(false, atomic::Ordering::Relaxed);
                }
            };
        }
        log::trace!("/update_neighbour_stats");
        Ok(())
    }

    pub fn got_neighbour_capabilities(
        &self, 
        peer: &Arc<KeyId>, 
        _roundtrip: u64, 
        capabilities: &Capabilities
    ) -> Result<()> {
        if let Some(it) = &self.peers.get(peer) {
  //          log::trace!("got_neighbour_capabilities: capabilities: {:?}", capabilities);
  //          log::trace!("got_neighbour_capabilities: roundtrip: {} ms", roundtrip);
            it.update_proto_version(capabilities);
  //      } else {
  //          log::trace!("got_neighbour_capabilities: self.identificators not contains peer");
        }
        Ok(())
    }

    async fn ping_neighbours(self: &Arc<Self>) -> Result<()> {
        let count = self.peers.count();
        if count == 0 {
            log::trace!("No peers in overlay {}", self.overlay_id);
            return Ok(())
        } else {
            log::trace!("neighbours: overlay {} count {}", self.overlay_id, count);
        }
        let max_count = if count < 6 {
            count
        } else {
            6
        };
        let (wait, mut queue_reader) = Wait::new();
        loop {
            let peer = if let Some(peer) = self.peers.next_for_ping(&self.start)? {
                peer
            } else {
                tokio::time::sleep(Duration::from_millis(Self::TIMEOUT_PING_MIN)).await;
                log::trace!("next_for_ping return None");
                continue
            };
            let last = self.start.elapsed().as_millis() as u64 - peer.last_ping();
            if last < Self::TIMEOUT_PING_MAX {
                tokio::time::sleep(Duration::from_millis(Self::TIMEOUT_PING_MAX - last)).await;
            } else {
                tokio::time::sleep(Duration::from_millis(Self::TIMEOUT_PING_MIN)).await;
            }
            let self_cloned = self.clone();
            let wait_cloned = wait.clone();
            let mut count = wait.request();
            tokio::spawn(
                async move {
                    if let Err(e) = self_cloned.update_capabilities(peer).await {
                        log::warn!("{}", e)
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

    async fn update_capabilities(self: Arc<Self>, peer: Arc<Neighbour>) -> Result<()> {
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
                log::trace!("Got capabilities from {} {}: {:?}", peer.id, self.overlay_id, caps);
                let roundtrip = now.elapsed().as_millis() as u64;
                self.update_neighbour_stats(&peer.id, roundtrip, true, false, false)?;
                self.got_neighbour_capabilities(&peer.id, roundtrip, &caps)?;
                Ok(())
            },
            _ => {
                fail!("Capabilities were not received from {} {}", peer.id, self.overlay_id)
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

    pub fn insert(&self, peer: Arc<KeyId>) -> Result<bool> {
        self.cache.insert(peer)
    }

    pub fn count(&self) -> usize {
        self.cache.count()
    }

    pub fn get(&self, peer: &Arc<KeyId>) -> Option<Arc<Neighbour>> {
        self.cache.get(peer)
    }

    pub fn next_for_ping(&self, start: &Instant) -> Result<Option<Arc<Neighbour>>> {
        self.cache.next_for_ping(start)
    }

    pub fn replace(&self, old: &Arc<KeyId>, new: Arc<KeyId>) -> Result<bool> {
        self.cache.replace(old, new)
    }

    fn insert_ex(&self, peer: Arc<KeyId>, silent_insert: bool) -> Result<bool> {
        self.cache.insert_ex(peer, silent_insert)
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
                instance.insert(peer.clone())?;
                index = index + 1;
            }
        }

        Ok(instance)
    }

    pub fn contains(&self, peer: &Arc<KeyId>) -> bool {
        self.values.get(peer).is_some()
    }

    pub fn insert(&self, peer: Arc<KeyId>) -> Result<bool> {
        let status = self.insert_ex(peer, false)?;
        Ok(status)
    }

    pub fn count(&self) -> usize {
        self.count.load(atomic::Ordering::Relaxed) as usize
    }

    pub fn get(&self, peer: &Arc<KeyId>) -> Option<Arc<Neighbour>> {
        let result = if let Some (result) = &self.values.get(peer) {
            Some(result.val().clone())
        } else {
            None
        };

        result
    }

    pub fn next_for_ping(&self, start: &Instant) -> Result<Option<Arc<Neighbour>>> {
        let mut next = self.next.load(atomic::Ordering::Relaxed);
        let count = self.count.load(atomic::Ordering::Relaxed);
        let started_from = next;
        let mut ret: Option<Arc<Neighbour>> = None;
        loop {
            let key_id = if let Some(key_id) = self.indices.get(&next) {
                key_id
            } else {
                fail!("Neighbour index is not found!");
            };
            if let Some(neighbour) = self.values.get(key_id.val()) {
                next = if next >= count - 1 {
                    0   // ping cyclically
                } else {
                    next + 1
                };
                self.next.store(next, atomic::Ordering::Relaxed);
                let neighbour = neighbour.val();
                if start.elapsed().as_millis() as u64 - neighbour.last_ping() < 1000 {
                    // Pinged recently
                    if next == started_from {
                        break
                    } else {
                        if let Some(ret) = &mut ret {
                            if neighbour.last_ping() >= ret.last_ping() {
                                continue
                            }
                        }
                    }
                }
                ret.replace(neighbour.clone());
            } else {
                // Value has been updated. Repeat step
                continue
            }
            break
        }
        Ok(ret)
    }

    fn insert_ex(&self, peer: Arc<KeyId>, silent_insert: bool) -> Result<bool> {
        let count = self.count.load(atomic::Ordering::Relaxed);
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
                    index = self.count.fetch_add(1, atomic::Ordering::Relaxed);
                    if index >= MAX_NEIGHBOURS as u32 {
                        self.count.fetch_sub(1, atomic::Ordering::Relaxed);
                        is_overflow = true;
                    }
                }

                if is_overflow {
                    lockfree::map::Preview::Discard
                } else {
                    lockfree::map::Preview::New(Arc::new(Neighbour::new(peer.clone(), self.default_rldp_roundtrip)))
                }
            }
        );

        if is_overflow {
            failure::bail!("NeighboursCache overflow!");
        }

        let status = match insertion {
            lockfree::map::Insertion::Created => true,
            lockfree::map::Insertion::Failed(_) => false,
            lockfree::map::Insertion::Updated(_) => {
                failure::bail!("neighbours: unreachable Insertion::Updated")
            },
        };

        if status && !silent_insert {
            self.indices.insert(index, peer);
        }

        Ok(status)
    }

    pub fn replace(&self, old: &Arc<KeyId>, new: Arc<KeyId>) -> Result<bool> {
        log::info!("started replace (old: {}, new: {})", &old, &new);
        let index = if let Some(index) = self.get_index(old) {
            index
        } else {
            failure::bail!("replaced neighbour not found!")
        };
        log::info!("replace func use index: {} (old: {}, new: {})", &index, &old, &new);
        let status_insert = self.insert_ex(new.clone(), true)?;

        if status_insert {
            self.indices.insert(index, new);
            self.values.remove(old);
        }
        log::info!("finish replace (old: {})", &old);
        Ok(status_insert)
    }

    fn get_index(&self, peer: &Arc<KeyId>) -> Option<u32> {
        for index in self.indices.iter() {
            if index.1.cmp(peer) == Ordering::Equal {
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
