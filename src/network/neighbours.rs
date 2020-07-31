use adnl::common::{KeyId, KeyOption, Query, Wait};
use dht::DhtNode;
use overlay::{OverlayShortId, OverlayNode};
use rand::{Rng};
use rldp::RldpNode;
use std::{
    cmp::Ordering, time::{Duration, Instant}, sync::Arc,
    sync::atomic::{
        self, AtomicU32, AtomicI32, AtomicU64, AtomicI64
    }
};
use tokio::time::delay_for;
use ton_types::{error, fail, Result};
use ton_api::ton::{
    TLObject, rpc::ton_node::GetCapabilities, ton_node::Capabilities
};

#[derive(Debug)]
pub struct Neighbour {
    id : Arc<KeyId>,
    last_ping: AtomicU64,
    proto_version: AtomicI32,
    capabilities: AtomicI64,
    roundtrip_adnl: AtomicU64,
    roundtrip_rldp: AtomicU64,
    unreliability: AtomicI32
}

pub struct Neighbours {
    peers: NeighboursCache,
    overlay_id: Arc<OverlayShortId>,
    overlay: Arc<OverlayNode>,
    dht: Arc<DhtNode>,
    start: Instant
}

pub const DOWNLOAD_NEXT_PRIORITY: u8 = 1;
pub const PROTO_VERSION: i32 = 2;
pub const PROTO_CAPABILITIES: i64 = 1;
pub const STOP_UNRELIABILITY: i32 = 5;
pub const FAIL_UNRELIABILITY: i32 = 10;

impl Neighbour {

    pub fn new(id : Arc<KeyId>) ->  Result<Self> {
        Ok (Neighbour {
            id: id,
            last_ping: AtomicU64::new(0),
            proto_version: AtomicI32::new(0),
            capabilities: AtomicI64::new(0),
            roundtrip_adnl: AtomicU64::new(0),
            roundtrip_rldp: AtomicU64::new(0),
            //roundtrip_relax_at: 0,
            //roundtrip_weight: 0.0,
            unreliability: AtomicI32::new(0),
        })
    }

    pub fn update_proto_version(&self, q: &Capabilities) {
        self.proto_version.store(q.version().clone(), atomic::Ordering::Relaxed);
        self.capabilities.store(q.capabilities().clone(), atomic::Ordering::Relaxed);
    }

    pub fn id(&self) -> &Arc<KeyId> {
        &self.id
    }
    
    pub fn query_success(&self, t: &Duration, is_rldp: bool) {
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
                    log::trace!("query_success (key_id {}) new value: {}", self.id, new_un);
                }
            }
            break;
        } 
        if is_rldp {
            self.update_roundtrip_rldp(t)
        } else {
            self.update_roundtrip_adnl(t)
        }
    }

    pub fn query_failed(&self, t: &Duration, is_rldp: bool) {
        let un = self.unreliability.fetch_add(1, atomic::Ordering::Relaxed) + 1;
        log::trace!("query_failed (key_id {}, overlay: ) new value: {}", self.id, un);
        if is_rldp {
            self.update_roundtrip_rldp(t)
        } else {
            self.update_roundtrip_adnl(t)
        }
    }
    
    pub fn roundtrip_adnl(&self) -> Option<u64> {
        Self::roundtrip(&self.roundtrip_adnl)
    }

    pub fn roundtrip_rldp(&self) -> Option<u64> {
        Self::roundtrip(&self.roundtrip_rldp)
    }

    pub fn update_roundtrip_adnl(&self, t: &Duration) {
        Self::set_roundtrip(&self.roundtrip_adnl, t)
    }

    pub fn update_roundtrip_rldp(&self, t: &Duration) {
        Self::set_roundtrip(&self.roundtrip_rldp, t)
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

    fn set_roundtrip(storage: &AtomicU64, t: &Duration) {
        let roundtrip = storage.load(atomic::Ordering::Relaxed);
        let roundtrip = (t.as_millis() as u64 + roundtrip) / 2;
        log::trace!("roundtrip new value: {}", roundtrip);
        storage.store(roundtrip, atomic::Ordering::Relaxed);
    }

}

pub const MAX_NEIGHBOURS: usize = 16;
pub const ATTEMPTS: u32 = 1;

impl Neighbours {

    pub fn new(
        start_peers: &Vec<Arc<KeyId>>,
        dht: &Arc<DhtNode>,
        _rldp: Arc<RldpNode>,
        overlay: &Arc<OverlayNode>,
        overlay_id: Arc<OverlayShortId>) -> Result<Self> {

        Ok(Neighbours {
            peers: NeighboursCache::new(start_peers)?,
            dht: dht.clone(),
            overlay: overlay.clone(),
            overlay_id,
            start: Instant::now()
        })
    }

    pub fn count(&self) -> usize {
        self.peers.count()
    }

    pub fn add(&self, peer: Arc<KeyId>) -> Result<bool> {
        if self.count() >= MAX_NEIGHBOURS {
            return Ok(false);
        }
        self.peers.insert_ex(peer, true)
    }

    pub fn contains(&self, peer: &Arc<KeyId>) -> bool {
        self.peers.contains(peer)
    }

    pub fn got_neighbours(&self, peers: Vec<Arc<KeyId>>) -> Result<()> {
        log::trace!("got_neighbours");
        let mut ex = false;
        let mut rng = rand::thread_rng();

        for elem in peers.iter() {
            if self.contains(&elem) {
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
                } else {
                   ex = true;
                }
                self.peers.replace(
                    &deleted_peer
                        .ok_or(failure::err_msg("Internal error: removed peer <a> is not set!"))?, 
                    elem.clone())?;
            } else {
                self.peers.insert(elem.clone())?;
            }

            if ex {
                break;
            }
        }

        log::trace!("/got_neighbours");
        Ok(())
    }

    pub fn start_reload(self: Arc<Self>) {
        let _handler = tokio::spawn(async move {
            loop {
                let sleep_time = rand::thread_rng().gen_range(10, 30);
                delay_for(Duration::from_secs(sleep_time)).await;

                let res_ping = self.reload().await;
                match res_ping {
                    Ok(_) => {},
                    Err(e) => {
                        log::error!("reload neighbours err: {:?}", e);
                    },
                };
            }
        });
    }

    pub fn start_ping(self: Arc<Self>) {
        let _handler = tokio::spawn(async move {
            loop {
                if let Err(e) = self.ping_neighbours().await {
                    log::warn!("ERROR: {}", e)
                }
            }
        });
    }

    pub async fn reload(&self) -> Result<()> {
        self.reload_neighbours(&self.overlay_id).await?;
        Ok(())
    }
    
/*
    pub async fn reload_neighbours(&self, overlay_id: &Arc<OverlayShortId>) -> Result<()> {
        let mut peers: Vec<Arc<KeyId>> = vec![];

        for peer in self.peers.get_iter() {
            let random_peers = self.overlay.get_random_peers(&peer.id, overlay_id).await?;
            if let Some(rnd_peers) = random_peers {
                for random_peer in rnd_peers.iter() {
                    let peer_key = KeyOption::from_tl_public_key(&random_peer.id)?;
                    log::trace!("reload_neighbours: peer {}", peer_key.id());
                    if self.peers.contains(peer_key.id()) {
                        log::trace!("reload_neighbours: peer contains in identificators");
                        continue;
                    }
                    log::trace!("reload_neighbours start find address: peer {}", peer_key.id());
                    let addr = DhtNode::find_ip_address(&self.dht, peer_key.id()).await?;
                    log::info!("reload_neighbours: addr peer {}", addr);
                    peers.push(peer_key.id().clone());
                    self.overlay.add_peer(&addr, random_peer, overlay_id)?;
                }
            } else {
                log::trace!("reload_neighbours: random peers result is None!");
            }
        }

        if peers.len() != 0 {
            self.got_neighbours(peers)?;
        }
        Ok(())
    }
*/
        
    pub async fn reload_neighbours(&self, overlay_id: &Arc<OverlayShortId>) -> Result<()> {
        for peer in self.peers.get_iter() {
            let mut peers: Vec<Arc<KeyId>> = vec![];
            let random_peers = self.overlay.get_random_peers(&peer.id, overlay_id, None).await?;
            if let Some(rnd_peers) = random_peers {
                for random_peer in rnd_peers.iter() {
                    let peer_key = KeyOption::from_tl_public_key(&random_peer.id)?;
                    log::trace!("reload_neighbours: peer {}", peer_key.id());
                    if self.peers.contains(peer_key.id()) {
                        log::trace!("reload_neighbours: peer contains in identificators");
                        continue;
                    }
                    log::trace!("reload_neighbours start find address: peer {}", peer_key.id());
                    let (ip, _) = DhtNode::find_address(&self.dht, peer_key.id()).await?;
                    log::info!("reload_neighbours: addr peer {}", ip);
                    peers.push(peer_key.id().clone());
                    self.overlay.add_public_peer(&ip, random_peer, overlay_id)?;
                }
            } else {
                log::trace!("reload_neighbours: random peers result is None!");
            }
            if peers.len() != 0 {
                self.got_neighbours(peers)?;
            }
        }
/*
        if peers.len() != 0 {
            self.got_neighbours(peers)?;
        }*/
        Ok(())
    }

    pub fn start_rnd_peers_process(self: Arc<Self>) {
        let _handler = tokio::spawn(async move {

            let receiver = self.overlay.clone();
            let id = self.overlay_id.clone();
            log::trace!("wait random peers...");
            loop {
                tokio::time::delay_for(std::time::Duration::from_millis(1000)).await;
                for peer in self.peers.get_iter() {
                    let answer = receiver.get_random_peers(&peer.id(), &id, None).await;
                    match answer {
                        Ok(peers_opt) => {
                            match peers_opt {
                                Some(_peers) => {log::trace!("get_random_peers is some");},
                                None => {
                                    log::trace!("get_random_peers is none");
                                },
                            }
                        },
                        Err(e) => { log::trace!("call get_random_peers is error: {}", e);}
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
/*
        let mut min = i32::MAX;

        for neighbour in self.peers.get_iter() {
            let unr = neighbour.unreliability.load(atomic::Ordering::Relaxed);
            if unr < min {
                min = unr
            }     
        }
*/

        log::trace!("Select neighbour for overlay {}", self.overlay_id);
        for neighbour in self.peers.get_iter() {
            let mut unr = neighbour.unreliability.load(atomic::Ordering::Relaxed);
            let proto_version = neighbour.proto_version.load(atomic::Ordering::Relaxed);
            let capabilities = neighbour.capabilities.load(atomic::Ordering::Relaxed);
            
            if count == 1 {
                return Ok(Some(neighbour.clone()))
            }
            if proto_version < PROTO_VERSION {
                unr += 4;
            } else if proto_version == PROTO_VERSION && capabilities < PROTO_CAPABILITIES {
                unr += 2;
            }  
            log::trace!(
                "Neighbour {}, unr {}, rt ADNL {}, rt RLDP {}", 
                neighbour.id(), unr, 
                neighbour.roundtrip_adnl.load(atomic::Ordering::Relaxed),
                neighbour.roundtrip_rldp.load(atomic::Ordering::Relaxed)
            );
//            unr -= min;
            if unr <= FAIL_UNRELIABILITY {
                let w = (1 << (FAIL_UNRELIABILITY - unr)) as i64;
                sum += w;
                // gen_range(low, heigh): low < height
//                if sum - 1 == 0 {
//                    sum = sum + 1;
//                    w = w + 1;
//                }
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
        t: &Duration, 
        success: bool,
        is_rldp: bool
    ) -> Result<()> {
        log::trace!("update_neighbour_stats");
        let it = &self.peers.get(peer);

        if let Some(neightbour) = it {
            if success {
                neightbour.query_success(t, is_rldp);
            } else {
                neightbour.query_failed(t, is_rldp);
            }
        }
        log::trace!("/update_neighbour_stats");
        Ok(())
    }

    pub fn got_neighbour_capabilities(
        &self, 
        peer: &Arc<KeyId>, 
        t: &Duration, 
        capabilities: &Capabilities
    ) -> Result<()> {
        if let Some(it) = &self.peers.get(peer) {
            log::trace!("got_neighbour_capabilities: capabilities: {:?}", capabilities);
            log::trace!("got_neighbour_capabilities: t: {:?}", t);
            it.update_proto_version(capabilities);
//            it.update_roundtrip(t);
        } else {
            log::trace!("got_neighbour_capabilities: self.identificators not contains peer");
        }

        Ok(())
    }

    pub async fn ping_neighbours(self: &Arc<Self>) -> Result<()> {
        let count = self.peers.count();
        if count == 0 {
            fail!("No peers in overlay {}", self.overlay_id)
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
            let peer = self.peers.next_for_ping(&self.start)?;
            let last = self.start.elapsed().as_millis() as u64 - peer.last_ping();
            if last < 1000 {
                tokio::time::delay_for(Duration::from_millis(1000-last)).await;
            } else {
                tokio::time::delay_for(Duration::from_millis(10)).await;
            }
            let self_cloned = self.clone();
            let wait_cloned = wait.clone();
            let mut count = wait.request();
            tokio::spawn(
                async move {
                    if let Err(e) = self_cloned.update_capabilities(peer).await {
                        log::warn!("ERROR: {}", e)
                    }
                    wait_cloned.respond(Some(())); 
                }
            );
            while count >= max_count {
                wait.wait(&mut queue_reader, false).await;
                count -= 1;     
            }
        }
/*
        let total_now = Instant::now();
        while max_cnt > 0 {
            let peer = self.peers.next_for_ping()?;
            let now = Instant::now();
            let capabilities = self.get_capabilities(&peer.id).await;
            let t = now.elapsed();
            match capabilities {
                Ok(cap) => {
                    self.update_neighbour_stats(&peer.id, &t, true)?;
println!("Good caps {}: {}", peer.id, self.overlay_id);
                    self.got_neighbour_capabilities(&peer.id, &t, &cap)?;
                }
                Err(_e) => {
println!("Bad caps {}: {}", peer.id, self.overlay_id);
                    self.update_neighbour_stats(&peer.id, &t, false)?;
                }
            }
            max_cnt = max_cnt - 1;
        };
        let ret = total_now.elapsed();
        log::trace!("neighbours pinged: overlay {} time {}", self.overlay_id, ret.as_millis());
        Ok(ret)
*/
    }
                                                                            
    async fn update_capabilities(self: Arc<Self>, peer: Arc<Neighbour>) -> Result<()> {
        let now = Instant::now();
        peer.set_last_ping(self.start.elapsed().as_millis() as u64); 
        let query = TLObject::new(GetCapabilities);
log::trace!("Query capabilities from {} {}", peer.id, self.overlay_id);
        match self.overlay.query(&peer.id, &query, &self.overlay_id, None).await {
            Ok(Some(answer)) => {
                let caps: Capabilities = Query::parse(answer, &query)?;
                log::trace!("Got capabilities from {} {}: {:?}", peer.id, self.overlay_id, caps);
                let elapsed = now.elapsed();
                self.update_neighbour_stats(&peer.id, &elapsed, true, false)?;
log::trace!("Good caps {}: {}", peer.id, self.overlay_id);
                self.got_neighbour_capabilities(&peer.id, &elapsed, &caps)?;
                Ok(())
            },
            _ => {
log::trace!("Bad caps {}: {}", peer.id, self.overlay_id);
                let elapsed = now.elapsed();
                self.update_neighbour_stats(&peer.id, &elapsed, false, false)?;
                fail!("Capabilities were not received from {}", peer.id);
            }
        }
    }

}

#[derive(Clone)]
pub struct NeighboursCache {
    cache: Arc<NeighboursCacheCore>
}

impl NeighboursCache {
    pub fn new(start_peers: &Vec<Arc<KeyId>>) -> Result<Self> {
        let cache = NeighboursCacheCore::new(start_peers)?;
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

    pub fn next_for_ping(&self, start: &Instant) -> Result<Arc<Neighbour>> {
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
    values: lockfree::map::Map<Arc<KeyId>, Arc<Neighbour>>
}

impl NeighboursCacheCore {
    pub fn new(start_peers: &Vec<Arc<KeyId>>) -> Result<Self> {
        let instance = NeighboursCacheCore {
            count: AtomicU32::new(0),
            next: AtomicU32::new(0),
            indices: lockfree::map::Map::new(),
            values: lockfree::map::Map::new()
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

    pub fn next_for_ping(&self, start: &Instant) -> Result<Arc<Neighbour>> {
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
                next = if next == count - 1 {
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
        let ret = ret.ok_or_else(|| error!("No neighbours available"))?;
        Ok(ret)
    }

    fn insert_ex(&self, peer: Arc<KeyId>, silent_insert: bool) -> Result<bool> {
        let count = self.count.load(atomic::Ordering::Relaxed);
        if !silent_insert && (count >= MAX_NEIGHBOURS as u32) {
            failure::bail!("NeighboursCache overflow!");
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
                    lockfree::map::Preview::New(Arc::new(Neighbour::new(peer.clone()).unwrap()))
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
        let index = if let Some(index) = self.get_index(old) {
            index
        } else {
            failure::bail!("replaced neighbour not found!")
        };

        let status_insert = self.insert_ex(new.clone(), true)?;

        if status_insert {
            self.indices.insert(index, new);
            self.values.remove(old);
        }
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
