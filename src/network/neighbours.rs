use crate::network::node_network::NodeNetwork;
use adnl::{
    common::{KeyId, KeyOption, serialize_append}, node::AdnlNodeConfig
};
use dht::DhtNode;
use overlay::{OverlayShortId, OverlayNode};
use rand::{Rng};
use rldp::RldpNode;
use std::{
    cmp::Ordering, {time::Duration}, sync::Arc, io::Cursor,
    sync::atomic::{
        self, AtomicU32, AtomicI32, AtomicU64, AtomicI64
    }
};
use tokio::time::delay_for;
use ton_api::{Deserializer, BoxedSerialize, BoxedDeserialize};
use ton_types::{fail, Result};
use ton_api::ton::{
    rpc::{
        ton_node::{GetCapabilities}
    },
    ton_node::{Capabilities},
};

#[derive(Debug)]
pub struct Neighbour {
    id : Arc<KeyId>,
    proto_version: AtomicI32,
    capabilities: AtomicI64,
    roundtrip: AtomicU64,
    unreliability: AtomicI32
}

pub struct Neighbours {
    peers: NeighboursCache,
    overlay_id: Arc<OverlayShortId>,
    overlay: Arc<OverlayNode>,
    rldp: Arc<RldpNode>,
    dht: Arc<DhtNode>
}


pub const DOWNLOAD_NEXT_PRIORITY: u8 = 1;
pub const PROTO_VERSION: i32 = 2;
pub const PROTO_CAPABILITIES: i64 = 1;
pub const STOP_UNRELIABILITY: i32 = 50;
pub const FAIL_UNRELIABILITY: i32 = 100;

impl Neighbour {

    pub fn new(id : Arc<KeyId>) ->  Result<Self> {
        Ok (Neighbour {
            id: id,
            proto_version: AtomicI32::new(0),
            capabilities: AtomicI64::new(0),
            roundtrip: AtomicU64::new(0),
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
    
    pub fn query_success(&self, t: &Duration) {
        log::trace!("query_success");
        let mut un = self.unreliability.load(atomic::Ordering::Relaxed);
        un -= 10;
        if un < 0 {
            un = 0;
        }
        log::trace!("query_success (key_id {}) new value: {}", self.id, un);
        self.unreliability.store(un, atomic::Ordering::Relaxed);

        self.update_roundtrip(t);
    }

    pub fn query_failed(&self) {
        log::trace!("query_failed");
        let mut un = self.unreliability.load(atomic::Ordering::Relaxed);
        un += 10;
        log::trace!("query_failed (key_id {}, overlay: ) new value: {}", self.id, un);
        self.unreliability.store(un, atomic::Ordering::Relaxed);
    }

    pub fn update_roundtrip(&self, t: &Duration) {
        let roundtrip = self.roundtrip.load(atomic::Ordering::Relaxed);
        let roundtrip = (t.as_millis() as u64 + roundtrip) / 2;
        log::trace!("roundtrip new value: {}", roundtrip);
        self.roundtrip.store(roundtrip, atomic::Ordering::Relaxed);
        //self.roundtrip = (t.as_millis() + self.roundtrip) / 2;
    }
}

pub const MAX_NEIGHTBOURS: u32 = 16;
pub const ATTEMPTS: u32 = 1;

impl Neighbours {
    pub fn new(
        start_peers: &Vec<Arc<KeyId>>,
        dht: &Arc<DhtNode>,
        rldp: Arc<RldpNode>,
        overlay: &Arc<OverlayNode>,
        overlay_id: Arc<OverlayShortId>) -> Result<Self> {

        Ok (Neighbours {
                peers: NeighboursCache::new(start_peers)?,
                dht: dht.clone(),
                overlay: overlay.clone(),
                overlay_id: overlay_id,
                rldp: rldp
        })
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

            if count == MAX_NEIGHTBOURS {
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
                        log::trace!("reload neighbours err: {}", e);
                    },
                };
            }
        });
    }

    pub fn start_ping(self: Arc<Self>) {
        let _handler = tokio::spawn(async move {
            loop {
                let sleep_time = rand::thread_rng().gen_range(500, 1000);
                delay_for(Duration::from_millis(sleep_time)).await;
                let res_ping = self.ping_neighbours().await;
                match res_ping {
                    Ok(_) => {},
                    Err(e) => {
                        log::trace!("ping neighbours err: {}", e);
                    },
                };
            }
        });
    }

    pub async fn reload(&self) -> Result<()> {
        self.reload_neighbours(&self.overlay_id).await?;
        Ok(())
    }

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
                    let addr = self.dht.find_ip_address(peer_key.id()).await?;
                    log::info!("reload_neighbours: addr peer {}", addr );
                    let overlay_peer = AdnlNodeConfig::from_ip_address_and_key(
                        &addr.to_string(), 
                        peer_key,
                        NodeNetwork::TAG_OVERLAY_KEY
                    )?;
                    peers.push(overlay_peer.key_by_tag(NodeNetwork::TAG_OVERLAY_KEY)?.id().clone());
                    self.overlay.add_peer(
                        overlay_peer.ip_address(),
                        &overlay_peer.key_by_tag(NodeNetwork::TAG_OVERLAY_KEY)?
                    )?;
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


    pub fn start_rnd_peers_process(self: Arc<Self>) {
        let _handler = tokio::spawn(async move {

            let receiver = self.overlay.clone();
            let id = self.overlay_id.clone();
            log::trace!("wait random peers...");
            loop {
                tokio::time::delay_for(std::time::Duration::from_millis(1000)).await;
                for peer in self.peers.get_iter() {
                    let answer = receiver.get_random_peers(&peer.id(), &id).await;
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

        for neighbour in self.peers.get_iter() {
            let mut unr: i32 = neighbour.unreliability.load(atomic::Ordering::Relaxed);
            let proto_version = neighbour.proto_version.load(atomic::Ordering::Relaxed);
            let capabilities = neighbour.capabilities.load(atomic::Ordering::Relaxed);
            
            if count == 1 {
                return Ok(Some(neighbour.clone()))
            }
            if proto_version < PROTO_VERSION {
                unr += 40;
            } else if proto_version == PROTO_VERSION && capabilities == PROTO_CAPABILITIES {
                unr += 20;
            }
            if unr <= FAIL_UNRELIABILITY {
                let mut w = 1 << ((FAIL_UNRELIABILITY - unr) / 10) as i64;
                sum += w;
                // gen_range(low, heigh): low < height
                if sum - 1 == 0 {
                    sum = sum + 1;
                    w = w + 1;
                }
                if rng.gen_range(0, sum - 1) <= w - 1 {
                    best = Some(neighbour.clone());
                }
            }
        }

        Ok(best)
    }

    pub fn update_neighbour_stats(&self, peer: &Arc<KeyId>, t: &Duration, success: bool) -> Result<()> {
        log::trace!("update_neighbour_stats");
        let it = &self.peers.get(peer);

        if let Some(neightbour) = it {
            if success {
                neightbour.query_success(t);
            } else {
                neightbour.query_failed();
            }
        }
        log::trace!("/update_neighbour_stats");
        Ok(())
    }

    pub fn got_neighbour_capabilities(&self, peer: &Arc<KeyId>, t: &Duration, capabilities: &Capabilities) -> Result<()> {
        if let Some(it) = &self.peers.get(peer) {
            log::trace!("got_neighbour_capabilities: capabilities: {:?}", capabilities);
            log::trace!("got_neighbour_capabilities: t: {:?}", t);
            it.update_proto_version(capabilities);
            it.update_roundtrip(t);
        } else {
            log::trace!("got_neighbour_capabilities: self.identificators not contains peer");
        }

        Ok(())
    }

    pub async fn ping_neighbours(&self) -> Result<()> {
        log::trace!("ping neighbours");
        let count = self.peers.count();
        log::trace!("neigbours count : {}", count);
        if count == 0 {
            return Ok(());
        }
        let mut max_cnt = 6;

        if max_cnt > count{
            max_cnt = count;
        }
        while max_cnt > 0 {
            let peer = self.peers.next_for_ping()?;
            let now = std::time::Instant::now();
            let capabilities = self.get_capabilities(&peer.id, ATTEMPTS).await;
            let t = now.elapsed();
            match capabilities {
                Ok(cap) => {
                    self.update_neighbour_stats(&peer.id, &t, true)?;
                    self.got_neighbour_capabilities(&peer.id, &t, &cap)?;
                }
                Err(_e) => {
                    self.update_neighbour_stats(&peer.id, &t, false)?;
                }
            }
            max_cnt = max_cnt - 1;
        };
        Ok(())
    }

    async fn send_query<T: BoxedSerialize, D: BoxedDeserialize>(
        &self,
        peer: &Arc<KeyId>,
        request: T,
        attempts: u32
    ) -> Result<D> {
        let res = self.send_data_query(request, peer, attempts).await?;
        Ok(Deserializer::new(&mut Cursor::new(res)).read_boxed()?)
    }

    async fn send_data_query<T: BoxedSerialize>(
        &self,
        request: T,
        peer: &Arc<KeyId>,
        attempts: u32
    ) -> Result<Vec<u8>> {
        let mut query = self.overlay.get_query_prefix(&self.overlay_id)?;
        serialize_append(&mut query, &request)?;
        let data = Arc::new(query);

        'attempts: for _ in 0..attempts {
            
            let res = self.overlay.query_via_rldp(
                &self.rldp,
                peer,
                &data,
                Some(10 * 1024 * 1024)
            ).await?;

            if let Some(res) = res {
                return Ok(res)
            } else {
                continue 'attempts;
            }
        }
        failure::bail!("Data was not downloaded after {} attempts!", attempts);
    }

    async fn get_capabilities(&self, peer: &Arc<KeyId>, attempts: u32) -> Result<Capabilities> {
        Ok(
            self.send_query(
                peer,
                GetCapabilities {},
                attempts
            ).await?
        )
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

    pub fn count(&self) -> u32 {
        self.cache.count()
    }

    pub fn get(&self, peer: &Arc<KeyId>) -> Option<Arc<Neighbour>> {
        self.cache.get(peer)
    }

    pub fn next_for_ping(&self) -> Result<Arc<Neighbour>> {
        self.cache.next_for_ping()
    }

    pub fn replace(&self, old: &Arc<KeyId>, new: Arc<KeyId>) -> Result<bool> {
        self.cache.replace(old, new)
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
            if index < MAX_NEIGHTBOURS {
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

    pub fn count(&self) -> u32 {
        self.count.load(atomic::Ordering::Relaxed)
    }

    pub fn get(&self, peer: &Arc<KeyId>) -> Option<Arc<Neighbour>> {
        let result = if let Some (result) = &self.values.get(peer) {
            Some(result.val().clone())
        } else {
            None
        };

        result
    }

    pub fn next_for_ping(&self) -> Result<Arc<Neighbour>> {
        let mut next = self.next.load(atomic::Ordering::Relaxed);
        let count = self.count.load(atomic::Ordering::Relaxed);
        for _ in 0..5 {

            let id = if let Some(key_id) = &self.indices.get(&next) {
                key_id.val().clone()
            } else {
                fail!("index neighbour not found!");
            };

            if let Some(neighbour) = &self.values.get(&id) {
                next = if next == count - 1 {
                    0   // ping cyclically
                } else {
                    next + 1
                };

                self.next.store(next, atomic::Ordering::Relaxed);
                return Ok(neighbour.val().clone());
            }else {
                // Value has been updated. Repeat step
                continue;
            }
        }

        fail!("neighbours cache is fail!")
    }

    fn insert_ex(&self, peer: Arc<KeyId>, silent_insert: bool) -> Result<bool> {
        let count = self.count.load(atomic::Ordering::Relaxed);
        if !silent_insert && count >= MAX_NEIGHTBOURS {
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
                    if index >= MAX_NEIGHTBOURS {
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