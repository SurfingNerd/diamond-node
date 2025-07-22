use crate::host::HostInfo;
use ethereum_types::{H256, H512};
use lru_cache::LruCache;
use mio::net::TcpStream;
use std::{collections::{BTreeMap, BTreeSet}, sync::Arc, time::Duration};
use crate::{io::*, node_table::*, session::Session};
use network::{Error, ErrorKind, NetworkIoMessage, PeerId};
use parking_lot::{Mutex, RwLock, RwLockReadGuard};

pub type SharedSession = Arc<Mutex<Session>>;
pub type NodeId = H512;

fn socket_address_to_string(socket: &TcpStream) -> String {
    socket.peer_addr().map_or("unknown".to_string(), |a| a.to_string())
}

pub struct SessionContainer {
    max_sessions: usize,
    max_handshakes: usize,
    handshakes: Arc<RwLock<BTreeMap<usize, SharedSession>>>,
    sessions: Arc<RwLock<BTreeMap<usize, SharedSession>>>,
    expired_sessions: Arc<RwLock<Vec<SharedSession>>>,
    handshake_slots_used: Mutex<BTreeSet<usize>>,
    session_slots_used: Mutex<BTreeSet<usize>>,
    node_id_to_session: Mutex<LruCache<NodeId, usize>>,
    handshake_slot_min: usize,
    handshake_slot_max: usize,
    session_slot_min: usize,
}

impl SessionContainer {
    pub fn new(first_handshake_token: usize, max_sessions: usize, max_node_mappings: usize, max_handshakes: usize) -> Self {
        let handshake_slot_max = first_handshake_token + max_handshakes;
        let session_slot_min = handshake_slot_max + 1;

        SessionContainer {
            max_sessions,
            max_handshakes,
            handshakes: Arc::new(RwLock::new(BTreeMap::new())),
            sessions: Arc::new(RwLock::new(BTreeMap::new())),
            expired_sessions: Arc::new(RwLock::new(Vec::new())),
            handshake_slots_used: Mutex::new(BTreeSet::new()),
            session_slots_used: Mutex::new(BTreeSet::new()),
            node_id_to_session: Mutex::new(LruCache::new(max_node_mappings)),
            handshake_slot_min: first_handshake_token,
            handshake_slot_max,
            session_slot_min,
        }
    }

    pub fn read(&self) -> RwLockReadGuard<BTreeMap<usize, SharedSession>> {
        self.sessions.read()
    }

    pub fn read_handshakes(&self) -> RwLockReadGuard<BTreeMap<usize, SharedSession>> {
        self.handshakes.read()
    }

    fn is_handshake_slot(&self, token: usize) -> bool {
        token >= self.handshake_slot_min && token <= self.handshake_slot_max
    }

    fn is_session_slot(&self, token: usize) -> bool {
        token >= self.session_slot_min
    }

    fn allocate_handshake_slot(&self) -> Option<usize> {
        let mut used_slots = self.handshake_slots_used.lock();
        (self.handshake_slot_min..=self.handshake_slot_max)
            .find(|&slot| !used_slots.contains(&slot))
            .map(|slot| {
                used_slots.insert(slot);
                slot
            })
    }

    fn allocate_session_slot(&self, node_id: &NodeId) -> Option<usize> {
        let mut used_slots = self.session_slots_used.lock();
        let session_slot_max = self.session_slot_min + self.max_sessions - 1;

        (self.session_slot_min..=session_slot_max)
            .find(|&slot| !used_slots.contains(&slot))
            .map(|slot| {
                used_slots.insert(slot);
                if let Some(old) = self.node_id_to_session.lock().insert(node_id.clone(), slot) {
                    warn!(target: "network", "Node ID {} already exists with slot {}, overwriting with {}", node_id, old, slot);
                }
                slot
            })
    }

    fn release_handshake_slot(&self, slot: usize) {
        if self.is_handshake_slot(slot) {
            self.handshake_slots_used.lock().remove(&slot);
        }
    }

    fn release_session_slot(&self, slot: usize) {
        if self.is_session_slot(slot) {
            self.session_slots_used.lock().remove(&slot);
        }
    }

    fn reuse_session_slot(&self, slot: usize, node_id: &NodeId) -> bool {
        if !self.is_session_slot(slot) {
            return false;
        }

        let mut used_slots = self.session_slots_used.lock();
        used_slots.insert(slot);

        if let Some(old) = self.node_id_to_session.lock().insert(node_id.clone(), slot) {
            if old != slot {
                debug!(target: "network", "Node ID {} changed from slot {} to {}", node_id, old, slot);
            }
        }

        true
    }

    pub fn session_count_try(&self, lock_duration: Duration) -> Option<(usize, usize, usize)> {
        let mut handshakes = 0;
        let mut egress = 0;
        let mut ingress = 0;
        let deadline = time_utils::DeadlineStopwatch::new(lock_duration);

        if let Some(handshake_lock) = self.handshakes.try_read_for(deadline.time_left()) {
            for (_, session) in handshake_lock.iter() {
                if session.try_lock_for(deadline.time_left()).is_none() {
                    return None;
                }
                handshakes += 1;
            }
        } else {
            return None;
        }

        if let Some(session_lock) = self.sessions.try_read_for(deadline.time_left()) {
            for (_, session) in session_lock.iter() {
                let session = session.try_lock_for(deadline.time_left());
                if session.is_none() {
                    return None;
                }
                let session = session.unwrap();
                if session.is_ready() {
                    if session.info.originated {
                        egress += 1;
                    } else {
                        ingress += 1;
                    }
                }
            }
        } else {
            return None;
        }

        Some((handshakes, egress, ingress))
    }

    pub fn create_handshake_connection(
        &self,
        socket: TcpStream,
        io: &IoContext<NetworkIoMessage>,
        nonce: &H256,
        host: &HostInfo,
    ) -> Result<usize, Error> {
        let handshake_slot = self.allocate_handshake_slot().ok_or(ErrorKind::TooManyConnections)?;
        trace!(target: "network", "Creating handshake session for peer: {} slot: {}", socket_address_to_string(&socket), handshake_slot);

        let new_session = Session::new(io, socket, handshake_slot, None, nonce, host)?;

        let session_arc = Arc::new(Mutex::new(new_session));
        let mut handshakes = self.handshakes.write();
        handshakes.insert(handshake_slot, session_arc);

        io.register_stream(handshake_slot).map_err(|err| {
            debug!(target: "network", "Failed to register stream for handshake slot: {} : {}", handshake_slot, err);
            handshakes.remove(&handshake_slot);
            self.release_handshake_slot(handshake_slot);
            err
        })?;

        Ok(handshake_slot)
    }

    pub fn create_connection(
        &self,
        socket: TcpStream,
        id: Option<&NodeId>,
        io: &IoContext<NetworkIoMessage>,
        nonce: &H256,
        host: &HostInfo,
    ) -> Result<usize, Error> {
        if id.is_none() {
            return self.create_handshake_connection(socket, io, nonce, host);
        }

        let node_id = id.unwrap();
        let mut expired_sessions = self.expired_sessions.write();
        let mut sessions = self.sessions.write();

        if let Some(existing_slot) = self.node_id_to_session.lock().get_mut(node_id).cloned() {
            if let Some(existing_session_mutex) = sessions.get(&existing_slot).cloned() {
                let session_expired = {
                    let session = existing_session_mutex.lock();
                    if session.info.id != Some(*node_id) {
                        error!(target: "network", "host cache inconsistency: Session node id mismatch. expected slot: {} has node id: {}.", existing_slot, session.info.id.unwrap_or_default());
                        return Err(ErrorKind::HostCacheInconsistency.into());
                    }
                    session.expired()
                };

                if session_expired {
                    debug!(target: "network", "Creating new session for expired {} slot: {} node id: {:?}", socket_address_to_string(&socket), existing_slot, node_id);

                    let new_session = Session::new(io, socket, existing_slot, Some(node_id), nonce, host)?;
                    let new_session_arc = Arc::new(Mutex::new(new_session));
                    let old_session = sessions.insert(existing_slot, new_session_arc);

                    if let Some(old) = old_session {
                        expired_sessions.push(old);
                    } else {
                        error!(target: "network", "host cache inconsistency: Session for node id {} was not found in sessions map, but it should be there.", node_id);
                    }

                    self.reuse_session_slot(existing_slot, node_id);
                    if let Err(err) = io.register_stream(existing_slot) {
                        debug!(target: "network", "Failed to register stream for slot: {} : {}", existing_slot, err);
                    }

                    Ok(existing_slot)
                } else {
                    debug!(target: "network", "Session already exists for node id: {}", node_id);
                    Err(ErrorKind::AlreadyExists.into())
                }
            } else {
                debug!(target: "network", "reusing slot {} for node: {}", existing_slot, node_id);
                let new_session = Session::new(io, socket, existing_slot, Some(node_id), nonce, host)?;
                sessions.insert(existing_slot, Arc::new(Mutex::new(new_session)));
                self.reuse_session_slot(existing_slot, node_id);

                if let Err(err) = io.register_stream(existing_slot) {
                    warn!(target: "network", "Failed to register stream for reused slot: {} : {}", existing_slot, err);
                }

                Ok(existing_slot)
            }
        } else {
            let new_slot = self.allocate_session_slot(node_id).ok_or(ErrorKind::TooManyConnections)?;
            trace!(target: "network", "Creating new session {} for new node id: {} with slot {}", socket_address_to_string(&socket), node_id, new_slot);

            let new_session = Session::new(io, socket, new_slot, Some(node_id), nonce, host)?;
            let session_arc = Arc::new(Mutex::new(new_session));
            sessions.insert(new_slot, session_arc);

            io.register_stream(new_slot).map_err(|err| {
                debug!(target: "network", "Failed to register stream for slot: {} : {}", new_slot, err);
                sessions.remove(&new_slot);
                self.release_session_slot(new_slot);
                err
            })?;

            Ok(new_slot)
        }
    }

    pub fn promote_handshake_to_session(
        &self,
        handshake_slot: usize,
        node_id: &NodeId,
        io: &IoContext<NetworkIoMessage>,
    ) -> Result<usize, Error> {
        if !self.is_handshake_slot(handshake_slot) {
            return Err(ErrorKind::HostCacheInconsistency.into());
        }

        let mut expired_sessions = self.expired_sessions.write();
        let mut handshakes = self.handshakes.write();
        let mut sessions = self.sessions.write();

        let handshake_session = handshakes.remove(&handshake_slot).ok_or(ErrorKind::HostCacheInconsistency)?;
        self.release_handshake_slot(handshake_slot);

        if let Some(existing_slot) = self.node_id_to_session.lock().get_mut(node_id).cloned() {
            if let Some(existing_session_mutex) = sessions.get(&existing_slot).cloned() {
                let session_expired = existing_session_mutex.lock().expired();

                if session_expired {
                    debug!(target: "network", "Promoting handshake {} to replace expired session {} for node {}", handshake_slot, existing_slot, node_id);

                    let mut session = handshake_session.lock();
                    session.set_token(existing_slot)?;

                    let old_session = sessions.insert(existing_slot, handshake_session.clone());
                    if let Some(old) = old_session {
                        expired_sessions.push(old);
                    }

                    self.reuse_session_slot(existing_slot, node_id);

                    if let Err(err) = io.deregister_stream(handshake_slot) {
                        debug!(target: "network", "Failed to deregister handshake stream {}: {}", handshake_slot, err);
                    }

                    if let Err(err) = io.register_stream(existing_slot) {
                        debug!(target: "network", "Failed to register promoted session stream {}: {}", existing_slot, err);
                    }

                    Ok(existing_slot)
                } else {
                    expired_sessions.push(handshake_session);
                    debug!(target: "network", "Active session already exists for node {}, discarding handshake {}", node_id, handshake_slot);
                    Err(ErrorKind::AlreadyExists.into())
                }
            } else {
                debug!(target: "network", "Promoting handshake {} to reuse slot {} for node {}", handshake_slot, existing_slot, node_id);

                let mut session = handshake_session.lock();
                session.set_token(existing_slot)?;

                sessions.insert(existing_slot, handshake_session.clone());
                self.reuse_session_slot(existing_slot, node_id);

                if let Err(err) = io.deregister_stream(handshake_slot) {
                    debug!(target: "network", "Failed to deregister handshake stream {}: {}", handshake_slot, err);
                }

                if let Err(err) = io.register_stream(existing_slot) {
                    warn!(target: "network", "Failed to register promoted session stream {}: {}", existing_slot, err);
                }

                Ok(existing_slot)
            }
        } else {
            let new_session_slot = self.allocate_session_slot(node_id).ok_or(ErrorKind::TooManyConnections)?;

            debug!(target: "network", "Promoting handshake {} to new session {} for node {}", handshake_slot, new_session_slot, node_id);

            let mut session = handshake_session.lock();
            session.set_token(new_session_slot)?;

            sessions.insert(new_session_slot, handshake_session.clone());

            if let Err(err) = io.deregister_stream(handshake_slot) {
                debug!(target: "network", "Failed to deregister handshake stream {}: {}", handshake_slot, err);
            }

            if let Err(err) = io.register_stream(new_session_slot) {
                debug!(target: "network", "Failed to register promoted session stream {}: {}", new_session_slot, err);
                sessions.remove(&new_session_slot);
                self.release_session_slot(new_session_slot);
                return Err(err.into());
            }

            Ok(new_session_slot)
        }
    }

    pub fn get_handshake(&self, slot: usize) -> Option<SharedSession> {
        if self.is_handshake_slot(slot) {
            self.handshakes.read().get(&slot).cloned()
        } else {
            None
        }
    }

    pub fn get_session(&self, slot: usize) -> Option<SharedSession> {
        if self.is_session_slot(slot) {
            self.sessions.read().get(&slot).cloned()
        } else {
            None
        }
    }

    pub fn get_session_for(&self, id: &NodeId) -> Option<SharedSession> {
        self.node_id_to_session.lock().get_mut(id).and_then(|slot| {
            let sessions = self.sessions.read();
            sessions.get(slot).cloned()
        })
    }

    pub fn node_id_to_peer_id(&self, node_id: &NodeId, only_available_sessions: bool) -> Option<usize> {
        self.node_id_to_session.lock().get_mut(node_id).and_then(|slot| {
            if !only_available_sessions {
                Some(*slot)
            } else {
                let sessions = self.sessions.read();
                if sessions.contains_key(slot) {
                    Some(*slot)
                } else {
                    None
                }
            }
        })
    }

    pub fn register_finalized_handshake(&self, slot: usize, id: Option<&NodeId>) {
        let node_id = match id {
            Some(id) => id.clone(),
            None => {
                error!(target: "network", "Tried to register finalized handshake without node id");
                return;
            }
        };

        if self.is_session_slot(slot) {
            if let Some(old) = self.node_id_to_session.lock().insert(node_id, slot) {
                if old != slot {
                    debug!(target: "network", "handshake completed: changed primary session for node id {} from slot {} to {}", node_id, old, slot);
                }
            } else {
                debug!(target: "network", "handshake completed: node id {} registered primary session slot {}", node_id, slot);
            }
        }
    }

    pub fn should_delete_duplicate_session(&self, session: &SharedSession) -> Option<PeerId> {
        let (node_id, slot, uid) = {
            let lock = session.lock();
            let slot = lock.token();
            let node_id = lock.id().cloned();
            let uid = lock.info.session_uid.clone();

            match (node_id, uid) {
                (Some(node_id), Some(uid)) => (node_id, slot, uid),
                _ => {
                    if self.is_handshake_slot(slot) {
                        return None;
                    }
                    warn!(target: "network", "Tried to delete duplicate session without node id or session uid");
                    return None;
                }
            }
        };

        if self.is_handshake_slot(slot) {
            return None;
        }

        if let Some(existing_slot) = self.node_id_to_peer_id(&node_id, true) {
            if existing_slot != slot {
                if let Some(existing_session_mutex) = self.get_session_for(&node_id) {
                    let existing_lock = existing_session_mutex.lock();
                    if existing_lock.expired() {
                        trace!(target: "network", "existing peer session {existing_slot} is already about to get deleted.");
                        return None;
                    }

                    if let Some(existing_uid) = existing_lock.info.session_uid {
                        if existing_uid.lt(&uid) {
                            trace!(target: "network", "Session {slot} has a duplicate :{existing_slot} for {node_id}, deleting this session");
                            return Some(slot);
                        } else {
                            trace!(target: "network", "Session {slot} has a duplicate :{existing_slot} for {node_id}, deleting duplicated session");
                            return Some(existing_slot);
                        }
                    }
                } else {
                    trace!(target: "network", "No session active for {node_id} with slot {existing_slot}");
                }
                trace!(target: "network", "Session {slot} has a duplicate :{existing_slot} {node_id}");
                return Some(existing_slot);
            }
        } else {
            trace!(target: "network", "No session known for {node_id}");
        }

        None
    }

    pub fn cleanup_expired_handshakes(&self, io: &IoContext<NetworkIoMessage>) {
        let expired_sessions = self.expired_sessions.clone();
        let handshakes = self.handshakes.clone();

        let mut expired_sessions_guard = expired_sessions.write();
        let mut handshakes_guard = handshakes.write();

        let mut to_remove = Vec::new();

        for (slot, session) in handshakes_guard.iter() {
            if session.lock().expired() {
                to_remove.push(*slot);
            }
        }

        for slot in to_remove {
            if let Some(session) = handshakes_guard.remove(&slot) {
                expired_sessions_guard.push(session);
                self.release_handshake_slot(slot);
                if let Err(err) = io.deregister_stream(slot) {
                    debug!(target: "network", "Failed to deregister expired handshake stream {}: {}", slot, err);
                }
            }
        }
    }

    pub(crate) fn deregister_session_stream<Host: mio::deprecated::Handler>(
        &self,
        stream: usize,
        event_loop: &mut mio::deprecated::EventLoop<Host>,
    ) {
        if self.is_handshake_slot(stream) {
            let mut handshakes = self.handshakes.write();
            if let Some(connection) = handshakes.get(&stream).cloned() {
                let c = connection.lock();
                if c.expired() {
                    c.deregister_socket(event_loop).expect("Error deregistering socket");
                    handshakes.remove(&stream);
                    self.release_handshake_slot(stream);
                }
            }
        } else {
            let mut sessions = self.sessions.write();
            if let Some(connection) = sessions.get(&stream).cloned() {
                let c = connection.lock();
                if c.expired() {
                    c.deregister_socket(event_loop).expect("Error deregistering socket");
                    sessions.remove(&stream);
                    self.release_session_slot(stream);
                }
            }
        }
    }
}
