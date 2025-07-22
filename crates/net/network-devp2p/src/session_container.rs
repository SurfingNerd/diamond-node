use crate::host::HostInfo;
use ethereum_types::H256;
use lru_cache::LruCache;
use mio::net::TcpStream;
use std::{collections::{BTreeMap, BTreeSet}, sync::Arc, time::Duration};

use crate::{io::*, node_table::*, session::Session};
use network::{Error, ErrorKind, NetworkIoMessage, PeerId};
use parking_lot::{Mutex, RwLock, RwLockReadGuard};

pub type SharedSession = Arc<Mutex<Session>>;

fn socket_address_to_string(socket: &TcpStream) -> String {
    socket
        .peer_addr()
        .map_or("unknown".to_string(), |a| a.to_string())
}

pub struct SessionContainer {
    // Configuration
    max_sessions: usize,
    max_handshakes: usize,
    
    // Separate storage for handshakes and regular sessions
    handshakes: Arc<RwLock<BTreeMap<usize, SharedSession>>>,  // 100-199 tokens
    sessions: Arc<RwLock<BTreeMap<usize, SharedSession>>>,    // 200+ tokens
    expired_sessions: Arc<RwLock<Vec<SharedSession>>>,
    
    // Slot allocation tracking
    handshake_slots_used: Mutex<BTreeSet<usize>>,
    session_slots_used: Mutex<BTreeSet<usize>>,
    
    // Node ID mapping (only for regular sessions)
    node_id_to_session: Mutex<LruCache<ethereum_types::H512, usize>>,
    
    // Constants for slot ranges
    handshake_slot_min: usize,  // 100
    handshake_slot_max: usize,  // 199
    session_slot_min: usize,    // 200
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

    /// Returns a Read guard to all regular sessions (not handshakes).
    pub fn read(&self) -> RwLockReadGuard<BTreeMap<usize, SharedSession>> {
        self.sessions.read()
    }

    /// Returns a Read guard to all handshake sessions.
    pub fn read_handshakes(&self) -> RwLockReadGuard<BTreeMap<usize, SharedSession>> {
        self.handshakes.read()
    }

    /// Checks if a token belongs to handshake slots (0-99)
    fn is_handshake_slot(&self, token: usize) -> bool {
        token >= self.handshake_slot_min && token <= self.handshake_slot_max
    }

    /// Checks if a token belongs to session slots (100+)
    fn is_session_slot(&self, token: usize) -> bool {
        token >= self.session_slot_min
    }

    /// Finds the next available handshake slot (0-99)
    fn allocate_handshake_slot(&self) -> Option<usize> {
        let mut used_slots = self.handshake_slots_used.lock();
        
        for slot in self.handshake_slot_min..=self.handshake_slot_max {
            if !used_slots.contains(&slot) {
                used_slots.insert(slot);
                return Some(slot);
            }
        }
        
        None // All handshake slots are occupied
    }

    /// Finds the next available session slot (100+)
    fn allocate_session_slot(&self, node_id: &NodeId, node_ids: &mut LruCache<ethereum_types::H512, usize>) -> Option<usize> {
        let mut used_slots = self.session_slots_used.lock();
        
        let session_slot_max = self.session_slot_min + self.max_sessions - 1;
        for slot in self.session_slot_min..=session_slot_max {
            if !used_slots.contains(&slot) {
                used_slots.insert(slot);
                
                if let Some(old) = node_ids.insert(node_id.clone(), slot) {
                    warn!(target: "network", "Node ID {} already exists with slot {}, overwriting with {}", node_id, old, slot);
                }
                
                return Some(slot);
            }
        }
        
        None // All session slots are occupied
    }

    /// Releases a handshake slot
    fn release_handshake_slot(&self, slot: usize) {
        if self.is_handshake_slot(slot) {
            self.handshake_slots_used.lock().remove(&slot);
        }
    }

    /// Releases a session slot
    fn release_session_slot(&self, slot: usize) {
        if self.is_session_slot(slot) {
            self.session_slots_used.lock().remove(&slot);
        }
    }

    /// Reuses an existing session slot for a known node ID
    fn reuse_session_slot(&self, slot: usize, node_id: &NodeId, node_ids: &mut LruCache<ethereum_types::H512, usize>) -> bool {
        if !self.is_session_slot(slot) {
            return false;
        }
        
        let mut used_slots = self.session_slots_used.lock();
        used_slots.insert(slot);
        
        if let Some(old) = node_ids.insert(node_id.clone(), slot) {
            if old != slot {
                debug!(target: "network", "Node ID {} changed from slot {} to {}", node_id, old, slot);
            }
        }
        
        true
    }

    // Session count with timeout, organized by slot type
    pub fn session_count_try(&self, lock_duration: Duration) -> Option<(usize, usize, usize)> {
        let mut handshakes = 0;
        let mut egress = 0;
        let mut ingress = 0;

        let deadline = time_utils::DeadlineStopwatch::new(lock_duration);

        // Count handshakes
        if let Some(handshake_lock) = self.handshakes.try_read_for(deadline.time_left()) {
            for (_, session) in handshake_lock.iter() {
                match session.try_lock_for(deadline.time_left()) {
                    Some(_) => handshakes += 1,
                    None => return None,
                }
            }
        } else {
            return None;
        }

        // Count regular sessions
        if let Some(session_lock) = self.sessions.try_read_for(deadline.time_left()) {
            for (_, session) in session_lock.iter() {
                match session.try_lock_for(deadline.time_left()) {
                    Some(ref s) => {
                        if s.is_ready() {
                            if s.info.originated {
                                egress += 1
                            } else {
                                ingress += 1;
                            }
                        }
                    }
                    None => return None,
                }
            }
        } else {
            return None;
        }

        Some((handshakes, egress, ingress))
    }

    /// Creates a new handshake session for ingress connections where NodeID is unknown
    pub fn create_handshake_connection(
        &self,
        socket: TcpStream,
        io: &IoContext<NetworkIoMessage>,
        nonce: &H256,
        host: &HostInfo,
    ) -> Result<usize, Error> {
        let handshake_slot = self.allocate_handshake_slot()
            .ok_or(ErrorKind::TooManyConnections)?;

        trace!(target: "network", "Creating handshake session for peer: {} slot: {}", 
               socket_address_to_string(&socket), handshake_slot);

        let new_session = Session::new(io, socket, handshake_slot, None, nonce, host);
        
        match new_session {
            Ok(session) => {
                let session_arc = Arc::new(Mutex::new(session));
                let mut handshakes = self.handshakes.write();
                handshakes.insert(handshake_slot, session_arc);
                
                if let Err(err) = io.register_stream(handshake_slot) {
                    debug!(target: "network", "Failed to register stream for handshake slot: {} : {}", handshake_slot, err);
                    // Clean up on failure
                    handshakes.remove(&handshake_slot);
                    self.release_handshake_slot(handshake_slot);
                    return Err(err.into());
                }

                Ok(handshake_slot)
            }
            Err(e) => {
                error!(target: "network", "Failed to create handshake session for slot: {}", handshake_slot);
                self.release_handshake_slot(handshake_slot);
                Err(e)
            }
        }
    }

    /// Creates a new regular session and adds it to the session container.
    /// returns the slot ID of the new session, or an Error if not successful.
    pub fn create_connection(
        &self,
        socket: TcpStream,
        id: Option<&ethereum_types::H512>,
        io: &IoContext<NetworkIoMessage>,
        nonce: &H256,
        host: &HostInfo,
    ) -> Result<usize, Error> {
        // If no node ID is provided, this is an ingress handshake
        if id.is_none() {
            return self.create_handshake_connection(socket, io, nonce, host);
        }

        let node_id = id.unwrap();

        // Handle known node ID connections (egress or post-handshake ingress)
        let mut expired_sessions = self.expired_sessions.write();
        //let mut node_ids = self.node_id_to_session.lock();
        let mut sessions = self.sessions.write();

        // Check if there is already a connection for the given node id
        if let Some(existing_slot) = self.node_id_to_session.lock().get_mut(node_id).cloned() {
            let existing_session_mutex_o = sessions.get(&existing_slot).cloned();

            if let Some(existing_session_mutex) = existing_session_mutex_o {
                let session_expired = {
                    let session = existing_session_mutex.lock();
                    if let Some(id_from_session) = &session.info.id {
                        if session.info.id == Some(*node_id) {
                            session.expired()
                        } else {
                            error!(target: "network", "host cache inconsistency: Session node id mismatch. expected slot: {} has node id: {}.", existing_slot, id_from_session);
                            return Err(ErrorKind::HostCacheInconsistency.into());
                        }
                    } else {
                        error!(target: "network", "host cache inconsistency: Session has no Node_id defined where it should for slot {}", existing_slot);
                        return Err(ErrorKind::HostCacheInconsistency.into());
                    }
                };

                if session_expired {
                    debug!(target: "network", "Creating new session for expired {} slot: {} node id: {:?}", 
                           socket_address_to_string(&socket), existing_slot, id);
                    
                    let new_session = Session::new(io, socket, existing_slot, Some(node_id), nonce, host);
                    match new_session {
                        Ok(session) => {
                            let new_session_arc = Arc::new(Mutex::new(session));
                            let old_session_o = sessions.insert(existing_slot, new_session_arc);

                            if let Some(old) = old_session_o {
                                expired_sessions.push(old);
                            } else {
                                error!(target: "network", "host cache inconsistency: Session for node id {} was not found in sessions map, but it should be there.", node_id);
                            }

                            // Ensure slot is marked as used
                            self.reuse_session_slot(existing_slot, node_id, &mut self.node_id_to_session.lock());

                            if let Err(err) = io.register_stream(existing_slot) {
                                debug!(target: "network", "Failed to register stream for slot: {} : {}", existing_slot, err);
                            }

                            Ok(existing_slot)
                        }
                        Err(e) => {
                            error!(target: "network", "Failed to create session for node id: {}", node_id);
                            Err(e)
                        }
                    }
                } else {
                    debug!(target: "network", "Session already exists for node id: {}", node_id);
                    Err(ErrorKind::AlreadyExists.into())
                }
            } else {
                debug!(target: "network", "reusing slot {} for node: {}", existing_slot, node_id);
                match Session::new(io, socket, existing_slot, Some(node_id), nonce, host) {
                    Ok(new_session) => {
                        sessions.insert(existing_slot, Arc::new(Mutex::new(new_session)));
                        self.reuse_session_slot(existing_slot, node_id,&mut self.node_id_to_session.lock());
                        
                        if let Err(err) = io.register_stream(existing_slot) {
                            warn!(target: "network", "Failed to register stream for reused slot: {} : {}", existing_slot, err);
                        }
                        Ok(existing_slot)
                    }
                    Err(err) => {
                        debug!(target: "network", "reusing slot {} for node: {}, but could not create session", existing_slot, node_id);
                        Err(err)
                    }
                }
            }
        } else {
            // New node ID - allocate a new session slot
            let new_slot = self.allocate_session_slot(&node_id, &mut self.node_id_to_session.lock())
                .ok_or(ErrorKind::TooManyConnections)?;
            
            trace!(target: "network", "Creating new session {} for new node id:{} with slot {}", 
                   socket_address_to_string(&socket), node_id, new_slot);
            
            let new_session = Session::new(io, socket, new_slot, Some(node_id), nonce, host);
            
            match new_session {
                Ok(session) => {
                    let session_arc = Arc::new(Mutex::new(session));
                    sessions.insert(new_slot, session_arc);
                    
                    if let Err(err) = io.register_stream(new_slot) {
                        debug!(target: "network", "Failed to register stream for slot: {} : {}", new_slot, err);
                        sessions.remove(&new_slot);
                        self.release_session_slot(new_slot);
                        return Err(err.into());
                    }

                    Ok(new_slot)
                }
                Err(e) => {
                    error!(target: "network", "Failed to create session for node id: {}", node_id);
                    self.release_session_slot(new_slot);
                    Err(e)
                }
            }
        }
    }

    /// Promotes a successful handshake session to a regular session
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
        //let mut node_ids = self.node_id_to_session.lock();
        let mut handshakes = self.handshakes.write();
        let mut sessions = self.sessions.write();

        // Get the handshake session
        let handshake_session = handshakes.remove(&handshake_slot)
            .ok_or(ErrorKind::HostCacheInconsistency)?;

        // Release handshake slot
        self.release_handshake_slot(handshake_slot);

        // Check if there's already a session for this node ID
        if let Some(existing_slot) = self.node_id_to_session.lock().get_mut(node_id).cloned() {
            let existing_session_mutex_o = sessions.get(&existing_slot).cloned();

            if let Some(existing_session_mutex) = existing_session_mutex_o {
                let session_expired = {
                    let session = existing_session_mutex.lock();
                    session.expired()
                };

                if session_expired {
                    // Replace expired session
                    debug!(target: "network", "Promoting handshake {} to replace expired session {} for node {}", 
                           handshake_slot, existing_slot, node_id);
                    
                    // Update the handshake session's slot and node ID
                    {
                        let mut session = handshake_session.lock();
                        session.set_token(existing_slot)?;
                        
                    }

                    let old_session_o = sessions.insert(existing_slot, handshake_session);
                    if let Some(old) = old_session_o {
                        expired_sessions.push(old);
                    }

                    // Ensure slot is marked as used
                    self.reuse_session_slot(existing_slot, node_id, &mut self.node_id_to_session.lock());

                    // Update stream registration
                    if let Err(err) = io.deregister_stream(handshake_slot) {
                        debug!(target: "network", "Failed to deregister handshake stream {}: {}", handshake_slot, err);
                    }
                    if let Err(err) = io.register_stream(existing_slot) {
                        debug!(target: "network", "Failed to register promoted session stream {}: {}", existing_slot, err);
                    }

                    Ok(existing_slot)
                } else {
                    // Session already exists and is active
                    expired_sessions.push(handshake_session);
                    debug!(target: "network", "Active session already exists for node {}, discarding handshake {}", node_id, handshake_slot);
                    Err(ErrorKind::AlreadyExists.into())
                }
            } else {
                // Reuse existing slot
                debug!(target: "network", "Promoting handshake {} to reuse slot {} for node {}", 
                       handshake_slot, existing_slot, node_id);
                
                {
                    let mut session = handshake_session.lock();
                    session.set_token(existing_slot)?;
                }

                sessions.insert(existing_slot, handshake_session);
                self.reuse_session_slot(existing_slot, node_id, &mut self.node_id_to_session.lock());
                
                if let Err(err) = io.deregister_stream(handshake_slot) {
                    debug!(target: "network", "Failed to deregister handshake stream {}: {}", handshake_slot, err);
                }
                if let Err(err) = io.register_stream(existing_slot) {
                    warn!(target: "network", "Failed to register promoted session stream {}: {}", existing_slot, err);
                }

                Ok(existing_slot)
            }
        } else {
            // Create new session slot for this node
            let new_session_slot = self.allocate_session_slot(&node_id, &mut self.node_id_to_session.lock())
                .ok_or(ErrorKind::TooManyConnections)?;
            
            debug!(target: "network", "Promoting handshake {} to new session {} for node {}", 
                   handshake_slot, new_session_slot, node_id);

            {
                let mut session = handshake_session.lock();
                session.set_token(new_session_slot)?;
            }

            sessions.insert(new_session_slot, handshake_session);

            if let Err(err) = io.deregister_stream(handshake_slot) {
                debug!(target: "network", "Failed to deregister handshake stream {}: {}", handshake_slot, err);
            }
            if let Err(err) = io.register_stream(new_session_slot) {
                debug!(target: "network", "Failed to register promoted session stream {}: {}", new_session_slot, err);
                // Clean up on failure
                sessions.remove(&new_session_slot);
                self.release_session_slot(new_session_slot);
                return Err(err.into());
            }

            Ok(new_session_slot)
        }
    }

    /// Get a handshake session by slot number
    pub fn get_handshake(&self, slot: usize) -> Option<SharedSession> {
        if self.is_handshake_slot(slot) {
            self.handshakes.read().get(&slot).cloned()
        } else {
            None
        }
    }

    /// Get a regular session by slot number
    pub fn get_session(&self, slot: usize) -> Option<SharedSession> {
        if self.is_session_slot(slot) {
            self.sessions.read().get(&slot).cloned()
        } else {
            None
        }
    }

    /// Get any session (handshake or regular) by slot number
    pub fn get_any_session(&self, slot: usize) -> Option<SharedSession> {
        if self.is_handshake_slot(slot) {
            self.get_handshake(slot)
        } else {
            self.get_session(slot)
        }
    }

    pub fn get_session_for(&self, id: &NodeId) -> Option<SharedSession> {
        self.node_id_to_session
            .lock()
            .get_mut(id)
            .cloned()
            .map_or(None, |slot| {
                let sessions = self.sessions.read();
                sessions.get(&slot).cloned()
            })
    }

    pub fn node_id_to_peer_id(
        &self,
        node_id: &NodeId,
        only_available_sessions: bool,
    ) -> Option<usize> {
        self.node_id_to_session
            .lock()
            .get_mut(node_id)
            .map_or(None, |slot| {
                if !only_available_sessions {
                    return Some(*slot);
                }
                let sessions = self.sessions.read();

                if sessions.contains_key(slot) {
                    return Some(*slot);
                }

                None
            })
    }

    pub fn register_finalized_handshake(&self, slot: usize, id: Option<&ethereum_types::H512>) {
        let node_id = match id {
            Some(id) => id.clone(),
            None => {
                error!(target: "network", "Tried to register finalized handshake without node id");
                return;
            }
        };

        // Only register for regular sessions, not handshakes
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

    // handles duplicated sessions and decides which one to be deleted
    pub fn should_delete_duplicate_session(&self, session: &SharedSession) -> Option<PeerId> {
        let (node_id, slot, uid) = {
            let lock = session.lock();
            let slot = lock.token().clone();

            let node_id = match lock.id() {
                Some(id) => id.clone(),
                None => {
                    // Handshake sessions might not have node IDs yet
                    if self.is_handshake_slot(slot) {
                        return None; // Don't handle duplicates for handshake sessions
                    }
                    warn!(target: "network", "Tried to delete duplicate session without node id");
                    return None;
                }
            };

            let uid = match lock.info.session_uid {
                Some(u) => u.clone(),
                None => {
                    warn!(target: "network", "Tried to delete duplicate session without session uid");
                    return None;
                }
            };

            (node_id, slot, uid)
        };

        // Only handle duplicates for regular sessions, not handshake sessions
        if self.is_handshake_slot(slot) {
            return None;
        }

        if let Some(existing_slot) = self.node_id_to_peer_id(&node_id, true) {
            if existing_slot != slot {
                let existing_session = self.get_session_for(&node_id);
                if let Some(existing_session_mutex) = existing_session {
                    let existing_lock = existing_session_mutex.lock();
                    if existing_lock.expired() {
                        trace!(target:"network", "existing peer session {existing_slot} is already about to get deleted.");
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

    /// Clean up expired handshake sessions
    pub fn cleanup_expired_handshakes(&self, io: &IoContext<NetworkIoMessage>) {
        let expired_sessions = self.expired_sessions.clone();
        let handshakes = self.handshakes.clone();
        
        let mut expired_sessions_guard = expired_sessions.write();
        let mut handshakes_guard = handshakes.write();
        
        let mut to_remove = Vec::new();
        
        for (slot, session) in handshakes_guard.iter() {
            let expired = {
                let lock = session.lock();
                lock.expired()
            };
            
            if expired {
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
        // Try handshakes first
        if self.is_handshake_slot(stream) {
            let mut handshakes = self.handshakes.write();
            if let Some(connection) = handshakes.get(&stream).cloned() {
                let c = connection.lock();
                if c.expired() {
                    c.deregister_socket(event_loop)
                        .expect("Error deregistering socket");
                    handshakes.remove(&stream);
                    self.release_handshake_slot(stream);
                }
            }
        } else {
            // Handle regular sessions
            let mut sessions = self.sessions.write();
            if let Some(connection) = sessions.get(&stream).cloned() {
                let c = connection.lock();
                if c.expired() {
                    c.deregister_socket(event_loop)
                        .expect("Error deregistering socket");
                    sessions.remove(&stream);
                    self.release_session_slot(stream);
                }
            }
        }
    }
}