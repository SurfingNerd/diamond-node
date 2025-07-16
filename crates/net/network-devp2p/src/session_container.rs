use crate::host::HostInfo;
use ethereum_types::H256;
use mio::net::TcpStream;
use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crate::{io::*, node_table::*, session::Session};
use network::{Error, ErrorKind, NetworkIoMessage, PeerId};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub type SharedSession = Arc<Mutex<Session>>;

fn socket_address_to_string(socket: &TcpStream) -> String {
    socket
        .peer_addr()
        .map_or("unknown".to_string(), |a| a.to_string())
}

pub struct SessionContainer {
    max_sessions: usize,
    sessions: Arc<RwLock<std::collections::BTreeMap<usize, SharedSession>>>,
    expired_sessions: Arc<RwLock<Vec<SharedSession>>>,
    node_id_to_session: Mutex<BTreeMap<ethereum_types::H512, usize>>, // used to map Node IDs to assigned session tokens.
    sessions_token_max: Mutex<usize>, // Used to generate new session tokens for handshakes
    unassigned_tokens: Mutex<Vec<usize>>, // Pool of token IDs that are not assigned to any NodeID
}

impl SessionContainer {
    pub fn new(first_session_token: usize, max_sessions: usize) -> Self {
        SessionContainer {
            sessions: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            expired_sessions: Arc::new(RwLock::new(Vec::new())),
            node_id_to_session: Mutex::new(BTreeMap::new()),
            sessions_token_max: Mutex::new(first_session_token),
            unassigned_tokens: Mutex::new(Vec::new()),
            max_sessions,
        }
    }

    /// Returns a Read guard to the sessions.
    pub fn read(&self) -> RwLockReadGuard<BTreeMap<usize, SharedSession>> {
        self.sessions.read()
    }

    pub fn write(&self) -> RwLockWriteGuard<BTreeMap<usize, SharedSession>> {
        self.sessions.write()
    }

    /// Gets or assigns a token ID for a specific NodeID
    /// The same NodeID will always get the same token ID
    fn get_or_assign_token_for_node(&self, node_id: &NodeId) -> usize {
        let mut node_id_to_session = self.node_id_to_session.lock();
        
        // Check if this NodeID already has an assigned token
        if let Some(&existing_token) = node_id_to_session.get(node_id) {
            debug!(target: "network", "Reusing existing token {} for node {}", existing_token, node_id);
            return existing_token;
        }

        // NodeID doesn't have a token yet, assign one
        let mut unassigned_tokens = self.unassigned_tokens.lock();
        
        let token = if let Some(token) = unassigned_tokens.pop() {
            // Reuse an unassigned token
            debug!(target: "network", "Assigning recycled token {} to node {}", token, node_id);
            token
        } else {
            // Create a new token
            let mut session_token_max = self.sessions_token_max.lock();
            let new_token = *session_token_max;
            *session_token_max += 1;
            debug!(target: "network", "Assigning new token {} to node {}", new_token, node_id);
            new_token
        };

        // Assign the token to this NodeID permanently
        node_id_to_session.insert(node_id.clone(), token);
        token
    }

    /// Gets a token ID for handshake (temporary, not assigned to any NodeID)
    fn get_handshake_token(&self) -> usize {
        let mut unassigned_tokens = self.unassigned_tokens.lock();
        
        if let Some(token) = unassigned_tokens.pop() {
            debug!(target: "network", "Using recycled token {} for handshake", token);
            token
        } else {
            let mut session_token_max = self.sessions_token_max.lock();
            let new_token = *session_token_max;
            *session_token_max += 1;
            debug!(target: "network", "Using new token {} for handshake", new_token);
            new_token
        }
    }

    /// Returns a token to the unassigned pool (only for handshake tokens or when NodeID is forgotten)
    fn return_token_to_pool(&self, token: usize) {
        let mut unassigned_tokens = self.unassigned_tokens.lock();
        unassigned_tokens.push(token);
        trace!(target: "network", "Returned token {} to unassigned pool", token);
    }

    // like session count, but does not block if read can not be achieved.
    pub fn session_count_try(&self, lock_duration: Duration) -> Option<(usize, usize, usize)> {
        let mut handshakes = 0;
        let mut egress = 0;
        let mut ingress = 0;

        let deadline = time_utils::DeadlineStopwatch::new(lock_duration);

        if let Some(lock) = self.sessions.try_read_for(deadline.time_left()) {
            for s in lock.iter() {
                match s.1.try_lock_for(deadline.time_left()) {
                    Some(ref s) => {
                        if s.is_ready() {
                            if s.info.originated {
                                egress += 1
                            } else {
                                ingress += 1;
                            }
                        } else {
                            handshakes += 1;
                        }
                    }

                    None => return None,
                }
            }
            return Some((handshakes, egress, ingress));
        }

        return None;
    }

    /// Creates a new session and adds it to the session container.
    /// returns the token ID of the new session, or an Error if not successful.
    pub fn create_connection(
        &self,
        socket: TcpStream,
        id: Option<&ethereum_types::H512>,
        io: &IoContext<NetworkIoMessage>,
        nonce: &H256,
        host: &HostInfo,
    ) -> Result<usize, Error> {
        // make sure noone else is trying to modify the sessions at the same time.
        // creating a connection is a very rare event.

        // always lock the expired_sessions first, then the sessions.
        let mut expired_session = self.expired_sessions.write();
        let mut sessions = self.sessions.write();

        if sessions.len() >= self.max_sessions {
            return Err(ErrorKind::TooManyConnections.into());
        }

        if let Some(node_id) = id {
            // Get the assigned token for this NodeID (same token every time)
            let token = self.get_or_assign_token_for_node(node_id);

            // Check if there's already an active session for this token
            if let Some(existing_session_mutex) = sessions.get(&token) {
                let session_expired = {
                    let session = existing_session_mutex.lock();
                    if let Some(id_from_session) = &session.info.id {
                        if session.info.id == Some(*node_id) {
                            // we got already got a session for the specified node.
                            // maybe the old session is already scheduled for getting deleted.
                            session.expired()
                        } else {
                            error!(target: "network", "host cache inconsistency: Session node id missmatch. expected: {} is {}.", token, id_from_session);
                            return Err(ErrorKind::HostCacheInconsistency.into());
                        }
                    } else {
                        error!(target: "network", "host cache inconsistency: Session has no Node_id defined where it should for {}", token);
                        return Err(ErrorKind::HostCacheInconsistency.into());
                    }
                }; // session guard is dropped here

                if session_expired {
                    debug!(target: "network", "Creating new session for expired {} token: {} node id: {:?}", socket_address_to_string(&socket), token, id);
                    // if the session is expired, we will just create a new session for this node.
                    let new_session = Session::new(io, socket, token, id, nonce, host);
                    match new_session {
                        Ok(session) => {
                            let new_session_arc = Arc::new(Mutex::new(session));
                            let old_session_o = sessions.insert(token, new_session_arc);

                            match old_session_o {
                                Some(old) => {
                                    // we remember the expired session, so it can get closed and cleaned up in a nice way later.
                                    expired_session.push(old);
                                }
                                None => {
                                    // we have a cache missmatch.
                                    // but the only thing is missing is a clean ending of the old session.
                                    // nothing mission critical.
                                    error!(target: "network", "host cache inconsistency: Session for node id {} was not found in sessions map, but it should be there.", node_id);
                                }
                            }

                            // in this context, the stream might already be unregistered ?!
                            // we can just register the stream again.
                            if let Err(err) = io.register_stream(token) {
                                // todo: research this topic, watch out for this message,
                                // maybe we can keep track of stream registrations as well somehow.
                                debug!(target: "network", "Failed to register stream for token: {} : {}", token, err);
                            }

                            return Ok(token);
                        }
                        Err(e) => {
                            error!(target: "network", "Failed to create session for node id: {}", node_id);
                            return Err(e);
                        }
                    }
                } else {
                    // this might happen if 2 nodes try to connect to each other at the same time.
                    debug!(target: "network", "Session already exists for node id: {}", node_id);
                    return Err(ErrorKind::AlreadyExists.into());
                }
            } else {
                debug!(target: "network", "Creating session for node {} with assigned token {}", node_id, token);
                // No active session for this token, create a new one
                match Session::new(io, socket, token, id, nonce, host) {
                    Ok(new_session) => {
                        sessions.insert(token, Arc::new(Mutex::new(new_session)));
                        if let Err(err) = io.register_stream(token) {
                            warn!(target: "network", "Failed to register stream for token: {} : {}", token, err);
                        }
                        return Ok(token);
                    }
                    Err(err) => {
                        debug!(target: "network", "Could not create session for node {} with token {}", node_id, token);
                        return Err(err);
                    }
                }
            }
        } else {
            // Handshake session - use a temporary token
            let token = self.get_handshake_token();

            trace!(target: "network", "creating session for handshaking peer: {} token: {}", socket_address_to_string(&socket), token);
            // we dont know the NodeID,
            // we still need a session to do the handshake.

            let new_session = Session::new(io, socket, token, id, nonce, host);
            match new_session {
                Ok(session) => {
                    let session = Arc::new(Mutex::new(session));
                    sessions.insert(token, session.clone());
                    // register the stream for the new session.
                    if let Err(err) = io.register_stream(token) {
                        debug!(target: "network", "Failed to register stream for token: {} : {}", token, err);
                    }

                    return Ok(token);
                }
                Err(e) => {
                    error!(target: "network", "Failed to create handshake session for: {}", token);
                    // Return the token to the pool since session creation failed
                    self.return_token_to_pool(token);
                    return Err(e);
                }
            }
        }
    }

    pub fn get_session_for(&self, id: &NodeId) -> Option<SharedSession> {
        self.node_id_to_session
            .lock()
            .get(id)
            .cloned()
            .map_or(None, |peer_id| {
                let sessions = self.sessions.read();
                sessions.get(&peer_id).cloned()
            })
    }

    pub fn node_id_to_peer_id(
        &self,
        node_id: &NodeId,
        only_available_sessions: bool,
    ) -> Option<usize> {
        self.node_id_to_session
            .lock()
            .get(node_id)
            .map_or(None, |peer_id| {
                if !only_available_sessions {
                    return Some(*peer_id);
                }
                let sessions = self.sessions.read();

                // we can do additional checks:
                // we could ensure that the Node ID matches.
                // we could also read the flag and check if it is not marked for
                // getting disconnected.

                if sessions.contains_key(peer_id) {
                    return Some(*peer_id);
                }

                return None;
            })
    }

    pub fn register_finalized_handshake(&self, session: &mut Session) {
        
        let token = session.token();
        let id = session.id();

        let node_id = match id {
            Some(id) => id.clone(),
            None => {
                error!(target: "network", "Tried to register finalized handshake for session {token} without node id");
                // we have no node id, so we can not register it.
                return;
            }
        };

        let mut node_id_to_session = self.node_id_to_session.lock();
        
        // Check if this NodeID already has an assigned token
        if let Some(&existing_token) = node_id_to_session.get(&node_id) {
            if existing_token != token {
                // The handshake was done with a temporary token, but we already have a permanent token for this node
                // We need to move the session from the temporary token to the permanent token
                debug!(target: "network", "Handshake completed with temporary token {}, but node {} already has permanent token {}", token, node_id, existing_token);
                
                // Move the session from temporary token to permanent token
                let mut sessions = self.sessions.write();
                if let Some(session) = sessions.remove(&token) {
                    // Update the session's internal token
                    {
                        let mut session_lock = session.lock();
                        session_lock.switch_session_token(existing_token);
                    }
                    sessions.insert(existing_token, session);
                    
                    // Return the temporary token to the pool
                    self.return_token_to_pool(token);
                    
                    debug!(target: "network", "Moved session from temporary token {} to permanent token {} for node {}", token, existing_token, node_id);
                } else {
                    error!(target: "network", "Could not find session with temporary token {} to move to permanent token {}", token, existing_token);
                }
            } else {
                debug!(target: "network", "handshake completed: node id {} already has correct token {}", node_id, token);
            }
        } else {
            // This NodeID doesn't have a permanent token yet, so assign the current token permanently
            node_id_to_session.insert(node_id.clone(), token);
            debug!(target: "network", "handshake completed: node id {} assigned permanent token {}", node_id, token);
        }
    }

    // handles duplicated sessions and desides wich one to be deleted. a duplicated session if it exists in a deterministic way, so both sides agree on the same session to keep.
    // returns if this session is marked for deletion, and not being accepted by the SessionContainer.
    // see: https://github.com/DMDcoin/diamond-node/issues/252
    pub fn should_delete_duplicate_session(&self, session: &SharedSession) -> Option<PeerId> {
        let (node_id, peer_id, uid) = {
            let lock = session.lock();
            let peer_id = lock.token().clone();

            let node_id = match lock.id() {
                Some(id) => id.clone(),
                None => {
                    // based on the control flow of the software, this should never happen.
                    warn!(target: "network", "Tried to delete duplicate session without node id");
                    return None; // we have no node id, so we can not delete it.
                }
            };

            let uid = match lock.info.session_uid {
                Some(u) => u.clone(),
                None => {
                    // based on the control flow of the software, this should never happen.
                    warn!(target: "network", "Tried to delete duplicate session without session uid");
                    return None; // we have no session uid, so we can not delete it.
                }
            };

            (node_id, peer_id, uid)
        };

        if let Some(existing_peer_id) = self.node_id_to_peer_id(&node_id, true) {
            if existing_peer_id != peer_id {
                // there may be an active session for this peer id.
                let existing_session = self.get_session_for(&node_id);
                if let Some(existing_session_mutex) = existing_session {
                    let existing_lock = existing_session_mutex.lock();
                    if existing_lock.expired() {
                        // other session is already about to get deleted.
                        trace!(target:"network", "existing peer session {existing_peer_id} is already about to get deleted.");
                        return None;
                    }
                    if let Some(existing_uid) = existing_lock.info.session_uid {
                        // the highest RNG wins.
                        if existing_uid.lt(&uid) {
                            // we keep the existing session, and delete the new one.
                            trace!(target: "network", "Session {peer_id} has a duplicate :{existing_peer_id} for {node_id}, deleting this session");
                            // we savely mark this connection to get killed softly.
                            return Some(peer_id);
                        } else {
                            trace!(target: "network", "Session {peer_id} has a duplicate :{existing_peer_id} for {node_id}, deleting duplicated session");
                            // and delete the existing one.
                            return Some(existing_peer_id);
                        }
                    }
                } else {
                    trace!(target: "network", "No session active for {node_id} with peer id {existing_peer_id}");
                    // todo: make sure the mapping is rewritten.
                }

                trace!(target: "network", "Session {peer_id} has a duplicate :{existing_peer_id} {node_id}");
                return Some(existing_peer_id);
            }
        } else {
            trace!(target: "network", "No session known for {node_id}");
        }

        return None;
    }
}