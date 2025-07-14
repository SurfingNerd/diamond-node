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
    node_id_to_session: Mutex<BTreeMap<ethereum_types::H512, usize>>, // used to map Node IDs to last used session tokens.
    sessions_token_max: Mutex<usize>, // Used to generate new session tokens
}

impl SessionContainer {
    pub fn new(first_session_token: usize, max_sessions: usize) -> Self {
        SessionContainer {
            sessions: Arc::new(RwLock::new(std::collections::BTreeMap::new())),
            expired_sessions: Arc::new(RwLock::new(Vec::new())),
            node_id_to_session: Mutex::new(BTreeMap::new()),
            sessions_token_max: Mutex::new(first_session_token),
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

    /// gets the next token ID and store this information
    fn create_token_id(
        &self,
        node_id: &NodeId,
        tokens: &mut BTreeMap<ethereum_types::H512, usize>,
    ) -> usize {
        let mut session_token_max = self.sessions_token_max.lock();
        let next_id = session_token_max.clone();

        *session_token_max += 1;

        // TODO: if we run out of token ids,
        // we need to recycle Ids that are not used anymore.

        if let Some(old) = tokens.insert(node_id.clone(), next_id) {
            warn!(target: "network", "Node ID {} already exists with token {}, overwriting with {}", node_id, old, next_id);
        }

        return next_id;
    }

    fn create_token_id_for_handshake(&self) -> usize {
        let mut session_token_max = self.sessions_token_max.lock();
        let next_id = session_token_max.clone();

        *session_token_max += 1;

        // TODO: if we run out of token ids,
        // we need to recycle Ids that are not used anymore.

        return next_id;
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

        // always lock the node_id_to_session first, then the sessions.
        let mut expired_session = self.expired_sessions.write();
        let mut node_ids = self.node_id_to_session.lock();
        let mut sessions = self.sessions.write();

        if sessions.len() >= self.max_sessions {
            return Err(ErrorKind::TooManyConnections.into());
        }

        if let Some(node_id) = id {
            // check if there is already a connection for the given node id.
            if let Some(existing_peer_id) = node_ids.get(node_id) {
                let existing_session_mutex_o = sessions.get(existing_peer_id).clone();

                if let Some(existing_session_mutex) = existing_session_mutex_o {
                    let session_expired = {
                        let session = existing_session_mutex.lock();
                        if let Some(id_from_session) = &session.info.id {
                            if session.info.id == Some(*node_id) {
                                // we got already got a session for the specified node.
                                // maybe the old session is already scheduled for getting deleted.
                                session.expired()
                            } else {
                                error!(target: "network", "host cache inconsistency: Session node id missmatch. expected: {} is {}.", existing_peer_id, id_from_session);
                                return Err(ErrorKind::HostCacheInconsistency.into());
                            }
                        } else {
                            error!(target: "network", "host cache inconsistency: Session has no Node_id defined where it should for {}", existing_peer_id);
                            return Err(ErrorKind::HostCacheInconsistency.into());
                        }
                    }; // session guard is dropped here

                    if session_expired {
                        debug!(target: "network", "Creating new session for expired {} token: {} node id: {:?}", socket_address_to_string(&socket), existing_peer_id, id);
                        // if the session is expired, we will just create n new session for this node.
                        let new_session =
                            Session::new(io, socket, existing_peer_id.clone(), id, nonce, host);
                        match new_session {
                            Ok(session) => {
                                let new_session_arc = Arc::new(Mutex::new(session));
                                let old_session_o =
                                    sessions.insert(*existing_peer_id, new_session_arc);

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
                                if let Err(err) = io.register_stream(*existing_peer_id) {
                                    // todo: research this topic, watch out for this message,
                                    // maybe we can keep track of stream registrations as well somehow.
                                    debug!(target: "network", "Failed to register stream for token: {} : {}", existing_peer_id, err);
                                }

                                return Ok(*existing_peer_id);
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
                    debug!(target: "network", "reusing peer ID {} for node: {}", existing_peer_id, node_id);
                    // we have a node id, but there is no session for it (anymore)
                    // we reuse that peer_id, so other in flight actions are pointing to the same node again.
                    match Session::new(io, socket, existing_peer_id.clone(), id, nonce, host) {
                        Ok(new_session) => {
                            sessions.insert(
                                existing_peer_id.clone(),
                                Arc::new(Mutex::new(new_session)),
                            );
                            if let Err(err) = io.register_stream(existing_peer_id.clone()) {
                                warn!(target: "network", "Failed to register stream for reused token: {} : {}", existing_peer_id, err);
                            }
                            return Ok(existing_peer_id.clone());
                        }
                        Err(err) => {
                            debug!(target: "network", "reusing peer ID {} for node: {}, but could not create session", existing_peer_id, node_id);
                            return Err(err);
                        }
                    }
                }
            } else {
                // this should be the most common scenario.
                // we have a new node id, we were either never connected to, or we forgot about it.

                let next_free_token = self.create_token_id(&node_id, &mut node_ids);
                trace!(target: "network", "Creating new session {} for new node id:{} with token {}", socket_address_to_string(&socket), node_id, next_free_token);
                let new_session =
                    Session::new(io, socket, next_free_token.clone(), id, nonce, host);
                // the token is already registerd.
                match new_session {
                    Ok(session) => {
                        let session = Arc::new(Mutex::new(session));
                        sessions.insert(next_free_token, session.clone());
                        // register the stream for the new session.
                        if let Err(err) = io.register_stream(next_free_token) {
                            debug!(target: "network", "Failed to register stream for token: {} : {}", next_free_token, err);
                        }
                        node_ids.insert(node_id.clone(), next_free_token);

                        return Ok(next_free_token);
                    }
                    Err(e) => {
                        error!(target: "network", "Failed to create session for node id: {}", node_id);
                        return Err(e);
                    }
                }
            }
        } else {
            let next_free_token = self.create_token_id_for_handshake();

            trace!(target: "network", "creating session for handshaking peer: {} token: {}", socket_address_to_string(&socket), next_free_token);
            // we dont know the NodeID,
            // we still need a session to do the handshake.

            let new_session = Session::new(io, socket, next_free_token.clone(), id, nonce, host);
            // the token is already registerd.
            match new_session {
                Ok(session) => {
                    let session = Arc::new(Mutex::new(session));
                    sessions.insert(next_free_token, session.clone());
                    // register the stream for the new session.
                    if let Err(err) = io.register_stream(next_free_token) {
                        debug!(target: "network", "Failed to register stream for token: {} : {}", next_free_token, err);
                    }
                    //node_ids.insert(node_id.clone(), next_free_token);

                    return Ok(next_free_token);
                }
                Err(e) => {
                    error!(target: "network", "Failed to create handshake session for: {}", next_free_token);
                    return Err(e);
                }
            }
        }
        // if we dont know a NodeID
        // debug!(target: "network", "Session create error: {:?}", e);
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

    pub fn register_finalized_handshake(&self, token: usize, id: Option<&ethereum_types::H512>) {
        let node_id = match id {
            Some(id) => id.clone(),
            None => {
                error!(target: "network", "Tried to register finalized handshake without node id");
                // we have no node id, so we can not register it.
                return;
            }
        };

        // register this session.
        if let Some(old) = self.node_id_to_session.lock().insert(node_id, token) {
            // in scenarios where we did have registered the session to this node,
            // the token id wont change.
            // but still we need a lock to node_id_to_session anyway.
            if old != token {
                debug!(target: "network", "handshake completed: changed primary session for node id {} from {}  to {}", node_id, token, old);
            }
        } else {
            debug!(target: "network", "handshake completed: node id {} registered primary session token {}", node_id, token);
        }
    }

    // handles duplicated sessions and desides wich one to be deleted. a duplicated session if it exists in a deterministic way, so both sides agree on the same session to keep.
    // returns if this session is marked for deletion, and not being accepted by the SessionContainer.
    // see: https://github.com/DMDcoin/diamond-node/issues/252
    pub fn should_delete_duplicate_session(&self, session: &SharedSession) -> Option<PeerId> {

        let (node_id, peer_id, uid) =
        {
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

    // makes a shallow search if there is already a session for that connection
    fn is_duplicate(&self, session: &SharedSession) -> bool {
        let (id, token) = {
            let lock = session.lock();
            (lock.id().cloned(), lock.token().clone())
        };

        if let Some(node_id) = id {
            if let Some(existing_peer_id) = self.node_id_to_peer_id(&node_id, true) {
                if existing_peer_id != token {
                    trace!(target: "network", "Session {token} has a duplicate :{existing_peer_id} {node_id}");
                    return true;
                }
            }
        } // else we dont have enough information to make a decision.

        return false;
    }
}
