use std::sync::{Arc, Weak};

use ethereum_types::Address;
use io::IoHandler;
use parking_lot::{Mutex, RwLock};

use crate::{client::EngineClient, engines::EngineError, error::Error};

use super::{
    NodeId, contracts::validator_set::staking_by_mining_address,
    hbbft_peers_management::HbbftPeersManagement,
};

pub enum HbbftConnectToPeersMessage {
    SetSignerAddress(Address),
    ConnectToPendingPeers(Vec<Address>),
    ConnectToCurrentPeers(Vec<NodeId>),
    AnnounceOwnInternetAddress(Address),
    DisconnectAllValidators,
}

/// IOChannel Wrapper for doing the HbbftPeersManagement asynconous.
pub struct HbbftPeersHandler {
    peers_management: Mutex<HbbftPeersManagement>,
    client: Arc<RwLock<Option<Weak<dyn EngineClient>>>>,
}

impl HbbftPeersHandler {
    pub fn new(client: Arc<RwLock<Option<Weak<dyn EngineClient>>>>) -> Self {
        Self {
            peers_management: Mutex::new(HbbftPeersManagement::new()),
            client,
        }
    }

    fn client_arc(&self) -> Option<Arc<dyn EngineClient>> {
        self.client.read().as_ref().and_then(Weak::upgrade)
    }

    fn announce_own_internet_address(&self, mining_address: &Address) -> Result<(), Error> {
        info!(target: "engine", "trying to Announce own internet address for mining address: {:?}", mining_address);

        let engine_client = self.client_arc().ok_or(EngineError::RequiresClient)?;

        // TODO:
        // staking by mining address could be cached.
        // but it COULD also get changed in the contracts, during the time the node is running.
        // most likely since a Node can get staked, and than it becomes a mining address.
        // a good solution for this is not to do this expensive operation that fequently.
        let staking_address = match staking_by_mining_address(
            engine_client.as_ref(),
            &mining_address,
        ) {
            Ok(staking_address) => {
                if staking_address.is_zero() {
                    //TODO: here some fine handling can improve performance.
                    //with this implementation every node (validator or not)
                    //needs to query this state every block.
                    //trace!(target: "engine", "availability handling not a validator");
                    return Ok(());
                }
                staking_address
            }
            Err(call_error) => {
                let message = format!(
                    "unable to ask for corresponding staking address for given mining address: {:?}",
                    call_error
                );
                error!(target: "engine", "{:?}", message);

                return Err(EngineError::SystemCallResultInvalid(message).into());
            }
        };

        let block_chain_client = engine_client
            .as_full_client()
            .ok_or("BlockchainClient required")?;

        if let Err(error) = self.peers_management.lock().announce_own_internet_address(
            block_chain_client,
            engine_client.as_ref(),
            &mining_address,
            &staking_address,
        ) {
            error!(target: "engine", "Error trying to announce own internet address: {:?}", error);
        }

        info!(target: "engine", "Success: trying to announce own internet address for mining address: {:?}", mining_address);

        return Ok(());
    }
}

impl IoHandler<HbbftConnectToPeersMessage> for HbbftPeersHandler {
    fn message(
        &self,
        _io: &io::IoContext<HbbftConnectToPeersMessage>,
        message: &HbbftConnectToPeersMessage,
    ) {
        // we can savely lock the whole peers_management here, because this handler is the only one that locks that data.

        // working peers management is a nice to have, but it is not worth a deadlock.

        if let Some(client_arc) = self.client_arc() {
            match message {
                HbbftConnectToPeersMessage::ConnectToPendingPeers(peers) => {
                    match self
                        .peers_management
                        .lock()
                        .connect_to_pending_validators(&client_arc, peers)
                    {
                        Ok(value) => {
                            if value > 0 {
                                debug!(target: "engine", "Added {:?} reserved peers because they are pending validators.", value);
                            }
                        }
                        Err(err) => {
                            warn!(target: "engine", "Error connecting to other pending validators: {:?}", err);
                        }
                    }
                }
                HbbftConnectToPeersMessage::ConnectToCurrentPeers(validator_set) => {
                    // connecting to current validators.
                    self.peers_management
                        .lock()
                        .connect_to_current_validators(validator_set, &client_arc);
                }

                HbbftConnectToPeersMessage::AnnounceOwnInternetAddress(mining_address) => {
                    if let Err(error) = self.announce_own_internet_address(mining_address) {
                        error!(target: "engine", "Error announcing own internet address: {:?}", error);
                    }
                }

                HbbftConnectToPeersMessage::SetSignerAddress(signer_address) => {
                    self.peers_management
                        .lock()
                        .set_validator_address(signer_address.clone());
                }

                HbbftConnectToPeersMessage::DisconnectAllValidators => {
                    self.peers_management
                        .lock()
                        .disconnect_all_validators(&client_arc);
                }
            }
        }
    }
}

// fn do_keygen_peers_management(
//     client: Arc<dyn EngineClient>,
//     validators: Vec<Address>,
//     peers_management: &Mutex<HbbftPeersManagement>,
// ) {
//     parity_runtime::tokio::spawn(async move {

//     });
// }
