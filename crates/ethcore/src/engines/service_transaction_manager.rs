use std::collections::VecDeque;
use common_types::ids::BlockId;
use ethereum_types::{Address, U256};
use crate::{client::{traits::TransactionRequest, BlockChainClient}};

pub(crate) struct ServiceTransactionManager {

    nonce: U256, 
    address: Address,
    transactions: VecDeque<TransactionRequest>
}

impl ServiceTransactionManager {

    pub fn new(full_client: &dyn BlockChainClient, address: &Address) -> Self {
        
        let mut nonce = full_client.nonce(address, BlockId::Latest).unwrap_or_default();
        Self {
            nonce: nonce,
            address: address.clone(),
            transactions: VecDeque::new()
        }
    }

    pub fn add_transaction(&mut self, transaction: TransactionRequest) {

        if transaction.nonce.is_some() {
            warn!(target: "engine", "Nonce already set for transaction: {transaction:?}");
        }

        self.nonce = self.nonce + 1;
        transaction.nonce(self.nonce);
        self.transactions.push_back(transaction);
    }

    pub fn import_service_transactions(&mut self, client: &dyn BlockChainClient) {

        let nonce_on_chain = client.nonce(&self.address, BlockId::Latest).unwrap_or_default();
        // now we verify the nonce chain, to detect if we have a nonce gap or double usage of nonces.

        for x in 0..self.transactions.len() {
            let transaction = self.transactions.get(x).unwrap();
            if let Some(nonce) = transaction.nonce {
                if nonce != nonce_on_chain + x as u64 {
                    warn!(target: "engine", "Nonce gap detected for nonce: {nonce} on chain: {nonce_on_chain} at index: {x}. tx: {transaction:?}");
                }
            }
        }

        //client.transact_silently(tx_request)
    }

}