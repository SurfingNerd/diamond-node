


pub(crate) struct ServiceTransactionManager {

    nonce: u64,
    transactions: Vec<TransactionRequest>
}


impl ServiceTransactionManager {

    pub fn new() -> Self {
        Self {
            none: 0,
            transactions: Vec::new()
        }
    }

    pub fn add_transaction(&mut self, transaction: TransactionRequest) {
        self.transactions.push(transaction);
    }

}