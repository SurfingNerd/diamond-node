// Copyright 2015-2020 Parity Technologies (UK) Ltd.
// This file is part of OpenEthereum.

// OpenEthereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// OpenEthereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with OpenEthereum.  If not, see <http://www.gnu.org/licenses/>.

//! Parity Signer-related rpc interface.

use ethereum_types::U256;
use jsonrpc_core::{BoxFuture, Result};
use jsonrpc_derive::rpc;
use jsonrpc_pubsub::{SubscriptionId, typed::Subscriber};

use crate::v1::types::{
    Bytes, ConfirmationRequest, ConfirmationResponse, ConfirmationResponseWithToken,
    TransactionModification,
};

/// Signer extension for confirmations rpc interface.
#[rpc(server)]
pub trait Signer {
    /// RPC Metadata
    type Metadata;

    /// Returns a list of items to confirm.
    #[rpc(name = "signer_requestsToConfirm")]
    fn requests_to_confirm(&self) -> Result<Vec<ConfirmationRequest>>;

    /// Confirm specific request.
    #[rpc(name = "signer_confirmRequest")]
    fn confirm_request(
        &self,
        _: U256,
        _: TransactionModification,
        _: String,
    ) -> BoxFuture<ConfirmationResponse>;

    /// Confirm specific request with token.
    #[rpc(name = "signer_confirmRequestWithToken")]
    fn confirm_request_with_token(
        &self,
        _: U256,
        _: TransactionModification,
        _: String,
    ) -> BoxFuture<ConfirmationResponseWithToken>;

    /// Confirm specific request with already signed data.
    #[rpc(name = "signer_confirmRequestRaw")]
    fn confirm_request_raw(&self, _: U256, _: Bytes) -> Result<ConfirmationResponse>;

    /// Reject the confirmation request.
    #[rpc(name = "signer_rejectRequest")]
    fn reject_request(&self, _: U256) -> Result<bool>;

    /// Generates new authorization token.
    #[rpc(name = "signer_generateAuthorizationToken")]
    fn generate_token(&self) -> Result<String>;

    /// Subscribe to new pending requests on signer interface.
    #[pubsub(
        subscription = "signer_pending",
        subscribe,
        name = "signer_subscribePending"
    )]
    fn subscribe_pending(&self, _: Self::Metadata, _: Subscriber<Vec<ConfirmationRequest>>);

    /// Unsubscribe from pending requests subscription.
    #[pubsub(
        subscription = "signer_pending",
        unsubscribe,
        name = "signer_unsubscribePending"
    )]
    fn unsubscribe_pending(&self, _: Option<Self::Metadata>, _: SubscriptionId) -> Result<bool>;
}
