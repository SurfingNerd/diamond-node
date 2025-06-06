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

//! Secondary chunk creation and restoration, implementations for different consensus
//! engines.

use std::sync::{Arc, atomic::AtomicBool};

use crate::{
    blockchain::{BlockChain, BlockChainDB},
    engines::EthEngine,
    snapshot::{Error, ManifestData, Progress},
    types::BlockNumber,
};

use ethereum_types::H256;

mod authority;
mod work;

pub use self::{authority::*, work::*};

/// A sink for produced chunks.
pub type ChunkSink<'a> = dyn FnMut(&[u8]) -> ::std::io::Result<()> + 'a;

/// Components necessary for snapshot creation and restoration.
pub trait SnapshotComponents: Send {
    /// Create secondary snapshot chunks; these corroborate the state data
    /// in the state chunks.
    ///
    /// Chunks shouldn't exceed the given preferred size, and should be fed
    /// uncompressed into the sink.
    ///
    /// This will vary by consensus engine, so it's exposed as a trait.
    fn chunk_all(
        &mut self,
        chain: &BlockChain,
        block_at: H256,
        chunk_sink: &mut ChunkSink,
        progress: &Progress,
        preferred_size: usize,
        eip1559_transition: BlockNumber,
    ) -> Result<(), Error>;

    /// Create a rebuilder, which will have chunks fed into it in aribtrary
    /// order and then be finalized.
    ///
    /// The manifest, a database, and fresh `BlockChain` are supplied.
    ///
    /// The engine passed to the `Rebuilder` methods will be the same instance
    /// that created the `SnapshotComponents`.
    fn rebuilder(
        &self,
        chain: BlockChain,
        db: Arc<dyn BlockChainDB>,
        manifest: &ManifestData,
    ) -> Result<Box<dyn Rebuilder>, crate::error::Error>;

    /// Minimum supported snapshot version number.
    fn min_supported_version(&self) -> u64;

    /// Current version number
    fn current_version(&self) -> u64;
}

/// Restore from secondary snapshot chunks.
pub trait Rebuilder: Send {
    /// Feed a chunk, potentially out of order.
    ///
    /// Check `abort_flag` periodically while doing heavy work. If set to `false`, should bail with
    /// `Error::RestorationAborted`.
    fn feed(
        &mut self,
        chunk: &[u8],
        engine: &dyn EthEngine,
        abort_flag: &AtomicBool,
    ) -> Result<(), crate::error::Error>;

    /// Finalize the restoration. Will be done after all chunks have been
    /// fed successfully.
    ///
    /// This should apply the necessary "glue" between chunks,
    /// and verify against the restored state.
    fn finalize(&mut self, engine: &dyn EthEngine) -> Result<(), crate::error::Error>;
}
