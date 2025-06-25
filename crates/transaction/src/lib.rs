// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use bypass::{BypassRx, BypassTx};
pub use error::Error;
use reifydb_core::hook::Hooks;
use reifydb_storage::{UnversionedStorage, VersionedStorage};
pub use rx::*;
pub use tx::*;

mod error;
pub mod mvcc;

mod bypass;
mod rx;
pub mod test_utils;
mod transaction;
mod tx;

pub type Result<T> = std::result::Result<T, Error>;

pub trait Transaction<VS: VersionedStorage, US: UnversionedStorage>: Send + Sync {
    type Rx: Rx;
    type Tx: Tx<VS, US>;

    fn begin_read_only(&self) -> Result<Self::Rx>;

    fn begin(&self) -> Result<Self::Tx>;

    fn hooks(&self) -> Hooks;

    fn versioned(&self) -> VS;
}
