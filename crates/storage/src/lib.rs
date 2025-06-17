// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb_core::hook::Hooks;
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, Version};
pub use unversioned::{
    UnversionedApply, UnversionedGet, UnversionedIter, UnversionedRemove, UnversionedScan,
    UnversionedSet, UnversionedStorage,
};
pub use versioned::{
    VersionedApply, VersionedContains, VersionedGet, VersionedIter, VersionedScan,
    VersionedScanRange, VersionedScanRangeRev, VersionedScanRev, VersionedStorage,
};

pub mod lmdb;
pub mod memory;
pub mod sqlite;
mod unversioned;
mod versioned;

pub trait GetHooks {
    fn hooks(&self) -> Hooks;
}

#[derive(Debug)]
pub struct Versioned {
    pub key: EncodedKey,
    pub row: EncodedRow,
    pub version: Version,
}

#[derive(Debug)]
pub struct Unversioned {
    pub key: EncodedKey,
    pub row: EncodedRow,
}

pub trait Storage: VersionedStorage + UnversionedStorage {}
