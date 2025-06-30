// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::hook::Hooks;
use crate::interface::{Unversioned, UnversionedStorage, Versioned, VersionedStorage};
use crate::row::EncodedRow;
use crate::{EncodedKey, EncodedKeyRange, Error};
use std::sync::MutexGuard;

pub trait Transaction<VS: VersionedStorage, US: UnversionedStorage, BP: Bypass<US>>:
    Send + Sync
{
    type Rx: Rx;
    type Tx: Tx<VS, US, BP>;

    fn begin_read_only(&self) -> Result<Self::Rx, Error>;

    fn begin(&self) -> Result<Self::Tx, Error>;

    fn hooks(&self) -> Hooks;

    fn versioned(&self) -> VS;
}

pub type BoxedVersionedIter<'a> = Box<dyn Iterator<Item = Versioned> + 'a>;

pub trait Rx {
    fn get(&mut self, key: &EncodedKey) -> Result<Option<Versioned>, Error>;

    fn contains_key(&mut self, key: &EncodedKey) -> Result<bool, Error>;

    fn scan(&mut self) -> Result<BoxedVersionedIter, Error>;

    fn scan_rev(&mut self) -> Result<BoxedVersionedIter, Error>;

    fn scan_range(&mut self, range: EncodedKeyRange) -> Result<BoxedVersionedIter, Error>;

    fn scan_range_rev(&mut self, range: EncodedKeyRange) -> Result<BoxedVersionedIter, Error>;

    fn scan_prefix(&mut self, prefix: &EncodedKey) -> Result<BoxedVersionedIter, Error>;

    fn scan_prefix_rev(&mut self, prefix: &EncodedKey) -> Result<BoxedVersionedIter, Error>;
}

pub trait Bypass<US: UnversionedStorage>: Send + Sync {
    fn get(&mut self, key: &EncodedKey) -> Result<Option<Unversioned>, Error>;

    fn scan(&mut self) -> Result<US::ScanIter<'_>, Error>;

    fn scan_range(&mut self, range: EncodedKeyRange) -> Result<US::ScanRange<'_>, Error>;

    fn scan_prefix(&mut self, key: &EncodedKey) -> Result<US::ScanRange<'_>, Error>;

    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<(), Error>;

    fn remove(&mut self, key: &EncodedKey) -> Result<(), Error>;
}

pub trait Tx<VS: VersionedStorage, US: UnversionedStorage, BP: Bypass<US>>: Rx {
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<(), Error>;

    fn remove(&mut self, key: &EncodedKey) -> Result<(), Error>;

    fn commit(self) -> Result<(), Error>;

    fn rollback(self) -> Result<(), Error>;

    fn bypass(&mut self) -> MutexGuard<BP>;
}
