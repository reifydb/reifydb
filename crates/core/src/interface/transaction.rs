// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{UnversionedStorage, Versioned, VersionedStorage};
use crate::row::EncodedRow;
use crate::{EncodedKey, EncodedKeyRange, Error};
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

pub trait Transaction<VS: VersionedStorage, US: UnversionedStorage>:
    Send + Sync + Clone + 'static
{
    type Rx: Rx;
    type Tx: Tx<VS, US>;

    fn begin_rx(&self) -> Result<Self::Rx, Error>;

    fn begin_tx(&self) -> Result<Self::Tx, Error>;

    fn begin_unversioned_rx(&self) -> RwLockReadGuard<US>;

    fn begin_unversioned_tx(&self) -> RwLockWriteGuard<US>;

    fn versioned(&self) -> VS;
}

pub type BoxedVersionedIter<'a> = Box<dyn Iterator<Item = Versioned> + Send + 'a>;

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

pub trait Tx<VS: VersionedStorage, US: UnversionedStorage>: Rx {
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<(), Error>;

    fn remove(&mut self, key: &EncodedKey) -> Result<(), Error>;

    fn commit(self) -> Result<(), Error>;

    fn rollback(self) -> Result<(), Error>;

    fn unversioned(&mut self) -> RwLockWriteGuard<US>;
}
