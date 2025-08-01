// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{GetHooks, Unversioned, UnversionedStorage, Versioned, VersionedStorage};
use crate::row::EncodedRow;
use crate::{EncodedKey, EncodedKeyRange, Error};
use std::sync::MutexGuard;

pub type BoxedVersionedIter<'a> = Box<dyn Iterator<Item = Versioned> + Send + 'a>;
pub type BoxedUnversionedIter<'a> = Box<dyn Iterator<Item = Unversioned> + Send + 'a>;

pub trait NewTransaction: Send + Sync + Clone + 'static {
    type Read: ReadTransaction;
    type Write: WriteTransaction;

    fn begin_read(&self) -> crate::Result<Self::Read>;

    fn begin_write(&self) -> Result<Self::Write, Error>;

    fn with_read<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut Self::Read) -> crate::Result<R>,
    {
        let mut tx = self.begin_read()?;
        f(&mut tx)
    }

    fn with_write<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut Self::Write) -> crate::Result<R>,
    {
        let mut tx = self.begin_write()?;
        let result = f(&mut tx)?;
        tx.commit()?;
        Ok(result)
    }
}

pub trait ReadTransaction {
    type Item;
    type Iter<'a>
    where
        Self: 'a;

    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Self::Item>>;

    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool>;

    fn scan(&mut self) -> crate::Result<Self::Iter<'_>>;

    fn scan_rev(&mut self) -> crate::Result<Self::Iter<'_>>;

    fn range(&mut self, range: EncodedKeyRange) -> crate::Result<Self::Iter<'_>>;

    fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<Self::Iter<'_>>;

    fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<Self::Iter<'_>> {
        self.range(EncodedKeyRange::prefix(prefix))
    }

    fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<Self::Iter<'_>> {
        self.range_rev(EncodedKeyRange::prefix(prefix))
    }
}

pub trait WriteTransaction: ReadTransaction {
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()>;

    fn remove(&mut self, key: &EncodedKey) -> crate::Result<()>;

    fn commit(self) -> crate::Result<()>;

    fn rollback(self) -> crate::Result<()>;
}

// FIXME to be deleted
pub trait Transaction<VS: VersionedStorage, US: UnversionedStorage>:
    GetHooks + Send + Sync + Clone + 'static
{
    type Rx: Rx;
    type Tx: Tx<VS, US>;

    fn begin_rx(&self) -> Result<Self::Rx, Error>;

    fn begin_tx(&self) -> Result<Self::Tx, Error>;

    fn begin_unversioned(&self) -> MutexGuard<US>;
}

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

    fn unversioned(&mut self) -> MutexGuard<US>;
}
