// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{GetHooks, UnversionedStorage, Versioned, VersionedStorage};
use crate::row::EncodedRow;
use crate::{EncodedKey, EncodedKeyRange};

pub type BoxedVersionedIter<'a> = Box<dyn Iterator<Item = Versioned> + Send + 'a>;

pub trait VersionedTransaction<VS: VersionedStorage, US: UnversionedStorage>:
    GetHooks + Send + Sync + Clone + 'static
{
    type Read: VersionedReadTransaction;
    type Write: VersionedWriteTransaction<VS, US>;

    fn begin_read(&self) -> crate::Result<Self::Read>;

    fn begin_write(&self) -> crate::Result<Self::Write>;

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

pub trait VersionedReadTransaction {
    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Versioned>>;

    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool>;

    fn scan(&mut self) -> crate::Result<BoxedVersionedIter>;

    fn scan_rev(&mut self) -> crate::Result<BoxedVersionedIter>;

    fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedVersionedIter>;

    fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedVersionedIter>;

    fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedVersionedIter>;

    fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedVersionedIter>;
}

pub trait VersionedWriteTransaction<VS: VersionedStorage, US: UnversionedStorage>:
    VersionedReadTransaction
{
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()>;

    fn remove(&mut self, key: &EncodedKey) -> crate::Result<()>;

    fn commit(self) -> crate::Result<()>;

    fn rollback(self) -> crate::Result<()>;
}
