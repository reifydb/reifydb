// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{GetHooks, Unversioned};
use crate::row::EncodedRow;
use crate::{EncodedKey, EncodedKeyRange};

pub type BoxedUnversionedIter<'a> = Box<dyn Iterator<Item = Unversioned> + Send + 'a>;

pub trait UnversionedTransaction: GetHooks + Send + Sync + Clone + 'static {
    type Read<'a>: UnversionedReadTransaction;
    type Write<'a>: UnversionedWriteTransaction;

    fn begin_read(&self) -> crate::Result<Self::Read<'_>>;

    fn begin_write(&self) -> crate::Result<Self::Write<'_>>;

    fn with_read<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut Self::Read<'_>) -> crate::Result<R>,
    {
        let mut tx = self.begin_read()?;
        f(&mut tx)
    }

    fn with_write<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut Self::Write<'_>) -> crate::Result<R>,
    {
        let mut tx = self.begin_write()?;
        let result = f(&mut tx)?;
        tx.commit()?;
        Ok(result)
    }
}

pub trait UnversionedReadTransaction {
    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Unversioned>>;

    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool>;

    fn scan(&mut self) -> crate::Result<BoxedUnversionedIter>;

    fn scan_rev(&mut self) -> crate::Result<BoxedUnversionedIter>;

    fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedUnversionedIter>;

    fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedUnversionedIter>;

    fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedUnversionedIter> {
        self.range(EncodedKeyRange::prefix(prefix))
    }

    fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedUnversionedIter> {
        self.range_rev(EncodedKeyRange::prefix(prefix))
    }
}

pub trait UnversionedWriteTransaction: UnversionedReadTransaction {
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()>;

    fn remove(&mut self, key: &EncodedKey) -> crate::Result<()>;

    fn commit(self) -> crate::Result<()>;

    fn rollback(self) -> crate::Result<()>;
}
