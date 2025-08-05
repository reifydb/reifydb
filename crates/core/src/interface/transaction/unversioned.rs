// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{GetHooks, Unversioned};
use crate::row::EncodedRow;
use crate::{EncodedKey, EncodedKeyRange};

pub type BoxedUnversionedIter<'a> = Box<dyn Iterator<Item = Unversioned> + Send + 'a>;

pub trait UnversionedTransaction: GetHooks + Send + Sync + Clone + 'static {
    type Query<'a>: UnversionedQueryTransaction;
    type Command<'a>: UnversionedCommandTransaction;

    fn begin_query(&self) -> crate::Result<Self::Query<'_>>;

    fn begin_command(&self) -> crate::Result<Self::Command<'_>>;

    fn with_query<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut Self::Query<'_>) -> crate::Result<R>,
    {
        let mut tx = self.begin_query()?;
        f(&mut tx)
    }

    fn with_command<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut Self::Command<'_>) -> crate::Result<R>,
    {
        let mut tx = self.begin_command()?;
        let result = f(&mut tx)?;
        tx.commit()?;
        Ok(result)
    }
}

pub trait UnversionedQueryTransaction {
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

pub trait UnversionedCommandTransaction: UnversionedQueryTransaction {
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()>;

    fn remove(&mut self, key: &EncodedKey) -> crate::Result<()>;

    fn commit(self) -> crate::Result<()>;

    fn rollback(self) -> crate::Result<()>;
}
