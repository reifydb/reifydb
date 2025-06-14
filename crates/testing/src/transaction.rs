// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange};
use reifydb_storage::{UnversionedStorage, VersionedStorage};
use reifydb_transaction::mvcc::conflict::BTreeConflict;
use reifydb_transaction::mvcc::transaction::TransactionValue;
use reifydb_transaction::mvcc::transaction::iter::TransactionIter;
use reifydb_transaction::mvcc::transaction::iter_rev::TransactionIterRev;
use reifydb_transaction::mvcc::transaction::range::TransactionRange;
use reifydb_transaction::mvcc::transaction::range_rev::TransactionRangeRev;
use reifydb_transaction::{BypassTx, Tx};

pub struct TestTransaction<VS, US>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
{
    versioned: VS,
    unversioned: US,
}

impl<VS, US> TestTransaction<VS, US>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
{
    pub fn new(versioned: VS, unversioned: US) -> Self {
        Self { versioned, unversioned }
    }
}

impl<VS, US> Tx<VS, US> for TestTransaction<VS, US>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
{
    fn get(&mut self, key: &EncodedKey) -> reifydb_transaction::Result<Option<TransactionValue>> {
        todo!()
    }

    fn contains_key(&mut self, key: &EncodedKey) -> reifydb_transaction::Result<bool> {
        todo!()
    }

    fn scan(&mut self) -> reifydb_transaction::Result<TransactionIter<'_, VS, BTreeConflict>> {
        todo!()
    }

    fn scan_rev(
        &mut self,
    ) -> reifydb_transaction::Result<TransactionIterRev<'_, VS, BTreeConflict>> {
        todo!()
    }

    fn scan_range(
        &mut self,
        range: EncodedKeyRange,
    ) -> reifydb_transaction::Result<TransactionRange<'_, VS, BTreeConflict>> {
        todo!()
    }

    fn scan_range_rev(
        &mut self,
        range: EncodedKeyRange,
    ) -> reifydb_transaction::Result<TransactionRangeRev<'_, VS, BTreeConflict>> {
        todo!()
    }

    fn scan_prefix(
        &mut self,
        prefix: &EncodedKey,
    ) -> reifydb_transaction::Result<TransactionRange<'_, VS, BTreeConflict>> {
        todo!()
    }

    fn scan_prefix_rev(
        &mut self,
        prefix: &EncodedKey,
    ) -> reifydb_transaction::Result<TransactionRangeRev<'_, VS, BTreeConflict>> {
        todo!()
    }

    fn set(&mut self, key: EncodedKey, row: EncodedRow) -> reifydb_transaction::Result<()> {
        todo!()
    }

    fn remove(&mut self, key: EncodedKey) -> reifydb_transaction::Result<()> {
        todo!()
    }

    fn commit(self) -> reifydb_transaction::Result<()> {
        todo!()
    }

    fn rollback(self) -> reifydb_transaction::Result<()> {
        todo!()
    }

    fn bypass(&mut self) -> reifydb_transaction::BypassTx<US> {
        BypassTx::new(self.unversioned.clone())
    }
}
