// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use crate::catalog::{Catalog, Schema};
use crate::skipdb::skipdbcore::iter::TransactionIter;
use crate::skipdb::skipdbcore::range::TransactionRange;
use crate::skipdb::skipdbcore::rev_iter::WriteTransactionRevIter;
use crate::skipdb::skipdbcore::rev_range::WriteTransactionRevRange;
use crate::skipdb::skipdbcore::types::Ref;
use crate::skipdb::txn::{HashCmOptions, PwmComparableRange, error::WtmError};
use crate::{CATALOG, CatalogRx, CatalogTx, InsertResult, Transaction};
use reifydb_core::encoding::{Value as _, bincode, keycode};
use reifydb_core::{Key, Row, RowIter, Value, key_prefix};
use reifydb_persistence::Persistence;
use std::convert::Infallible;
use std::fmt::Debug;

/// A optimistic concurrency control transaction over the [`OptimisticDb`].
pub struct OptimisticTransaction<K, V, S = RandomState> {
    db: OptimisticDb<K, V, S>,
    pub(super) wtm: Wtm<K, V, HashCm<K, S>, BTreePwm<K, V>>,
}

impl<P: Persistence> Transaction<P> for OptimisticDb<Vec<u8>, Vec<u8>, RandomState> {
    type Rx = OptimisticTransaction<Vec<u8>, Vec<u8>, RandomState>;
    type Tx = OptimisticTransaction<Vec<u8>, Vec<u8>, RandomState>;

    fn begin_read_only(&self) -> crate::Result<Self::Rx> {
        // Ok(self.db.read())
        todo!()
    }

    fn begin(&self) -> crate::Result<Self::Tx> {
        Ok(self.write())
    }
}

impl crate::Rx for OptimisticTransaction<Vec<u8>, Vec<u8>, RandomState> {
    type Catalog = Catalog;
    type Schema = Schema;

    fn catalog(&self) -> crate::Result<&Self::Catalog> {
        // FIXME replace this
        unsafe { Ok(*CATALOG.get().unwrap().0.get()) }
    }

    fn schema(&self, schema: &str) -> crate::Result<&Self::Schema> {
        Ok(self.catalog().unwrap().get(schema).unwrap())
    }

    fn get(&self, store: &str, ids: &[Key]) -> crate::Result<Vec<Row>> {
        todo!()
    }

    fn scan_table(&mut self, schema: &str, store: &str) -> crate::Result<RowIter> {
        Ok(Box::new(
            self.range(keycode::prefix_range(&key_prefix!("{}::{}::row::", schema, store)))
                .unwrap()
                // .scan(start_key..end_key) // range is [start_key, end_key)
                .map(|r| Row::decode(&r.value()).unwrap())
                .collect::<Vec<_>>()
                .into_iter(),
        ))
    }
}

impl crate::Tx for OptimisticTransaction<Vec<u8>, Vec<u8>, RandomState> {
    type CatalogMut = Catalog;
    type SchemaMut = Schema;

    fn catalog_mut(&mut self) -> crate::Result<&mut Self::CatalogMut> {
        // FIXME replace this
        unsafe { Ok(*CATALOG.get().unwrap().0.get()) }
    }

    fn schema_mut(&mut self, schema: &str) -> crate::Result<&mut Self::SchemaMut> {
        let schema = self.catalog_mut().unwrap().get_mut(schema).unwrap();

        Ok(schema)
    }

    fn insert_into_table(
        &mut self,
        schema: &str,
        table: &str,
        rows: Vec<Row>,
    ) -> crate::Result<InsertResult> {
        todo!()
    }

    fn insert_into_series(
        &mut self,
        schema: &str,
        series: &str,
        rows: Vec<Vec<Value>>,
    ) -> crate::Result<InsertResult> {
        let last_id = self
            .range(keycode::prefix_range(&key_prefix!("{}::{}::row::", schema, series)))
            .unwrap()
            .count();

        // FIXME assumes every row gets inserted - not updated etc..
        let inserted = rows.len();

        for (id, row) in rows.iter().enumerate() {
            self.insert(
                key_prefix!("{}::{}::row::{}", schema, series, (last_id + id + 1)).clone(),
                bincode::serialize(row),
            )
            .unwrap();
        }
        // let mut persistence = self.persistence.lock().unwrap();
        // let inserted = persistence.table_append_rows(schema, table, &rows).unwrap();
        Ok(InsertResult { inserted })
    }

    fn commit(mut self) -> crate::Result<()> {
        OptimisticTransaction::commit(&mut self).unwrap();

        Ok(())
    }

    fn rollback(mut self) -> crate::Result<()> {
        OptimisticTransaction::rollback(&mut self).unwrap();

        Ok(())
    }
}

impl<K, V, S> OptimisticTransaction<K, V, S>
where
    K: Ord + Hash + Eq,
    S: BuildHasher + Clone,
{
    #[inline]
    pub(super) fn new(db: OptimisticDb<K, V, S>, cap: Option<usize>) -> Self {
        let wtm = db
            .inner
            .tm
            .write((), HashCmOptions::with_capacity(db.inner.hasher.clone(), cap.unwrap_or(8)))
            .unwrap();
        Self { db, wtm }
    }
}

impl<K, V, S> OptimisticTransaction<K, V, S>
where
    K: Ord + Hash + Eq + Debug,
    V: Send + 'static + Debug,
    S: BuildHasher,
{
    /// Commits the transaction, following these steps:
    ///
    /// 1. If there are no writes, return immediately.
    ///
    /// 2. Check if read rows were updated since txn started. If so, return `TransactionError::Conflict`.
    ///
    /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
    ///
    /// 4. Batch up all writes, write them to database.
    ///
    /// 5. If callback is provided, Badger will return immediately after checking
    ///    for conflicts. Writes to the database will happen in the background.  If
    ///    there is a conflict, an error will be returned and the callback will not
    ///    run. If there are no conflicts, the callback will be called in the
    ///    background upon successful completion of writes or any error during write.
    #[inline]
    pub fn commit(&mut self) -> Result<(), WtmError<Infallible, Infallible, Infallible>> {
        self.wtm.commit(|operations| {
            self.db.inner.map.apply(operations);
            Ok(())
        })
    }
}

impl<K, V, S> OptimisticTransaction<K, V, S>
where
    K: Ord + Hash + Eq + Send + Sync + 'static,
    V: Send + Sync + 'static,
    S: BuildHasher + Send + Sync + 'static,
{
    /// Acts like [`commit`](WriteTransaction::commit), but takes a callback, which gets run via a
    /// thread to avoid blocking this function. Following these steps:
    ///
    /// 1. If there are no writes, return immediately, callback will be invoked.
    ///
    /// 2. Check if read rows were updated since txn started. If so, return `TransactionError::Conflict`.
    ///
    /// 3. If no conflict, generate a commit timestamp and update written rows' commit ts.
    ///
    /// 4. Batch up all writes, write them to database.
    ///
    /// 5. Return immediately after checking for conflicts.
    ///    If there is a conflict, an error will be returned immediately and the callback will not
    ///    run. If there are no conflicts, the callback will be called in the
    ///    background upon successful completion of writes or any error during write.
    #[inline]
    pub fn commit_with_callback<E, R>(
        &mut self,
        callback: impl FnOnce(Result<(), E>) -> R + Send + 'static,
    ) -> Result<std::thread::JoinHandle<R>, WtmError<Infallible, Infallible, E>>
    where
        E: std::error::Error,
        R: Send + 'static,
    {
        let db = self.db.clone();

        self.wtm.commit_with_callback(
            move |ents| {
                db.inner.map.apply(ents);
                Ok(())
            },
            callback,
        )
    }
}

impl<K, V, S> OptimisticTransaction<K, V, S>
where
    K: Ord + Hash + Eq,
    V: 'static,
    S: BuildHasher,
{
    /// Returns the read version of the transaction.
    #[inline]
    pub fn version(&self) -> u64 {
        self.wtm.version()
    }

    /// Rollback the transaction.
    #[inline]
    pub fn rollback(&mut self) -> Result<(), TransactionError<Infallible, Infallible>> {
        self.wtm.rollback()
    }

    /// Returns true if the given key exists in the database.
    #[inline]
    pub fn contains_key<Q>(
        &mut self,
        key: &Q,
    ) -> Result<bool, TransactionError<Infallible, Infallible>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Ord + ?Sized,
    {
        let version = self.wtm.version();
        match self.wtm.contains_key_equivalent_cm_comparable_pm(key)? {
            Some(true) => Ok(true),
            Some(false) => Ok(false),
            None => Ok(self.db.inner.map.contains_key(key, version)),
        }
    }

    /// Get a value from the database.
    #[inline]
    pub fn get<'a, 'b: 'a, Q>(
        &'a mut self,
        key: &'b Q,
    ) -> Result<Option<Ref<'a, K, V>>, TransactionError<Infallible, Infallible>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + Ord + ?Sized,
    {
        let version = self.wtm.version();
        match self.wtm.get_equivalent_cm_comparable_pm(key)? {
            Some(v) => {
                if v.value().is_some() {
                    Ok(Some(v.into()))
                } else {
                    Ok(None)
                }
            }
            None => Ok(self.db.inner.map.get(key, version).map(Into::into)),
        }
    }

    /// Insert a new key-value pair.
    #[inline]
    pub fn insert(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), TransactionError<Infallible, Infallible>> {
        self.wtm.insert(key, value)
    }

    /// Remove a key.
    #[inline]
    pub fn remove(&mut self, key: K) -> Result<(), TransactionError<Infallible, Infallible>> {
        self.wtm.remove(key)
    }

    /// Iterate over the entries of the write transaction.
    #[inline]
    pub fn iter(
        &mut self,
    ) -> Result<TransactionIter<'_, K, V, HashCm<K, S>>, TransactionError<Infallible, Infallible>>
    {
        let version = self.wtm.version();
        let (marker, pm) = self.wtm.marker_with_pm().ok_or(TransactionError::Discard)?;
        let committed = self.db.inner.map.iter(version);
        let pendings = pm.iter();

        Ok(TransactionIter::new(pendings, committed, Some(marker)))
    }

    /// Iterate over the entries of the write transaction in reverse order.
    #[inline]
    pub fn iter_rev(
        &mut self,
    ) -> Result<
        WriteTransactionRevIter<'_, K, V, HashCm<K, S>>,
        TransactionError<Infallible, Infallible>,
    > {
        let version = self.wtm.version();
        let (marker, pm) = self.wtm.marker_with_pm().ok_or(TransactionError::Discard)?;
        let committed = self.db.inner.map.iter_rev(version);
        let pendings = pm.iter().rev();

        Ok(WriteTransactionRevIter::new(pendings, committed, Some(marker)))
    }

    /// Returns an iterator over the subset of entries of the database.
    #[inline]
    pub fn range<'a, Q, R>(
        &'a mut self,
        range: R,
    ) -> Result<
        TransactionRange<'a, Q, R, K, V, HashCm<K, S>>,
        TransactionError<Infallible, Infallible>,
    >
    where
        K: Borrow<Q>,
        R: RangeBounds<Q> + 'a,
        Q: Ord + ?Sized,
    {
        let version = self.wtm.version();
        let (marker, pm) = self.wtm.marker_with_pm().ok_or(TransactionError::Discard)?;
        let start = range.start_bound();
        let end = range.end_bound();
        let pendings = pm.range_comparable((start, end));
        let committed = self.db.inner.map.range(range, version);

        Ok(TransactionRange::new(pendings, committed, Some(marker)))
    }

    /// Returns an iterator over the subset of entries of the database in reverse order.
    #[inline]
    pub fn range_rev<'a, Q, R>(
        &'a mut self,
        range: R,
    ) -> Result<
        WriteTransactionRevRange<'a, Q, R, K, V, HashCm<K, S>>,
        TransactionError<Infallible, Infallible>,
    >
    where
        K: Borrow<Q>,
        R: RangeBounds<Q> + 'a,
        Q: Ord + ?Sized,
    {
        let version = self.wtm.version();
        let (marker, pm) = self.wtm.marker_with_pm().ok_or(TransactionError::Discard)?;
        let start = range.start_bound();
        let end = range.end_bound();
        let pendings = pm.range_comparable((start, end));
        let committed = self.db.inner.map.range_rev(range, version);

        Ok(WriteTransactionRevRange::new(pendings.rev(), committed, Some(marker)))
    }
}
