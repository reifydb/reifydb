// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::diagnostic::transaction;
use crate::interface::{
    BoxedVersionedIter, UnversionedTransaction, Versioned, VersionedQueryTransaction,
    VersionedTransaction, VersionedCommandTransaction,
};
use crate::return_error;
use crate::row::EncodedRow;
use crate::{EncodedKey, EncodedKeyRange};

/// An active query transaction that holds a versioned query transaction
/// and provides query-only access to unversioned storage.
pub struct ActiveQueryTransaction<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    versioned: VT::Query,
    unversioned: UT,
}

/// An active command transaction that holds a versioned command transaction
/// and provides query/command access to unversioned storage.
///
/// The transaction will auto-rollback on drop if not explicitly committed.
pub struct ActiveCommandTransaction<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    versioned: Option<VT::Command>,
    unversioned: UT,
    state: TransactionState,
}

#[derive(Clone, Copy, PartialEq)]
enum TransactionState {
    Active,
    Committed,
    RolledBack,
}

impl<VT, UT> ActiveQueryTransaction<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    /// Creates a new active query transaction
    pub fn new(versioned: VT::Query, unversioned: UT) -> Self {
        Self { versioned, unversioned }
    }

    /// Execute a function with query access to the unversioned transaction.
    pub fn with_unversioned_query<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut UT::Query<'_>) -> crate::Result<R>,
    {
        self.unversioned.with_query(f)
    }
}

impl<VT, UT> VersionedQueryTransaction for ActiveQueryTransaction<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    #[inline]
    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Versioned>> {
        self.versioned.get(key)
    }

    #[inline]
    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
        self.versioned.contains_key(key)
    }

    #[inline]
    fn scan(&mut self) -> crate::Result<BoxedVersionedIter> {
        self.versioned.scan()
    }

    #[inline]
    fn scan_rev(&mut self) -> crate::Result<BoxedVersionedIter> {
        self.versioned.scan_rev()
    }

    #[inline]
    fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedVersionedIter> {
        self.versioned.range(range)
    }

    #[inline]
    fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedVersionedIter> {
        self.versioned.range_rev(range)
    }

    #[inline]
    fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedVersionedIter> {
        self.versioned.prefix(prefix)
    }

    #[inline]
    fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedVersionedIter> {
        self.versioned.prefix_rev(prefix)
    }
}

impl<VT, UT> ActiveCommandTransaction<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    /// Creates a new active command transaction
    pub fn new(versioned: VT::Command, unversioned: UT) -> Self {
        Self { versioned: Some(versioned), unversioned, state: TransactionState::Active }
    }

    /// Check if transaction is still active and return appropriate error if not
    fn check_active(&self) -> crate::Result<()> {
        match self.state {
            TransactionState::Active => Ok(()),
            TransactionState::Committed => {
                return_error!(transaction::transaction_already_committed())
            }
            TransactionState::RolledBack => {
                return_error!(transaction::transaction_already_rolled_back())
            }
        }
    }

    /// Execute a function with query access to the unversioned transaction.
    pub fn with_unversioned_query<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut UT::Query<'_>) -> crate::Result<R>,
    {
        self.check_active()?;
        self.unversioned.with_query(f)
    }

    /// Execute a function with command access to the unversioned transaction.
    ///
    /// Note: If this operation fails, the versioned transaction is NOT automatically rolled back.
    /// The caller should handle transaction rollback if needed.
    pub fn with_unversioned_command<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut UT::Command<'_>) -> crate::Result<R>,
    {
        self.check_active()?;
        self.unversioned.with_command(f)
    }

    /// Commit the transaction.
    /// Since unversioned transactions are short-lived and auto-commit,
    /// this only commits the versioned transaction.
    pub fn commit(&mut self) -> crate::Result<()> {
        self.check_active()?;
        if let Some(versioned) = self.versioned.take() {
            self.state = TransactionState::Committed;
            versioned.commit()
        } else {
            // This should never happen due to check_active
            unreachable!("Transaction state inconsistency")
        }
    }

    /// Rollback the transaction.
    pub fn rollback(&mut self) -> crate::Result<()> {
        self.check_active()?;
        if let Some(versioned) = self.versioned.take() {
            self.state = TransactionState::RolledBack;
            versioned.rollback()
        } else {
            // This should never happen due to check_active
            unreachable!("Transaction state inconsistency")
        }
    }
}

impl<VT, UT> VersionedQueryTransaction for ActiveCommandTransaction<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    #[inline]
    fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<Versioned>> {
        self.check_active()?;
        self.versioned.as_mut().unwrap().get(key)
    }

    #[inline]
    fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
        self.check_active()?;
        self.versioned.as_mut().unwrap().contains_key(key)
    }

    #[inline]
    fn scan(&mut self) -> crate::Result<BoxedVersionedIter> {
        self.check_active()?;
        self.versioned.as_mut().unwrap().scan()
    }

    #[inline]
    fn scan_rev(&mut self) -> crate::Result<BoxedVersionedIter> {
        self.check_active()?;
        self.versioned.as_mut().unwrap().scan_rev()
    }

    #[inline]
    fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedVersionedIter> {
        self.check_active()?;
        self.versioned.as_mut().unwrap().range(range)
    }

    #[inline]
    fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedVersionedIter> {
        self.check_active()?;
        self.versioned.as_mut().unwrap().range_rev(range)
    }

    #[inline]
    fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedVersionedIter> {
        self.check_active()?;
        self.versioned.as_mut().unwrap().prefix(prefix)
    }

    #[inline]
    fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedVersionedIter> {
        self.check_active()?;
        self.versioned.as_mut().unwrap().prefix_rev(prefix)
    }
}

impl<VT, UT> VersionedCommandTransaction for ActiveCommandTransaction<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    #[inline]
    fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()> {
        self.check_active()?;
        self.versioned.as_mut().unwrap().set(key, row)
    }

    #[inline]
    fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
        self.check_active()?;
        self.versioned.as_mut().unwrap().remove(key)
    }

    #[inline]
    fn commit(mut self) -> crate::Result<()> {
        self.check_active()?;
        self.state = TransactionState::Committed;
        self.versioned.take().unwrap().commit()
    }

    #[inline]
    fn rollback(mut self) -> crate::Result<()> {
        self.check_active()?;
        self.state = TransactionState::RolledBack;
        self.versioned.take().unwrap().rollback()
    }
}

impl<VT, UT> Drop for ActiveCommandTransaction<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn drop(&mut self) {
        if let Some(versioned) = self.versioned.take() {
            // Auto-rollback if still active (not committed or rolled back)
            if self.state == TransactionState::Active {
                let _ = versioned.rollback();
            }
        }
    }
}
