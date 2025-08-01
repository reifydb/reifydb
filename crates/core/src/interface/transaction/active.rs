// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::diagnostic::transaction;
use crate::interface::{
    BoxedVersionedIter, UnversionedStorage, UnversionedTransaction, Versioned,
    VersionedReadTransaction, VersionedStorage, VersionedTransaction, VersionedWriteTransaction,
};
use crate::return_error;
use crate::row::EncodedRow;
use crate::{EncodedKey, EncodedKeyRange};

/// An active read transaction that holds a versioned read transaction
/// and provides read-only access to unversioned storage.
pub struct ActiveReadTransaction<VS, US, VT, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    VT: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    versioned: VT::Read,
    unversioned: UT,
}

/// An active write transaction that holds a versioned write transaction
/// and provides read/write access to unversioned storage.
///
/// The transaction will auto-rollback on drop if not explicitly committed.
pub struct ActiveWriteTransaction<VS, US, VT, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    VT: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    versioned: Option<VT::Write>,
    unversioned: UT,
    state: TransactionState,
}

#[derive(Clone, Copy, PartialEq)]
enum TransactionState {
    Active,
    Committed,
    RolledBack,
}

impl<VS, US, VT, UT> ActiveReadTransaction<VS, US, VT, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    VT: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    /// Creates a new active read transaction
    pub fn new(versioned: VT::Read, unversioned: UT) -> Self {
        Self { versioned, unversioned }
    }

    /// Execute a function with read access to the unversioned transaction.
    pub fn with_unversioned_read<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut UT::Read<'_>) -> crate::Result<R>,
    {
        self.unversioned.with_read(f)
    }
}

impl<VS, US, VT, UT> VersionedReadTransaction for ActiveReadTransaction<VS, US, VT, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    VT: VersionedTransaction<VS, US>,
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

impl<VS, US, VT, UT> ActiveWriteTransaction<VS, US, VT, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    VT: VersionedTransaction<VS, US>,
    UT: UnversionedTransaction,
{
    /// Creates a new active write transaction
    pub fn new(versioned: VT::Write, unversioned: UT) -> Self {
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

    /// Execute a function with read access to the unversioned transaction.
    pub fn with_unversioned_read<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut UT::Read<'_>) -> crate::Result<R>,
    {
        self.check_active()?;
        self.unversioned.with_read(f)
    }

    /// Execute a function with write access to the unversioned transaction.
    ///
    /// Note: If this operation fails, the versioned transaction is NOT automatically rolled back.
    /// The caller should handle transaction rollback if needed.
    pub fn with_unversioned_write<F, R>(&self, f: F) -> crate::Result<R>
    where
        F: FnOnce(&mut UT::Write<'_>) -> crate::Result<R>,
    {
        self.check_active()?;
        self.unversioned.with_write(f)
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

impl<VS, US, VT, UT> VersionedReadTransaction for ActiveWriteTransaction<VS, US, VT, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    VT: VersionedTransaction<VS, US>,
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

impl<VS, US, VT, UT> VersionedWriteTransaction<VS, US> for ActiveWriteTransaction<VS, US, VT, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    VT: VersionedTransaction<VS, US>,
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

impl<VS, US, VT, UT> Drop for ActiveWriteTransaction<VS, US, VT, UT>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    VT: VersionedTransaction<VS, US>,
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
