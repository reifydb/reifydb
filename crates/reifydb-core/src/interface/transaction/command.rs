// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	EncodedKey, EncodedKeyRange,
	diagnostic::transaction,
	hook::Hooks,
	interceptor::Interceptors,
	interface::{
		BoxedVersionedIter, Transaction, UnversionedTransaction,
		Versioned, VersionedCommandTransaction,
		VersionedQueryTransaction, VersionedTransaction,
		interceptor::TransactionInterceptor,
		transaction::pending::PendingWrite,
	},
	return_error,
	row::EncodedRow,
};

/// An active command transaction that holds a versioned command transaction
/// and provides query/command access to unversioned storage.
///
/// The transaction will auto-rollback on drop if not explicitly committed.
pub struct CommandTransaction<T: Transaction> {
	versioned: Option<<T::Versioned as VersionedTransaction>::Command>,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	state: TransactionState,
	pending: Vec<PendingWrite>,
	hooks: Hooks,
	pub(crate) interceptors: Interceptors<T>,
}

#[derive(Clone, Copy, PartialEq)]
enum TransactionState {
	Active,
	Committed,
	RolledBack,
}

impl<T: Transaction> CommandTransaction<T> {
	/// Creates a new active command transaction with a pre-commit callback
	pub fn new(
		versioned: <T::Versioned as VersionedTransaction>::Command,
		unversioned: T::Unversioned,
		cdc: T::Cdc,
		hooks: Hooks,
		interceptors: Interceptors<T>,
	) -> Self {
		Self {
			versioned: Some(versioned),
			unversioned,
			cdc,
			state: TransactionState::Active,
			hooks,
			pending: Vec::new(),
			interceptors,
		}
	}

	pub fn hooks(&self) -> &Hooks {
		&self.hooks
	}

	/// Check if transaction is still active and return appropriate error if
	/// not
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
		F: FnOnce(
			&mut <T::Unversioned as UnversionedTransaction>::Query<
				'_,
			>,
		) -> crate::Result<R>,
	{
		self.check_active()?;
		self.unversioned.with_query(f)
	}

	/// Execute a function with command access to the unversioned
	/// transaction.
	///
	/// Note: If this operation fails, the versioned transaction is NOT
	/// automatically rolled back. The caller should handle transaction
	/// rollback if needed.
	pub fn with_unversioned_command<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(
			&mut <T::Unversioned as UnversionedTransaction>::Command<
				'_,
			>,
		) -> crate::Result<R>,
	{
		self.check_active()?;
		self.unversioned.with_command(f)
	}

	/// Execute a function with access to the versioned command transaction.
	/// This operates within the same transaction context.
	pub fn with_versioned_command<F, R>(&mut self, f: F) -> crate::Result<R>
	where
		F: FnOnce(
			&mut <T::Versioned as VersionedTransaction>::Command,
		) -> crate::Result<R>,
	{
		self.check_active()?;
		let result = f(self.versioned.as_mut().unwrap());

		// If there was an error, we should roll back the transaction
		if result.is_err() {
			if let Some(versioned) = self.versioned.take() {
				self.state = TransactionState::RolledBack;
				let _ = versioned.rollback(); // Ignore rollback errors
			}
		}

		result
	}

	/// Execute a function with access to the versioned query capabilities.
	/// This operates within the same transaction context and provides
	/// read-only access.
	pub fn with_versioned_query<F, R>(&mut self, f: F) -> crate::Result<R>
	where
		F: FnOnce(
			&mut <T::Versioned as VersionedTransaction>::Command,
		) -> crate::Result<R>,
		<T::Versioned as VersionedTransaction>::Command:
			VersionedQueryTransaction,
	{
		self.check_active()?;
		let result = f(self.versioned.as_mut().unwrap());

		// If there was an error, we should roll back the transaction
		if result.is_err() {
			if let Some(versioned) = self.versioned.take() {
				self.state = TransactionState::RolledBack;
				let _ = versioned.rollback(); // Ignore rollback errors
			}
		}

		result
	}

	/// Commit the transaction.
	/// Since unversioned transactions are short-lived and auto-commit,
	/// this only commits the versioned transaction.
	pub fn commit(&mut self) -> crate::Result<crate::Version> {
		self.check_active()?;

		TransactionInterceptor::pre_commit(self)?;

		if let Some(versioned) = self.versioned.take() {
			self.state = TransactionState::Committed;
			let version = versioned.commit()?;

			TransactionInterceptor::post_commit(self, version)?;

			Ok(version)
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

	/// Get access to the CDC transaction interface
	pub fn cdc(&self) -> &T::Cdc {
		&self.cdc
	}

	/// Add a pending change to be processed at commit time
	pub fn add_pending(&mut self, pending: PendingWrite) {
		self.pending.push(pending);
	}

	/// Get all pending changes
	pub fn take_pending(&mut self) -> Vec<PendingWrite> {
		std::mem::take(&mut self.pending)
	}
}

impl<T: Transaction> VersionedQueryTransaction for CommandTransaction<T> {
	#[inline]
	fn get(
		&mut self,
		key: &EncodedKey,
	) -> crate::Result<Option<Versioned>> {
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
	fn range(
		&mut self,
		range: EncodedKeyRange,
	) -> crate::Result<BoxedVersionedIter> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().range(range)
	}

	#[inline]
	fn range_rev(
		&mut self,
		range: EncodedKeyRange,
	) -> crate::Result<BoxedVersionedIter> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().range_rev(range)
	}

	#[inline]
	fn prefix(
		&mut self,
		prefix: &EncodedKey,
	) -> crate::Result<BoxedVersionedIter> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().prefix(prefix)
	}

	#[inline]
	fn prefix_rev(
		&mut self,
		prefix: &EncodedKey,
	) -> crate::Result<BoxedVersionedIter> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().prefix_rev(prefix)
	}
}

impl<T: Transaction> VersionedCommandTransaction for CommandTransaction<T> {
	#[inline]
	fn set(
		&mut self,
		key: &EncodedKey,
		row: EncodedRow,
	) -> crate::Result<()> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().set(key, row)
	}

	#[inline]
	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().remove(key)
	}

	#[inline]
	fn commit(mut self) -> crate::Result<crate::Version> {
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

impl<T: Transaction> Drop for CommandTransaction<T> {
	fn drop(&mut self) {
		if let Some(versioned) = self.versioned.take() {
			// Auto-rollback if still active (not committed or
			// rolled back)
			if self.state == TransactionState::Active {
				let _ = versioned.rollback();
			}
		}
	}
}
