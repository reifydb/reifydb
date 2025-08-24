// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interceptor::{
	Chain, PostCommitInterceptor, PreCommitInterceptor,
	TablePostDeleteInterceptor, TablePreDeleteInterceptor,
};
use reifydb_core::interceptor::{
	TablePostInsertInterceptor, TablePreInsertInterceptor,
	TablePreUpdateInterceptor,
};
use reifydb_core::interface::interceptor::WithInterceptors;
use reifydb_core::interface::{
	CommandTransaction, QueryTransaction, TransactionId,
	TransactionalChanges, WithHooks,
};
use reifydb_core::{
	catalog::MaterializedCatalog, diagnostic::transaction,
	hook::Hooks,
	interceptor,
	interceptor::Interceptors,
	interface::{
		interceptor::TransactionInterceptor, BoxedVersionedIter, CdcTransaction,
		Transaction, UnversionedTransaction, Versioned,
		VersionedCommandTransaction, VersionedQueryTransaction,
		VersionedTransaction,
	},
	return_error,
	row::EncodedRow,
	EncodedKey,
	EncodedKeyRange,
};
use std::marker::PhantomData;

/// An active command transaction that holds a versioned command transaction
/// and provides query/command access to unversioned storage.
///
/// The transaction will auto-rollback on drop if not explicitly committed.
pub struct StandardCommandTransaction<T: Transaction> {
	versioned: Option<<T::Versioned as VersionedTransaction>::Command>,
	unversioned: T::Unversioned,
	cdc: T::Cdc,
	state: TransactionState,
	hooks: Hooks,
	catalog: MaterializedCatalog,
	changes: TransactionalChanges,

	pub(crate) interceptors: Interceptors<Self>,
	// Marker to prevent Send and Sync
	_not_send_sync: PhantomData<*const ()>,
}

#[derive(Clone, Copy, PartialEq)]
enum TransactionState {
	Active,
	Committed,
	RolledBack,
}

impl<T: Transaction> StandardCommandTransaction<T> {
	/// Creates a new active command transaction with a pre-commit callback
	pub fn new(
		versioned: <T::Versioned as VersionedTransaction>::Command,
		unversioned: T::Unversioned,
		cdc: T::Cdc,
		hooks: Hooks,
		interceptors: Interceptors<Self>,
		catalog: MaterializedCatalog,
	) -> Self {
		let txn_id = versioned.id();
		Self {
			versioned: Some(versioned),
			unversioned,
			cdc,
			state: TransactionState::Active,
			hooks,
			interceptors,
			catalog,
			changes: TransactionalChanges::new(txn_id),
			_not_send_sync: PhantomData,
		}
	}

	pub fn hooks(&self) -> &Hooks {
		&self.hooks
	}

	/// Check if transaction is still active and return appropriate error if
	/// not
	fn check_active(&self) -> reifydb_core::Result<()> {
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
	pub fn with_unversioned_query<F, R>(
		&self,
		f: F,
	) -> reifydb_core::Result<R>
	where
		F: FnOnce(
			&mut <T::Unversioned as UnversionedTransaction>::Query<
				'_,
			>,
		) -> reifydb_core::Result<R>,
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
	pub fn with_unversioned_command<F, R>(
		&self,
		f: F,
	) -> reifydb_core::Result<R>
	where
		F: FnOnce(
			&mut <T::Unversioned as UnversionedTransaction>::Command<
				'_,
			>,
		) -> reifydb_core::Result<R>,
	{
		self.check_active()?;
		self.unversioned.with_command(f)
	}

	/// Execute a function with access to the versioned command transaction.
	/// This operates within the same transaction context.
	pub fn with_versioned_command<F, R>(
		&mut self,
		f: F,
	) -> reifydb_core::Result<R>
	where
		F: FnOnce(
			&mut <T::Versioned as VersionedTransaction>::Command,
		) -> reifydb_core::Result<R>,
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
	pub fn with_versioned_query<F, R>(
		&mut self,
		f: F,
	) -> reifydb_core::Result<R>
	where
		F: FnOnce(
			&mut <T::Versioned as VersionedTransaction>::Command,
		) -> reifydb_core::Result<R>,
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
	pub fn commit(
		&mut self,
	) -> reifydb_core::Result<reifydb_core::Version> {
		self.check_active()?;

		TransactionInterceptor::pre_commit(self)?;

		if let Some(versioned) = self.versioned.take() {
			let id = versioned.id();
			self.state = TransactionState::Committed;
			let version = versioned.commit()?;

			// Pass catalog changes if there are any
			let catalogchanges = if self.changes.haschanges() {
				Some(self.changes.clone())
			} else {
				None
			};

			TransactionInterceptor::post_commit(
				self,
				id,
				version,
				catalogchanges,
			)?;

			Ok(version)
		} else {
			// This should never happen due to check_active
			unreachable!("Transaction state inconsistency")
		}
	}

	/// Rollback the transaction.
	pub fn rollback(&mut self) -> reifydb_core::Result<()> {
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
}

impl<T: Transaction> VersionedQueryTransaction
	for StandardCommandTransaction<T>
{
	#[inline]
	fn version(&self) -> reifydb_core::Version {
		self.versioned.as_ref().unwrap().version()
	}

	#[inline]
	fn id(&self) -> TransactionId {
		self.versioned.as_ref().unwrap().id()
	}

	#[inline]
	fn get(
		&mut self,
		key: &EncodedKey,
	) -> reifydb_core::Result<Option<Versioned>> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().get(key)
	}

	#[inline]
	fn contains_key(
		&mut self,
		key: &EncodedKey,
	) -> reifydb_core::Result<bool> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().contains_key(key)
	}

	#[inline]
	fn scan(&mut self) -> reifydb_core::Result<BoxedVersionedIter> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().scan()
	}

	#[inline]
	fn scan_rev(&mut self) -> reifydb_core::Result<BoxedVersionedIter> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().scan_rev()
	}

	#[inline]
	fn range(
		&mut self,
		range: EncodedKeyRange,
	) -> reifydb_core::Result<BoxedVersionedIter> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().range(range)
	}

	#[inline]
	fn range_rev(
		&mut self,
		range: EncodedKeyRange,
	) -> reifydb_core::Result<BoxedVersionedIter> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().range_rev(range)
	}

	#[inline]
	fn prefix(
		&mut self,
		prefix: &EncodedKey,
	) -> reifydb_core::Result<BoxedVersionedIter> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().prefix(prefix)
	}

	#[inline]
	fn prefix_rev(
		&mut self,
		prefix: &EncodedKey,
	) -> reifydb_core::Result<BoxedVersionedIter> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().prefix_rev(prefix)
	}
}

impl<T: Transaction> VersionedCommandTransaction
	for StandardCommandTransaction<T>
{
	#[inline]
	fn set(
		&mut self,
		key: &EncodedKey,
		row: EncodedRow,
	) -> reifydb_core::Result<()> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().set(key, row)
	}

	#[inline]
	fn remove(&mut self, key: &EncodedKey) -> reifydb_core::Result<()> {
		self.check_active()?;
		self.versioned.as_mut().unwrap().remove(key)
	}

	#[inline]
	fn commit(mut self) -> reifydb_core::Result<reifydb_core::Version> {
		self.check_active()?;
		self.state = TransactionState::Committed;
		self.versioned.take().unwrap().commit()
	}

	#[inline]
	fn rollback(mut self) -> reifydb_core::Result<()> {
		self.check_active()?;
		self.state = TransactionState::RolledBack;
		self.versioned.take().unwrap().rollback()
	}
}

impl<T: Transaction> WithHooks for StandardCommandTransaction<T> {
	fn hooks(&self) -> &Hooks {
		&self.hooks
	}
}

impl<T: Transaction> QueryTransaction for StandardCommandTransaction<T> {
	type UnversionedQuery<'a> =
		<T::Unversioned as UnversionedTransaction>::Query<'a>;

	type CdcQuery<'a> = <T::Cdc as CdcTransaction>::Query<'a>;

	fn begin_unversioned_query(
		&self,
	) -> reifydb_core::Result<Self::UnversionedQuery<'_>> {
		self.check_active()?;
		self.unversioned.begin_query()
	}

	fn begin_cdc_query(&self) -> reifydb_core::Result<Self::CdcQuery<'_>> {
		self.check_active()?;
		self.cdc.begin_query()
	}
}

impl<T: Transaction> CommandTransaction for StandardCommandTransaction<T> {
	type UnversionedCommand<'a> =
		<T::Unversioned as UnversionedTransaction>::Command<'a>;

	fn begin_unversioned_command(
		&self,
	) -> reifydb_core::Result<Self::UnversionedCommand<'_>> {
		self.check_active()?;
		self.unversioned.begin_command()
	}

	fn get_changes(&self) -> &TransactionalChanges {
		&self.changes
	}

	fn get_changes_mut(&mut self) -> &mut TransactionalChanges {
		&mut self.changes
	}
}

impl<T: Transaction> WithInterceptors<StandardCommandTransaction<T>>
	for StandardCommandTransaction<T>
{
	fn table_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn TablePreInsertInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.table_pre_insert
	}

	fn table_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn TablePostInsertInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.table_post_insert
	}

	fn table_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn TablePreUpdateInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.table_pre_update
	}

	fn table_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::TablePostUpdateInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.table_post_update
	}

	fn table_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn TablePreDeleteInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.table_pre_delete
	}

	fn table_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn TablePostDeleteInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.table_post_delete
	}

	fn pre_commit_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn PreCommitInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.pre_commit
	}

	fn post_commit_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn PostCommitInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.post_commit
	}

	// Schema definition interceptors
	fn schema_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::SchemaDefPostCreateInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.schema_def_post_create
	}

	fn schema_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::SchemaDefPreUpdateInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.schema_def_pre_update
	}

	fn schema_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::SchemaDefPostUpdateInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.schema_def_post_update
	}

	fn schema_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::SchemaDefPreDeleteInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.schema_def_pre_delete
	}

	// Table definition interceptors
	fn table_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::TableDefPostCreateInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.table_def_post_create
	}

	fn table_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::TableDefPreUpdateInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.table_def_pre_update
	}

	fn table_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::TableDefPostUpdateInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.table_def_post_update
	}

	fn table_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::TableDefPreDeleteInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.table_def_pre_delete
	}

	// View definition interceptors
	fn view_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::ViewDefPostCreateInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.view_def_post_create
	}

	fn view_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::ViewDefPreUpdateInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.view_def_pre_update
	}

	fn view_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::ViewDefPostUpdateInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.view_def_post_update
	}

	fn view_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::ViewDefPreDeleteInterceptor<
				StandardCommandTransaction<T>,
			>,
	> {
		&mut self.interceptors.view_def_pre_delete
	}
}

impl<T: Transaction> Drop for StandardCommandTransaction<T> {
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
