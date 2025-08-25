// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use Operation::Table;
use OperationType::{Create, Delete};
use reifydb_catalog::{
	CatalogTransaction, CatalogTransactionContext, MaterializedCatalog,
};
use reifydb_core::{
	EncodedKey, EncodedKeyRange, Version,
	diagnostic::{
		catalog::{
			cannot_delete_already_deleted_table,
			cannot_delete_already_deleted_view,
		},
		transaction,
	},
	hook::Hooks,
	interceptor,
	interceptor::{
		Chain, Interceptors, PostCommitInterceptor,
		PreCommitInterceptor, TablePostDeleteInterceptor,
		TablePostInsertInterceptor, TablePreDeleteInterceptor,
		TablePreInsertInterceptor, TablePreUpdateInterceptor,
	},
	interface::{
		BoxedVersionedIter, CdcTransaction, Change, CommandTransaction,
		Operation, OperationType, QueryTransaction, TableDef,
		Transaction, TransactionId, TransactionalChanges,
		UnversionedTransaction, Versioned, VersionedCommandTransaction,
		VersionedQueryTransaction, VersionedTransaction, ViewDef,
		WithHooks,
		interceptor::{TransactionInterceptor, WithInterceptors},
	},
	return_error,
	row::EncodedRow,
};

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
	changes: TransactionalChanges,
	catalog: MaterializedCatalog,

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
		catalog: MaterializedCatalog,
		interceptors: Interceptors<Self>,
	) -> Self {
		let txn_id = versioned.id();
		Self {
			versioned: Some(versioned),
			unversioned,
			cdc,
			state: TransactionState::Active,
			hooks,
			catalog,
			interceptors,
			changes: TransactionalChanges::new(txn_id),
			_not_send_sync: PhantomData,
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
	pub fn commit(&mut self) -> crate::Result<Version> {
		self.check_active()?;

		TransactionInterceptor::pre_commit(self)?;

		if let Some(versioned) = self.versioned.take() {
			let id = versioned.id();
			self.state = TransactionState::Committed;

			let changes = std::mem::take(&mut self.changes);

			let version = versioned.commit()?;
			TransactionInterceptor::post_commit(
				self, id, version, changes,
			)?;

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
}

impl<T: Transaction> VersionedQueryTransaction
	for StandardCommandTransaction<T>
{
	#[inline]
	fn version(&self) -> Version {
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

impl<T: Transaction> VersionedCommandTransaction
	for StandardCommandTransaction<T>
{
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
	fn commit(mut self) -> crate::Result<Version> {
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
	) -> crate::Result<Self::UnversionedQuery<'_>> {
		self.check_active()?;
		self.unversioned.begin_query()
	}

	fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>> {
		self.check_active()?;
		self.cdc.begin_query()
	}
}

impl<T: Transaction> CommandTransaction for StandardCommandTransaction<T> {
	type UnversionedCommand<'a> =
		<T::Unversioned as UnversionedTransaction>::Command<'a>;

	fn begin_unversioned_command(
		&self,
	) -> crate::Result<Self::UnversionedCommand<'_>> {
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

// Implement CatalogTransactionContext to provide catalog-specific state
impl<T: Transaction> CatalogTransactionContext
	for StandardCommandTransaction<T>
{
	fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}

	fn version(&self) -> Version {
		self.versioned.as_ref().unwrap().version()
	}

	fn track_schema_created(
		&mut self,
		schema: reifydb_core::interface::SchemaDef,
	) -> crate::Result<()> {
		use reifydb_core::{
			diagnostic::catalog::schema_already_pending_in_transaction,
			interface::Change,
		};

		if self.changes.schema_def.contains_key(&schema.id) {
			return_error!(schema_already_pending_in_transaction(
				&schema.name
			));
		}

		self.changes.change_schema_def(
			schema.id,
			Change {
				pre: None,
				post: Some(schema),
				op: Create,
			},
		);

		Ok(())
	}

	fn track_schema_updated(
		&mut self,
		pre: reifydb_core::interface::SchemaDef,
		post: reifydb_core::interface::SchemaDef,
	) -> crate::Result<()> {
		use reifydb_core::{
			diagnostic::catalog::cannot_update_deleted_schema,
			interface::{Change, OperationType},
		};

		match self.changes.schema_def.get_mut(&post.id) {
			Some(existing) if existing.op == Create => {
				// Coalesce with create - just update the "post"
				// state
				existing.post = Some(post);
				Ok(())
			}
			Some(existing)
				if existing.op == OperationType::Update =>
			{
				// Coalesce multiple updates - keep original
				// "pre", update "post"
				existing.post = Some(post);
				Ok(())
			}
			Some(_) => {
				return_error!(cannot_update_deleted_schema(
					&post.name
				));
			}
			None => {
				self.changes.change_schema_def(
					post.id,
					Change {
						pre: Some(pre),
						post: Some(post),
						op: OperationType::Update,
					},
				);
				Ok(())
			}
		}
	}

	fn track_schema_deleted(
		&mut self,
		schema: reifydb_core::interface::SchemaDef,
	) -> crate::Result<()> {
		use reifydb_core::{
			diagnostic::catalog::cannot_delete_already_deleted_schema,
			interface::{Change, Operation, OperationType},
		};

		match self.changes.schema_def.get_mut(&schema.id) {
			Some(existing) if existing.op == Create => {
				// Created and deleted in same transaction -
				// remove entirely
				self.changes.schema_def.remove(&schema.id);
				// Remove from operation log
				self.changes.log.retain(
					|op| !matches!(op, Operation::Schema { id, .. } if *id == schema.id),
				);
				Ok(())
			}
			Some(existing)
				if existing.op == OperationType::Update =>
			{
				// Convert update to delete, keep original pre
				// state
				existing.post = None;
				existing.op = Delete;
				// Update operation log
				if let Some(op) =
					self.changes.log.iter_mut().rev().find(
						|op| matches!(op, Operation::Schema { id, .. } if *id == schema.id),
					) {
					*op = Operation::Schema {
						id: schema.id,
						op: Delete,
					};
				}
				Ok(())
			}
			Some(_) => {
				return_error!(
					cannot_delete_already_deleted_schema(
						&schema.name
					)
				);
			}
			None => {
				self.changes.change_schema_def(
					schema.id,
					Change {
						pre: Some(schema.clone()),
						post: None,
						op: Delete,
					},
				);
				Ok(())
			}
		}
	}

	fn track_table_created(
		&mut self,
		table: TableDef,
	) -> crate::Result<()> {
		use reifydb_core::{
			diagnostic::catalog::table_already_pending_in_transaction,
			interface::Change,
		};

		if self.changes.table_def.contains_key(&table.id) {
			// Get schema name for error message
			let schema_name = self
				.changes
				.schema_def
				.get(&table.schema)
				.and_then(|c| {
					c.post.as_ref().or(c.pre.as_ref())
				})
				.map(|s| s.name.clone())
				.or_else(|| {
					self.catalog
						.find_schema(
							table.schema,
							CatalogTransactionContext::version(self),
						)
						.map(|s| s.name)
				})
				.unwrap_or_else(|| {
					format!("schema_{}", table.schema)
				});

			return_error!(table_already_pending_in_transaction(
				&schema_name,
				&table.name
			));
		}

		self.changes.change_table_def(
			table.id,
			Change {
				pre: None,
				post: Some(table),
				op: Create,
			},
		);

		Ok(())
	}

	fn track_table_updated(
		&mut self,
		pre: TableDef,
		post: TableDef,
	) -> crate::Result<()> {
		use reifydb_core::{
			diagnostic::catalog::cannot_update_deleted_table,
			interface::{Change, OperationType},
		};

		match self.changes.table_def.get_mut(&post.id) {
			Some(existing) if existing.op == Create => {
				existing.post = Some(post);
				Ok(())
			}
			Some(existing)
				if existing.op == OperationType::Update =>
			{
				existing.post = Some(post);
				Ok(())
			}
			Some(_) => {
				// Get schema name for error message
				let schema_name = self
					.catalog
					.find_schema(
						post.schema,
						CatalogTransactionContext::version(self),
					)
					.map(|s| s.name)
					.unwrap_or_else(|| {
						format!(
							"schema_{}",
							post.schema
						)
					});

				return_error!(cannot_update_deleted_table(
					&schema_name,
					&post.name
				));
			}
			None => {
				self.changes.change_table_def(
					post.id,
					Change {
						pre: Some(pre),
						post: Some(post),
						op: OperationType::Update,
					},
				);
				Ok(())
			}
		}
	}

	fn track_table_deleted(
		&mut self,
		table: TableDef,
	) -> crate::Result<()> {
		match self.changes.table_def.get_mut(&table.id) {
			Some(existing) if existing.op == Create => {
				self.changes.table_def.remove(&table.id);
				self.changes.log.retain(
					|op| !matches!(op, Table { id, .. } if *id == table.id),
				);
				Ok(())
			}
			Some(existing)
				if existing.op == OperationType::Update =>
			{
				existing.post = None;
				existing.op = Delete;
				if let Some(op) =
					self.changes.log.iter_mut().rev().find(
						|op| matches!(op, Table { id, .. } if *id == table.id),
					) {
					*op = Table {
						id: table.id,
						op: Delete,
					};
				}
				Ok(())
			}
			Some(_) => {
				// Get schema name for error message
				let schema_name = self
					.catalog
					.find_schema(
						table.schema,
						CatalogTransactionContext::version(self),
					)
					.map(|s| s.name)
					.unwrap_or_else(|| {
						format!(
							"schema_{}",
							table.schema
						)
					});

				return_error!(
					cannot_delete_already_deleted_table(
						&schema_name,
						&table.name
					)
				);
			}
			None => {
				self.changes.change_table_def(
					table.id,
					Change {
						pre: Some(table.clone()),
						post: None,
						op: Delete,
					},
				);
				Ok(())
			}
		}
	}

	fn track_view_created(&mut self, view: ViewDef) -> crate::Result<()> {
		use reifydb_core::{
			diagnostic::catalog::view_already_pending_in_transaction,
			interface::Change,
		};

		if self.changes.view_def.contains_key(&view.id) {
			// Get schema name for error message
			let schema_name = self
				.catalog
				.find_schema(
					view.schema,
					CatalogTransactionContext::version(
						self,
					),
				)
				.map(|s| s.name)
				.unwrap_or_else(|| {
					format!("schema_{}", view.schema)
				});

			return_error!(view_already_pending_in_transaction(
				&schema_name,
				&view.name
			));
		}

		self.changes.change_view_def(
			view.id,
			Change {
				pre: None,
				post: Some(view),
				op: Create,
			},
		);

		Ok(())
	}

	fn track_view_updated(
		&mut self,
		pre: ViewDef,
		post: ViewDef,
	) -> crate::Result<()> {
		use reifydb_core::{
			diagnostic::catalog::cannot_update_deleted_view,
			interface::{Change, OperationType},
		};

		match self.changes.view_def.get_mut(&post.id) {
			Some(existing) if existing.op == Create => {
				existing.post = Some(post);
				Ok(())
			}
			Some(existing)
				if existing.op == OperationType::Update =>
			{
				existing.post = Some(post);
				Ok(())
			}
			Some(_) => {
				// Get schema name for error message
				let schema_name = self
					.catalog
					.find_schema(
						post.schema,
						CatalogTransactionContext::version(self),
					)
					.map(|s| s.name)
					.unwrap_or_else(|| {
						format!(
							"schema_{}",
							post.schema
						)
					});

				return_error!(cannot_update_deleted_view(
					&schema_name,
					&post.name
				));
			}
			None => {
				self.changes.change_view_def(
					post.id,
					Change {
						pre: Some(pre),
						post: Some(post),
						op: OperationType::Update,
					},
				);
				Ok(())
			}
		}
	}

	fn track_view_deleted(&mut self, view: ViewDef) -> crate::Result<()> {
		match self.changes.view_def.get_mut(&view.id) {
			Some(existing) if existing.op == Create => {
				self.changes.view_def.remove(&view.id);
				self.changes.log.retain(
					|op| !matches!(op, Operation::View { id, .. } if *id == view.id),
				);
				Ok(())
			}
			Some(existing)
				if existing.op == OperationType::Update =>
			{
				existing.post = None;
				existing.op = Delete;
				if let Some(op) =
					self.changes.log.iter_mut().rev().find(
						|op| matches!(op, Operation::View { id, .. } if *id == view.id),
					) {
					*op = Operation::View {
						id: view.id,
						op: Delete,
					};
				}
				Ok(())
			}
			Some(_) => {
				// Get schema name for error message
				let schema_name = self
					.catalog
					.find_schema(
						view.schema,
						CatalogTransactionContext::version(self),
					)
					.map(|s| s.name)
					.unwrap_or_else(|| {
						format!(
							"schema_{}",
							view.schema
						)
					});

				return_error!(
					cannot_delete_already_deleted_view(
						&schema_name,
						&view.name
					)
				);
			}
			None => {
				self.changes.change_view_def(
					view.id,
					Change {
						pre: Some(view.clone()),
						post: None,
						op: Delete,
					},
				);
				Ok(())
			}
		}
	}
}

// Implement the blanket CatalogTransaction trait
impl<T: Transaction> CatalogTransaction for StandardCommandTransaction<T> {}
