// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_catalog::{MaterializedCatalog, transaction::MaterializedCatalogTransaction};
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	diagnostic::transaction,
	event::EventBus,
	interceptor,
	interceptor::{
		Chain, Interceptors, PostCommitInterceptor, PreCommitInterceptor, RingBufferPostDeleteInterceptor,
		RingBufferPostInsertInterceptor, RingBufferPostUpdateInterceptor, RingBufferPreDeleteInterceptor,
		RingBufferPreInsertInterceptor, RingBufferPreUpdateInterceptor, TablePostDeleteInterceptor,
		TablePostInsertInterceptor, TablePreDeleteInterceptor, TablePreInsertInterceptor,
		TablePreUpdateInterceptor,
	},
	interface::{
		CdcTransaction, CommandTransaction, MultiVersionBatch, MultiVersionCommandTransaction,
		MultiVersionQueryTransaction, MultiVersionTransaction, MultiVersionValues, QueryTransaction, RowChange,
		SingleVersionTransaction, TransactionId, TransactionalChanges, TransactionalDefChanges, WithEventBus,
		interceptor::{TransactionInterceptor, WithInterceptors},
	},
	return_error,
	value::encoded::EncodedValues,
};
use reifydb_transaction::{
	cdc::TransactionCdc,
	multi::{TransactionMultiVersion, pending::PendingWrites},
	single::TransactionSingle,
};
use tracing::instrument;

use crate::{transaction::query::StandardQueryTransaction, util::block_on};

/// An active command transaction that holds a multi command transaction
/// and provides query/command access to single storage.
///
/// The transaction will auto-rollback on drop if not explicitly committed.
pub struct StandardCommandTransaction {
	pub multi: TransactionMultiVersion,
	pub single: TransactionSingle,
	pub(crate) cdc: TransactionCdc,
	state: TransactionState,

	pub(crate) cmd: Option<<TransactionMultiVersion as MultiVersionTransaction>::Command>,
	pub(crate) event_bus: EventBus,
	pub(crate) changes: TransactionalDefChanges,
	pub(crate) catalog: MaterializedCatalog,

	// Track row changes for post-commit events
	pub(crate) row_changes: Vec<RowChange>,

	pub(crate) interceptors: Interceptors<Self>,
}

#[derive(Clone, Copy, PartialEq)]
enum TransactionState {
	Active,
	Committed,
	RolledBack,
}

impl StandardCommandTransaction {
	/// Creates a new active command transaction with a pre-commit callback
	#[instrument(name = "engine::transaction::command::new", level = "debug", skip_all)]
	pub async fn new(
		multi: TransactionMultiVersion,
		single: TransactionSingle,
		cdc: TransactionCdc,
		event_bus: EventBus,
		catalog: MaterializedCatalog,
		interceptors: Interceptors<Self>,
	) -> reifydb_core::Result<Self> {
		let cmd = multi.begin_command().await?;
		let txn_id = cmd.id();
		Ok(Self {
			cmd: Some(cmd),
			multi,
			single,
			cdc,
			state: TransactionState::Active,
			event_bus,
			catalog,
			interceptors,
			changes: TransactionalDefChanges::new(txn_id),
			row_changes: Vec::new(),
		})
	}

	#[instrument(name = "engine::transaction::command::event_bus", level = "trace", skip(self))]
	pub fn event_bus(&self) -> &EventBus {
		&self.event_bus
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

	/// Commit the transaction.
	/// Since single transactions are short-lived and auto-commit,
	/// this only commits the multi transaction.
	#[instrument(name = "engine::transaction::command::commit", level = "debug", skip(self))]
	pub fn commit(&mut self) -> crate::Result<CommitVersion> {
		self.check_active()?;

		block_on(TransactionInterceptor::pre_commit(self))?;

		if let Some(mut multi) = self.cmd.take() {
			let id = multi.id();
			self.state = TransactionState::Committed;

			let changes = std::mem::take(&mut self.changes);
			let row_changes = std::mem::take(&mut self.row_changes);

			let version = block_on(multi.commit())?;
			block_on(TransactionInterceptor::post_commit(self, id, version, changes, row_changes))?;

			Ok(version)
		} else {
			// This should never happen due to check_active
			unreachable!("Transaction state inconsistency")
		}
	}

	/// Rollback the transaction.
	#[instrument(name = "engine::transaction::command::rollback", level = "debug", skip(self))]
	pub fn rollback(&mut self) -> crate::Result<()> {
		self.check_active()?;
		if let Some(mut multi) = self.cmd.take() {
			self.state = TransactionState::RolledBack;
			multi.rollback()
		} else {
			// This should never happen due to check_active
			unreachable!("Transaction state inconsistency")
		}
	}

	/// Get access to the CDC transaction interface
	#[instrument(name = "engine::transaction::command::cdc", level = "trace", skip(self))]
	pub fn cdc(&self) -> &TransactionCdc {
		&self.cdc
	}

	/// Get access to the pending writes in this transaction
	///
	/// This allows checking for key conflicts when committing FlowTransactions
	/// to ensure they operate on non-overlapping keyspaces.
	#[instrument(name = "engine::transaction::command::pending_writes", level = "trace", skip(self))]
	pub fn pending_writes(&self) -> &PendingWrites {
		self.cmd.as_ref().unwrap().pending_writes()
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "engine::transaction::command::with_single_query", level = "trace", skip(self, keys, f))]
	pub fn with_single_query<'a, I, F, R>(&self, keys: I, f: F) -> crate::Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut <TransactionSingle as SingleVersionTransaction>::Query<'_>) -> crate::Result<R> + Send,
		R: Send,
	{
		self.check_active()?;
		block_on(self.single.with_query(keys, f))
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "engine::transaction::command::with_single_command", level = "trace", skip(self, keys, f))]
	pub fn with_single_command<'a, I, F, R>(&self, keys: I, f: F) -> crate::Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut <TransactionSingle as SingleVersionTransaction>::Command<'_>) -> crate::Result<R> + Send,
		R: Send,
	{
		self.check_active()?;
		block_on(self.single.with_command(keys, f))
	}

	/// Execute a function with a query transaction view.
	/// This creates a new query transaction using the stored multi-version storage.
	/// The query transaction will operate independently but share the same single/CDC storage.
	#[instrument(name = "engine::transaction::command::with_multi_query", level = "trace", skip(self, f))]
	pub async fn with_multi_query<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut StandardQueryTransaction) -> crate::Result<R>,
	{
		self.check_active()?;

		let mut query_txn = StandardQueryTransaction::new(
			self.multi.begin_query().await?,
			self.single.clone(),
			self.cdc.clone(),
			self.catalog.clone(),
		);

		f(&mut query_txn)
	}

	#[instrument(
		name = "engine::transaction::command::with_multi_query_as_of_exclusive",
		level = "trace",
		skip(self, f)
	)]
	pub async fn with_multi_query_as_of_exclusive<F, R>(&self, version: CommitVersion, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut StandardQueryTransaction) -> crate::Result<R>,
	{
		self.check_active()?;

		let mut query_txn = StandardQueryTransaction::new(
			self.multi.begin_query().await?,
			self.single.clone(),
			self.cdc.clone(),
			self.catalog.clone(),
		);

		block_on(query_txn.read_as_of_version_exclusive(version))?;

		f(&mut query_txn)
	}

	#[instrument(
		name = "engine::transaction::command::with_multi_query_as_of_inclusive",
		level = "trace",
		skip(self, f)
	)]
	pub async fn with_multi_query_as_of_inclusive<F, R>(&self, version: CommitVersion, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut StandardQueryTransaction) -> crate::Result<R>,
	{
		self.check_active()?;

		let mut query_txn = StandardQueryTransaction::new(
			self.multi.begin_query().await?,
			self.single.clone(),
			self.cdc.clone(),
			self.catalog.clone(),
		);

		block_on(query_txn.read_as_of_version_inclusive(version))?;

		f(&mut query_txn)
	}
}

impl MaterializedCatalogTransaction for StandardCommandTransaction {
	fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}
}

#[async_trait]
impl MultiVersionQueryTransaction for StandardCommandTransaction {
	#[inline]
	fn version(&self) -> CommitVersion {
		MultiVersionQueryTransaction::version(self.cmd.as_ref().unwrap())
	}

	#[inline]
	fn id(&self) -> TransactionId {
		MultiVersionQueryTransaction::id(self.cmd.as_ref().unwrap())
	}

	#[inline]
	async fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<MultiVersionValues>> {
		self.check_active()?;
		MultiVersionQueryTransaction::get(self.cmd.as_mut().unwrap(), key).await
	}

	#[inline]
	async fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.check_active()?;
		MultiVersionQueryTransaction::contains_key(self.cmd.as_mut().unwrap(), key).await
	}

	#[inline]
	async fn range_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> crate::Result<MultiVersionBatch> {
		self.check_active()?;
		MultiVersionQueryTransaction::range_batch(self.cmd.as_mut().unwrap(), range, batch_size).await
	}

	#[inline]
	async fn range_rev_batch(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch> {
		self.check_active()?;
		MultiVersionQueryTransaction::range_rev_batch(self.cmd.as_mut().unwrap(), range, batch_size).await
	}

	#[inline]
	async fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> crate::Result<()> {
		self.check_active()?;
		MultiVersionQueryTransaction::read_as_of_version_exclusive(self.cmd.as_mut().unwrap(), version).await
	}
}

#[async_trait]
impl MultiVersionCommandTransaction for StandardCommandTransaction {
	#[inline]
	async fn set(&mut self, key: &EncodedKey, row: EncodedValues) -> crate::Result<()> {
		self.check_active()?;
		MultiVersionCommandTransaction::set(self.cmd.as_mut().unwrap(), key, row).await
	}

	#[inline]
	async fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		self.check_active()?;
		MultiVersionCommandTransaction::remove(self.cmd.as_mut().unwrap(), key).await
	}

	#[inline]
	async fn commit(&mut self) -> crate::Result<CommitVersion> {
		self.check_active()?;
		let result = MultiVersionCommandTransaction::commit(self.cmd.as_mut().unwrap()).await;
		if result.is_ok() {
			self.state = TransactionState::Committed;
		}
		result
	}

	#[inline]
	async fn rollback(&mut self) -> crate::Result<()> {
		self.check_active()?;
		let result = MultiVersionCommandTransaction::rollback(self.cmd.as_mut().unwrap()).await;
		if result.is_ok() {
			self.state = TransactionState::RolledBack;
		}
		result
	}
}

impl WithEventBus for StandardCommandTransaction {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

impl QueryTransaction for StandardCommandTransaction {
	type SingleVersionQuery<'a> = <TransactionSingle as SingleVersionTransaction>::Query<'a>;

	type CdcQuery<'a> = <TransactionCdc as CdcTransaction>::Query<'a>;

	fn begin_single_query<'a, I>(&self, keys: I) -> crate::Result<Self::SingleVersionQuery<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		self.check_active()?;
		block_on(self.single.begin_query(keys))
	}

	fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>> {
		self.check_active()?;
		self.cdc.begin_query()
	}
}

impl CommandTransaction for StandardCommandTransaction {
	type SingleVersionCommand<'a> = <TransactionSingle as SingleVersionTransaction>::Command<'a>;

	fn begin_single_command<'a, I>(&self, keys: I) -> crate::Result<Self::SingleVersionCommand<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		self.check_active()?;
		block_on(self.single.begin_command(keys))
	}

	fn get_changes(&self) -> &TransactionalDefChanges {
		&self.changes
	}
}

impl WithInterceptors<StandardCommandTransaction> for StandardCommandTransaction {
	fn table_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn TablePreInsertInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.table_pre_insert
	}

	fn table_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn TablePostInsertInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.table_post_insert
	}

	fn table_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn TablePreUpdateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.table_pre_update
	}

	fn table_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::TablePostUpdateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.table_post_update
	}

	fn table_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn TablePreDeleteInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.table_pre_delete
	}

	fn table_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn TablePostDeleteInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.table_post_delete
	}

	fn ringbuffer_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn RingBufferPreInsertInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.ringbuffer_pre_insert
	}

	fn ringbuffer_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn RingBufferPostInsertInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.ringbuffer_post_insert
	}

	fn ringbuffer_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn RingBufferPreUpdateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.ringbuffer_pre_update
	}

	fn ringbuffer_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn RingBufferPostUpdateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.ringbuffer_post_update
	}

	fn ringbuffer_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn RingBufferPreDeleteInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.ringbuffer_pre_delete
	}

	fn ringbuffer_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn RingBufferPostDeleteInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.ringbuffer_post_delete
	}

	fn pre_commit_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn PreCommitInterceptor<StandardCommandTransaction> + Send + Sync>
	{
		&mut self.interceptors.pre_commit
	}

	fn post_commit_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn PostCommitInterceptor<StandardCommandTransaction> + Send + Sync>
	{
		&mut self.interceptors.post_commit
	}

	// Namespace definition interceptors
	fn namespace_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::NamespaceDefPostCreateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.namespace_def_post_create
	}

	fn namespace_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::NamespaceDefPreUpdateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.namespace_def_pre_update
	}

	fn namespace_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::NamespaceDefPostUpdateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.namespace_def_post_update
	}

	fn namespace_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::NamespaceDefPreDeleteInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.namespace_def_pre_delete
	}

	// Table definition interceptors
	fn table_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::TableDefPostCreateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.table_def_post_create
	}

	fn table_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::TableDefPreUpdateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.table_def_pre_update
	}

	fn table_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::TableDefPostUpdateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.table_def_post_update
	}

	fn table_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::TableDefPreDeleteInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.table_def_pre_delete
	}

	// View definition interceptors
	fn view_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::ViewDefPostCreateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.view_def_post_create
	}

	fn view_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::ViewDefPreUpdateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.view_def_pre_update
	}

	fn view_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::ViewDefPostUpdateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.view_def_post_update
	}

	fn view_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::ViewDefPreDeleteInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.view_def_pre_delete
	}

	// Ring buffer definition interceptors
	fn ringbuffer_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::RingBufferDefPostCreateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.ringbuffer_def_post_create
	}

	fn ringbuffer_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::RingBufferDefPreUpdateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.ringbuffer_def_pre_update
	}

	fn ringbuffer_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::RingBufferDefPostUpdateInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.ringbuffer_def_post_update
	}

	fn ringbuffer_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::RingBufferDefPreDeleteInterceptor<StandardCommandTransaction> + Send + Sync,
	> {
		&mut self.interceptors.ringbuffer_def_pre_delete
	}
}

impl TransactionalChanges for StandardCommandTransaction {}

impl Drop for StandardCommandTransaction {
	fn drop(&mut self) {
		if let Some(mut multi) = self.cmd.take() {
			// Auto-rollback if still active (not committed or
			// rolled back)
			if self.state == TransactionState::Active {
				let _ = multi.rollback();
			}
		}
	}
}
