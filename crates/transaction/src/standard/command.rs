// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
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
		CdcTransaction, CommandTransaction, MultiVersionBatch, MultiVersionTransaction, MultiVersionValues,
		QueryTransaction, RowChange, SingleVersionTransaction, TransactionId, TransactionalChanges,
		TransactionalDefChanges, WithEventBus,
		interceptor::{TransactionInterceptor, WithInterceptors},
	},
	return_error,
	value::encoded::EncodedValues,
};
use reifydb_type::Result;
use tracing::instrument;

use crate::{
	cdc::TransactionCdc,
	multi::{TransactionMultiVersion, pending::PendingWrites},
	single::TransactionSingle,
	standard::query::StandardQueryTransaction,
};

/// An active command transaction that holds a multi command transaction
/// and provides query/command access to single storage.
///
/// The transaction will auto-rollback on drop if not explicitly committed.
pub struct StandardCommandTransaction {
	pub multi: TransactionMultiVersion,
	pub single: TransactionSingle,
	pub cdc: TransactionCdc,
	state: TransactionState,

	pub cmd: Option<<TransactionMultiVersion as MultiVersionTransaction>::Command>,
	pub event_bus: EventBus,
	pub changes: TransactionalDefChanges,

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
	#[instrument(name = "transaction::standard::command::new", level = "debug", skip_all)]
	pub async fn new(
		multi: TransactionMultiVersion,
		single: TransactionSingle,
		cdc: TransactionCdc,
		event_bus: EventBus,
		interceptors: Interceptors<Self>,
	) -> Result<Self> {
		let cmd = multi.begin_command().await?;
		let txn_id = cmd.id();
		Ok(Self {
			cmd: Some(cmd),
			multi,
			single,
			cdc,
			state: TransactionState::Active,
			event_bus,
			interceptors,
			changes: TransactionalDefChanges::new(txn_id),
			row_changes: Vec::new(),
		})
	}

	#[instrument(name = "transaction::standard::command::event_bus", level = "trace", skip(self))]
	pub fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}

	/// Check if transaction is still active and return appropriate error if
	/// not
	fn check_active(&self) -> Result<()> {
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
	#[instrument(name = "transaction::standard::command::commit", level = "debug", skip(self))]
	pub async fn commit(&mut self) -> Result<CommitVersion> {
		self.check_active()?;

		TransactionInterceptor::pre_commit(self).await?;

		if let Some(mut multi) = self.cmd.take() {
			let id = multi.id();
			self.state = TransactionState::Committed;

			let changes = std::mem::take(&mut self.changes);
			let row_changes = std::mem::take(&mut self.row_changes);

			let version = multi.commit().await?;
			TransactionInterceptor::post_commit(self, id, version, changes, row_changes).await?;

			Ok(version)
		} else {
			// This should never happen due to check_active
			unreachable!("Transaction state inconsistency")
		}
	}

	/// Rollback the transaction.
	#[instrument(name = "transaction::standard::command::rollback", level = "debug", skip(self))]
	pub fn rollback(&mut self) -> Result<()> {
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
	#[instrument(name = "transaction::standard::command::cdc", level = "trace", skip(self))]
	pub fn cdc(&self) -> &TransactionCdc {
		&self.cdc
	}

	/// Get access to the pending writes in this transaction
	///
	/// This allows checking for key conflicts when committing FlowTransactions
	/// to ensure they operate on non-overlapping keyspaces.
	#[instrument(name = "transaction::standard::command::pending_writes", level = "trace", skip(self))]
	pub fn pending_writes(&self) -> &PendingWrites {
		self.cmd.as_ref().unwrap().pending_writes()
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "transaction::standard::command::with_single_query", level = "trace", skip(self, keys, f))]
	pub async fn with_single_query<'a, I, F, R>(&self, keys: I, f: F) -> Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut <TransactionSingle as SingleVersionTransaction>::Query<'_>) -> Result<R> + Send,
		R: Send,
	{
		self.check_active()?;
		self.single.with_query(keys, f).await
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(
		name = "transaction::standard::command::with_single_command",
		level = "trace",
		skip(self, keys, f)
	)]
	pub async fn with_single_command<'a, I, F, R>(&self, keys: I, f: F) -> Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut <TransactionSingle as SingleVersionTransaction>::Command<'_>) -> Result<R> + Send,
		R: Send,
	{
		self.check_active()?;
		self.single.with_command(keys, f).await
	}

	/// Execute a function with a query transaction view.
	/// This creates a new query transaction using the stored multi-version storage.
	/// The query transaction will operate independently but share the same single/CDC storage.
	#[instrument(name = "transaction::standard::command::with_multi_query", level = "trace", skip(self, f))]
	pub async fn with_multi_query<F, R>(&self, f: F) -> Result<R>
	where
		F: FnOnce(&mut StandardQueryTransaction) -> Result<R>,
	{
		self.check_active()?;

		let mut query_txn = StandardQueryTransaction::new(
			self.multi.begin_query().await?,
			self.single.clone(),
			self.cdc.clone(),
		);

		f(&mut query_txn)
	}

	#[instrument(
		name = "transaction::standard::command::with_multi_query_as_of_exclusive",
		level = "trace",
		skip(self, f)
	)]
	pub async fn with_multi_query_as_of_exclusive<F, R>(&self, version: CommitVersion, f: F) -> Result<R>
	where
		F: FnOnce(&mut StandardQueryTransaction) -> Result<R>,
	{
		self.check_active()?;

		let mut query_txn = StandardQueryTransaction::new(
			self.multi.begin_query().await?,
			self.single.clone(),
			self.cdc.clone(),
		);

		query_txn.read_as_of_version_exclusive(version).await?;

		f(&mut query_txn)
	}

	#[instrument(
		name = "transaction::standard::command::with_multi_query_as_of_inclusive",
		level = "trace",
		skip(self, f)
	)]
	pub async fn with_multi_query_as_of_inclusive<F, R>(&self, version: CommitVersion, f: F) -> Result<R>
	where
		F: FnOnce(&mut StandardQueryTransaction) -> Result<R>,
	{
		self.check_active()?;

		let mut query_txn = StandardQueryTransaction::new(
			self.multi.begin_query().await?,
			self.single.clone(),
			self.cdc.clone(),
		);

		query_txn.read_as_of_version_inclusive(version).await?;

		f(&mut query_txn)
	}

	/// Begin a single-version query transaction for specific keys
	#[instrument(name = "transaction::standard::command::begin_single_query", level = "trace", skip(self, keys))]
	pub async fn begin_single_query<'a, I>(
		&self,
		keys: I,
	) -> Result<<TransactionSingle as SingleVersionTransaction>::Query<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		self.check_active()?;
		self.single.begin_query(keys).await
	}

	/// Begin a CDC query transaction
	#[instrument(name = "transaction::standard::command::begin_cdc_query", level = "trace", skip(self))]
	pub async fn begin_cdc_query(&self) -> Result<<TransactionCdc as CdcTransaction>::Query<'_>> {
		self.check_active()?;
		Ok(self.cdc.begin_query()?)
	}

	/// Begin a single-version command transaction for specific keys
	#[instrument(name = "transaction::standard::command::begin_single_command", level = "trace", skip(self, keys))]
	pub async fn begin_single_command<'a, I>(
		&self,
		keys: I,
	) -> Result<<TransactionSingle as SingleVersionTransaction>::Command<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		self.check_active()?;
		self.single.begin_command(keys).await
	}

	/// Get reference to catalog changes for this transaction
	pub fn get_changes(&self) -> &TransactionalDefChanges {
		&self.changes
	}

	/// Track a row change for post-commit event emission
	pub fn track_row_change(&mut self, change: RowChange) {
		self.row_changes.push(change);
	}
}

#[async_trait]
impl QueryTransaction for StandardCommandTransaction {
	type SingleVersionQuery<'a> = <TransactionSingle as SingleVersionTransaction>::Query<'a>;
	type CdcQuery<'a> = <TransactionCdc as CdcTransaction>::Query<'a>;

	#[inline]
	fn version(&self) -> CommitVersion {
		QueryTransaction::version(self.cmd.as_ref().unwrap())
	}

	#[inline]
	fn id(&self) -> TransactionId {
		QueryTransaction::id(self.cmd.as_ref().unwrap())
	}

	#[inline]
	async fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>> {
		self.check_active()?;
		QueryTransaction::get(self.cmd.as_mut().unwrap(), key).await
	}

	#[inline]
	async fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		self.check_active()?;
		QueryTransaction::contains_key(self.cmd.as_mut().unwrap(), key).await
	}

	#[inline]
	async fn range_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> Result<MultiVersionBatch> {
		self.check_active()?;
		QueryTransaction::range_batch(self.cmd.as_mut().unwrap(), range, batch_size).await
	}

	#[inline]
	async fn range_rev_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> Result<MultiVersionBatch> {
		self.check_active()?;
		QueryTransaction::range_rev_batch(self.cmd.as_mut().unwrap(), range, batch_size).await
	}

	#[inline]
	async fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<()> {
		self.check_active()?;
		QueryTransaction::read_as_of_version_exclusive(self.cmd.as_mut().unwrap(), version).await
	}

	async fn begin_single_query<'a, I>(&self, keys: I) -> Result<Self::SingleVersionQuery<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		self.check_active()?;
		self.single.begin_query(keys).await
	}

	async fn begin_cdc_query(&self) -> Result<Self::CdcQuery<'_>> {
		self.check_active()?;
		Ok(self.cdc.begin_query()?)
	}
}

#[async_trait]
impl CommandTransaction for StandardCommandTransaction {
	type SingleVersionCommand<'a> = <TransactionSingle as SingleVersionTransaction>::Command<'a>;

	#[inline]
	async fn set(&mut self, key: &EncodedKey, row: EncodedValues) -> Result<()> {
		self.check_active()?;
		CommandTransaction::set(self.cmd.as_mut().unwrap(), key, row).await
	}

	#[inline]
	async fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.check_active()?;
		CommandTransaction::remove(self.cmd.as_mut().unwrap(), key).await
	}

	#[inline]
	async fn commit(&mut self) -> Result<CommitVersion> {
		self.check_active()?;
		let result = CommandTransaction::commit(self.cmd.as_mut().unwrap()).await;
		if result.is_ok() {
			self.state = TransactionState::Committed;
		}
		result
	}

	#[inline]
	async fn rollback(&mut self) -> Result<()> {
		self.check_active()?;
		let result = CommandTransaction::rollback(self.cmd.as_mut().unwrap()).await;
		if result.is_ok() {
			self.state = TransactionState::RolledBack;
		}
		result
	}

	async fn begin_single_command<'a, I>(&self, keys: I) -> Result<Self::SingleVersionCommand<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		self.check_active()?;
		self.single.begin_command(keys).await
	}
}

impl WithEventBus for StandardCommandTransaction {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
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
			// Auto-rollback if still active (not committed or rolled back)
			if self.state == TransactionState::Active {
				let _ = multi.rollback();
			}
		}
	}
}
