// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	encoded::key::{EncodedKey, EncodedKeyRange},
	interface::{
		catalog::{
			authentication::{AuthenticationDef, AuthenticationId},
			dictionary::DictionaryDef,
			flow::{FlowDef, FlowId},
			handler::HandlerDef,
			id::{
				HandlerId, MigrationId, NamespaceId, ProcedureId, RingBufferId, SeriesId, SinkId,
				SourceId, SubscriptionId, TableId, TestId, ViewId,
			},
			identity::{IdentityDef, IdentityRoleDef, RoleDef, RoleId},
			migration::MigrationDef,
			namespace::Namespace,
			policy::{PolicyDef, PolicyId},
			procedure::ProcedureDef,
			ringbuffer::RingBufferDef,
			series::SeriesDef,
			sink::SinkDef,
			source::SourceDef,
			subscription::SubscriptionDef,
			sumtype::SumTypeDef,
			table::TableDef,
			test::TestDef,
			view::ViewDef,
		},
		store::{MultiVersionBatch, MultiVersionRow},
	},
};
use reifydb_type::{
	Result,
	params::Params,
	value::{dictionary::DictionaryId, frame::frame::Frame, identity::IdentityId, sumtype::SumTypeId},
};
use tracing::instrument;

use crate::{
	TransactionId,
	change::{
		TransactionalAuthenticationChanges, TransactionalChanges, TransactionalDictionaryChanges,
		TransactionalFlowChanges, TransactionalHandlerChanges, TransactionalIdentityChanges,
		TransactionalIdentityRoleChanges, TransactionalMigrationChanges, TransactionalNamespaceChanges,
		TransactionalPolicyChanges, TransactionalProcedureChanges, TransactionalRingBufferChanges,
		TransactionalRoleChanges, TransactionalSeriesChanges, TransactionalSinkChanges,
		TransactionalSourceChanges, TransactionalSubscriptionChanges, TransactionalSumTypeChanges,
		TransactionalTableChanges, TransactionalTestChanges, TransactionalViewChanges,
	},
	multi::transaction::read::MultiReadTransaction,
	single::{SingleTransaction, read::SingleReadTransaction},
	transaction::{RqlExecutor, Transaction},
};

/// An active query transaction that holds a multi query transaction
/// and provides query-only access to single storage.
pub struct QueryTransaction {
	pub(crate) multi: MultiReadTransaction,
	pub(crate) single: SingleTransaction,

	/// The identity executing this transaction.
	pub identity: IdentityId,

	/// Optional RQL executor for running RQL within this transaction.
	pub(crate) executor: Option<Arc<dyn RqlExecutor>>,
}

impl QueryTransaction {
	/// Creates a new active query transaction
	#[instrument(name = "transaction::query::new", level = "debug", skip_all)]
	pub fn new(multi: MultiReadTransaction, single: SingleTransaction, identity: IdentityId) -> Self {
		Self {
			multi,
			single,
			identity,
			executor: None,
		}
	}

	/// Set the RQL executor for this transaction.
	pub fn set_executor(&mut self, executor: Arc<dyn RqlExecutor>) {
		self.executor = Some(executor);
	}

	/// Execute RQL within this transaction using the attached executor.
	///
	/// Panics if no `RqlExecutor` has been set on this transaction.
	pub fn rql(&mut self, rql: &str, params: Params) -> Result<Vec<Frame>> {
		let executor = self.executor.clone().expect("RqlExecutor not set");
		executor.rql(&mut Transaction::Query(self), rql, params)
	}

	/// Get the transaction version
	#[inline]
	pub fn version(&self) -> CommitVersion {
		self.multi.version()
	}

	/// Get the transaction ID
	#[inline]
	pub fn id(&self) -> TransactionId {
		self.multi.tm.id()
	}

	/// Get a value by key
	#[inline]
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionRow>> {
		Ok(self.multi.get(key)?.map(|v| v.into_multi_version_row()))
	}

	/// Check if a key exists
	#[inline]
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		self.multi.contains_key(key)
	}

	/// Get a prefix batch
	#[inline]
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.multi.prefix(prefix)
	}

	/// Get a reverse prefix batch
	#[inline]
	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.multi.prefix_rev(prefix)
	}

	/// Read as of version exclusive
	#[inline]
	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<()> {
		self.multi.read_as_of_version_exclusive(version);
		Ok(())
	}

	/// Create a streaming iterator for forward range queries.
	#[inline]
	pub fn range(
		&self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		self.multi.range(range, batch_size)
	}

	/// Create a streaming iterator for reverse range queries.
	#[inline]
	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		self.multi.range_rev(range, batch_size)
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "transaction::query::with_single_query", level = "trace", skip(self, keys, f))]
	pub fn with_single_query<'a, I, F, R>(&self, keys: I, f: F) -> Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut SingleReadTransaction<'_>) -> Result<R> + Send,
		R: Send,
	{
		self.single.with_query(keys, f)
	}

	/// Execute a function with access to the multi query transaction.
	/// This operates within the same transaction context.
	#[instrument(name = "transaction::query::with_multi_query", level = "trace", skip(self, f))]
	pub fn with_multi_query<F, R>(&mut self, f: F) -> Result<R>
	where
		F: FnOnce(&mut MultiReadTransaction) -> Result<R>,
	{
		f(&mut self.multi)
	}

	/// Begin a single-version query transaction for specific keys
	#[instrument(name = "transaction::query::begin_single_query", level = "trace", skip(self, keys))]
	pub fn begin_single_query<'a, I>(&self, keys: I) -> Result<SingleReadTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		self.single.begin_query(keys)
	}
}

// No-op implementations of TransactionalChanges for QueryTransaction.
// Query transactions don't track changes, so all methods return None/false.

impl TransactionalDictionaryChanges for QueryTransaction {
	fn find_dictionary(&self, _id: DictionaryId) -> Option<&DictionaryDef> {
		None
	}

	fn find_dictionary_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&DictionaryDef> {
		None
	}

	fn is_dictionary_deleted(&self, _id: DictionaryId) -> bool {
		false
	}

	fn is_dictionary_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalFlowChanges for QueryTransaction {
	fn find_flow(&self, _id: FlowId) -> Option<&FlowDef> {
		None
	}

	fn find_flow_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&FlowDef> {
		None
	}

	fn is_flow_deleted(&self, _id: FlowId) -> bool {
		false
	}

	fn is_flow_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalNamespaceChanges for QueryTransaction {
	fn find_namespace(&self, _id: NamespaceId) -> Option<&Namespace> {
		None
	}

	fn find_namespace_by_name(&self, _name: &str) -> Option<&Namespace> {
		None
	}

	fn is_namespace_deleted(&self, _id: NamespaceId) -> bool {
		false
	}

	fn is_namespace_deleted_by_name(&self, _name: &str) -> bool {
		false
	}
}

impl TransactionalProcedureChanges for QueryTransaction {
	fn find_procedure(&self, _id: ProcedureId) -> Option<&ProcedureDef> {
		None
	}

	fn find_procedure_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&ProcedureDef> {
		None
	}

	fn is_procedure_deleted(&self, _id: ProcedureId) -> bool {
		false
	}

	fn is_procedure_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalTestChanges for QueryTransaction {
	fn find_test(&self, _id: TestId) -> Option<&TestDef> {
		None
	}

	fn find_test_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&TestDef> {
		None
	}

	fn is_test_deleted(&self, _id: TestId) -> bool {
		false
	}

	fn is_test_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalRingBufferChanges for QueryTransaction {
	fn find_ringbuffer(&self, _id: RingBufferId) -> Option<&RingBufferDef> {
		None
	}

	fn find_ringbuffer_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&RingBufferDef> {
		None
	}

	fn is_ringbuffer_deleted(&self, _id: RingBufferId) -> bool {
		false
	}

	fn is_ringbuffer_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalSeriesChanges for QueryTransaction {
	fn find_series(&self, _id: SeriesId) -> Option<&SeriesDef> {
		None
	}

	fn find_series_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&SeriesDef> {
		None
	}

	fn is_series_deleted(&self, _id: SeriesId) -> bool {
		false
	}

	fn is_series_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalTableChanges for QueryTransaction {
	fn find_table(&self, _id: TableId) -> Option<&TableDef> {
		None
	}

	fn find_table_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&TableDef> {
		None
	}

	fn is_table_deleted(&self, _id: TableId) -> bool {
		false
	}

	fn is_table_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalViewChanges for QueryTransaction {
	fn find_view(&self, _id: ViewId) -> Option<&ViewDef> {
		None
	}

	fn find_view_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&ViewDef> {
		None
	}

	fn is_view_deleted(&self, _id: ViewId) -> bool {
		false
	}

	fn is_view_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalSumTypeChanges for QueryTransaction {
	fn find_sumtype(&self, _id: SumTypeId) -> Option<&SumTypeDef> {
		None
	}

	fn find_sumtype_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&SumTypeDef> {
		None
	}

	fn is_sumtype_deleted(&self, _id: SumTypeId) -> bool {
		false
	}

	fn is_sumtype_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalSubscriptionChanges for QueryTransaction {
	fn find_subscription(&self, _id: SubscriptionId) -> Option<&SubscriptionDef> {
		None
	}

	fn is_subscription_deleted(&self, _id: SubscriptionId) -> bool {
		false
	}
}

impl TransactionalHandlerChanges for QueryTransaction {
	fn find_handler_by_id(&self, _id: HandlerId) -> Option<&HandlerDef> {
		None
	}

	fn find_handler_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&HandlerDef> {
		None
	}

	fn is_handler_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalIdentityChanges for QueryTransaction {
	fn find_identity(&self, _id: IdentityId) -> Option<&IdentityDef> {
		None
	}

	fn find_identity_by_name(&self, _name: &str) -> Option<&IdentityDef> {
		None
	}

	fn is_identity_deleted(&self, _id: IdentityId) -> bool {
		false
	}

	fn is_identity_deleted_by_name(&self, _name: &str) -> bool {
		false
	}
}

impl TransactionalRoleChanges for QueryTransaction {
	fn find_role(&self, _id: RoleId) -> Option<&RoleDef> {
		None
	}

	fn find_role_by_name(&self, _name: &str) -> Option<&RoleDef> {
		None
	}

	fn is_role_deleted(&self, _id: RoleId) -> bool {
		false
	}

	fn is_role_deleted_by_name(&self, _name: &str) -> bool {
		false
	}
}

impl TransactionalIdentityRoleChanges for QueryTransaction {
	fn find_identity_role(&self, _identity: IdentityId, _role: RoleId) -> Option<&IdentityRoleDef> {
		None
	}

	fn is_identity_role_deleted(&self, _identity: IdentityId, _role: RoleId) -> bool {
		false
	}
}

impl TransactionalPolicyChanges for QueryTransaction {
	fn find_policy(&self, _id: PolicyId) -> Option<&PolicyDef> {
		None
	}

	fn find_policy_by_name(&self, _name: &str) -> Option<&PolicyDef> {
		None
	}

	fn is_policy_deleted(&self, _id: PolicyId) -> bool {
		false
	}

	fn is_policy_deleted_by_name(&self, _name: &str) -> bool {
		false
	}
}

impl TransactionalMigrationChanges for QueryTransaction {
	fn find_migration(&self, _id: MigrationId) -> Option<&MigrationDef> {
		None
	}

	fn find_migration_by_name(&self, _name: &str) -> Option<&MigrationDef> {
		None
	}

	fn is_migration_deleted(&self, _id: MigrationId) -> bool {
		false
	}

	fn is_migration_deleted_by_name(&self, _name: &str) -> bool {
		false
	}
}

impl TransactionalAuthenticationChanges for QueryTransaction {
	fn find_authentication(&self, _id: AuthenticationId) -> Option<&AuthenticationDef> {
		None
	}

	fn find_authentication_by_identity_and_method(
		&self,
		_identity: IdentityId,
		_method: &str,
	) -> Option<&AuthenticationDef> {
		None
	}

	fn is_authentication_deleted(&self, _id: AuthenticationId) -> bool {
		false
	}
}

impl TransactionalSourceChanges for QueryTransaction {
	fn find_source(&self, _id: SourceId) -> Option<&SourceDef> {
		None
	}

	fn find_source_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&SourceDef> {
		None
	}

	fn is_source_deleted(&self, _id: SourceId) -> bool {
		false
	}

	fn is_source_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalSinkChanges for QueryTransaction {
	fn find_sink(&self, _id: SinkId) -> Option<&SinkDef> {
		None
	}

	fn find_sink_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&SinkDef> {
		None
	}

	fn is_sink_deleted(&self, _id: SinkId) -> bool {
		false
	}

	fn is_sink_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalChanges for QueryTransaction {}
