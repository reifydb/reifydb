// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_core::{
	common::CommitVersion,
	execution::ExecutionResult,
	interface::{
		catalog::{
			authentication::{Authentication, AuthenticationId},
			binding::Binding,
			column_snapshot::ColumnSnapshot,
			dictionary::Dictionary,
			flow::{Flow, FlowId, OperatorId},
			handler::Handler,
			id::{
				BindingId, ColumnSnapshotId, HandlerId, NamespaceId, ProcedureId, QueueId,
				RelationshipId, RingBufferId, SeriesId, SinkId, SourceId, TableId, TestId, ViewId,
			},
			identity::{
				GrantedRole, Identity, IdentityAttribute, IdentityAttributeId, IdentityAttributeValue,
				Role, RoleId,
			},
			migration::Migration,
			namespace::Namespace,
			policy::{Policy, PolicyId},
			procedure::Procedure,
			queue::Queue,
			relationship::Relationship,
			ringbuffer::RingBuffer,
			series::Series,
			sink::Sink,
			source::Source,
			storage::StorageId,
			sumtype::SumType,
			table::Table,
			test::Test,
			view::View,
		},
		store::{MultiVersionBatch, MultiVersionRow},
	},
	row::{OperatorSettings, RowSettings},
};
use reifydb_value::{
	Result,
	params::Params,
	value::{dictionary::DictionaryId, identity::IdentityId, sumtype::SumTypeId},
};
use tracing::instrument;

use crate::{
	TransactionId,
	change::{
		TransactionalAuthenticationChanges, TransactionalBindingChanges, TransactionalChanges,
		TransactionalColumnSnapshotChanges, TransactionalDictionaryChanges, TransactionalFlowChanges,
		TransactionalGrantedRoleChanges, TransactionalHandlerChanges, TransactionalIdentityAttributeChanges,
		TransactionalIdentityAttributeValueChanges, TransactionalIdentityChanges,
		TransactionalMigrationChanges, TransactionalNamespaceChanges, TransactionalOperatorSettingsChanges,
		TransactionalPolicyChanges, TransactionalProcedureChanges, TransactionalQueueChanges,
		TransactionalRelationshipChanges, TransactionalRingBufferChanges, TransactionalRoleChanges,
		TransactionalRowSettingsChanges, TransactionalSeriesChanges, TransactionalSinkChanges,
		TransactionalSourceChanges, TransactionalSumTypeChanges, TransactionalTableChanges,
		TransactionalTestChanges, TransactionalViewChanges,
	},
	multi::{RangeScope, transaction::read::MultiReadTransaction},
	single::{SingleTransaction, read::SingleReadTransaction},
	transaction::{RqlExecutor, Transaction},
};

pub struct QueryTransaction {
	pub(crate) multi: MultiReadTransaction,
	pub(crate) single: Option<SingleTransaction>,

	pub identity: IdentityId,

	pub(crate) executor: Option<Arc<dyn RqlExecutor>>,
}

impl QueryTransaction {
	#[instrument(name = "transaction::query::new", level = "debug", skip_all)]
	pub fn new(multi: MultiReadTransaction, single: SingleTransaction, identity: IdentityId) -> Self {
		Self {
			multi,
			single: Some(single),
			identity,
			executor: None,
		}
	}

	pub fn set_executor(&mut self, executor: Arc<dyn RqlExecutor>) {
		self.executor = Some(executor);
	}

	pub fn rql(&mut self, rql: &str, params: Params) -> ExecutionResult {
		let executor = self.executor.clone().expect("RqlExecutor not set");
		executor.rql(&mut Transaction::Query(self), rql, params)
	}

	#[inline]
	pub fn version(&self) -> CommitVersion {
		self.multi.version()
	}

	#[inline]
	pub fn id(&self) -> TransactionId {
		self.multi.tm.id()
	}

	#[inline]
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionRow>> {
		Ok(self.multi.get(key)?.map(|v| v.into_multi_version_row()))
	}

	#[inline]
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		self.multi.contains_key(key)
	}

	#[inline]
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.multi.prefix(prefix)
	}

	#[inline]
	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.multi.prefix_rev(prefix)
	}

	#[inline]
	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<()> {
		self.multi.read_as_of_version_exclusive(version);
		Ok(())
	}

	#[inline]
	pub fn range(
		&self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		self.multi.range(range, scope, batch_size)
	}

	#[inline]
	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_> {
		self.multi.range_rev(range, scope, batch_size)
	}

	#[instrument(name = "transaction::query::begin_single_query", level = "trace", skip(self, keys))]
	pub fn begin_single_query<'a, I>(&self, keys: I) -> Result<SingleReadTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		self.single.as_ref().expect("single not available in read-only query context").begin_query(keys)
	}
}

impl TransactionalDictionaryChanges for QueryTransaction {
	fn find_dictionary(&self, _id: DictionaryId) -> Option<&Dictionary> {
		None
	}

	fn find_dictionary_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&Dictionary> {
		None
	}

	fn is_dictionary_deleted(&self, _id: DictionaryId) -> bool {
		false
	}

	fn is_dictionary_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalColumnSnapshotChanges for QueryTransaction {
	fn find_column_snapshot(&self, _id: ColumnSnapshotId) -> Option<&ColumnSnapshot> {
		None
	}

	fn is_column_snapshot_deleted(&self, _id: ColumnSnapshotId) -> bool {
		false
	}
}

impl TransactionalFlowChanges for QueryTransaction {
	fn find_flow(&self, _id: FlowId) -> Option<&Flow> {
		None
	}

	fn find_flow_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&Flow> {
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
	fn find_procedure(&self, _id: ProcedureId) -> Option<&Procedure> {
		None
	}

	fn find_procedure_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&Procedure> {
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
	fn find_test(&self, _id: TestId) -> Option<&Test> {
		None
	}

	fn find_test_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&Test> {
		None
	}

	fn is_test_deleted(&self, _id: TestId) -> bool {
		false
	}

	fn is_test_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalQueueChanges for QueryTransaction {
	fn find_queue(&self, _id: QueueId) -> Option<&Queue> {
		None
	}

	fn find_queue_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&Queue> {
		None
	}

	fn is_queue_deleted(&self, _id: QueueId) -> bool {
		false
	}

	fn is_queue_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalRingBufferChanges for QueryTransaction {
	fn find_ringbuffer(&self, _id: RingBufferId) -> Option<&RingBuffer> {
		None
	}

	fn find_ringbuffer_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&RingBuffer> {
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
	fn find_series(&self, _id: SeriesId) -> Option<&Series> {
		None
	}

	fn find_series_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&Series> {
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
	fn find_table(&self, _id: TableId) -> Option<&Table> {
		None
	}

	fn find_table_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&Table> {
		None
	}

	fn is_table_deleted(&self, _id: TableId) -> bool {
		false
	}

	fn is_table_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalRelationshipChanges for QueryTransaction {
	fn find_relationship(&self, _id: RelationshipId) -> Option<&Relationship> {
		None
	}

	fn find_relationship_by_name(
		&self,
		_namespace: NamespaceId,
		_source_table: TableId,
		_name: &str,
	) -> Option<&Relationship> {
		None
	}

	fn is_relationship_deleted(&self, _id: RelationshipId) -> bool {
		false
	}

	fn is_relationship_deleted_by_name(
		&self,
		_namespace: NamespaceId,
		_source_table: TableId,
		_name: &str,
	) -> bool {
		false
	}
}

impl TransactionalViewChanges for QueryTransaction {
	fn find_view(&self, _id: ViewId) -> Option<&View> {
		None
	}

	fn find_view_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&View> {
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
	fn find_sumtype(&self, _id: SumTypeId) -> Option<&SumType> {
		None
	}

	fn find_sumtype_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&SumType> {
		None
	}

	fn is_sumtype_deleted(&self, _id: SumTypeId) -> bool {
		false
	}

	fn is_sumtype_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalHandlerChanges for QueryTransaction {
	fn find_handler_by_id(&self, _id: HandlerId) -> Option<&Handler> {
		None
	}

	fn find_handler_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&Handler> {
		None
	}

	fn is_handler_deleted(&self, _id: HandlerId) -> bool {
		false
	}

	fn is_handler_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalIdentityChanges for QueryTransaction {
	fn find_identity(&self, _id: IdentityId) -> Option<&Identity> {
		None
	}

	fn find_identity_by_name(&self, _name: &str) -> Option<&Identity> {
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
	fn find_role(&self, _id: RoleId) -> Option<&Role> {
		None
	}

	fn find_role_by_name(&self, _name: &str) -> Option<&Role> {
		None
	}

	fn is_role_deleted(&self, _id: RoleId) -> bool {
		false
	}

	fn is_role_deleted_by_name(&self, _name: &str) -> bool {
		false
	}
}

impl TransactionalGrantedRoleChanges for QueryTransaction {
	fn find_granted_roles_for_identity(&self, _identity: IdentityId) -> Vec<&GrantedRole> {
		Vec::new()
	}

	fn is_granted_role_deleted(&self, _identity: IdentityId, _role: RoleId) -> bool {
		false
	}
}

impl TransactionalIdentityAttributeChanges for QueryTransaction {
	fn find_identity_attribute(&self, _id: IdentityAttributeId) -> Option<&IdentityAttribute> {
		None
	}

	fn find_identity_attribute_by_name(&self, _name: &str) -> Option<&IdentityAttribute> {
		None
	}

	fn is_identity_attribute_deleted(&self, _id: IdentityAttributeId) -> bool {
		false
	}

	fn is_identity_attribute_deleted_by_name(&self, _name: &str) -> bool {
		false
	}
}

impl TransactionalIdentityAttributeValueChanges for QueryTransaction {
	fn find_identity_attribute_value(
		&self,
		_identity: IdentityId,
		_attribute: IdentityAttributeId,
	) -> Option<&IdentityAttributeValue> {
		None
	}

	fn find_identity_attribute_values_for_identity(&self, _identity: IdentityId) -> Vec<&IdentityAttributeValue> {
		Vec::new()
	}

	fn find_identity_attribute_values_for_attribute(
		&self,
		_attribute: IdentityAttributeId,
	) -> Vec<&IdentityAttributeValue> {
		Vec::new()
	}

	fn is_identity_attribute_value_deleted(&self, _identity: IdentityId, _attribute: IdentityAttributeId) -> bool {
		false
	}
}

impl TransactionalPolicyChanges for QueryTransaction {
	fn find_policy(&self, _id: PolicyId) -> Option<&Policy> {
		None
	}

	fn find_policy_by_name(&self, _name: &str) -> Option<&Policy> {
		None
	}

	fn is_policy_deleted_by_name(&self, _name: &str) -> bool {
		false
	}
}

impl TransactionalMigrationChanges for QueryTransaction {
	fn find_migration_by_name(&self, _name: &str) -> Option<&Migration> {
		None
	}
}

impl TransactionalAuthenticationChanges for QueryTransaction {
	fn find_authentication(&self, _id: AuthenticationId) -> Option<&Authentication> {
		None
	}

	fn find_authentication_by_identity_and_method(
		&self,
		_identity: IdentityId,
		_method: &str,
	) -> Option<&Authentication> {
		None
	}

	fn is_authentication_deleted(&self, _id: AuthenticationId) -> bool {
		false
	}

	fn is_authentication_deleted_by_identity_and_method(&self, _identity: IdentityId, _method: &str) -> bool {
		false
	}
}

impl TransactionalSourceChanges for QueryTransaction {
	fn find_source(&self, _id: SourceId) -> Option<&Source> {
		None
	}

	fn find_source_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&Source> {
		None
	}

	fn is_source_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalSinkChanges for QueryTransaction {
	fn find_sink(&self, _id: SinkId) -> Option<&Sink> {
		None
	}

	fn find_sink_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&Sink> {
		None
	}

	fn is_sink_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalRowSettingsChanges for QueryTransaction {
	fn find_row_settings(&self, _storage: StorageId) -> Option<&RowSettings> {
		None
	}
}

impl TransactionalOperatorSettingsChanges for QueryTransaction {
	fn find_operator_settings(&self, _operator: OperatorId) -> Option<&OperatorSettings> {
		None
	}
}

impl TransactionalBindingChanges for QueryTransaction {
	fn find_binding(&self, _id: BindingId) -> Option<&Binding> {
		None
	}

	fn find_binding_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&Binding> {
		None
	}

	fn is_binding_deleted(&self, _id: BindingId) -> bool {
		false
	}

	fn is_binding_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalChanges for QueryTransaction {}
