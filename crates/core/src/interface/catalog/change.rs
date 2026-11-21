// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::error::ValueError;

use crate::{
	interface::catalog::{
		authentication::Authentication,
		authentication_method::AuthenticationMethod,
		binding::Binding,
		column_block::ColumnBlock,
		config::Config,
		dictionary::Dictionary,
		flow::FlowEntry,
		handler::Handler,
		identity::{GrantedRole, Identity, IdentityAttribute, IdentityAttributeValue, Role},
		key::PrimaryKey,
		migration::{Migration, MigrationEvent},
		namespace::Namespace,
		object::ObjectId,
		policy::Policy,
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
		windowed_view_group::WindowedViewGroup,
	},
	row::RowSettings,
};

pub trait CatalogTrackConfigChangeOperations {
	fn track_config_set(&mut self, pre: Config, post: Config) -> Result<(), ValueError>;
}

pub trait CatalogTrackTableChangeOperations {
	fn track_table_created(&mut self, table: Table) -> Result<(), ValueError>;

	fn track_table_updated(&mut self, pre: Table, post: Table) -> Result<(), ValueError>;

	fn track_table_deleted(&mut self, table: Table) -> Result<(), ValueError>;
}

pub trait CatalogTrackRelationshipChangeOperations {
	fn track_relationship_created(&mut self, relationship: Relationship) -> Result<(), ValueError>;

	fn track_relationship_deleted(&mut self, relationship: Relationship) -> Result<(), ValueError>;
}

pub trait CatalogTrackNamespaceChangeOperations {
	fn track_namespace_created(&mut self, namespace: Namespace) -> Result<(), ValueError>;

	fn track_namespace_deleted(&mut self, namespace: Namespace) -> Result<(), ValueError>;
}

pub trait CatalogTrackFlowChangeOperations {
	fn track_flow_created(&mut self, entry: FlowEntry) -> Result<(), ValueError>;

	fn track_flow_updated(&mut self, pre: FlowEntry, post: FlowEntry) -> Result<(), ValueError>;

	fn track_flow_deleted(&mut self, entry: FlowEntry) -> Result<(), ValueError>;
}

pub trait CatalogTrackPrimaryKeyChangeOperations {
	fn track_primary_key_created(&mut self, object: ObjectId, primary_key: PrimaryKey) -> Result<(), ValueError>;

	fn track_primary_key_deleted(&mut self, object: ObjectId, primary_key: PrimaryKey) -> Result<(), ValueError>;
}

pub trait CatalogTrackViewChangeOperations {
	fn track_view_created(&mut self, view: View) -> Result<(), ValueError>;

	fn track_view_deleted(&mut self, view: View) -> Result<(), ValueError>;
}

pub trait CatalogTrackDictionaryChangeOperations {
	fn track_dictionary_created(&mut self, dictionary: Dictionary) -> Result<(), ValueError>;

	fn track_dictionary_deleted(&mut self, dictionary: Dictionary) -> Result<(), ValueError>;
}

pub trait CatalogTrackColumnBlockChangeOperations {
	fn track_column_block_created(&mut self, block: ColumnBlock) -> Result<(), ValueError>;

	fn track_column_block_deleted(&mut self, block: ColumnBlock) -> Result<(), ValueError>;
}

pub trait CatalogTrackSeriesChangeOperations {
	fn track_series_created(&mut self, series: Series) -> Result<(), ValueError>;

	fn track_series_updated(&mut self, pre: Series, post: Series) -> Result<(), ValueError>;

	fn track_series_deleted(&mut self, series: Series) -> Result<(), ValueError>;
}

pub trait CatalogTrackRingBufferChangeOperations {
	fn track_ringbuffer_created(&mut self, ringbuffer: RingBuffer) -> Result<(), ValueError>;

	fn track_ringbuffer_deleted(&mut self, ringbuffer: RingBuffer) -> Result<(), ValueError>;
}

pub trait CatalogTrackQueueChangeOperations {
	fn track_queue_created(&mut self, queue: Queue) -> Result<(), ValueError>;

	fn track_queue_deleted(&mut self, queue: Queue) -> Result<(), ValueError>;
}

pub trait CatalogTrackSumTypeChangeOperations {
	fn track_sumtype_created(&mut self, sumtype: SumType) -> Result<(), ValueError>;

	fn track_sumtype_deleted(&mut self, sumtype: SumType) -> Result<(), ValueError>;
}

pub trait CatalogTrackProcedureChangeOperations {
	fn track_procedure_created(&mut self, procedure: Procedure) -> Result<(), ValueError>;

	fn track_procedure_deleted(&mut self, procedure: Procedure) -> Result<(), ValueError>;
}

pub trait CatalogTrackTestChangeOperations {
	fn track_test_created(&mut self, test: Test) -> Result<(), ValueError>;

	fn track_test_deleted(&mut self, test: Test) -> Result<(), ValueError>;
}

pub trait CatalogTrackHandlerChangeOperations {
	fn track_handler_created(&mut self, handler: Handler) -> Result<(), ValueError>;

	fn track_handler_deleted(&mut self, handler: Handler) -> Result<(), ValueError>;
}

pub trait CatalogTrackIdentityChangeOperations {
	fn track_identity_created(&mut self, identity: Identity) -> Result<(), ValueError>;

	fn track_identity_updated(&mut self, pre: Identity, post: Identity) -> Result<(), ValueError>;

	fn track_identity_deleted(&mut self, identity: Identity) -> Result<(), ValueError>;
}

pub trait CatalogTrackRoleChangeOperations {
	fn track_role_created(&mut self, role: Role) -> Result<(), ValueError>;

	fn track_role_deleted(&mut self, role: Role) -> Result<(), ValueError>;
}

pub trait CatalogTrackGrantedRoleChangeOperations {
	fn track_granted_role_created(&mut self, granted_role: GrantedRole) -> Result<(), ValueError>;

	fn track_granted_role_deleted(&mut self, granted_role: GrantedRole) -> Result<(), ValueError>;
}

pub trait CatalogTrackIdentityAttributeChangeOperations {
	fn track_identity_attribute_created(&mut self, attribute: IdentityAttribute) -> Result<(), ValueError>;

	fn track_identity_attribute_deleted(&mut self, attribute: IdentityAttribute) -> Result<(), ValueError>;
}

pub trait CatalogTrackIdentityAttributeValueChangeOperations {
	fn track_identity_attribute_value_created(&mut self, value: IdentityAttributeValue) -> Result<(), ValueError>;

	fn track_identity_attribute_value_deleted(&mut self, value: IdentityAttributeValue) -> Result<(), ValueError>;
}

pub trait CatalogTrackPolicyChangeOperations {
	fn track_policy_created(&mut self, policy: Policy) -> Result<(), ValueError>;

	fn track_policy_updated(&mut self, pre: Policy, post: Policy) -> Result<(), ValueError>;

	fn track_policy_deleted(&mut self, policy: Policy) -> Result<(), ValueError>;
}

pub trait CatalogTrackMigrationChangeOperations {
	fn track_migration_created(&mut self, migration: Migration) -> Result<(), ValueError>;
}

pub trait CatalogTrackMigrationEventChangeOperations {
	fn track_migration_event_created(&mut self, event: MigrationEvent) -> Result<(), ValueError>;
}

pub trait CatalogTrackAuthenticationChangeOperations {
	fn track_authentication_created(&mut self, auth: Authentication) -> Result<(), ValueError>;

	fn track_authentication_deleted(&mut self, auth: Authentication) -> Result<(), ValueError>;
}

pub trait CatalogTrackAuthenticationMethodChangeOperations {
	fn track_authentication_method_created(&mut self, method: AuthenticationMethod) -> Result<(), ValueError>;

	fn track_authentication_method_updated(
		&mut self,
		pre: AuthenticationMethod,
		post: AuthenticationMethod,
	) -> Result<(), ValueError>;

	fn track_authentication_method_deleted(&mut self, method: AuthenticationMethod) -> Result<(), ValueError>;
}

pub trait CatalogTrackSourceChangeOperations {
	fn track_source_created(&mut self, source: Source) -> Result<(), ValueError>;

	fn track_source_deleted(&mut self, source: Source) -> Result<(), ValueError>;
}

pub trait CatalogTrackBindingChangeOperations {
	fn track_binding_created(&mut self, binding: Binding) -> Result<(), ValueError>;

	fn track_binding_deleted(&mut self, binding: Binding) -> Result<(), ValueError>;
}

pub trait CatalogTrackSinkChangeOperations {
	fn track_sink_created(&mut self, sink: Sink) -> Result<(), ValueError>;

	fn track_sink_deleted(&mut self, sink: Sink) -> Result<(), ValueError>;
}

pub trait CatalogTrackWindowedViewGroupChangeOperations {
	fn track_windowed_view_group_created(&mut self, group: WindowedViewGroup) -> Result<(), ValueError>;

	fn track_windowed_view_group_deleted(&mut self, group: WindowedViewGroup) -> Result<(), ValueError>;
}

pub trait CatalogTrackRowSettingsChangeOperations {
	fn track_row_settings_created(&mut self, storage: StorageId, settings: RowSettings) -> Result<(), ValueError>;
}

pub trait CatalogTrackChangeOperations:
	CatalogTrackBindingChangeOperations
	+ CatalogTrackColumnBlockChangeOperations
	+ CatalogTrackDictionaryChangeOperations
	+ CatalogTrackFlowChangeOperations
	+ CatalogTrackPrimaryKeyChangeOperations
	+ CatalogTrackHandlerChangeOperations
	+ CatalogTrackMigrationChangeOperations
	+ CatalogTrackMigrationEventChangeOperations
	+ CatalogTrackNamespaceChangeOperations
	+ CatalogTrackProcedureChangeOperations
	+ CatalogTrackQueueChangeOperations
	+ CatalogTrackRelationshipChangeOperations
	+ CatalogTrackRingBufferChangeOperations
	+ CatalogTrackRoleChangeOperations
	+ CatalogTrackPolicyChangeOperations
	+ CatalogTrackSeriesChangeOperations
	+ CatalogTrackSinkChangeOperations
	+ CatalogTrackSourceChangeOperations
	+ CatalogTrackSumTypeChangeOperations
	+ CatalogTrackTableChangeOperations
	+ CatalogTrackTestChangeOperations
	+ CatalogTrackAuthenticationChangeOperations
	+ CatalogTrackAuthenticationMethodChangeOperations
	+ CatalogTrackIdentityChangeOperations
	+ CatalogTrackGrantedRoleChangeOperations
	+ CatalogTrackIdentityAttributeChangeOperations
	+ CatalogTrackIdentityAttributeValueChangeOperations
	+ CatalogTrackViewChangeOperations
	+ CatalogTrackWindowedViewGroupChangeOperations
	+ CatalogTrackConfigChangeOperations
	+ CatalogTrackRowSettingsChangeOperations
{
}
