// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::Result;

use crate::{
	interface::catalog::{
		authentication::Authentication,
		binding::Binding,
		config::Config,
		dictionary::Dictionary,
		flow::{Flow, FlowNodeId},
		handler::Handler,
		identity::{GrantedRole, Identity, Role},
		migration::{Migration, MigrationEvent},
		namespace::Namespace,
		policy::Policy,
		procedure::Procedure,
		ringbuffer::RingBuffer,
		series::Series,
		shape::ShapeId,
		sink::Sink,
		source::Source,
		sumtype::SumType,
		table::Table,
		test::Test,
		view::View,
	},
	row::Ttl,
};

pub trait CatalogTrackConfigChangeOperations {
	fn track_config_set(&mut self, pre: Config, post: Config) -> Result<()>;
}

pub trait CatalogTrackTableChangeOperations {
	fn track_table_created(&mut self, table: Table) -> Result<()>;

	fn track_table_updated(&mut self, pre: Table, post: Table) -> Result<()>;

	fn track_table_deleted(&mut self, table: Table) -> Result<()>;
}

pub trait CatalogTrackNamespaceChangeOperations {
	fn track_namespace_created(&mut self, namespace: Namespace) -> Result<()>;

	fn track_namespace_updated(&mut self, pre: Namespace, post: Namespace) -> Result<()>;

	fn track_namespace_deleted(&mut self, namespace: Namespace) -> Result<()>;
}

pub trait CatalogTrackFlowChangeOperations {
	fn track_flow_created(&mut self, flow: Flow) -> Result<()>;

	fn track_flow_updated(&mut self, pre: Flow, post: Flow) -> Result<()>;

	fn track_flow_deleted(&mut self, flow: Flow) -> Result<()>;
}

pub trait CatalogTrackViewChangeOperations {
	fn track_view_created(&mut self, view: View) -> Result<()>;

	fn track_view_updated(&mut self, pre: View, post: View) -> Result<()>;

	fn track_view_deleted(&mut self, view: View) -> Result<()>;
}

pub trait CatalogTrackDictionaryChangeOperations {
	fn track_dictionary_created(&mut self, dictionary: Dictionary) -> Result<()>;

	fn track_dictionary_updated(&mut self, pre: Dictionary, post: Dictionary) -> Result<()>;

	fn track_dictionary_deleted(&mut self, dictionary: Dictionary) -> Result<()>;
}

pub trait CatalogTrackSeriesChangeOperations {
	fn track_series_created(&mut self, series: Series) -> Result<()>;

	fn track_series_updated(&mut self, pre: Series, post: Series) -> Result<()>;

	fn track_series_deleted(&mut self, series: Series) -> Result<()>;
}

pub trait CatalogTrackRingBufferChangeOperations {
	fn track_ringbuffer_created(&mut self, ringbuffer: RingBuffer) -> Result<()>;

	fn track_ringbuffer_updated(&mut self, pre: RingBuffer, post: RingBuffer) -> Result<()>;

	fn track_ringbuffer_deleted(&mut self, ringbuffer: RingBuffer) -> Result<()>;
}

pub trait CatalogTrackSumTypeChangeOperations {
	fn track_sumtype_created(&mut self, sumtype: SumType) -> Result<()>;

	fn track_sumtype_updated(&mut self, pre: SumType, post: SumType) -> Result<()>;

	fn track_sumtype_deleted(&mut self, sumtype: SumType) -> Result<()>;
}

pub trait CatalogTrackProcedureChangeOperations {
	fn track_procedure_created(&mut self, procedure: Procedure) -> Result<()>;

	fn track_procedure_updated(&mut self, pre: Procedure, post: Procedure) -> Result<()>;

	fn track_procedure_deleted(&mut self, procedure: Procedure) -> Result<()>;
}

pub trait CatalogTrackTestChangeOperations {
	fn track_test_created(&mut self, test: Test) -> Result<()>;

	fn track_test_deleted(&mut self, test: Test) -> Result<()>;
}

pub trait CatalogTrackHandlerChangeOperations {
	fn track_handler_created(&mut self, handler: Handler) -> Result<()>;

	fn track_handler_deleted(&mut self, handler: Handler) -> Result<()>;
}

pub trait CatalogTrackIdentityChangeOperations {
	fn track_identity_created(&mut self, identity: Identity) -> Result<()>;

	fn track_identity_updated(&mut self, pre: Identity, post: Identity) -> Result<()>;

	fn track_identity_deleted(&mut self, identity: Identity) -> Result<()>;
}

pub trait CatalogTrackRoleChangeOperations {
	fn track_role_created(&mut self, role: Role) -> Result<()>;

	fn track_role_updated(&mut self, pre: Role, post: Role) -> Result<()>;

	fn track_role_deleted(&mut self, role: Role) -> Result<()>;
}

pub trait CatalogTrackGrantedRoleChangeOperations {
	fn track_granted_role_created(&mut self, granted_role: GrantedRole) -> Result<()>;

	fn track_granted_role_deleted(&mut self, granted_role: GrantedRole) -> Result<()>;
}

pub trait CatalogTrackPolicyChangeOperations {
	fn track_policy_created(&mut self, policy: Policy) -> Result<()>;

	fn track_policy_updated(&mut self, pre: Policy, post: Policy) -> Result<()>;

	fn track_policy_deleted(&mut self, policy: Policy) -> Result<()>;
}

pub trait CatalogTrackMigrationChangeOperations {
	fn track_migration_created(&mut self, migration: Migration) -> Result<()>;

	fn track_migration_deleted(&mut self, migration: Migration) -> Result<()>;
}

pub trait CatalogTrackMigrationEventChangeOperations {
	fn track_migration_event_created(&mut self, event: MigrationEvent) -> Result<()>;
}

pub trait CatalogTrackAuthenticationChangeOperations {
	fn track_authentication_created(&mut self, auth: Authentication) -> Result<()>;

	fn track_authentication_deleted(&mut self, auth: Authentication) -> Result<()>;
}

pub trait CatalogTrackSourceChangeOperations {
	fn track_source_created(&mut self, source: Source) -> Result<()>;

	fn track_source_deleted(&mut self, source: Source) -> Result<()>;
}

pub trait CatalogTrackBindingChangeOperations {
	fn track_binding_created(&mut self, binding: Binding) -> Result<()>;

	fn track_binding_deleted(&mut self, binding: Binding) -> Result<()>;
}

pub trait CatalogTrackSinkChangeOperations {
	fn track_sink_created(&mut self, sink: Sink) -> Result<()>;

	fn track_sink_deleted(&mut self, sink: Sink) -> Result<()>;
}

pub trait CatalogTrackRowTtlChangeOperations {
	fn track_row_ttl_created(&mut self, shape: ShapeId, ttl: Ttl) -> Result<()>;

	fn track_row_ttl_updated(&mut self, shape: ShapeId, pre: Ttl, post: Ttl) -> Result<()>;

	fn track_row_ttl_deleted(&mut self, shape: ShapeId, ttl: Ttl) -> Result<()>;
}

pub trait CatalogTrackOperatorTtlChangeOperations {
	fn track_operator_ttl_created(&mut self, node: FlowNodeId, ttl: Ttl) -> Result<()>;

	fn track_operator_ttl_updated(&mut self, node: FlowNodeId, pre: Ttl, post: Ttl) -> Result<()>;

	fn track_operator_ttl_deleted(&mut self, node: FlowNodeId, ttl: Ttl) -> Result<()>;
}

pub trait CatalogTrackChangeOperations:
	CatalogTrackBindingChangeOperations
	+ CatalogTrackDictionaryChangeOperations
	+ CatalogTrackFlowChangeOperations
	+ CatalogTrackHandlerChangeOperations
	+ CatalogTrackMigrationChangeOperations
	+ CatalogTrackMigrationEventChangeOperations
	+ CatalogTrackNamespaceChangeOperations
	+ CatalogTrackProcedureChangeOperations
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
	+ CatalogTrackIdentityChangeOperations
	+ CatalogTrackGrantedRoleChangeOperations
	+ CatalogTrackViewChangeOperations
	+ CatalogTrackConfigChangeOperations
	+ CatalogTrackRowTtlChangeOperations
	+ CatalogTrackOperatorTtlChangeOperations
{
}
