// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Catalog change tracking traits.
//!
//! These traits are used by command transactions to track changes to catalog entities
//! during a transaction, allowing for proper transactional semantics and rollback.

use reifydb_type::Result;

use crate::interface::catalog::{
	authentication::Authentication,
	config::Config,
	dictionary::Dictionary,
	flow::Flow,
	handler::Handler,
	identity::{GrantedRole, Identity, Role},
	migration::{Migration, MigrationEvent},
	namespace::Namespace,
	policy::Policy,
	procedure::Procedure,
	ringbuffer::RingBuffer,
	series::Series,
	sink::Sink,
	source::Source,
	sumtype::SumType,
	table::Table,
	test::Test,
	view::View,
};

/// Trait for tracking configuration changes during a transaction.
pub trait CatalogTrackConfigChangeOperations {
	fn track_config_set(&mut self, pre: Config, post: Config) -> Result<()>;
}

/// Trait for tracking table definition changes during a transaction.
pub trait CatalogTrackTableChangeOperations {
	fn track_table_created(&mut self, table: Table) -> Result<()>;

	fn track_table_updated(&mut self, pre: Table, post: Table) -> Result<()>;

	fn track_table_deleted(&mut self, table: Table) -> Result<()>;
}

/// Trait for tracking namespace definition changes during a transaction.
pub trait CatalogTrackNamespaceChangeOperations {
	fn track_namespace_created(&mut self, namespace: Namespace) -> Result<()>;

	fn track_namespace_updated(&mut self, pre: Namespace, post: Namespace) -> Result<()>;

	fn track_namespace_deleted(&mut self, namespace: Namespace) -> Result<()>;
}

/// Trait for tracking flow definition changes during a transaction.
pub trait CatalogTrackFlowChangeOperations {
	fn track_flow_created(&mut self, flow: Flow) -> Result<()>;

	fn track_flow_updated(&mut self, pre: Flow, post: Flow) -> Result<()>;

	fn track_flow_deleted(&mut self, flow: Flow) -> Result<()>;
}

/// Trait for tracking view definition changes during a transaction.
pub trait CatalogTrackViewChangeOperations {
	fn track_view_created(&mut self, view: View) -> Result<()>;

	fn track_view_updated(&mut self, pre: View, post: View) -> Result<()>;

	fn track_view_deleted(&mut self, view: View) -> Result<()>;
}

/// Trait for tracking dictionary definition changes during a transaction.
pub trait CatalogTrackDictionaryChangeOperations {
	fn track_dictionary_created(&mut self, dictionary: Dictionary) -> Result<()>;

	fn track_dictionary_updated(&mut self, pre: Dictionary, post: Dictionary) -> Result<()>;

	fn track_dictionary_deleted(&mut self, dictionary: Dictionary) -> Result<()>;
}

/// Trait for tracking series definition changes during a transaction.
pub trait CatalogTrackSeriesChangeOperations {
	fn track_series_created(&mut self, series: Series) -> Result<()>;

	fn track_series_updated(&mut self, pre: Series, post: Series) -> Result<()>;

	fn track_series_deleted(&mut self, series: Series) -> Result<()>;
}

/// Trait for tracking ringbuffer definition changes during a transaction.
pub trait CatalogTrackRingBufferChangeOperations {
	fn track_ringbuffer_created(&mut self, ringbuffer: RingBuffer) -> Result<()>;

	fn track_ringbuffer_updated(&mut self, pre: RingBuffer, post: RingBuffer) -> Result<()>;

	fn track_ringbuffer_deleted(&mut self, ringbuffer: RingBuffer) -> Result<()>;
}

/// Trait for tracking sum type definition changes during a transaction.
pub trait CatalogTrackSumTypeChangeOperations {
	fn track_sumtype_created(&mut self, sumtype: SumType) -> Result<()>;

	fn track_sumtype_updated(&mut self, pre: SumType, post: SumType) -> Result<()>;

	fn track_sumtype_deleted(&mut self, sumtype: SumType) -> Result<()>;
}

/// Trait for tracking procedure definition changes during a transaction.
pub trait CatalogTrackProcedureChangeOperations {
	fn track_procedure_created(&mut self, procedure: Procedure) -> Result<()>;

	fn track_procedure_updated(&mut self, pre: Procedure, post: Procedure) -> Result<()>;

	fn track_procedure_deleted(&mut self, procedure: Procedure) -> Result<()>;
}

/// Trait for tracking test definition changes during a transaction.
pub trait CatalogTrackTestChangeOperations {
	fn track_test_created(&mut self, test: Test) -> Result<()>;

	fn track_test_deleted(&mut self, test: Test) -> Result<()>;
}

/// Trait for tracking handler definition changes during a transaction.
pub trait CatalogTrackHandlerChangeOperations {
	fn track_handler_created(&mut self, handler: Handler) -> Result<()>;

	fn track_handler_deleted(&mut self, handler: Handler) -> Result<()>;
}

/// Trait for tracking identity definition changes during a transaction.
pub trait CatalogTrackIdentityChangeOperations {
	fn track_identity_created(&mut self, identity: Identity) -> Result<()>;

	fn track_identity_updated(&mut self, pre: Identity, post: Identity) -> Result<()>;

	fn track_identity_deleted(&mut self, identity: Identity) -> Result<()>;
}

/// Trait for tracking role definition changes during a transaction.
pub trait CatalogTrackRoleChangeOperations {
	fn track_role_created(&mut self, role: Role) -> Result<()>;

	fn track_role_updated(&mut self, pre: Role, post: Role) -> Result<()>;

	fn track_role_deleted(&mut self, role: Role) -> Result<()>;
}

/// Trait for tracking granted-role changes during a transaction.
pub trait CatalogTrackGrantedRoleChangeOperations {
	fn track_granted_role_created(&mut self, granted_role: GrantedRole) -> Result<()>;

	fn track_granted_role_deleted(&mut self, granted_role: GrantedRole) -> Result<()>;
}

/// Trait for tracking policy definition changes during a transaction.
pub trait CatalogTrackPolicyChangeOperations {
	fn track_policy_created(&mut self, policy: Policy) -> Result<()>;

	fn track_policy_updated(&mut self, pre: Policy, post: Policy) -> Result<()>;

	fn track_policy_deleted(&mut self, policy: Policy) -> Result<()>;
}

/// Trait for tracking migration definition changes during a transaction.
pub trait CatalogTrackMigrationChangeOperations {
	fn track_migration_created(&mut self, migration: Migration) -> Result<()>;

	fn track_migration_deleted(&mut self, migration: Migration) -> Result<()>;
}

/// Trait for tracking migration event changes during a transaction.
pub trait CatalogTrackMigrationEventChangeOperations {
	fn track_migration_event_created(&mut self, event: MigrationEvent) -> Result<()>;
}

/// Trait for tracking authentication definition changes during a transaction.
pub trait CatalogTrackAuthenticationChangeOperations {
	fn track_authentication_created(&mut self, auth: Authentication) -> Result<()>;

	fn track_authentication_deleted(&mut self, auth: Authentication) -> Result<()>;
}

/// Trait for tracking source definition changes during a transaction.
pub trait CatalogTrackSourceChangeOperations {
	fn track_source_created(&mut self, source: Source) -> Result<()>;

	fn track_source_deleted(&mut self, source: Source) -> Result<()>;
}

/// Trait for tracking sink definition changes during a transaction.
pub trait CatalogTrackSinkChangeOperations {
	fn track_sink_created(&mut self, sink: Sink) -> Result<()>;

	fn track_sink_deleted(&mut self, sink: Sink) -> Result<()>;
}

/// Umbrella trait for all catalog change tracking operations.
pub trait CatalogTrackChangeOperations:
	CatalogTrackDictionaryChangeOperations
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
{
}
