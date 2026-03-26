// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Catalog change tracking traits.
//!
//! These traits are used by command transactions to track changes to catalog entities
//! during a transaction, allowing for proper transactional semantics and rollback.

use reifydb_type::Result;

use crate::interface::catalog::{
	authentication::AuthenticationDef,
	dictionary::DictionaryDef,
	flow::FlowDef,
	handler::HandlerDef,
	identity::{IdentityDef, IdentityRoleDef, RoleDef},
	migration::{MigrationDef, MigrationEvent},
	namespace::Namespace,
	policy::PolicyDef,
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
};

/// Trait for tracking table definition changes during a transaction.
pub trait CatalogTrackTableChangeOperations {
	fn track_table_def_created(&mut self, table: TableDef) -> Result<()>;

	fn track_table_def_updated(&mut self, pre: TableDef, post: TableDef) -> Result<()>;

	fn track_table_def_deleted(&mut self, table: TableDef) -> Result<()>;
}

/// Trait for tracking namespace definition changes during a transaction.
pub trait CatalogTrackNamespaceChangeOperations {
	fn track_namespace_created(&mut self, namespace: Namespace) -> Result<()>;

	fn track_namespace_updated(&mut self, pre: Namespace, post: Namespace) -> Result<()>;

	fn track_namespace_deleted(&mut self, namespace: Namespace) -> Result<()>;
}

/// Trait for tracking flow definition changes during a transaction.
pub trait CatalogTrackFlowChangeOperations {
	fn track_flow_def_created(&mut self, flow: FlowDef) -> Result<()>;

	fn track_flow_def_updated(&mut self, pre: FlowDef, post: FlowDef) -> Result<()>;

	fn track_flow_def_deleted(&mut self, flow: FlowDef) -> Result<()>;
}

/// Trait for tracking view definition changes during a transaction.
pub trait CatalogTrackViewChangeOperations {
	fn track_view_def_created(&mut self, view: ViewDef) -> Result<()>;

	fn track_view_def_updated(&mut self, pre: ViewDef, post: ViewDef) -> Result<()>;

	fn track_view_def_deleted(&mut self, view: ViewDef) -> Result<()>;
}

/// Trait for tracking dictionary definition changes during a transaction.
pub trait CatalogTrackDictionaryChangeOperations {
	fn track_dictionary_def_created(&mut self, dictionary: DictionaryDef) -> Result<()>;

	fn track_dictionary_def_updated(&mut self, pre: DictionaryDef, post: DictionaryDef) -> Result<()>;

	fn track_dictionary_def_deleted(&mut self, dictionary: DictionaryDef) -> Result<()>;
}

/// Trait for tracking series definition changes during a transaction.
pub trait CatalogTrackSeriesChangeOperations {
	fn track_series_def_created(&mut self, series: SeriesDef) -> Result<()>;

	fn track_series_def_updated(&mut self, pre: SeriesDef, post: SeriesDef) -> Result<()>;

	fn track_series_def_deleted(&mut self, series: SeriesDef) -> Result<()>;
}

/// Trait for tracking ringbuffer definition changes during a transaction.
pub trait CatalogTrackRingBufferChangeOperations {
	fn track_ringbuffer_def_created(&mut self, ringbuffer: RingBufferDef) -> Result<()>;

	fn track_ringbuffer_def_updated(&mut self, pre: RingBufferDef, post: RingBufferDef) -> Result<()>;

	fn track_ringbuffer_def_deleted(&mut self, ringbuffer: RingBufferDef) -> Result<()>;
}

/// Trait for tracking subscription definition changes during a transaction.
pub trait CatalogTrackSubscriptionChangeOperations {
	fn track_subscription_def_created(&mut self, subscription: SubscriptionDef) -> Result<()>;

	fn track_subscription_def_updated(&mut self, pre: SubscriptionDef, post: SubscriptionDef) -> Result<()>;

	fn track_subscription_def_deleted(&mut self, subscription: SubscriptionDef) -> Result<()>;
}

/// Trait for tracking sum type definition changes during a transaction.
pub trait CatalogTrackSumTypeChangeOperations {
	fn track_sumtype_def_created(&mut self, sumtype: SumTypeDef) -> Result<()>;

	fn track_sumtype_def_updated(&mut self, pre: SumTypeDef, post: SumTypeDef) -> Result<()>;

	fn track_sumtype_def_deleted(&mut self, sumtype: SumTypeDef) -> Result<()>;
}

/// Trait for tracking procedure definition changes during a transaction.
pub trait CatalogTrackProcedureChangeOperations {
	fn track_procedure_def_created(&mut self, procedure: ProcedureDef) -> Result<()>;

	fn track_procedure_def_updated(&mut self, pre: ProcedureDef, post: ProcedureDef) -> Result<()>;

	fn track_procedure_def_deleted(&mut self, procedure: ProcedureDef) -> Result<()>;
}

/// Trait for tracking test definition changes during a transaction.
pub trait CatalogTrackTestChangeOperations {
	fn track_test_def_created(&mut self, test: TestDef) -> Result<()>;

	fn track_test_def_deleted(&mut self, test: TestDef) -> Result<()>;
}

/// Trait for tracking handler definition changes during a transaction.
pub trait CatalogTrackHandlerChangeOperations {
	fn track_handler_def_created(&mut self, handler: HandlerDef) -> Result<()>;

	fn track_handler_def_deleted(&mut self, handler: HandlerDef) -> Result<()>;
}

/// Trait for tracking identity definition changes during a transaction.
pub trait CatalogTrackIdentityChangeOperations {
	fn track_identity_def_created(&mut self, identity: IdentityDef) -> Result<()>;

	fn track_identity_def_updated(&mut self, pre: IdentityDef, post: IdentityDef) -> Result<()>;

	fn track_identity_def_deleted(&mut self, identity: IdentityDef) -> Result<()>;
}

/// Trait for tracking role definition changes during a transaction.
pub trait CatalogTrackRoleChangeOperations {
	fn track_role_def_created(&mut self, role: RoleDef) -> Result<()>;

	fn track_role_def_updated(&mut self, pre: RoleDef, post: RoleDef) -> Result<()>;

	fn track_role_def_deleted(&mut self, role: RoleDef) -> Result<()>;
}

/// Trait for tracking identity-role definition changes during a transaction.
pub trait CatalogTrackIdentityRoleChangeOperations {
	fn track_identity_role_def_created(&mut self, identity_role: IdentityRoleDef) -> Result<()>;

	fn track_identity_role_def_deleted(&mut self, identity_role: IdentityRoleDef) -> Result<()>;
}

/// Trait for tracking policy definition changes during a transaction.
pub trait CatalogTrackPolicyChangeOperations {
	fn track_policy_def_created(&mut self, policy: PolicyDef) -> Result<()>;

	fn track_policy_def_updated(&mut self, pre: PolicyDef, post: PolicyDef) -> Result<()>;

	fn track_policy_def_deleted(&mut self, policy: PolicyDef) -> Result<()>;
}

/// Trait for tracking migration definition changes during a transaction.
pub trait CatalogTrackMigrationChangeOperations {
	fn track_migration_def_created(&mut self, migration: MigrationDef) -> Result<()>;

	fn track_migration_def_deleted(&mut self, migration: MigrationDef) -> Result<()>;
}

/// Trait for tracking migration event changes during a transaction.
pub trait CatalogTrackMigrationEventChangeOperations {
	fn track_migration_event_created(&mut self, event: MigrationEvent) -> Result<()>;
}

/// Trait for tracking authentication definition changes during a transaction.
pub trait CatalogTrackAuthenticationChangeOperations {
	fn track_authentication_def_created(&mut self, auth: AuthenticationDef) -> Result<()>;

	fn track_authentication_def_deleted(&mut self, auth: AuthenticationDef) -> Result<()>;
}

/// Trait for tracking source definition changes during a transaction.
pub trait CatalogTrackSourceChangeOperations {
	fn track_source_def_created(&mut self, source: SourceDef) -> Result<()>;

	fn track_source_def_deleted(&mut self, source: SourceDef) -> Result<()>;
}

/// Trait for tracking sink definition changes during a transaction.
pub trait CatalogTrackSinkChangeOperations {
	fn track_sink_def_created(&mut self, sink: SinkDef) -> Result<()>;

	fn track_sink_def_deleted(&mut self, sink: SinkDef) -> Result<()>;
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
	+ CatalogTrackSubscriptionChangeOperations
	+ CatalogTrackSumTypeChangeOperations
	+ CatalogTrackTableChangeOperations
	+ CatalogTrackTestChangeOperations
	+ CatalogTrackAuthenticationChangeOperations
	+ CatalogTrackIdentityChangeOperations
	+ CatalogTrackIdentityRoleChangeOperations
	+ CatalogTrackViewChangeOperations
{
}
