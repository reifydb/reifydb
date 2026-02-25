// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Catalog change tracking traits.
//!
//! These traits are used by command transactions to track changes to catalog entities
//! during a transaction, allowing for proper transactional semantics and rollback.

use crate::interface::catalog::{
	dictionary::DictionaryDef, flow::FlowDef, handler::HandlerDef, namespace::NamespaceDef,
	procedure::ProcedureDef, ringbuffer::RingBufferDef, series::SeriesDef, subscription::SubscriptionDef,
	sumtype::SumTypeDef, table::TableDef, view::ViewDef,
};

/// Trait for tracking table definition changes during a transaction.
pub trait CatalogTrackTableChangeOperations {
	fn track_table_def_created(&mut self, table: TableDef) -> reifydb_type::Result<()>;

	fn track_table_def_updated(&mut self, pre: TableDef, post: TableDef) -> reifydb_type::Result<()>;

	fn track_table_def_deleted(&mut self, table: TableDef) -> reifydb_type::Result<()>;
}

/// Trait for tracking namespace definition changes during a transaction.
pub trait CatalogTrackNamespaceChangeOperations {
	fn track_namespace_def_created(&mut self, namespace: NamespaceDef) -> reifydb_type::Result<()>;

	fn track_namespace_def_updated(&mut self, pre: NamespaceDef, post: NamespaceDef) -> reifydb_type::Result<()>;

	fn track_namespace_def_deleted(&mut self, namespace: NamespaceDef) -> reifydb_type::Result<()>;
}

/// Trait for tracking flow definition changes during a transaction.
pub trait CatalogTrackFlowChangeOperations {
	fn track_flow_def_created(&mut self, flow: FlowDef) -> reifydb_type::Result<()>;

	fn track_flow_def_updated(&mut self, pre: FlowDef, post: FlowDef) -> reifydb_type::Result<()>;

	fn track_flow_def_deleted(&mut self, flow: FlowDef) -> reifydb_type::Result<()>;
}

/// Trait for tracking view definition changes during a transaction.
pub trait CatalogTrackViewChangeOperations {
	fn track_view_def_created(&mut self, view: ViewDef) -> reifydb_type::Result<()>;

	fn track_view_def_updated(&mut self, pre: ViewDef, post: ViewDef) -> reifydb_type::Result<()>;

	fn track_view_def_deleted(&mut self, view: ViewDef) -> reifydb_type::Result<()>;
}

/// Trait for tracking dictionary definition changes during a transaction.
pub trait CatalogTrackDictionaryChangeOperations {
	fn track_dictionary_def_created(&mut self, dictionary: DictionaryDef) -> reifydb_type::Result<()>;

	fn track_dictionary_def_updated(&mut self, pre: DictionaryDef, post: DictionaryDef)
	-> reifydb_type::Result<()>;

	fn track_dictionary_def_deleted(&mut self, dictionary: DictionaryDef) -> reifydb_type::Result<()>;
}

/// Trait for tracking series definition changes during a transaction.
pub trait CatalogTrackSeriesChangeOperations {
	fn track_series_def_created(&mut self, series: SeriesDef) -> reifydb_type::Result<()>;

	fn track_series_def_updated(&mut self, pre: SeriesDef, post: SeriesDef) -> reifydb_type::Result<()>;

	fn track_series_def_deleted(&mut self, series: SeriesDef) -> reifydb_type::Result<()>;
}

/// Trait for tracking ringbuffer definition changes during a transaction.
pub trait CatalogTrackRingBufferChangeOperations {
	fn track_ringbuffer_def_created(&mut self, ringbuffer: RingBufferDef) -> reifydb_type::Result<()>;

	fn track_ringbuffer_def_updated(&mut self, pre: RingBufferDef, post: RingBufferDef)
	-> reifydb_type::Result<()>;

	fn track_ringbuffer_def_deleted(&mut self, ringbuffer: RingBufferDef) -> reifydb_type::Result<()>;
}

/// Trait for tracking subscription definition changes during a transaction.
pub trait CatalogTrackSubscriptionChangeOperations {
	fn track_subscription_def_created(&mut self, subscription: SubscriptionDef) -> reifydb_type::Result<()>;

	fn track_subscription_def_updated(
		&mut self,
		pre: SubscriptionDef,
		post: SubscriptionDef,
	) -> reifydb_type::Result<()>;

	fn track_subscription_def_deleted(&mut self, subscription: SubscriptionDef) -> reifydb_type::Result<()>;
}

/// Trait for tracking sum type definition changes during a transaction.
pub trait CatalogTrackSumTypeChangeOperations {
	fn track_sumtype_def_created(&mut self, sumtype: SumTypeDef) -> reifydb_type::Result<()>;

	fn track_sumtype_def_updated(&mut self, pre: SumTypeDef, post: SumTypeDef) -> reifydb_type::Result<()>;

	fn track_sumtype_def_deleted(&mut self, sumtype: SumTypeDef) -> reifydb_type::Result<()>;
}

/// Trait for tracking procedure definition changes during a transaction.
pub trait CatalogTrackProcedureChangeOperations {
	fn track_procedure_def_created(&mut self, procedure: ProcedureDef) -> reifydb_type::Result<()>;

	fn track_procedure_def_updated(&mut self, pre: ProcedureDef, post: ProcedureDef) -> reifydb_type::Result<()>;

	fn track_procedure_def_deleted(&mut self, procedure: ProcedureDef) -> reifydb_type::Result<()>;
}

/// Trait for tracking handler definition changes during a transaction.
pub trait CatalogTrackHandlerChangeOperations {
	fn track_handler_def_created(&mut self, handler: HandlerDef) -> reifydb_type::Result<()>;

	fn track_handler_def_deleted(&mut self, handler: HandlerDef) -> reifydb_type::Result<()>;
}

/// Umbrella trait for all catalog change tracking operations.
pub trait CatalogTrackChangeOperations:
	CatalogTrackDictionaryChangeOperations
	+ CatalogTrackFlowChangeOperations
	+ CatalogTrackHandlerChangeOperations
	+ CatalogTrackNamespaceChangeOperations
	+ CatalogTrackProcedureChangeOperations
	+ CatalogTrackRingBufferChangeOperations
	+ CatalogTrackSeriesChangeOperations
	+ CatalogTrackSubscriptionChangeOperations
	+ CatalogTrackSumTypeChangeOperations
	+ CatalogTrackTableChangeOperations
	+ CatalogTrackViewChangeOperations
{
}
