// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Catalog change tracking traits.
//!
//! These traits are used by command transactions to track changes to catalog entities
//! during a transaction, allowing for proper transactional semantics and rollback.

use crate::{
	Result,
	interface::{DictionaryDef, FlowDef, NamespaceDef, RingBufferDef, TableDef, ViewDef},
};

/// Trait for tracking table definition changes during a transaction.
pub trait CatalogTrackTableChangeOperations {
	fn track_table_def_created(&mut self, table: TableDef) -> Result<()>;

	fn track_table_def_updated(&mut self, pre: TableDef, post: TableDef) -> Result<()>;

	fn track_table_def_deleted(&mut self, table: TableDef) -> Result<()>;
}

/// Trait for tracking namespace definition changes during a transaction.
pub trait CatalogTrackNamespaceChangeOperations {
	fn track_namespace_def_created(&mut self, namespace: NamespaceDef) -> Result<()>;

	fn track_namespace_def_updated(&mut self, pre: NamespaceDef, post: NamespaceDef) -> Result<()>;

	fn track_namespace_def_deleted(&mut self, namespace: NamespaceDef) -> Result<()>;
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

/// Trait for tracking ringbuffer definition changes during a transaction.
pub trait CatalogTrackRingBufferChangeOperations {
	fn track_ringbuffer_def_created(&mut self, ringbuffer: RingBufferDef) -> Result<()>;

	fn track_ringbuffer_def_updated(&mut self, pre: RingBufferDef, post: RingBufferDef) -> Result<()>;

	fn track_ringbuffer_def_deleted(&mut self, ringbuffer: RingBufferDef) -> Result<()>;
}

/// Umbrella trait for all catalog change tracking operations.
pub trait CatalogTrackChangeOperations:
	CatalogTrackDictionaryChangeOperations
	+ CatalogTrackFlowChangeOperations
	+ CatalogTrackNamespaceChangeOperations
	+ CatalogTrackRingBufferChangeOperations
	+ CatalogTrackTableChangeOperations
	+ CatalogTrackViewChangeOperations
{
}
