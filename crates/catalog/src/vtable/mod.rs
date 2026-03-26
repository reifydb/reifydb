// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::{VTable, VTableId},
	sort::SortKey,
	value::column::columns::Columns,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::params::Params;

use crate::{
	Result,
	system::{SystemCatalog, ids::vtable::*},
};

/// A batch of columnar data returned from virtual table queries
#[derive(Debug)]
pub struct Batch {
	pub columns: Columns,
}

pub mod system;
pub mod tables;
pub mod user;

/// Context passed to virtual table queries
///
/// Note: For pushdown optimization with expressions, use the extended context in the engine crate.
pub enum VTableContext {
	/// Basic query context with just parameters
	Basic {
		/// Query parameters
		params: Params,
	},
	/// Pushdown optimization hints (without expression types to avoid circular deps)
	PushDown {
		/// Sort keys from order operations
		order_by: Vec<SortKey>,
		/// Limit from take operations
		limit: Option<usize>,
		/// Query parameters
		params: Params,
	},
}

/// Trait for virtual table instances that follow the volcano iterator pattern
pub trait BaseVTable: Send + Sync {
	/// Initialize the virtual table iterator with context
	/// Called once before iteration begins
	fn initialize(&mut self, txn: &mut Transaction<'_>, ctx: VTableContext) -> Result<()>;

	/// Get the next batch of results (volcano iterator pattern)
	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>>;

	/// Get the table definition
	fn definition(&self) -> &VTable;
}

/// Registry for virtual tables (definitions only)
pub struct VTableRegistry;

impl VTableRegistry {
	/// Find a virtual table by its ID
	/// Returns None if the virtual table doesn't exist
	pub fn find_vtable(_rx: &mut Transaction<'_>, id: VTableId) -> Result<Option<Arc<VTable>>> {
		Ok(match id {
			SEQUENCES => Some(SystemCatalog::get_system_sequences_table()),
			NAMESPACES => Some(SystemCatalog::get_system_namespaces_table()),
			TABLES => Some(SystemCatalog::get_system_tables_table()),
			VIEWS => Some(SystemCatalog::get_system_views_table()),
			COLUMNS => Some(SystemCatalog::get_system_columns_table()),
			COLUMN_PROPERTIES => Some(SystemCatalog::get_system_column_properties_table()),
			PRIMARY_KEYS => Some(SystemCatalog::get_system_primary_keys_table()),
			PRIMARY_KEY_COLUMNS => Some(SystemCatalog::get_system_primary_key_columns_table()),
			VERSIONS => Some(SystemCatalog::get_system_versions_table()),
			PRIMITIVE_RETENTION_POLICIES => {
				Some(SystemCatalog::get_system_primitive_retention_policies_table())
			}
			OPERATOR_RETENTION_POLICIES => {
				Some(SystemCatalog::get_system_operator_retention_policies_table())
			}
			CDC_CONSUMERS => Some(SystemCatalog::get_system_cdc_consumers_table()),
			FLOWS => Some(SystemCatalog::get_system_flows_table()),
			FLOW_OPERATORS => Some(SystemCatalog::get_system_flow_operators_table()),
			FLOW_NODES => Some(SystemCatalog::get_system_flow_nodes_table()),
			FLOW_EDGES => Some(SystemCatalog::get_system_flow_edges_table()),
			FLOW_NODE_TYPES => Some(SystemCatalog::get_system_flow_node_types_table()),
			FLOW_OPERATOR_INPUTS => Some(SystemCatalog::get_system_flow_operator_inputs_table()),
			FLOW_OPERATOR_OUTPUTS => Some(SystemCatalog::get_system_flow_operator_outputs_table()),
			DICTIONARIES => Some(SystemCatalog::get_system_dictionaries_table()),
			RINGBUFFERS => Some(SystemCatalog::get_system_ringbuffers_table()),
			SCHEMAS => Some(SystemCatalog::get_system_schemas_table()),
			SCHEMA_FIELDS => Some(SystemCatalog::get_system_schema_fields_table()),
			ENUMS => Some(SystemCatalog::get_system_enums_table()),
			ENUM_VARIANTS => Some(SystemCatalog::get_system_enum_variants_table()),
			EVENTS => Some(SystemCatalog::get_system_events_table()),
			EVENT_VARIANTS => Some(SystemCatalog::get_system_event_variants_table()),
			HANDLERS => Some(SystemCatalog::get_system_handlers_table()),
			TAGS => Some(SystemCatalog::get_system_tags_table()),
			TAG_VARIANTS => Some(SystemCatalog::get_system_tag_variants_table()),
			SERIES => Some(SystemCatalog::get_system_series_table()),
			IDENTITIES => Some(SystemCatalog::get_system_identities_table()),
			ROLES => Some(SystemCatalog::get_system_roles_table()),
			GRANTED_ROLES => Some(SystemCatalog::get_system_granted_roles_table()),
			POLICIES => Some(SystemCatalog::get_system_policies_table()),
			POLICY_OPERATIONS => Some(SystemCatalog::get_system_policy_operations_table()),
			VIRTUAL_TABLES => Some(SystemCatalog::get_system_virtual_tables_table()),
			VIRTUAL_TABLE_COLUMNS => Some(SystemCatalog::get_system_virtual_table_columns_table()),
			TYPES => Some(SystemCatalog::get_system_types_table()),
			TABLE_STORAGE_STATS => Some(SystemCatalog::get_system_table_storage_stats_table()),
			VIEW_STORAGE_STATS => Some(SystemCatalog::get_system_view_storage_stats_table()),
			FLOW_STORAGE_STATS => Some(SystemCatalog::get_system_flow_storage_stats_table()),
			FLOW_NODE_STORAGE_STATS => Some(SystemCatalog::get_system_flow_node_storage_stats_table()),
			INDEX_STORAGE_STATS => Some(SystemCatalog::get_system_index_storage_stats_table()),
			RINGBUFFER_STORAGE_STATS => Some(SystemCatalog::get_system_ringbuffer_storage_stats_table()),
			DICTIONARY_STORAGE_STATS => Some(SystemCatalog::get_system_dictionary_storage_stats_table()),
			MIGRATIONS => Some(SystemCatalog::get_system_migrations_table()),
			AUTHENTICATIONS => Some(SystemCatalog::get_system_authentications_table()),
			CONFIGS => Some(SystemCatalog::get_system_configs_table()),
			_ => None,
		})
	}

	/// List all virtual tables
	pub fn list_vtables(_rx: &mut Transaction<'_>) -> Result<Vec<Arc<VTable>>> {
		// Return all registered virtual tables
		Ok(vec![
			SystemCatalog::get_system_sequences_table(),
			SystemCatalog::get_system_namespaces_table(),
			SystemCatalog::get_system_tables_table(),
			SystemCatalog::get_system_views_table(),
			SystemCatalog::get_system_columns_table(),
			SystemCatalog::get_system_column_properties_table(),
			SystemCatalog::get_system_primary_keys_table(),
			SystemCatalog::get_system_primary_key_columns_table(),
			SystemCatalog::get_system_versions_table(),
			SystemCatalog::get_system_primitive_retention_policies_table(),
			SystemCatalog::get_system_operator_retention_policies_table(),
			SystemCatalog::get_system_cdc_consumers_table(),
			SystemCatalog::get_system_flows_table(),
			SystemCatalog::get_system_flow_operators_table(),
			SystemCatalog::get_system_flow_nodes_table(),
			SystemCatalog::get_system_flow_edges_table(),
			SystemCatalog::get_system_flow_node_types_table(),
			SystemCatalog::get_system_flow_operator_inputs_table(),
			SystemCatalog::get_system_flow_operator_outputs_table(),
			SystemCatalog::get_system_dictionaries_table(),
			SystemCatalog::get_system_ringbuffers_table(),
			SystemCatalog::get_system_schemas_table(),
			SystemCatalog::get_system_schema_fields_table(),
			SystemCatalog::get_system_enums_table(),
			SystemCatalog::get_system_enum_variants_table(),
			SystemCatalog::get_system_events_table(),
			SystemCatalog::get_system_event_variants_table(),
			SystemCatalog::get_system_handlers_table(),
			SystemCatalog::get_system_tags_table(),
			SystemCatalog::get_system_tag_variants_table(),
			SystemCatalog::get_system_series_table(),
			SystemCatalog::get_system_identities_table(),
			SystemCatalog::get_system_roles_table(),
			SystemCatalog::get_system_granted_roles_table(),
			SystemCatalog::get_system_policies_table(),
			SystemCatalog::get_system_policy_operations_table(),
			SystemCatalog::get_system_virtual_tables_table(),
			SystemCatalog::get_system_virtual_table_columns_table(),
			SystemCatalog::get_system_types_table(),
			SystemCatalog::get_system_table_storage_stats_table(),
			SystemCatalog::get_system_view_storage_stats_table(),
			SystemCatalog::get_system_flow_storage_stats_table(),
			SystemCatalog::get_system_flow_node_storage_stats_table(),
			SystemCatalog::get_system_index_storage_stats_table(),
			SystemCatalog::get_system_ringbuffer_storage_stats_table(),
			SystemCatalog::get_system_dictionary_storage_stats_table(),
			SystemCatalog::get_system_migrations_table(),
			SystemCatalog::get_system_authentications_table(),
			SystemCatalog::get_system_configs_table(),
		])
	}
}
