// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Virtual-table runtime backing the system tables declared in `system/`. No persisted bytes back a
//! system table; a handler materialises its rows from in-memory catalog state on each read.

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::vtable::{VTable, VTableId},
	sort::SortKey,
	value::column::columns::Columns,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::params::Params;

use crate::{
	Result,
	system::{SystemCatalog, ids::vtable::*},
};

#[derive(Debug)]
pub struct Batch {
	pub columns: Columns,
}

pub mod system;
pub mod tables;
pub mod user;

pub enum VTableContext {
	Basic {
		params: Params,
	},

	PushDown {
		order_by: Vec<SortKey>,

		limit: Option<usize>,

		params: Params,
	},
}

pub trait BaseVTable: Send + Sync {
	fn initialize(&mut self, txn: &mut Transaction<'_>, ctx: VTableContext) -> Result<()>;

	fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>>;

	fn vtable(&self) -> &VTable;
}

pub struct VTableRegistry;

impl VTableRegistry {
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
			CDC_CONSUMERS => Some(SystemCatalog::get_system_cdc_consumers_table()),
			FLOWS => Some(SystemCatalog::get_system_flows_table()),
			OPERATOR_LIBRARIES => Some(SystemCatalog::get_system_operator_libraries_table()),
			OPERATORS => Some(SystemCatalog::get_system_operators_table()),
			FLOW_EDGES => Some(SystemCatalog::get_system_flow_edges_table()),
			OPERATOR_TYPES => Some(SystemCatalog::get_system_operator_types_table()),
			OPERATOR_LIBRARY_INPUTS => Some(SystemCatalog::get_system_operator_library_inputs_table()),
			OPERATOR_LIBRARY_OUTPUTS => Some(SystemCatalog::get_system_operator_library_outputs_table()),
			DICTIONARIES => Some(SystemCatalog::get_system_dictionaries_table()),
			RINGBUFFERS => Some(SystemCatalog::get_system_ringbuffers_table()),
			QUEUES => Some(SystemCatalog::get_system_queues_table()),
			SHAPES => Some(SystemCatalog::get_system_row_shapes_table()),
			SHAPE_FIELDS => Some(SystemCatalog::get_system_row_shape_fields_table()),
			ENUMS => Some(SystemCatalog::get_system_enums_table()),
			ENUM_VARIANTS => Some(SystemCatalog::get_system_enum_variants_table()),
			EVENTS => Some(SystemCatalog::get_system_events_table()),
			EVENT_VARIANTS => Some(SystemCatalog::get_system_event_variants_table()),
			HANDLERS => Some(SystemCatalog::get_system_handlers_table()),
			TAGS => Some(SystemCatalog::get_system_tags_table()),
			TAG_VARIANTS => Some(SystemCatalog::get_system_tag_variants_table()),
			SERIES => Some(SystemCatalog::get_system_series_table()),
			IDENTITIES => Some(SystemCatalog::get_system_identities_table()),
			IDENTITY_ATTRIBUTES => Some(SystemCatalog::get_system_identity_attributes_table()),
			IDENTITY_ATTRIBUTE_VALUES => Some(SystemCatalog::get_system_identity_attribute_values_table()),
			ROLES => Some(SystemCatalog::get_system_roles_table()),
			GRANTED_ROLES => Some(SystemCatalog::get_system_granted_roles_table()),
			POLICIES => Some(SystemCatalog::get_system_policies_table()),
			POLICY_OPERATIONS => Some(SystemCatalog::get_system_policy_operations_table()),
			VIRTUAL_TABLES => Some(SystemCatalog::get_system_virtual_tables_table()),
			VIRTUAL_TABLE_COLUMNS => Some(SystemCatalog::get_system_virtual_table_columns_table()),
			TYPES => Some(SystemCatalog::get_system_types_table()),
			MIGRATIONS => Some(SystemCatalog::get_system_migrations_table()),
			AUTHENTICATIONS => Some(SystemCatalog::get_system_authentications_table()),
			CONFIGS => Some(SystemCatalog::get_configs_table()),
			_ => None,
		})
	}

	pub fn list_vtables(_rx: &mut Transaction<'_>) -> Result<Vec<Arc<VTable>>> {
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
			SystemCatalog::get_system_cdc_consumers_table(),
			SystemCatalog::get_system_flows_table(),
			SystemCatalog::get_system_operator_libraries_table(),
			SystemCatalog::get_system_operators_table(),
			SystemCatalog::get_system_flow_edges_table(),
			SystemCatalog::get_system_operator_types_table(),
			SystemCatalog::get_system_operator_library_inputs_table(),
			SystemCatalog::get_system_operator_library_outputs_table(),
			SystemCatalog::get_system_dictionaries_table(),
			SystemCatalog::get_system_ringbuffers_table(),
			SystemCatalog::get_system_queues_table(),
			SystemCatalog::get_system_row_shapes_table(),
			SystemCatalog::get_system_row_shape_fields_table(),
			SystemCatalog::get_system_enums_table(),
			SystemCatalog::get_system_enum_variants_table(),
			SystemCatalog::get_system_events_table(),
			SystemCatalog::get_system_event_variants_table(),
			SystemCatalog::get_system_handlers_table(),
			SystemCatalog::get_system_tags_table(),
			SystemCatalog::get_system_tag_variants_table(),
			SystemCatalog::get_system_series_table(),
			SystemCatalog::get_system_identities_table(),
			SystemCatalog::get_system_identity_attributes_table(),
			SystemCatalog::get_system_identity_attribute_values_table(),
			SystemCatalog::get_system_roles_table(),
			SystemCatalog::get_system_granted_roles_table(),
			SystemCatalog::get_system_policies_table(),
			SystemCatalog::get_system_policy_operations_table(),
			SystemCatalog::get_system_virtual_tables_table(),
			SystemCatalog::get_system_virtual_table_columns_table(),
			SystemCatalog::get_system_types_table(),
			SystemCatalog::get_system_migrations_table(),
			SystemCatalog::get_system_authentications_table(),
			SystemCatalog::get_configs_table(),
		])
	}
}
