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
			PRIMITIVE_RETENTION_STRATEGIES => {
				Some(SystemCatalog::get_system_shape_retention_strategies_table())
			}
			OPERATOR_RETENTION_STRATEGIES => {
				Some(SystemCatalog::get_system_operator_retention_strategies_table())
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
			SHAPES => Some(SystemCatalog::get_system_shapes_table()),
			SHAPE_FIELDS => Some(SystemCatalog::get_system_shape_fields_table()),
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
			METRICS_STORAGE_TABLE => Some(SystemCatalog::get_system_metrics_storage_table_table()),
			METRICS_STORAGE_VIEW => Some(SystemCatalog::get_system_metrics_storage_view_table()),
			METRICS_STORAGE_TABLE_VIRTUAL => {
				Some(SystemCatalog::get_system_metrics_storage_table_virtual_table())
			}
			METRICS_STORAGE_RINGBUFFER => {
				Some(SystemCatalog::get_system_metrics_storage_ringbuffer_table())
			}
			METRICS_STORAGE_DICTIONARY => {
				Some(SystemCatalog::get_system_metrics_storage_dictionary_table())
			}
			METRICS_STORAGE_SERIES => Some(SystemCatalog::get_system_metrics_storage_series_table()),
			METRICS_STORAGE_FLOW => Some(SystemCatalog::get_system_metrics_storage_flow_table()),
			METRICS_STORAGE_FLOW_NODE => Some(SystemCatalog::get_system_metrics_storage_flow_node_table()),
			METRICS_STORAGE_SYSTEM => Some(SystemCatalog::get_system_metrics_storage_system_table()),
			METRICS_CDC_TABLE => Some(SystemCatalog::get_system_metrics_cdc_table_table()),
			METRICS_CDC_VIEW => Some(SystemCatalog::get_system_metrics_cdc_view_table()),
			METRICS_CDC_TABLE_VIRTUAL => Some(SystemCatalog::get_system_metrics_cdc_table_virtual_table()),
			METRICS_CDC_RINGBUFFER => Some(SystemCatalog::get_system_metrics_cdc_ringbuffer_table()),
			METRICS_CDC_DICTIONARY => Some(SystemCatalog::get_system_metrics_cdc_dictionary_table()),
			METRICS_CDC_SERIES => Some(SystemCatalog::get_system_metrics_cdc_series_table()),
			METRICS_CDC_FLOW => Some(SystemCatalog::get_system_metrics_cdc_flow_table()),
			METRICS_CDC_FLOW_NODE => Some(SystemCatalog::get_system_metrics_cdc_flow_node_table()),
			METRICS_CDC_SYSTEM => Some(SystemCatalog::get_system_metrics_cdc_system_table()),
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
			SystemCatalog::get_system_shape_retention_strategies_table(),
			SystemCatalog::get_system_operator_retention_strategies_table(),
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
			SystemCatalog::get_system_shapes_table(),
			SystemCatalog::get_system_shape_fields_table(),
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
			SystemCatalog::get_system_metrics_storage_table_table(),
			SystemCatalog::get_system_metrics_storage_view_table(),
			SystemCatalog::get_system_metrics_storage_table_virtual_table(),
			SystemCatalog::get_system_metrics_storage_ringbuffer_table(),
			SystemCatalog::get_system_metrics_storage_dictionary_table(),
			SystemCatalog::get_system_metrics_storage_series_table(),
			SystemCatalog::get_system_metrics_storage_flow_table(),
			SystemCatalog::get_system_metrics_storage_flow_node_table(),
			SystemCatalog::get_system_metrics_storage_system_table(),
			SystemCatalog::get_system_metrics_cdc_table_table(),
			SystemCatalog::get_system_metrics_cdc_view_table(),
			SystemCatalog::get_system_metrics_cdc_table_virtual_table(),
			SystemCatalog::get_system_metrics_cdc_ringbuffer_table(),
			SystemCatalog::get_system_metrics_cdc_dictionary_table(),
			SystemCatalog::get_system_metrics_cdc_series_table(),
			SystemCatalog::get_system_metrics_cdc_flow_table(),
			SystemCatalog::get_system_metrics_cdc_flow_node_table(),
			SystemCatalog::get_system_metrics_cdc_system_table(),
			SystemCatalog::get_system_migrations_table(),
			SystemCatalog::get_system_authentications_table(),
			SystemCatalog::get_configs_table(),
		])
	}
}
