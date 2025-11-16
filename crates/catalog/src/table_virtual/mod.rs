// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::{QueryTransaction, TableVirtualDef, TableVirtualId};

use crate::system::SystemCatalog;

/// Registry for virtual tables
pub struct VirtualTableRegistry;

impl VirtualTableRegistry {
	/// Find a virtual table by its ID
	/// Returns None if the virtual table doesn't exist
	pub fn find_table_virtual(
		_rx: &mut impl QueryTransaction,
		id: TableVirtualId,
	) -> crate::Result<Option<Arc<TableVirtualDef>>> {
		use crate::system::ids::table_virtual::*;

		Ok(match id {
			SEQUENCES => Some(SystemCatalog::get_system_sequences_table_def()),
			NAMESPACES => Some(SystemCatalog::get_system_namespaces_table_def()),
			TABLES => Some(SystemCatalog::get_system_tables_table_def()),
			VIEWS => Some(SystemCatalog::get_system_views_table_def()),
			COLUMNS => Some(SystemCatalog::get_system_columns_table_def()),
			COLUMN_POLICIES => Some(SystemCatalog::get_system_column_policies_table_def()),
			PRIMARY_KEYS => Some(SystemCatalog::get_system_primary_keys_table_def()),
			PRIMARY_KEY_COLUMNS => Some(SystemCatalog::get_system_primary_key_columns_table_def()),
			VERSIONS => Some(SystemCatalog::get_system_versions_table_def()),
			SOURCE_RETENTION_POLICIES => {
				Some(SystemCatalog::get_system_source_retention_policies_table_def())
			}
			OPERATOR_RETENTION_POLICIES => {
				Some(SystemCatalog::get_system_operator_retention_policies_table_def())
			}
			CDC_CONSUMERS => Some(SystemCatalog::get_system_cdc_consumers_table_def()),
			FLOW_OPERATORS => Some(SystemCatalog::get_system_flow_operators_table_def()),
			_ => None,
		})
	}

	/// List all virtual tables
	pub fn list_table_virtuals(_rx: &mut impl QueryTransaction) -> crate::Result<Vec<Arc<TableVirtualDef>>> {
		// Return all registered virtual tables
		Ok(vec![
			SystemCatalog::get_system_sequences_table_def(),
			SystemCatalog::get_system_namespaces_table_def(),
			SystemCatalog::get_system_tables_table_def(),
			SystemCatalog::get_system_views_table_def(),
			SystemCatalog::get_system_columns_table_def(),
			SystemCatalog::get_system_column_policies_table_def(),
			SystemCatalog::get_system_primary_keys_table_def(),
			SystemCatalog::get_system_primary_key_columns_table_def(),
			SystemCatalog::get_system_versions_table_def(),
			SystemCatalog::get_system_source_retention_policies_table_def(),
			SystemCatalog::get_system_operator_retention_policies_table_def(),
			SystemCatalog::get_system_cdc_consumers_table_def(),
			SystemCatalog::get_system_flow_operators_table_def(),
		])
	}
}
