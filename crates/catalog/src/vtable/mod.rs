// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	SortKey,
	interface::{Params, VTableDef, VTableId},
	value::column::Columns,
};
use reifydb_transaction::IntoStandardTransaction;

use crate::system::SystemCatalog;

/// A batch of columnar data returned from virtual table queries
#[derive(Debug)]
pub struct Batch {
	pub columns: Columns,
}

pub mod system;
mod tables;
pub mod user;

pub use tables::{UserVTableDataFunction, VTables};
pub use user::{
	UserVTable, UserVTableBuilder, UserVTableColumnDef, UserVTableEntry, UserVTableIterator,
	UserVTablePushdownContext, UserVTableRegistry,
};

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
#[async_trait]
pub trait VTable<T: IntoStandardTransaction>: Send + Sync {
	/// Initialize the virtual table iterator with context
	/// Called once before iteration begins
	async fn initialize(&mut self, txn: &mut T, ctx: VTableContext) -> crate::Result<()>;

	/// Get the next batch of results (volcano iterator pattern)
	async fn next(&mut self, txn: &mut T) -> crate::Result<Option<Batch>>;

	/// Get the table definition
	fn definition(&self) -> &VTableDef;
}

/// Registry for virtual tables (definitions only)
pub struct VTableRegistry;

impl VTableRegistry {
	/// Find a virtual table by its ID
	/// Returns None if the virtual table doesn't exist
	pub fn find_vtable<T: IntoStandardTransaction>(
		_rx: &mut T,
		id: VTableId,
	) -> crate::Result<Option<Arc<VTableDef>>> {
		use crate::system::ids::vtable::*;

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
			PRIMITIVE_RETENTION_POLICIES => {
				Some(SystemCatalog::get_system_primitive_retention_policies_table_def())
			}
			OPERATOR_RETENTION_POLICIES => {
				Some(SystemCatalog::get_system_operator_retention_policies_table_def())
			}
			CDC_CONSUMERS => Some(SystemCatalog::get_system_cdc_consumers_table_def()),
			FLOW_OPERATORS => Some(SystemCatalog::get_system_flow_operators_table_def()),
			DICTIONARIES => Some(SystemCatalog::get_system_dictionaries_table_def()),
			RINGBUFFERS => Some(SystemCatalog::get_system_ringbuffers_table_def()),
			_ => None,
		})
	}

	/// List all virtual tables
	pub fn list_vtables<T: IntoStandardTransaction>(_rx: &mut T) -> crate::Result<Vec<Arc<VTableDef>>> {
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
			SystemCatalog::get_system_primitive_retention_policies_table_def(),
			SystemCatalog::get_system_operator_retention_policies_table_def(),
			SystemCatalog::get_system_cdc_consumers_table_def(),
			SystemCatalog::get_system_flow_operators_table_def(),
			SystemCatalog::get_system_dictionaries_table_def(),
			SystemCatalog::get_system_ringbuffers_table_def(),
		])
	}
}
