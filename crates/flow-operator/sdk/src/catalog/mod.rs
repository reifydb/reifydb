// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Catalog access for FFI operators
//!
//! Provides read-only access to catalog metadata (namespaces, tables) with
//! version-based queries for time-travel support.

mod raw;

use reifydb_core::{
	CommitVersion,
	interface::{NamespaceDef, NamespaceId, TableDef, TableId},
};

use crate::{FFIError, OperatorContext};

/// Read-only catalog access wrapper
///
/// Provides safe access to catalog metadata through FFI callbacks.
/// Mirrors the API of MaterializedCatalog.
pub struct Catalog<'a> {
	ctx: &'a mut OperatorContext,
}

impl<'a> Catalog<'a> {
	/// Create a new catalog accessor from an operator context
	pub(crate) fn new(ctx: &'a mut OperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	/// Find a namespace by ID at a specific version
	///
	/// Mirrors MaterializedCatalog::find_namespace
	///
	/// # Parameters
	/// - `namespace`: The namespace ID to look up
	/// - `version`: The commit version for time-travel queries
	///
	/// # Returns
	/// - `Ok(Some(namespace))` if found
	/// - `Ok(None)` if not found
	/// - `Err(_)` on error
	pub fn find_namespace(
		&self,
		namespace: NamespaceId,
		version: CommitVersion,
	) -> Result<Option<NamespaceDef>, FFIError> {
		raw::raw_catalog_find_namespace(self.ctx, namespace, version)
	}

	/// Find a namespace by name at a specific version
	///
	/// Mirrors MaterializedCatalog::find_namespace_by_name
	///
	/// # Parameters
	/// - `namespace`: The namespace name to look up
	/// - `version`: The commit version for time-travel queries
	///
	/// # Returns
	/// - `Ok(Some(namespace))` if found
	/// - `Ok(None)` if not found
	/// - `Err(_)` on error
	pub fn find_namespace_by_name(
		&self,
		namespace: &str,
		version: CommitVersion,
	) -> Result<Option<NamespaceDef>, FFIError> {
		raw::raw_catalog_find_namespace_by_name(self.ctx, namespace, version)
	}

	/// Find a table by ID at a specific version
	///
	/// Mirrors MaterializedCatalog::find_table
	///
	/// # Parameters
	/// - `table`: The table ID to look up
	/// - `version`: The commit version for time-travel queries
	///
	/// # Returns
	/// - `Ok(Some(table))` if found
	/// - `Ok(None)` if not found
	/// - `Err(_)` on error
	pub fn find_table(&self, table: TableId, version: CommitVersion) -> Result<Option<TableDef>, FFIError> {
		raw::raw_catalog_find_table(self.ctx, table, version)
	}

	/// Find a table by name in a namespace at a specific version
	///
	/// Mirrors MaterializedCatalog::find_table_by_name
	///
	/// # Parameters
	/// - `namespace`: The namespace ID containing the table
	/// - `name`: The table name to look up
	/// - `version`: The commit version for time-travel queries
	///
	/// # Returns
	/// - `Ok(Some(table))` if found
	/// - `Ok(None)` if not found
	/// - `Err(_)` on error
	pub fn find_table_by_name(
		&self,
		namespace: NamespaceId,
		name: &str,
		version: CommitVersion,
	) -> Result<Option<TableDef>, FFIError> {
		raw::raw_catalog_find_table_by_name(self.ctx, namespace, name, version)
	}
}
