// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use crate::{
	Result,
	interface::virtual_table::{VirtualTableDef, VirtualTableId},
};

/// Trait for virtual table instances that can execute queries
pub trait VirtualTable: Send + Sync {
	/// Get the table definition
	fn definition(&self) -> &VirtualTableDef;
}

/// Trait for creating virtual table instances
pub trait VirtualTableProvider: Send + Sync {
	/// Get the provider type identifier
	fn provider_type(&self) -> &str;

	/// Create a new virtual table instance from a definition
	fn create_virtual_table(
		&self,
		definition: VirtualTableDef,
	) -> Result<Arc<dyn VirtualTable>>;
}

/// Registry for virtual table providers
pub trait VirtualTableRegistry: Send + Sync {
	/// Register a virtual table provider
	fn register_provider(
		&mut self,
		provider: Arc<dyn VirtualTableProvider>,
	) -> Result<()>;

	/// Get a provider by type
	fn get_provider(
		&self,
		provider_type: &str,
	) -> Option<Arc<dyn VirtualTableProvider>>;

	/// List all registered provider types
	fn list_provider_types(&self) -> Vec<String>;

	/// Register a virtual table instance
	fn register_virtual_table(
		&mut self,
		table: Arc<dyn VirtualTable>,
	) -> Result<()>;

	/// Get a virtual table by ID
	fn get_virtual_table(
		&self,
		id: VirtualTableId,
	) -> Option<Arc<dyn VirtualTable>>;

	/// List all registered virtual tables
	fn list_virtual_tables(&self) -> Vec<VirtualTableId>;

	/// Remove a virtual table
	fn unregister_virtual_table(
		&mut self,
		id: VirtualTableId,
	) -> Result<()>;
}
