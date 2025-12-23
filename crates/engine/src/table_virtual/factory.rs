// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::interface::TableVirtualDef;

use super::TableVirtual;

/// Factory for creating virtual table instances.
///
/// Implementations must be thread-safe and create fresh instances on each call.
/// The factory pattern allows virtual tables to be registered once and instantiated
/// on-demand for each query execution.
///
/// # Example
///
/// ```ignore
/// struct MyTableFactory {
///     definition: Arc<TableVirtualDef>,
/// }
///
/// impl VirtualTableFactory for MyTableFactory {
///     fn create_boxed(&self) -> Box<dyn TableVirtual + Send + Sync> {
///         Box::new(MyVirtualTable::new(self.definition.clone()))
///     }
///
///     fn definition(&self) -> Arc<TableVirtualDef> {
///         self.definition.clone()
///     }
/// }
/// ```
pub trait VirtualTableFactory: Send + Sync + 'static {
	/// Create a new virtual table instance.
	///
	/// Each call should return a fresh instance ready to process a new query.
	fn create_boxed(&self) -> Box<dyn TableVirtual + Send + Sync>;

	/// Get the table definition (schema).
	///
	/// Returns the metadata including column names, types, and constraints.
	fn definition(&self) -> Arc<TableVirtualDef>;
}
