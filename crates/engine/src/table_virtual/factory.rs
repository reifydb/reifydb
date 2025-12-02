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
///     fn create_boxed(&self) -> Box<dyn TableVirtual<'static> + Send + Sync> {
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
	/// The returned instance must implement `TableVirtual<'a>` for all lifetimes.
	fn create_boxed(&self) -> Box<dyn TableVirtual<'static> + Send + Sync>;

	/// Get the table definition (schema).
	///
	/// Returns the metadata including column names, types, and constraints.
	fn definition(&self) -> Arc<TableVirtualDef>;
}

/// Extends the lifetime of a boxed virtual table.
///
/// # Safety
///
/// This is safe because all virtual table implementations are required to be `'static`
/// (they don't borrow any data). The `'a` lifetime in `TableVirtual<'a>` only constrains
/// method arguments and return values, not the struct itself.
#[inline]
pub(crate) fn extend_virtual_table_lifetime<'a>(
	vtable: Box<dyn TableVirtual<'static> + Send + Sync>,
) -> Box<dyn TableVirtual<'a>> {
	// SAFETY: The concrete type implementing TableVirtual is 'static,
	// so it can safely be used with any lifetime 'a.
	unsafe { std::mem::transmute(vtable) }
}
