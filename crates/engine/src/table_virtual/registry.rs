// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Registry for user-defined virtual table factories.

use std::{
	collections::HashMap,
	sync::{Arc, RwLock},
};

use async_trait::async_trait;
use reifydb_core::interface::{NamespaceId, TableVirtualDef, TableVirtualId};

use super::{
	TableVirtual, VirtualTableFactory,
	adapter::TableVirtualUserAdapter,
	user::{TableVirtualUser, TableVirtualUserIterator},
};
use crate::transaction::StandardTransaction;

/// Registry for user-defined virtual table factories.
///
/// This registry stores the runtime factories that create virtual table instances.
/// It works in conjunction with `MaterializedCatalog` which stores the definitions.
#[derive(Clone)]
pub struct TableVirtualUserRegistry {
	inner: Arc<RwLock<TableVirtualUserRegistryInner>>,
}

struct TableVirtualUserRegistryInner {
	/// Factories keyed by (namespace_id, table_name)
	factories: HashMap<(NamespaceId, String), Arc<dyn VirtualTableFactory>>,
	/// Factories by ID for fast lookup
	factories_by_id: HashMap<TableVirtualId, Arc<dyn VirtualTableFactory>>,
	/// Next ID to assign (starts at 1000 to leave room for system tables)
	next_id: u64,
}

impl Default for TableVirtualUserRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl TableVirtualUserRegistry {
	/// Create a new empty registry.
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(TableVirtualUserRegistryInner {
				factories: HashMap::new(),
				factories_by_id: HashMap::new(),
				next_id: 1000,
			})),
		}
	}

	/// Allocate a new table virtual ID.
	pub fn allocate_id(&self) -> TableVirtualId {
		let mut inner = self.inner.write().unwrap();
		let id = TableVirtualId(inner.next_id);
		inner.next_id += 1;
		id
	}

	/// Register a factory for a user virtual table.
	///
	/// The definition should already be registered in `MaterializedCatalog`.
	pub fn register(&self, namespace: NamespaceId, name: String, factory: Arc<dyn VirtualTableFactory>) {
		let mut inner = self.inner.write().unwrap();
		let id = factory.definition().id;
		inner.factories.insert((namespace, name), factory.clone());
		inner.factories_by_id.insert(id, factory);
	}

	/// Unregister a user virtual table.
	pub fn unregister(&self, namespace: NamespaceId, name: &str) -> Option<Arc<dyn VirtualTableFactory>> {
		let mut inner = self.inner.write().unwrap();
		if let Some(factory) = inner.factories.remove(&(namespace, name.to_string())) {
			let id = factory.definition().id;
			inner.factories_by_id.remove(&id);
			Some(factory)
		} else {
			None
		}
	}

	/// Find a factory by namespace and name.
	pub fn find_by_name(&self, namespace: NamespaceId, name: &str) -> Option<Arc<dyn VirtualTableFactory>> {
		let inner = self.inner.read().unwrap();
		inner.factories.get(&(namespace, name.to_string())).cloned()
	}

	/// Find a factory by ID.
	pub fn find_by_id(&self, id: TableVirtualId) -> Option<Arc<dyn VirtualTableFactory>> {
		let inner = self.inner.read().unwrap();
		inner.factories_by_id.get(&id).cloned()
	}

	/// List all registered factories.
	pub fn list_all(&self) -> Vec<Arc<dyn VirtualTableFactory>> {
		let inner = self.inner.read().unwrap();
		inner.factories.values().cloned().collect()
	}
}

/// Factory implementation for `TableVirtualUser` types.
pub struct SimpleVirtualTableFactory<T: TableVirtualUser + Clone> {
	template: T,
	definition: Arc<TableVirtualDef>,
}

impl<T: TableVirtualUser + Clone> SimpleVirtualTableFactory<T> {
	/// Create a new factory for the given user table.
	pub fn new(template: T, definition: Arc<TableVirtualDef>) -> Self {
		Self {
			template,
			definition,
		}
	}
}

impl<T: TableVirtualUser + Clone> VirtualTableFactory for SimpleVirtualTableFactory<T> {
	fn create_boxed(&self) -> Box<dyn TableVirtual + Send + Sync> {
		Box::new(TableVirtualUserAdapter::new(self.template.clone(), self.definition.clone()))
	}

	fn definition(&self) -> Arc<TableVirtualDef> {
		self.definition.clone()
	}
}

/// Factory implementation for `TableVirtualUserIterator` types.
///
/// This factory creates fresh iterator instances for each query.
pub struct IteratorVirtualTableFactory<F>
where
	F: Fn() -> Box<dyn TableVirtualUserIterator> + Send + Sync + 'static,
{
	creator: F,
	definition: Arc<TableVirtualDef>,
}

impl<F> IteratorVirtualTableFactory<F>
where
	F: Fn() -> Box<dyn TableVirtualUserIterator> + Send + Sync + 'static,
{
	/// Create a new factory with the given creator function.
	pub fn new(creator: F, definition: Arc<TableVirtualDef>) -> Self {
		Self {
			creator,
			definition,
		}
	}
}

impl<F> VirtualTableFactory for IteratorVirtualTableFactory<F>
where
	F: Fn() -> Box<dyn TableVirtualUserIterator> + Send + Sync + 'static,
{
	fn create_boxed(&self) -> Box<dyn TableVirtual + Send + Sync> {
		let iter = (self.creator)();
		Box::new(IteratorAdapter {
			inner: iter,
			definition: self.definition.clone(),
			initialized: false,
			batch_size: 1000,
		})
	}

	fn definition(&self) -> Arc<TableVirtualDef> {
		self.definition.clone()
	}
}

/// Internal adapter for boxed iterators.
struct IteratorAdapter {
	inner: Box<dyn TableVirtualUserIterator>,
	definition: Arc<TableVirtualDef>,
	initialized: bool,
	batch_size: usize,
}

#[async_trait]
impl TableVirtual for IteratorAdapter {
	async fn initialize<'a>(
		&mut self,
		_txn: &mut StandardTransaction<'a>,
		ctx: super::TableVirtualContext,
	) -> crate::Result<()> {
		use super::user::TableVirtualUserPushdownContext;

		let user_ctx = match ctx {
			super::TableVirtualContext::Basic {
				..
			} => None,
			super::TableVirtualContext::PushDown {
				limit,
				..
			} => Some(TableVirtualUserPushdownContext {
				limit,
			}),
		};

		self.inner.initialize(user_ctx.as_ref()).await?;
		self.initialized = true;
		Ok(())
	}

	async fn next<'a>(
		&mut self,
		_txn: &mut StandardTransaction<'a>,
	) -> crate::Result<Option<crate::execute::Batch>> {
		use reifydb_core::value::column::Columns;

		if !self.initialized {
			return Ok(None);
		}

		let user_columns = self.inner.columns();
		let user_rows = self.inner.next_batch(self.batch_size).await?;

		match user_rows {
			None => Ok(None),
			Some(rows) if rows.is_empty() => Ok(None),
			Some(rows) => {
				let columns = super::adapter::convert_rows_to_columns(&user_columns, rows);
				Ok(Some(crate::execute::Batch {
					columns: Columns::new(columns),
				}))
			}
		}
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}

// Make IteratorAdapter Send + Sync
unsafe impl Send for IteratorAdapter {}
unsafe impl Sync for IteratorAdapter {}
