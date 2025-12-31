// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Simplified registry for user-defined virtual tables.

use std::{
	collections::HashMap,
	sync::{Arc, RwLock},
};

use reifydb_core::interface::{NamespaceId, VTableDef, VTableId};

use crate::vtable::tables::{UserVTableDataFunction, VTables};

/// Entry in the user virtual table registry.
#[derive(Clone)]
pub struct UserVTableEntry {
	pub def: Arc<VTableDef>,
	pub data_fn: UserVTableDataFunction,
}

impl UserVTableEntry {
	/// Create a new VTableImpl instance from this entry.
	///
	/// This creates a fresh instance with reset state for each query.
	pub fn create_instance(&self) -> VTables {
		VTables::UserDefined {
			def: self.def.clone(),
			data_fn: self.data_fn.clone(),
			params: None,
			exhausted: false,
		}
	}
}

/// Simplified registry for user-defined virtual tables.
///
/// This registry stores the components needed to create fresh VTableImpl instances
/// for each query execution.
#[derive(Clone)]
pub struct UserVTableRegistry {
	inner: Arc<RwLock<UserVTableRegistryInner>>,
}

struct UserVTableRegistryInner {
	/// Entries keyed by (namespace_id, table_name)
	entries: HashMap<(NamespaceId, String), UserVTableEntry>,
	/// Entries keyed by ID for fast lookup
	entries_by_id: HashMap<VTableId, UserVTableEntry>,
	/// Next ID to assign (starts at 1000 to leave room for system tables)
	next_id: u64,
}

impl Default for UserVTableRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl UserVTableRegistry {
	/// Create a new empty registry.
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(UserVTableRegistryInner {
				entries: HashMap::new(),
				entries_by_id: HashMap::new(),
				next_id: 1000,
			})),
		}
	}

	/// Allocate a new table virtual ID.
	pub fn allocate_id(&self) -> VTableId {
		let mut inner = self.inner.write().unwrap();
		let id = VTableId(inner.next_id);
		inner.next_id += 1;
		id
	}

	/// Register a user virtual table.
	pub fn register(&self, namespace: NamespaceId, name: String, entry: UserVTableEntry) {
		let mut inner = self.inner.write().unwrap();
		let id = entry.def.id;
		inner.entries.insert((namespace, name), entry.clone());
		inner.entries_by_id.insert(id, entry);
	}

	/// Unregister a user virtual table.
	pub fn unregister(&self, namespace: NamespaceId, name: &str) -> Option<UserVTableEntry> {
		let mut inner = self.inner.write().unwrap();
		if let Some(entry) = inner.entries.remove(&(namespace, name.to_string())) {
			let id = entry.def.id;
			inner.entries_by_id.remove(&id);
			Some(entry)
		} else {
			None
		}
	}

	/// Find and create a new instance by namespace and name.
	pub fn find_by_name(&self, namespace: NamespaceId, name: &str) -> Option<VTables> {
		let inner = self.inner.read().unwrap();
		inner.entries.get(&(namespace, name.to_string())).map(|e| e.create_instance())
	}

	/// Find and create a new instance by ID.
	pub fn find_by_id(&self, id: VTableId) -> Option<VTables> {
		let inner = self.inner.read().unwrap();
		inner.entries_by_id.get(&id).map(|e| e.create_instance())
	}

	/// List all registered definitions.
	pub fn list_definitions(&self) -> Vec<Arc<VTableDef>> {
		let inner = self.inner.read().unwrap();
		inner.entries.values().map(|e| e.def.clone()).collect()
	}
}
