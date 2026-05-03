// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	sync::{Arc, RwLock},
};

use reifydb_core::interface::catalog::{
	id::NamespaceId,
	vtable::{VTable, VTableId},
};

use crate::vtable::tables::{UserVTableDataFunction, VTables};

#[derive(Clone)]
pub struct UserVTableEntry {
	pub def: Arc<VTable>,
	pub data_fn: UserVTableDataFunction,
}

impl UserVTableEntry {
	pub fn create_instance(&self) -> VTables {
		VTables::UserDefined {
			vtable: self.def.clone(),
			data_fn: self.data_fn.clone(),
			params: None,
			exhausted: false,
		}
	}
}

#[derive(Clone)]
pub struct UserVTableRegistry {
	inner: Arc<RwLock<UserVTableRegistryInner>>,
}

struct UserVTableRegistryInner {
	entries: HashMap<(NamespaceId, String), UserVTableEntry>,

	entries_by_id: HashMap<VTableId, UserVTableEntry>,

	next_id: u64,
}

impl Default for UserVTableRegistry {
	fn default() -> Self {
		Self::new()
	}
}

impl UserVTableRegistry {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(RwLock::new(UserVTableRegistryInner {
				entries: HashMap::new(),
				entries_by_id: HashMap::new(),
				next_id: 1000,
			})),
		}
	}

	pub fn allocate_id(&self) -> VTableId {
		let mut inner = self.inner.write().unwrap();
		let id = VTableId(inner.next_id);
		inner.next_id += 1;
		id
	}

	pub fn register(&self, namespace: NamespaceId, name: String, entry: UserVTableEntry) {
		let mut inner = self.inner.write().unwrap();
		let id = entry.def.id;
		inner.entries.insert((namespace, name), entry.clone());
		inner.entries_by_id.insert(id, entry);
	}

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

	pub fn find_by_name(&self, namespace: NamespaceId, name: &str) -> Option<VTables> {
		let inner = self.inner.read().unwrap();
		inner.entries.get(&(namespace, name.to_string())).map(|e| e.create_instance())
	}

	pub fn find_by_id(&self, id: VTableId) -> Option<VTables> {
		let inner = self.inner.read().unwrap();
		inner.entries_by_id.get(&id).map(|e| e.create_instance())
	}

	pub fn list_definitions(&self) -> Vec<Arc<VTable>> {
		let inner = self.inner.read().unwrap();
		inner.entries.values().map(|e| e.def.clone()).collect()
	}
}
