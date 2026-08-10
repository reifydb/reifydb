// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use reifydb_core::event::{EventListener, operator::OperatorLoadedEvent};
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::value::constraint::TypeConstraint;

#[derive(Clone, Debug)]
pub struct OperatorLibraryColumnInfo {
	pub name: String,
	pub field_type: TypeConstraint,
	pub description: String,
}

#[derive(Clone, Debug)]
pub struct OperatorLibraryInfo {
	pub operator: String,
	pub library_path: PathBuf,
	pub api: u32,
	pub capabilities: u32,
	pub input_columns: Vec<OperatorLibraryColumnInfo>,
	pub output_columns: Vec<OperatorLibraryColumnInfo>,
}

#[derive(Clone)]
pub struct OperatorLibrary {
	operators: Arc<RwLock<HashMap<String, OperatorLibraryInfo>>>,
}

impl Default for OperatorLibrary {
	fn default() -> Self {
		Self::new()
	}
}

impl OperatorLibrary {
	pub fn new() -> Self {
		Self {
			operators: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	pub fn add(&self, info: OperatorLibraryInfo) {
		self.operators.write().insert(info.operator.clone(), info);
	}

	pub fn list(&self) -> Vec<OperatorLibraryInfo> {
		self.operators.read().values().cloned().collect()
	}

	pub fn get(&self, operator: &str) -> Option<OperatorLibraryInfo> {
		self.operators.read().get(operator).cloned()
	}
}

pub struct OperatorLibraryEventListener {
	store: OperatorLibrary,
}

impl OperatorLibraryEventListener {
	pub fn new(store: OperatorLibrary) -> Self {
		Self {
			store,
		}
	}
}

impl EventListener<OperatorLoadedEvent> for OperatorLibraryEventListener {
	fn on(&self, event: &OperatorLoadedEvent) {
		self.store.add(OperatorLibraryInfo {
			operator: event.operator().clone(),
			library_path: event.library_path().clone(),
			api: *event.api(),
			capabilities: *event.capabilities(),
			input_columns: event
				.input()
				.iter()
				.map(|c| OperatorLibraryColumnInfo {
					name: c.name.clone(),
					field_type: c.field_type.clone(),
					description: c.description.clone(),
				})
				.collect(),
			output_columns: event
				.output()
				.iter()
				.map(|c| OperatorLibraryColumnInfo {
					name: c.name.clone(),
					field_type: c.field_type.clone(),
					description: c.description.clone(),
				})
				.collect(),
		});
	}
}
