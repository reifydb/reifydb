// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use reifydb_core::event::{EventListener, operator::OperatorLoadedEvent};
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::value::constraint::TypeConstraint;

#[derive(Clone, Debug)]
pub struct OperatorColumnInfo {
	pub name: String,
	pub field_type: TypeConstraint,
	pub description: String,
}

#[derive(Clone, Debug)]
pub struct OperatorInfo {
	pub operator: String,
	pub library_path: PathBuf,
	pub api: u32,
	pub capabilities: u32,
	pub input_columns: Vec<OperatorColumnInfo>,
	pub output_columns: Vec<OperatorColumnInfo>,
}

#[derive(Clone)]
pub struct OperatorStore {
	operators: Arc<RwLock<HashMap<String, OperatorInfo>>>,
}

impl Default for OperatorStore {
	fn default() -> Self {
		Self::new()
	}
}

impl OperatorStore {
	pub fn new() -> Self {
		Self {
			operators: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	pub fn add(&self, info: OperatorInfo) {
		self.operators.write().insert(info.operator.clone(), info);
	}

	pub fn list(&self) -> Vec<OperatorInfo> {
		self.operators.read().values().cloned().collect()
	}

	pub fn get(&self, operator: &str) -> Option<OperatorInfo> {
		self.operators.read().get(operator).cloned()
	}
}

pub struct OperatorEventListener {
	store: OperatorStore,
}

impl OperatorEventListener {
	pub fn new(store: OperatorStore) -> Self {
		Self {
			store,
		}
	}
}

impl EventListener<OperatorLoadedEvent> for OperatorEventListener {
	fn on(&self, event: &OperatorLoadedEvent) {
		self.store.add(OperatorInfo {
			operator: event.operator().clone(),
			library_path: event.library_path().clone(),
			api: *event.api(),
			capabilities: *event.capabilities(),
			input_columns: event
				.input()
				.iter()
				.map(|c| OperatorColumnInfo {
					name: c.name.clone(),
					field_type: c.field_type.clone(),
					description: c.description.clone(),
				})
				.collect(),
			output_columns: event
				.output()
				.iter()
				.map(|c| OperatorColumnInfo {
					name: c.name.clone(),
					field_type: c.field_type.clone(),
					description: c.description.clone(),
				})
				.collect(),
		});
	}
}
