// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::HashMap,
	path::PathBuf,
	sync::{Arc, RwLock},
};

use reifydb_core::event::{EventListener, flow::FlowOperatorLoadedEvent};
use reifydb_type::value::constraint::TypeConstraint;

/// Information about a single column vtable in an operator
#[derive(Clone, Debug)]
pub struct SystemOperatorColumnInfo {
	pub name: String,
	pub field_type: TypeConstraint,
	pub description: String,
}

/// Cached information about a loaded flow operator
#[derive(Clone, Debug)]
pub struct SystemFlowOperatorInfo {
	pub operator: String,
	pub library_path: PathBuf,
	pub api: u32,
	pub capabilities: u32,
	pub input_columns: Vec<SystemOperatorColumnInfo>,
	pub output_columns: Vec<SystemOperatorColumnInfo>,
}

/// Thread-safe in-memory store for flow operator information
#[derive(Clone)]
pub struct SystemFlowOperatorStore {
	// Key: operator
	operators: Arc<RwLock<HashMap<String, SystemFlowOperatorInfo>>>,
}

impl Default for SystemFlowOperatorStore {
	fn default() -> Self {
		Self::new()
	}
}

impl SystemFlowOperatorStore {
	pub fn new() -> Self {
		Self {
			operators: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	pub fn add(&self, info: SystemFlowOperatorInfo) {
		self.operators.write().unwrap().insert(info.operator.clone(), info);
	}

	pub fn list(&self) -> Vec<SystemFlowOperatorInfo> {
		self.operators.read().unwrap().values().cloned().collect()
	}
}

/// Event listener that maintains the flow operator store
pub struct SystemFlowOperatorEventListener {
	store: SystemFlowOperatorStore,
}

impl SystemFlowOperatorEventListener {
	pub fn new(store: SystemFlowOperatorStore) -> Self {
		Self {
			store,
		}
	}
}

impl EventListener<FlowOperatorLoadedEvent> for SystemFlowOperatorEventListener {
	fn on(&self, event: &FlowOperatorLoadedEvent) {
		self.store.add(SystemFlowOperatorInfo {
			operator: event.operator().clone(),
			library_path: event.library_path().clone(),
			api: *event.api(),
			capabilities: *event.capabilities(),
			input_columns: event
				.input()
				.iter()
				.map(|c| SystemOperatorColumnInfo {
					name: c.name.clone(),
					field_type: c.field_type.clone(),
					description: c.description.clone(),
				})
				.collect(),
			output_columns: event
				.output()
				.iter()
				.map(|c| SystemOperatorColumnInfo {
					name: c.name.clone(),
					field_type: c.field_type.clone(),
					description: c.description.clone(),
				})
				.collect(),
		});
	}
}
