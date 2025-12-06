// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use parking_lot::RwLock;
use reifydb_core::event::{EventListener, flow::FlowOperatorLoadedEvent};
use reifydb_type::TypeConstraint;

/// Information about a single column definition in an operator
#[derive(Clone, Debug)]
pub struct OperatorColumnInfo {
	pub name: String,
	pub field_type: TypeConstraint,
	pub description: String,
}

/// Cached information about a loaded flow operator
#[derive(Clone, Debug)]
pub struct FlowOperatorInfo {
	pub operator: String,
	pub library_path: PathBuf,
	pub api: u32,
	pub input_columns: Vec<OperatorColumnInfo>,
	pub output_columns: Vec<OperatorColumnInfo>,
}

/// Thread-safe in-memory store for flow operator information
#[derive(Clone)]
pub struct FlowOperatorStore {
	// Key: operator
	operators: Arc<RwLock<HashMap<String, FlowOperatorInfo>>>,
}

impl FlowOperatorStore {
	pub fn new() -> Self {
		Self {
			operators: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	pub fn add(&self, info: FlowOperatorInfo) {
		self.operators.write().insert(info.operator.clone(), info);
	}

	pub fn list(&self) -> Vec<FlowOperatorInfo> {
		self.operators.read().values().cloned().collect()
	}
}

/// Event listener that maintains the flow operator store
pub struct FlowOperatorEventListener {
	store: FlowOperatorStore,
}

impl FlowOperatorEventListener {
	pub fn new(store: FlowOperatorStore) -> Self {
		Self {
			store,
		}
	}
}

impl EventListener<FlowOperatorLoadedEvent> for FlowOperatorEventListener {
	fn on(&self, event: &FlowOperatorLoadedEvent) {
		self.store.add(FlowOperatorInfo {
			operator: event.operator.clone(),
			library_path: event.library_path.clone(),
			api: event.api,
			input_columns: event
				.input
				.iter()
				.map(|c| OperatorColumnInfo {
					name: c.name.clone(),
					field_type: c.field_type,
					description: c.description.clone(),
				})
				.collect(),
			output_columns: event
				.output
				.iter()
				.map(|c| OperatorColumnInfo {
					name: c.name.clone(),
					field_type: c.field_type,
					description: c.description.clone(),
				})
				.collect(),
		});
	}
}
