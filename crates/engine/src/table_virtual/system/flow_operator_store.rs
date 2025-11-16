// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use parking_lot::RwLock;
use reifydb_core::event::{EventListener, flow::FlowOperatorLoadedEvent};

/// Cached information about a loaded flow operator
#[derive(Clone, Debug)]
pub struct FlowOperatorInfo {
	pub operator_name: String,
	pub library_path: PathBuf,
	pub api_version: u32,
}

/// Thread-safe in-memory store for flow operator information
#[derive(Clone)]
pub struct FlowOperatorStore {
	// Key: operator_name
	operators: Arc<RwLock<HashMap<String, FlowOperatorInfo>>>,
}

impl FlowOperatorStore {
	pub fn new() -> Self {
		Self {
			operators: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	pub fn add(&self, info: FlowOperatorInfo) {
		self.operators.write().insert(info.operator_name.clone(), info);
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
			operator_name: event.operator_name.clone(),
			library_path: event.library_path.clone(),
			api_version: event.api_version,
		});
	}
}
