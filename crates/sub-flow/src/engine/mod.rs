// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod process;
mod register;

use std::collections::HashMap;

use reifydb_core::{
	flow::Flow,
	interface::{FlowId, FlowNodeId, SourceId, Transaction},
};
use reifydb_engine::StandardEvaluator;

use crate::operator::{
	Operators, stateful::registry::StatefulOperatorRegistry,
};

pub struct FlowEngine<T: Transaction> {
	evaluator: StandardEvaluator,
	operators: HashMap<FlowNodeId, Operators<T>>,
	flows: HashMap<FlowId, Flow>,
	// Maps sources to specific nodes that listen to them
	// This allows multiple nodes in the same flow to listen to the same
	// source
	sources: HashMap<SourceId, Vec<(FlowId, FlowNodeId)>>,
	sinks: HashMap<SourceId, Vec<(FlowId, FlowNodeId)>>,
	registry: StatefulOperatorRegistry<T>,
}

impl<T: Transaction> FlowEngine<T> {
	pub fn new(evaluator: StandardEvaluator) -> Self {
		Self {
			evaluator,
			operators: HashMap::new(),
			flows: HashMap::new(),
			sources: HashMap::new(),
			sinks: HashMap::new(),
			registry: StatefulOperatorRegistry::with_builtins(),
		}
	}

	pub fn with_registry(
		evaluator: StandardEvaluator,
		registry: StatefulOperatorRegistry<T>,
	) -> Self {
		Self {
			evaluator,
			operators: HashMap::new(),
			flows: HashMap::new(),
			sources: HashMap::new(),
			sinks: HashMap::new(),
			registry,
		}
	}

	pub fn has_registered_flows(&self) -> bool {
		!self.flows.is_empty()
	}
}
