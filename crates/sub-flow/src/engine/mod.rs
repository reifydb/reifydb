// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod process;
mod register;

use std::collections::HashMap;

use reifydb_core::interface::{FlowId, FlowNodeId, SourceId, Transaction};
use reifydb_engine::{StandardRowEvaluator, execute::Executor};
use reifydb_rql::flow::Flow;

use crate::operator::{Operators, transform::registry::TransformOperatorRegistry};

pub struct FlowEngine<T: Transaction> {
	evaluator: StandardRowEvaluator,
	executor: Executor,
	operators: HashMap<FlowNodeId, Operators<T>>,
	flows: HashMap<FlowId, Flow>,
	// Maps sources to specific nodes that listen to them
	// This allows multiple nodes in the same flow to listen to the same
	// source
	sources: HashMap<SourceId, Vec<(FlowId, FlowNodeId)>>,
	sinks: HashMap<SourceId, Vec<(FlowId, FlowNodeId)>>,
	registry: TransformOperatorRegistry<T>,
}

impl<T: Transaction> FlowEngine<T> {
	pub fn new(
		evaluator: StandardRowEvaluator,
		executor: Executor,
		registry: TransformOperatorRegistry<T>,
	) -> Self {
		Self {
			evaluator,
			executor,
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
