// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod process;
mod register;

use std::{collections::HashMap, sync::Arc};

use reifydb_core::interface::{FlowId, FlowNodeId, SourceId, TableId, ViewId};
use reifydb_engine::{StandardRowEvaluator, execute::Executor};
use reifydb_rql::flow::{Flow, FlowDependencyGraph, FlowGraphAnalyzer};

use crate::operator::{Operators, transform::registry::TransformOperatorRegistry};

pub struct FlowEngine {
	evaluator: StandardRowEvaluator,
	executor: Executor,
	operators: HashMap<FlowNodeId, Arc<Operators>>,
	flows: HashMap<FlowId, Flow>,
	// Maps sources to specific nodes that listen to them
	// This allows multiple nodes in the same flow to listen to the same
	// source
	sources: HashMap<SourceId, Vec<(FlowId, FlowNodeId)>>,
	sinks: HashMap<SourceId, Vec<(FlowId, FlowNodeId)>>,
	registry: TransformOperatorRegistry,
	analyzer: FlowGraphAnalyzer,
}

impl FlowEngine {
	pub fn new(evaluator: StandardRowEvaluator, executor: Executor, registry: TransformOperatorRegistry) -> Self {
		Self {
			evaluator,
			executor,
			operators: HashMap::new(),
			flows: HashMap::new(),
			sources: HashMap::new(),
			sinks: HashMap::new(),
			registry,
			analyzer: FlowGraphAnalyzer::new(),
		}
	}

	pub fn has_registered_flows(&self) -> bool {
		!self.flows.is_empty()
	}

	pub fn analyzer(&self) -> &FlowGraphAnalyzer {
		&self.analyzer
	}

	/// Get the dependency graph for all registered flows
	pub fn get_dependency_graph(&self) -> &FlowDependencyGraph {
		self.analyzer.get_dependency_graph()
	}

	pub fn get_flows_depending_on_table(&self, table_id: TableId) -> Vec<FlowId> {
		let dependency_graph = self.get_dependency_graph();
		self.analyzer.get_flows_depending_on_table(dependency_graph, table_id)
	}

	pub fn get_flows_depending_on_view(&self, view_id: ViewId) -> Vec<FlowId> {
		let dependency_graph = self.get_dependency_graph();
		self.analyzer.get_flows_depending_on_view(dependency_graph, view_id)
	}

	pub fn get_flow_producing_view(&self, view_id: ViewId) -> Option<FlowId> {
		let dependency_graph = self.get_dependency_graph();
		self.analyzer.get_flow_producing_view(dependency_graph, view_id)
	}

	pub fn calculate_execution_order(&self) -> Vec<FlowId> {
		let dependency_graph = self.get_dependency_graph();
		self.analyzer.calculate_execution_order(dependency_graph)
	}
}
