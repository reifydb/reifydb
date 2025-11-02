// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod process;
mod register;

use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use reifydb_core::interface::{FlowId, FlowNodeId, SourceId, TableId, ViewId};
use reifydb_engine::{StandardRowEvaluator, execute::Executor};
use reifydb_rql::flow::{Flow, FlowDependencyGraph, FlowGraphAnalyzer};

use crate::operator::{Operators, transform::registry::TransformOperatorRegistry};

struct FlowEngineInner {
	evaluator: StandardRowEvaluator,
	executor: Executor,
	registry: TransformOperatorRegistry,

	operators: RwLock<HashMap<FlowNodeId, Arc<Operators>>>,
	flows: RwLock<HashMap<FlowId, Flow>>,
	sources: RwLock<HashMap<SourceId, Vec<(FlowId, FlowNodeId)>>>,
	sinks: RwLock<HashMap<SourceId, Vec<(FlowId, FlowNodeId)>>>,
	analyzer: RwLock<FlowGraphAnalyzer>,
}

pub struct FlowEngine {
	inner: Arc<FlowEngineInner>,
}

impl Clone for FlowEngine {
	fn clone(&self) -> Self {
		Self {
			inner: Arc::clone(&self.inner),
		}
	}
}

impl FlowEngine {
	pub fn new(evaluator: StandardRowEvaluator, executor: Executor, registry: TransformOperatorRegistry) -> Self {
		Self {
			inner: Arc::new(FlowEngineInner {
				evaluator,
				executor,
				registry,
				operators: RwLock::new(HashMap::new()),
				flows: RwLock::new(HashMap::new()),
				sources: RwLock::new(HashMap::new()),
				sinks: RwLock::new(HashMap::new()),
				analyzer: RwLock::new(FlowGraphAnalyzer::new()),
			}),
		}
	}

	pub fn has_registered_flows(&self) -> bool {
		!self.inner.flows.read().is_empty()
	}

	pub fn get_dependency_graph(&self) -> FlowDependencyGraph {
		self.inner.analyzer.read().get_dependency_graph().clone()
	}

	pub fn get_flows_depending_on_table(&self, table_id: TableId) -> Vec<FlowId> {
		let analyzer = self.inner.analyzer.read();
		let dependency_graph = analyzer.get_dependency_graph();
		analyzer.get_flows_depending_on_table(dependency_graph, table_id)
	}

	pub fn get_flows_depending_on_view(&self, view_id: ViewId) -> Vec<FlowId> {
		let analyzer = self.inner.analyzer.read();
		let dependency_graph = analyzer.get_dependency_graph();
		analyzer.get_flows_depending_on_view(dependency_graph, view_id)
	}

	pub fn get_flow_producing_view(&self, view_id: ViewId) -> Option<FlowId> {
		let analyzer = self.inner.analyzer.read();
		let dependency_graph = analyzer.get_dependency_graph();
		analyzer.get_flow_producing_view(dependency_graph, view_id)
	}

	pub fn calculate_execution_order(&self) -> Vec<FlowId> {
		let analyzer = self.inner.analyzer.read();
		let dependency_graph = analyzer.get_dependency_graph();
		analyzer.calculate_execution_order(dependency_graph)
	}
}
