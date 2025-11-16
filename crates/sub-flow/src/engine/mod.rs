// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod partition;
mod process;
mod register;

use std::{collections::HashMap, ffi::CStr, fs::read_dir, path::PathBuf, sync::Arc};

use parking_lot::RwLock;
use reifydb_core::{
	Error,
	interface::{FlowId, FlowNodeId, SourceId, TableId, ViewId},
	log_debug, log_error,
};
use reifydb_engine::{StandardRowEvaluator, execute::Executor};
use reifydb_rql::flow::{Flow, FlowDependencyGraph, FlowGraphAnalyzer};
use reifydb_type::internal;

use crate::{
	ffi::loader::ffi_operator_loader,
	operator::{BoxedOperator, Operators, transform::registry::TransformOperatorRegistry},
};

pub(crate) struct FlowEngineInner {
	pub(crate) evaluator: StandardRowEvaluator,
	pub(crate) executor: Executor,
	pub(crate) registry: TransformOperatorRegistry,
	pub(crate) operators: RwLock<HashMap<FlowNodeId, Arc<Operators>>>,
	pub(crate) flows: RwLock<HashMap<FlowId, Flow>>,
	pub(crate) sources: RwLock<HashMap<SourceId, Vec<(FlowId, FlowNodeId)>>>,
	pub(crate) sinks: RwLock<HashMap<SourceId, Vec<(FlowId, FlowNodeId)>>>,
	pub(crate) analyzer: RwLock<FlowGraphAnalyzer>,
}

pub struct FlowEngine {
	pub(crate) inner: Arc<FlowEngineInner>,
}

impl Clone for FlowEngine {
	fn clone(&self) -> Self {
		Self {
			inner: Arc::clone(&self.inner),
		}
	}
}

impl FlowEngine {
	pub fn new(
		evaluator: StandardRowEvaluator,
		executor: Executor,
		registry: TransformOperatorRegistry,
		operators_dir: Option<PathBuf>,
	) -> Self {
		// Load FFI operators if directory specified
		if let Some(dir) = operators_dir {
			if let Err(e) = Self::load_ffi_operators(&dir) {
				log_error!("Failed to load FFI operators from {:?}: {}", dir, e);
			}
		}

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

	/// Load FFI operators from a directory into the global loader
	fn load_ffi_operators(dir: &PathBuf) -> reifydb_core::Result<()> {
		let loader = ffi_operator_loader();

		// Scan directory for shared libraries
		let entries = read_dir(dir).unwrap();

		for entry in entries {
			let entry = entry.unwrap();
			let path = entry.path();

			if !path.is_file() {
				continue;
			}

			let is_shared_lib = path.extension().map_or(false, |ext| ext == "so" || ext == "dylib");
			if !is_shared_lib {
				continue;
			}

			// Load the operator to register it in the global loader
			// Use a temporary node ID just to extract the name
			let mut guard = loader.write();
			let temp_operator = guard.load_operator(&path, &[], FlowNodeId(0))?;

			// Extract operator name from descriptor
			let operator_name = unsafe {
				CStr::from_ptr(temp_operator.descriptor().operator_name).to_str().unwrap().to_string()
			};

			log_debug!("Registered FFI operator: {} from {:?}", operator_name, path);
		}

		Ok(())
	}

	/// Create an FFI operator instance from the global singleton loader
	pub(crate) fn create_ffi_operator(
		&self,
		operator_name: &str,
		node_id: FlowNodeId,
	) -> crate::Result<BoxedOperator> {
		let loader = ffi_operator_loader();
		let mut loader_write = loader.write();

		let operator = loader_write
			.create_operator_by_name(operator_name, node_id, &[])
			.map_err(|e| Error(internal!("Failed to create FFI operator: {:?}", e)))?;

		Ok(Box::new(operator))
	}

	/// Check if an operator name corresponds to an FFI operator
	pub(crate) fn is_ffi_operator(&self, operator_name: &str) -> bool {
		let loader = ffi_operator_loader();
		let loader_read = loader.read();
		loader_read.has_operator(operator_name)
	}

	pub fn has_registered_flows(&self) -> bool {
		!self.inner.flows.read().is_empty()
	}

	/// Clears all registered flows, operators, sources, sinks, and dependency graph
	pub fn clear(&self) {
		self.inner.operators.write().clear();
		self.inner.flows.write().clear();
		self.inner.sources.write().clear();
		self.inner.sinks.write().clear();
		self.inner.analyzer.write().clear();
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
