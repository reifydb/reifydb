// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod partition;
mod process;
mod register;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use parking_lot::RwLock;
use reifydb_core::{
	interface::{FlowId, FlowNodeId, SourceId, TableId, ViewId},
	log_error,
};
use reifydb_engine::{StandardRowEvaluator, execute::Executor};
use reifydb_rql::{
	expression::Expression,
	flow::{Flow, FlowDependencyGraph, FlowGraphAnalyzer},
};

use crate::{
	ffi::loader::FFIOperatorLoader,
	operator::{Operator, Operators, transform::registry::TransformOperatorRegistry},
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
	pub(crate) loader: RwLock<FFIOperatorLoader>,
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
		mut registry: TransformOperatorRegistry,
		operators_dir: Option<PathBuf>,
	) -> Self {
		// Load FFI operators if directory specified
		let ffi_loader = if let Some(dir) = operators_dir {
			match Self::load_ffi_operators(&dir, &mut registry) {
				Ok(loader) => loader,
				Err(e) => {
					log_error!("Failed to load FFI operators from {:?}: {}", dir, e);
					FFIOperatorLoader::new()
				}
			}
		} else {
			FFIOperatorLoader::new()
		};

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
				loader: RwLock::new(ffi_loader),
			}),
		}
	}

	/// Load FFI operators from a directory
	fn load_ffi_operators(
		dir: &PathBuf,
		registry: &mut TransformOperatorRegistry,
	) -> reifydb_core::Result<FFIOperatorLoader> {
		use std::ffi::CStr;

		let mut loader = FFIOperatorLoader::new();

		// Scan directory for shared libraries
		let entries = std::fs::read_dir(dir).unwrap();

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

			// Load the operator to get its descriptor
			// Use a temporary node ID just to extract the name
			let temp_operator = loader.load_operator(&path, &[], FlowNodeId(0)).unwrap();

			// Extract operator name from descriptor
			let name_cstr = unsafe { CStr::from_ptr(temp_operator.descriptor().operator_name) };
			let operator_name = name_cstr.to_str().unwrap().to_string();

			// Create factory closure for this operator
			let path_clone = path.clone();
			let factory = move |node_id: FlowNodeId, _exprs: &[Expression<'static>]| {
				// Load a fresh instance for this node
				let mut loader = FFIOperatorLoader::new();
				let operator = loader.load_operator(&path_clone, &[], node_id).unwrap();

				Ok(Box::new(operator) as Box<dyn Operator>)
			};

			// Register the factory
			registry.register(operator_name.clone(), factory);

			println!("Registered FFI operator: {} from {:?}", operator_name, path);
		}

		Ok(loader)
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
