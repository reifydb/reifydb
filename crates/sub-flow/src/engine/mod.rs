// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod backfill;
mod eval;
mod partition;
mod process;
mod register;

use std::{
	collections::{HashMap, HashSet},
	fs::read_dir,
	path::PathBuf,
	sync::Arc,
};

use parking_lot::RwLock;
use reifydb_core::{
	CommitVersion, Error,
	event::{
		EventBus,
		flow::{FlowOperatorLoadedEvent, OperatorColumnDef},
	},
	interface::{FlowId, FlowNodeId, SourceId, TableId, ViewId},
};
use reifydb_engine::{StandardRowEvaluator, execute::Executor};
use reifydb_rql::flow::{Flow, FlowDependencyGraph, FlowGraphAnalyzer};
use reifydb_type::{Value, internal};
use tracing::{debug, error, instrument};

use crate::{
	ffi::loader::{ColumnDefInfo, ffi_operator_loader},
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
	pub(crate) event_bus: EventBus,
	pub(crate) flow_creation_versions: RwLock<HashMap<FlowId, CommitVersion>>,
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
	#[instrument(name = "flow::engine::new", level = "info", skip(evaluator, executor, registry, event_bus), fields(operators_dir = ?operators_dir))]
	pub fn new(
		evaluator: StandardRowEvaluator,
		executor: Executor,
		registry: TransformOperatorRegistry,
		event_bus: EventBus,
		operators_dir: Option<PathBuf>,
	) -> Self {
		// Load FFI operators if directory specified
		if let Some(dir) = operators_dir {
			if let Err(e) = Self::load_ffi_operators(&dir, &event_bus) {
				error!("Failed to load FFI operators from {:?}: {}", dir, e);
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
				event_bus,
				flow_creation_versions: RwLock::new(HashMap::new()),
			}),
		}
	}

	/// Load FFI operators from a directory into the global loader
	#[instrument(name = "flow::engine::load_ffi_operators", level = "debug", skip(event_bus), fields(dir = ?dir))]
	fn load_ffi_operators(dir: &PathBuf, event_bus: &EventBus) -> reifydb_core::Result<()> {
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

			// Register the operator without instantiating it
			let mut guard = loader.write();
			let info = match guard.register_operator(&path)? {
				Some(info) => info,
				None => {
					// Not a valid FFI operator, skip silently
					continue;
				}
			};

			debug!("Registered FFI operator: {} from {:?}", info.operator, path);

			// Convert column definitions to event format
			fn convert_column_defs(columns: &[ColumnDefInfo]) -> Vec<OperatorColumnDef> {
				columns.iter()
					.map(|c| OperatorColumnDef {
						name: c.name.clone(),
						field_type: c.field_type,
						description: c.description.clone(),
					})
					.collect()
			}

			// Emit event for loaded operator
			let event_bus = event_bus.clone();
			let event = FlowOperatorLoadedEvent {
				operator: info.operator,
				library_path: info.library_path,
				api: info.api,
				version: info.version,
				description: info.description,
				input: convert_column_defs(&info.input_columns),
				output: convert_column_defs(&info.output_columns),
				capabilities: info.capabilities,
			};
			// Only spawn if there's a tokio runtime available
			if let Ok(handle) = tokio::runtime::Handle::try_current() {
				handle.spawn(async move {
					event_bus.emit(event).await;
				});
			}
		}

		Ok(())
	}

	/// Create an FFI operator instance from the global singleton loader
	#[instrument(name = "flow::engine::create_ffi_operator", level = "debug", skip(self, config), fields(operator = %operator, node_id = ?node_id))]
	pub(crate) fn create_ffi_operator(
		&self,
		operator: &str,
		node_id: FlowNodeId,
		config: &HashMap<String, Value>,
	) -> crate::Result<BoxedOperator> {
		let loader = ffi_operator_loader();
		let mut loader_write = loader.write();

		// Serialize config to bincode
		let config_bytes = bincode::serde::encode_to_vec(config, bincode::config::standard())
			.map_err(|e| Error(internal!("Failed to serialize operator config: {:?}", e)))?;

		let operator = loader_write
			.create_operator_by_name(operator, node_id, &config_bytes)
			.map_err(|e| Error(internal!("Failed to create FFI operator: {:?}", e)))?;

		Ok(Box::new(operator))
	}

	/// Check if an operator name corresponds to an FFI operator
	pub(crate) fn is_ffi_operator(&self, operator: &str) -> bool {
		let loader = ffi_operator_loader();
		let loader_read = loader.read();
		loader_read.has_operator(operator)
	}

	pub fn has_registered_flows(&self) -> bool {
		!self.inner.flows.read().is_empty()
	}

	/// Returns a set of all currently registered flow IDs
	pub fn flow_ids(&self) -> HashSet<FlowId> {
		self.inner.flows.read().keys().copied().collect()
	}

	/// Clears all registered flows, operators, sources, sinks, dependency graph, and backfill versions
	pub fn clear(&self) {
		self.inner.operators.write().clear();
		self.inner.flows.write().clear();
		self.inner.sources.write().clear();
		self.inner.sinks.write().clear();
		self.inner.analyzer.write().clear();
		self.inner.flow_creation_versions.write().clear();
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
