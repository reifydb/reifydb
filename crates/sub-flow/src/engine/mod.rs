// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod eval;
mod process;
mod register;

use std::{
	collections::{HashMap, HashSet},
	fs::read_dir,
	path::PathBuf,
	sync::Arc,
};

use dashmap::DashMap;
use reifydb_catalog::Catalog;
use reifydb_core::{
	CommitVersion, Error,
	event::{
		EventBus,
		flow::{FlowOperatorLoadedEvent, OperatorColumnDef},
	},
	interface::{FlowId, FlowNodeId, PrimitiveId, TableId, ViewId},
};
use reifydb_engine::{StandardColumnEvaluator, execute::Executor};
use reifydb_rql::flow::{Flow, FlowDependencyGraph, FlowGraphAnalyzer};
use reifydb_type::{Value, internal};
use tokio::sync::RwLock;
use tracing::{debug, error, instrument};

use crate::{
	ffi::loader::{ColumnDefInfo, ffi_operator_loader},
	operator::{BoxedOperator, Operators, transform::registry::TransformOperatorRegistry},
};

pub(crate) struct FlowEngineInner {
	pub(crate) catalog: Catalog,
	pub(crate) evaluator: StandardColumnEvaluator,
	pub(crate) executor: Executor,
	pub(crate) registry: TransformOperatorRegistry,
	pub(crate) operators: DashMap<FlowNodeId, Arc<Operators>>,
	pub(crate) flows: RwLock<HashMap<FlowId, Flow>>,
	pub(crate) sources: DashMap<PrimitiveId, Vec<(FlowId, FlowNodeId)>>,
	pub(crate) sinks: DashMap<PrimitiveId, Vec<(FlowId, FlowNodeId)>>,
	pub(crate) analyzer: RwLock<FlowGraphAnalyzer>,
	#[allow(dead_code)]
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
	#[instrument(name = "flow::engine::new", level = "info", skip(catalog, evaluator, executor, registry, event_bus), fields(operators_dir = ?operators_dir))]
	pub fn new(
		catalog: Catalog,
		evaluator: StandardColumnEvaluator,
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
				catalog,
				evaluator,
				executor,
				registry,
				operators: DashMap::new(),
				flows: RwLock::new(HashMap::new()),
				sources: DashMap::new(),
				sinks: DashMap::new(),
				analyzer: RwLock::new(FlowGraphAnalyzer::new()),
				event_bus,
				flow_creation_versions: RwLock::new(HashMap::new()),
			}),
		}
	}

	/// Load FFI operators from a directory into the global loader.
	///
	/// This can be called at startup to eagerly load operators before any flows exist.
	#[instrument(name = "flow::engine::load_ffi_operators", level = "debug", skip(event_bus), fields(dir = ?dir))]
	pub fn load_ffi_operators(dir: &PathBuf, event_bus: &EventBus) -> reifydb_core::Result<()> {
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
			let mut guard = loader.write().unwrap();
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
		let mut loader_write = loader.write().unwrap();

		// Serialize config to postcard
		let config_bytes = postcard::to_stdvec(config)
			.map_err(|e| Error(internal!("Failed to serialize operator config: {:?}", e)))?;

		let operator = loader_write
			.create_operator_by_name(operator, node_id, &config_bytes)
			.map_err(|e| Error(internal!("Failed to create FFI operator: {:?}", e)))?;

		Ok(Box::new(operator))
	}

	/// Check if an operator name corresponds to an FFI operator
	pub(crate) fn is_ffi_operator(&self, operator: &str) -> bool {
		let loader = ffi_operator_loader();
		let loader_read = loader.read().unwrap();
		loader_read.has_operator(operator)
	}

	pub async fn has_registered_flows(&self) -> bool {
		!self.inner.flows.read().await.is_empty()
	}

	/// Returns a set of all currently registered flow IDs
	pub async fn flow_ids(&self) -> HashSet<FlowId> {
		self.inner.flows.read().await.keys().copied().collect()
	}

	/// Clears all registered flows, operators, sources, sinks, dependency graph, and backfill versions
	pub async fn clear(&self) {
		self.inner.operators.clear();
		self.inner.flows.write().await.clear();
		self.inner.sources.clear();
		self.inner.sinks.clear();
		self.inner.analyzer.write().await.clear();
		self.inner.flow_creation_versions.write().await.clear();
	}

	pub async fn get_dependency_graph(&self) -> FlowDependencyGraph {
		self.inner.analyzer.read().await.get_dependency_graph().clone()
	}

	pub async fn get_flows_depending_on_table(&self, table_id: TableId) -> Vec<FlowId> {
		let analyzer = self.inner.analyzer.read().await;
		let dependency_graph = analyzer.get_dependency_graph();
		analyzer.get_flows_depending_on_table(dependency_graph, table_id)
	}

	pub async fn get_flows_depending_on_view(&self, view_id: ViewId) -> Vec<FlowId> {
		let analyzer = self.inner.analyzer.read().await;
		let dependency_graph = analyzer.get_dependency_graph();
		analyzer.get_flows_depending_on_view(dependency_graph, view_id)
	}

	pub async fn get_flow_producing_view(&self, view_id: ViewId) -> Option<FlowId> {
		let analyzer = self.inner.analyzer.read().await;
		let dependency_graph = analyzer.get_dependency_graph();
		analyzer.get_flow_producing_view(dependency_graph, view_id)
	}

	pub async fn calculate_execution_order(&self) -> Vec<FlowId> {
		let analyzer = self.inner.analyzer.read().await;
		let dependency_graph = analyzer.get_dependency_graph();
		analyzer.calculate_execution_order(dependency_graph)
	}
}
