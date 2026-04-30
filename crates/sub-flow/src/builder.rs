// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	path::PathBuf,
	sync::Arc,
};

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_sdk::connector::{
	sink::{FFISink, FFISinkMetadata},
	source::{FFISource, FFISourceMetadata},
};
use reifydb_type::{Result, value::Value};

use crate::{connector::ConnectorRegistry, operator::BoxedOperator};

pub type OperatorFactory = Arc<dyn Fn(FlowNodeId, &BTreeMap<String, Value>) -> Result<BoxedOperator> + Send + Sync>;

pub struct FlowConfigurator {
	operators_dir: Option<PathBuf>,
	custom_operators: HashMap<String, OperatorFactory>,
	connector_registry: ConnectorRegistry,
}

impl Default for FlowConfigurator {
	fn default() -> Self {
		Self::new()
	}
}

impl FlowConfigurator {
	/// Create a new FlowConfigurator with default settings
	pub fn new() -> Self {
		Self {
			operators_dir: None,
			custom_operators: HashMap::new(),
			connector_registry: ConnectorRegistry::new(),
		}
	}

	/// Set the directory to scan for FFI operator shared libraries
	pub fn operators_dir(mut self, path: PathBuf) -> Self {
		self.operators_dir = Some(path);
		self
	}

	/// Register a native Rust operator factory by name.
	pub fn register_operator(
		mut self,
		name: impl Into<String>,
		factory: impl Fn(FlowNodeId, &BTreeMap<String, Value>) -> Result<BoxedOperator> + Send + Sync + 'static,
	) -> Self {
		self.custom_operators.insert(name.into(), Arc::new(factory));
		self
	}

	/// Register a native Rust source connector.
	pub fn register_source<S: FFISource + FFISourceMetadata>(mut self) -> Self {
		self.connector_registry.register_source::<S>();
		self
	}

	/// Register a native Rust sink connector.
	pub fn register_sink<S: FFISink + FFISinkMetadata>(mut self) -> Self {
		self.connector_registry.register_sink::<S>();
		self
	}

	/// Build the configuration (internal use only)
	pub(crate) fn configure(self) -> FlowConfig {
		FlowConfig {
			operators_dir: self.operators_dir,
			custom_operators: self.custom_operators,
			connector_registry: self.connector_registry,
		}
	}
}

/// Configuration for FlowSubsystem
pub struct FlowConfig {
	/// Directory containing FFI operator shared libraries (native only)
	pub operators_dir: Option<PathBuf>,
	/// Native Rust operator factories registered via FlowConfigurator::register_operator
	pub custom_operators: HashMap<String, OperatorFactory>,
	/// Registry of source and sink connectors
	pub connector_registry: ConnectorRegistry,
}
