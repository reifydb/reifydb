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
	pub fn new() -> Self {
		Self {
			operators_dir: None,
			custom_operators: HashMap::new(),
			connector_registry: ConnectorRegistry::new(),
		}
	}

	pub fn operators_dir(mut self, path: PathBuf) -> Self {
		self.operators_dir = Some(path);
		self
	}

	pub fn register_operator(
		mut self,
		name: impl Into<String>,
		factory: impl Fn(FlowNodeId, &BTreeMap<String, Value>) -> Result<BoxedOperator> + Send + Sync + 'static,
	) -> Self {
		self.custom_operators.insert(name.into(), Arc::new(factory));
		self
	}

	pub fn register_source<S: FFISource + FFISourceMetadata>(mut self) -> Self {
		self.connector_registry.register_source::<S>();
		self
	}

	pub fn register_sink<S: FFISink + FFISinkMetadata>(mut self) -> Self {
		self.connector_registry.register_sink::<S>();
		self
	}

	pub(crate) fn configure(self) -> FlowConfig {
		FlowConfig {
			operators_dir: self.operators_dir,
			custom_operators: self.custom_operators,
			connector_registry: self.connector_registry,
		}
	}
}

pub struct FlowConfig {
	pub operators_dir: Option<PathBuf>,

	pub custom_operators: HashMap<String, OperatorFactory>,

	pub connector_registry: ConnectorRegistry,
}
