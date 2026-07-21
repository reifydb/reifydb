// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use reifydb_core::interface::catalog::flow::FlowNodeId;
#[cfg(reifydb_target = "native")]
use reifydb_sdk::operator::{OperatorLogic, OperatorMetadata};
use reifydb_sdk::{
	config::Config,
	connector::{
		sink::{FFISink, FFISinkMetadata},
		source::{FFISource, FFISourceMetadata},
	},
};
use reifydb_value::Result;

use crate::connector::ConnectorRegistry;
#[cfg(reifydb_target = "native")]
use crate::operator::native::{NativeBridgedOperator, NativeOperatorAdapter};
use reifydb_flow::operator::BoxedOperator;

pub(crate) type OperatorFactory = Arc<dyn Fn(FlowNodeId, &Config) -> Result<BoxedOperator> + Send + Sync>;

#[derive(Clone, Default)]
pub struct CustomOperators {
	inner: Arc<HashMap<String, OperatorFactory>>,
}

impl CustomOperators {
	pub(crate) fn new(map: HashMap<String, OperatorFactory>) -> Self {
		Self {
			inner: Arc::new(map),
		}
	}

	pub(crate) fn get(&self, name: &str) -> Option<&OperatorFactory> {
		self.inner.get(name)
	}
}

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

	#[cfg(reifydb_target = "native")]
	pub fn register_operator<O>(mut self) -> Self
	where
		O: OperatorLogic + OperatorMetadata + 'static,
	{
		self.custom_operators.insert(
			O::NAME.to_string(),
			Arc::new(|node, config| {
				let logic = O::create(node, config)?;
				let adapter = NativeOperatorAdapter::new(logic, node, O::CAPABILITIES);
				let bridged: BoxedOperator =
					Box::new(NativeBridgedOperator::new(Box::new(adapter), node, O::CAPABILITIES));
				Ok(bridged)
			}),
		);
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
			custom_operators: CustomOperators::new(self.custom_operators),
			connector_registry: self.connector_registry,
		}
	}
}

pub struct FlowConfig {
	pub operators_dir: Option<PathBuf>,

	pub custom_operators: CustomOperators,

	pub connector_registry: ConnectorRegistry,
}
