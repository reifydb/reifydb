// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, path::PathBuf, sync::Arc};

#[cfg(reifydb_target = "native")]
use reifydb_abi::operator::capabilities::to_bitmask;
use reifydb_core::{event::operator::OperatorColumn, interface::catalog::flow::OperatorId};
use reifydb_flow::operator::BoxedOperator;
#[cfg(reifydb_target = "native")]
use reifydb_sdk::operator::{OperatorLogic, OperatorMetadata, column::operator::OperatorColumn as SdkOperatorColumn};
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

pub(crate) type OperatorFactory = Arc<dyn Fn(OperatorId, &Config) -> Result<BoxedOperator> + Send + Sync>;

#[derive(Clone)]
pub struct CustomOperatorEntry {
	pub factory: OperatorFactory,
	pub api: u32,
	pub version: String,
	pub description: String,
	pub capabilities: u32,
	pub input: Vec<OperatorColumn>,
	pub output: Vec<OperatorColumn>,
}

#[derive(Clone, Default)]
pub struct CustomOperators {
	inner: Arc<HashMap<String, CustomOperatorEntry>>,
}

impl CustomOperators {
	pub(crate) fn new(map: HashMap<String, CustomOperatorEntry>) -> Self {
		Self {
			inner: Arc::new(map),
		}
	}

	pub(crate) fn get(&self, name: &str) -> Option<&OperatorFactory> {
		self.inner.get(name).map(|entry| &entry.factory)
	}

	pub(crate) fn iter(&self) -> impl Iterator<Item = (&String, &CustomOperatorEntry)> {
		self.inner.iter()
	}
}

#[cfg(reifydb_target = "native")]
fn describe_columns(columns: &[SdkOperatorColumn]) -> Vec<OperatorColumn> {
	columns.iter()
		.map(|column| OperatorColumn {
			name: column.name.to_string(),
			field_type: column.type_constraint.clone(),
			description: column.description.to_string(),
		})
		.collect()
}

pub struct FlowConfigurator {
	operators_dir: Option<PathBuf>,
	custom_operators: HashMap<String, CustomOperatorEntry>,
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
			CustomOperatorEntry {
				factory: Arc::new(|operator, config| {
					let logic = O::create(operator, config)?;
					let adapter = NativeOperatorAdapter::new(logic, operator, O::CAPABILITIES);
					let bridged: BoxedOperator = Box::new(NativeBridgedOperator::new(
						Box::new(adapter),
						operator,
						O::CAPABILITIES,
					));
					Ok(bridged)
				}),
				api: <O as OperatorMetadata>::API,
				version: <O as OperatorMetadata>::VERSION.to_string(),
				description: <O as OperatorMetadata>::DESCRIPTION.to_string(),
				capabilities: to_bitmask(<O as OperatorMetadata>::CAPABILITIES),
				input: describe_columns(<O as OperatorMetadata>::INPUT_COLUMNS),
				output: describe_columns(<O as OperatorMetadata>::OUTPUT_COLUMNS),
			},
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
