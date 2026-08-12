// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, path::PathBuf, sync::Arc};

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use reifydb_core::interface::flow::to_bitmask;
use reifydb_core::{event::operator::OperatorColumn, interface::catalog::flow::OperatorId};
use reifydb_flow::operator::BoxedHostOperator;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use reifydb_sdk::flow::operator::GuestOperator;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use reifydb_sdk::flow::operator::{OperatorMetadata, column::operator::OperatorColumn as SdkOperatorColumn};
use reifydb_value::{Result, config::Config};

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use crate::operator::mount::mount;

pub(crate) type OperatorFactory = Arc<dyn Fn(OperatorId, &Config) -> Result<BoxedHostOperator> + Send + Sync>;

#[derive(Clone)]
pub struct CustomOperatorEntry {
	pub factory: OperatorFactory,
	pub abi: Option<u32>,
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

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
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
		}
	}

	pub fn operators_dir(mut self, path: PathBuf) -> Self {
		self.operators_dir = Some(path);
		self
	}

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	pub fn register_operator<O>(mut self) -> Self
	where
		O: GuestOperator + OperatorMetadata + 'static,
	{
		self.custom_operators.insert(
			O::NAME.to_string(),
			CustomOperatorEntry {
				factory: Arc::new(|operator, config| {
					let logic = O::create(operator, config)?;
					Ok(mount(logic, operator, O::CAPABILITIES))
				}),
				abi: None,
				version: <O as OperatorMetadata>::VERSION.to_string(),
				description: <O as OperatorMetadata>::DESCRIPTION.to_string(),
				capabilities: to_bitmask(<O as OperatorMetadata>::CAPABILITIES),
				input: describe_columns(<O as OperatorMetadata>::INPUT_COLUMNS),
				output: describe_columns(<O as OperatorMetadata>::OUTPUT_COLUMNS),
			},
		);
		self
	}

	pub(crate) fn configure(self) -> FlowConfig {
		FlowConfig {
			operators_dir: self.operators_dir,
			custom_operators: CustomOperators::new(self.custom_operators),
		}
	}
}

pub struct FlowConfig {
	pub operators_dir: Option<PathBuf>,

	pub custom_operators: CustomOperators,
}
