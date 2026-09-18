// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, path::PathBuf, sync::Arc};

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use reifydb_core::interface::flow::to_bitmask;
use reifydb_core::{
	common::OperatorClass, event::operator::OperatorColumn, interface::catalog::flow::OperatorId,
	operator_with::ApplyWith,
};
use reifydb_flow::operator::BoxedHostOperator;
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use reifydb_sdk::flow::operator::{
	ManagedMount, ManagedOperator, MountedOperator, NostateMount, NostateOperator, UnmanagedMount,
	UnmanagedOperator, WindowedDriver, context::ClassValue,
};
#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use reifydb_sdk::flow::operator::{OperatorMetadata, column::operator::OperatorColumn as SdkOperatorColumn};
use reifydb_value::{Result, config::ExtensionParams};

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
use crate::operator::mount::mount;

pub(crate) type OperatorFactory =
	Arc<dyn Fn(OperatorId, &ExtensionParams, &ApplyWith) -> Result<BoxedHostOperator> + Send + Sync>;

#[derive(Clone)]
pub struct CustomOperatorEntry {
	pub factory: OperatorFactory,
	pub abi: Option<u32>,
	pub version: String,
	pub description: String,
	pub capabilities: u32,
	pub input: Vec<OperatorColumn>,
	pub output: Vec<OperatorColumn>,
	pub class: OperatorClass,
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
	pub fn register_managed_operator<T>(self) -> Self
	where
		T: ManagedOperator + 'static,
	{
		self.register_mounted::<ManagedMount<T>>()
	}

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	pub fn register_unmanaged_operator<T>(self) -> Self
	where
		T: UnmanagedOperator + 'static,
	{
		self.register_mounted::<UnmanagedMount<T>>()
	}

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	pub fn register_nostate_operator<T>(self) -> Self
	where
		T: NostateOperator + 'static,
	{
		self.register_mounted::<NostateMount<T>>()
	}

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	pub fn register_windowed_operator<T>(self) -> Self
	where
		T: WindowedDriver + 'static,
	{
		self.register_mounted::<T>()
	}

	#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
	fn register_mounted<M>(mut self) -> Self
	where
		M: MountedOperator + OperatorMetadata + 'static,
	{
		self.custom_operators.insert(
			M::NAME.to_string(),
			CustomOperatorEntry {
				factory: Arc::new(|operator, params, with| {
					let logic = M::create(operator, params, with)?;
					Ok(mount(logic, operator, M::CAPABILITIES))
				}),
				abi: None,
				version: <M as OperatorMetadata>::VERSION.to_string(),
				description: <M as OperatorMetadata>::DESCRIPTION.to_string(),
				capabilities: to_bitmask(<M as OperatorMetadata>::CAPABILITIES),
				input: describe_columns(<M as OperatorMetadata>::INPUT_COLUMNS),
				output: describe_columns(<M as OperatorMetadata>::OUTPUT_COLUMNS),
				class: <M::Class as ClassValue>::CLASS,
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
