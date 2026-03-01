// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Builder pattern for configuring the flow subsystem

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_type::{Result, value::Value};

use crate::operator::BoxedOperator;

pub type OperatorFactory = Arc<dyn Fn(FlowNodeId, &HashMap<String, Value>) -> Result<BoxedOperator> + Send + Sync>;

pub struct FlowBuilder {
	operators_dir: Option<PathBuf>,
	num_workers: Option<usize>,
	custom_operators: HashMap<String, OperatorFactory>,
}

impl Default for FlowBuilder {
	fn default() -> Self {
		Self::new()
	}
}

impl FlowBuilder {
	/// Create a new FlowBuilder with default settings
	pub fn new() -> Self {
		Self {
			operators_dir: None,
			num_workers: None,
			custom_operators: HashMap::new(),
		}
	}

	/// Set the directory to scan for FFI operator shared libraries
	pub fn operators_dir(mut self, path: PathBuf) -> Self {
		self.operators_dir = Some(path);
		self
	}

	/// Set the number of worker threads for flow processing.
	/// Defaults to 1 if not set.
	pub fn num_workers(mut self, count: usize) -> Self {
		self.num_workers = Some(count);
		self
	}

	/// Register a native Rust operator factory by name.
	pub fn register_operator(
		mut self,
		name: impl Into<String>,
		factory: impl Fn(FlowNodeId, &HashMap<String, Value>) -> Result<BoxedOperator> + Send + Sync + 'static,
	) -> Self {
		self.custom_operators.insert(name.into(), Arc::new(factory));
		self
	}

	/// Build the configuration (internal use only)
	pub(crate) fn build_config(self) -> FlowBuilderConfig {
		FlowBuilderConfig {
			operators_dir: self.operators_dir,
			num_workers: self.num_workers.unwrap_or(1),
			custom_operators: self.custom_operators,
		}
	}
}

/// Configuration for FlowSubsystem
pub struct FlowBuilderConfig {
	/// Directory containing FFI operator shared libraries (native only)
	pub operators_dir: Option<PathBuf>,
	/// Number of worker threads for flow processing
	pub num_workers: usize,
	/// Native Rust operator factories registered via FlowBuilder::register_operator
	pub custom_operators: HashMap<String, OperatorFactory>,
}
