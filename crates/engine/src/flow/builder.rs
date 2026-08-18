// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_value::config::Config;
use reifydb_value::Result;

use crate::flow::operator::BoxedOperator;

pub type OperatorFactory = Arc<dyn Fn(OperatorId, &Config) -> Result<BoxedOperator> + Send + Sync>;

#[derive(Clone)]
pub struct CustomOperators {
	inner: Arc<HashMap<String, OperatorFactory>>,
}

impl CustomOperators {
	pub fn new(map: HashMap<String, OperatorFactory>) -> Self {
		Self {
			inner: Arc::new(map),
		}
	}

	pub fn get(&self, name: &str) -> Option<&OperatorFactory> {
		self.inner.get(name)
	}
}

pub struct FlowConfigurator {
	operators_dir: Option<PathBuf>,
	custom_operators: HashMap<String, OperatorFactory>,
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

	pub fn register_operator(
		mut self,
		name: impl Into<String>,
		factory: impl Fn(OperatorId, &Config) -> Result<BoxedOperator> + Send + Sync + 'static,
	) -> Self {
		self.custom_operators.insert(name.into(), Arc::new(factory));
		self
	}

	pub fn configure(self) -> FlowConfig {
		FlowConfig {
			operators_dir: self.operators_dir,
			custom_operators: self.custom_operators,
		}
	}
}

pub struct FlowConfig {
	pub operators_dir: Option<PathBuf>,

	pub custom_operators: HashMap<String, OperatorFactory>,

}
