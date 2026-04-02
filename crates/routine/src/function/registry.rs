// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Deref, sync::Arc};

use super::{Function, FunctionCapability};

#[derive(Clone)]
pub struct Functions(Arc<FunctionsInner>);

impl Functions {
	pub fn empty() -> Functions {
		Functions::builder().configure()
	}

	pub fn builder() -> FunctionsConfigurator {
		FunctionsConfigurator(FunctionsInner {
			functions: HashMap::new(),
		})
	}
}

impl Deref for Functions {
	type Target = FunctionsInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[derive(Clone)]
pub struct FunctionsInner {
	pub(crate) functions: HashMap<String, Arc<dyn Function>>,
}

impl FunctionsInner {
	pub fn get(&self, name: &str) -> Option<Arc<dyn Function>> {
		self.functions.get(name).cloned()
	}

	pub fn get_scalar(&self, name: &str) -> Option<Arc<dyn Function>> {
		self.functions.get(name).cloned().filter(|f| f.capabilities().contains(&FunctionCapability::Scalar))
	}

	pub fn get_aggregate(&self, name: &str) -> Option<Arc<dyn Function>> {
		self.functions.get(name).cloned().filter(|f| f.capabilities().contains(&FunctionCapability::Aggregate))
	}

	pub fn get_generator(&self, name: &str) -> Option<Arc<dyn Function>> {
		self.functions.get(name).cloned().filter(|f| f.capabilities().contains(&FunctionCapability::Generator))
	}

	pub fn scalar_names(&self) -> Vec<&str> {
		self.functions
			.iter()
			.filter(|(_, f)| f.capabilities().contains(&FunctionCapability::Scalar))
			.map(|(s, _)| s.as_str())
			.collect()
	}

	pub fn aggregate_names(&self) -> Vec<&str> {
		self.functions
			.iter()
			.filter(|(_, f)| f.capabilities().contains(&FunctionCapability::Aggregate))
			.map(|(s, _)| s.as_str())
			.collect()
	}

	pub fn generator_names(&self) -> Vec<&str> {
		self.functions
			.iter()
			.filter(|(_, f)| f.capabilities().contains(&FunctionCapability::Generator))
			.map(|(s, _)| s.as_str())
			.collect()
	}
}

pub struct FunctionsConfigurator(FunctionsInner);

impl FunctionsConfigurator {
	pub fn register_function(mut self, func: Arc<dyn Function>) -> Self {
		self.0.functions.insert(func.info().name.clone(), func);
		self
	}

	pub fn register_alias(mut self, alias: &str, canonical: &str) -> Self {
		if let Some(func) = self.0.functions.get(canonical).cloned() {
			self.0.functions.insert(alias.to_string(), func);
		}
		self
	}

	pub fn configure(self) -> Functions {
		Functions(Arc::new(self.0))
	}
}
