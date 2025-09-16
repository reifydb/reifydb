// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::HashMap, ops::Deref, sync::Arc};

use crate::function::{AggregateFunction, ScalarFunction};

#[derive(Clone)]
pub struct Functions(Arc<FunctionsInner>);

impl Functions {
	pub fn builder() -> FunctionsBuilder {
		FunctionsBuilder(FunctionsInner {
			scalars: HashMap::new(),
			aggregates: HashMap::new(),
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
	scalars: HashMap<
		String,
		Arc<dyn Fn() -> Box<dyn ScalarFunction> + Send + Sync>,
	>,
	aggregates: HashMap<
		String,
		Arc<dyn Fn() -> Box<dyn AggregateFunction> + Send + Sync>,
	>,
}

impl FunctionsInner {
	pub fn get_aggregate(
		&self,
		name: &str,
	) -> Option<Box<dyn AggregateFunction>> {
		self.aggregates.get(name).map(|func| func())
	}

	pub fn get_scalar(
		&self,
		name: &str,
	) -> Option<Box<dyn ScalarFunction>> {
		self.scalars.get(name).map(|func| func())
	}
}

pub struct FunctionsBuilder(FunctionsInner);

impl FunctionsBuilder {
	pub fn register_scalar<F, A>(mut self, name: &str, init: F) -> Self
	where
		F: Fn() -> A + Send + Sync + 'static,
		A: ScalarFunction + 'static,
	{
		self.0.scalars.insert(
			name.to_string(),
			Arc::new(move || {
				Box::new(init()) as Box<dyn ScalarFunction>
			}),
		);

		self
	}

	pub fn register_aggregate<F, A>(mut self, name: &str, init: F) -> Self
	where
		F: Fn() -> A + Send + Sync + 'static,
		A: AggregateFunction + 'static,
	{
		self.0.aggregates.insert(
			name.to_string(),
			Arc::new(move || {
				Box::new(init()) as Box<dyn AggregateFunction>
			}),
		);

		self
	}

	pub fn build(self) -> Functions {
		Functions(Arc::new(self.0))
	}
}
