// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_core::interface::{FlowNodeId, Transaction, expression::Expression};

use crate::operator::Operator;

type OperatorFactoryFn<T> =
	Box<dyn Fn(FlowNodeId, &[Expression<'static>]) -> crate::Result<Box<dyn Operator<T>>> + Send + Sync>;

pub struct TransformOperatorRegistry<T: Transaction> {
	factories: HashMap<String, OperatorFactoryFn<T>>,
}

impl<T: Transaction> TransformOperatorRegistry<T> {
	pub fn new() -> Self {
		Self {
			factories: HashMap::new(),
		}
	}

	pub fn register<F>(&mut self, name: String, factory: F)
	where
		F: Fn(FlowNodeId, &[Expression<'static>]) -> crate::Result<Box<dyn Operator<T>>>
			+ Send
			+ Sync
			+ 'static,
	{
		self.factories.insert(name, Box::new(factory));
	}

	pub fn create_operator(
		&self,
		name: &str,
		node: FlowNodeId,
		expressions: &[Expression<'static>],
	) -> crate::Result<Box<dyn Operator<T>>> {
		let factory = self.factories.get(name).unwrap_or_else(|| panic!("Unknown operator: {}", name));

		factory(node, expressions)
	}

	pub fn with_builtins() -> Self {
		let mut registry = Self::new();

		// registry.register("counter".to_string(), |node, exprs| {
		// 	CounterOperator::create_from_expressions(node, exprs)
		// });
		//
		// registry.register("running_sum".to_string(), |node, exprs| {
		// 	RunningSumOperator::create_from_expressions(node, exprs)
		// });
		//
		// registry.register("running_avg".to_string(), |node, exprs| {
		// 	RunningAvgOperator::create_from_expressions(node, exprs)
		// });

		registry
	}
}
