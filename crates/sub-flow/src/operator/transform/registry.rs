// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_core::interface::FlowNodeId;
use reifydb_rql::expression::Expression;

use crate::operator::Operator;

type OperatorFactoryFn =
	Box<dyn Fn(FlowNodeId, &[Expression<'static>]) -> crate::Result<Box<dyn Operator>> + Send + Sync>;

pub struct TransformOperatorRegistry {
	factories: HashMap<String, OperatorFactoryFn>,
}

impl TransformOperatorRegistry {
	pub fn new() -> Self {
		Self {
			factories: HashMap::new(),
		}
	}

	pub fn register<F>(&mut self, name: String, factory: F)
	where
		F: Fn(FlowNodeId, &[Expression<'static>]) -> crate::Result<Box<dyn Operator>> + Send + Sync + 'static,
	{
		self.factories.insert(name, Box::new(factory));
	}

	pub fn create_operator(
		&self,
		name: &str,
		node: FlowNodeId,
		expressions: &[Expression<'static>],
	) -> crate::Result<Box<dyn Operator>> {
		let factory = self.factories.get(name).unwrap_or_else(|| panic!("Unknown operator: {}", name));

		factory(node, expressions)
	}
}
