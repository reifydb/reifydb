// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use JoinType::{Inner, Left};
use reifydb_core::{
	JoinStrategy, JoinType,
	interface::{CommandTransaction, FlowNodeId},
};

use super::super::{
	CompileOperator, FlowCompiler, FlowNodeType,
	conversion::{to_owned_expressions, to_owned_physical_plan},
};
use crate::{
	Result,
	expression::Expression,
	plan::physical::{JoinInnerNode, JoinLeftNode, PhysicalPlan},
	query::QueryString,
};

pub(crate) struct JoinCompiler {
	pub join_type: JoinType,
	pub left: Box<PhysicalPlan<'static>>,
	pub right: Box<PhysicalPlan<'static>>,
	pub right_query: QueryString,
	pub on: Vec<Expression<'static>>,
	pub alias: Option<String>,
	pub strategy: JoinStrategy,
}

impl<'a> From<JoinInnerNode<'a>> for JoinCompiler {
	fn from(node: JoinInnerNode<'a>) -> Self {
		Self {
			join_type: Inner,
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
			right_query: node.right_query,
			on: to_owned_expressions(node.on),
			alias: node.alias.map(|f| f.text().to_string()),
			strategy: node.strategy,
		}
	}
}

impl<'a> From<JoinLeftNode<'a>> for JoinCompiler {
	fn from(node: JoinLeftNode<'a>) -> Self {
		Self {
			join_type: Left,
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
			right_query: node.right_query,
			on: to_owned_expressions(node.on),
			alias: node.alias.map(|f| f.text().to_string()),
			strategy: node.strategy,
		}
	}
}

impl<T: CommandTransaction> CompileOperator<T> for JoinCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		let left_node = compiler.compile_plan(*self.left)?;
		let right_node = compiler.compile_plan(*self.right)?;

		let (left_keys, right_keys) = extract_join_keys(&self.on);

		let node = compiler
			.build_node(FlowNodeType::Join {
				join_type: self.join_type,
				left: left_keys,
				right: right_keys,
				alias: self.alias,
				strategy: self.strategy,
				right_query: self.right_query,
			})
			.with_inputs([left_node, right_node])
			.build()?;

		Ok(node)
	}
}

// Extract the left and right column references from join conditions
pub(crate) fn extract_join_keys(
	conditions: &[Expression<'static>],
) -> (Vec<Expression<'static>>, Vec<Expression<'static>>) {
	let mut left_keys = Vec::new();
	let mut right_keys = Vec::new();

	for condition in conditions {
		match condition {
			Expression::Equal(eq) => {
				// For equality conditions, extract the left and
				// right expressions
				left_keys.push(*eq.left.clone());
				right_keys.push(*eq.right.clone());
			}
			// For now, we only support simple equality joins
			// More complex conditions could be added later
			_ => {
				// If it's not an equality, we'll add the whole
				// condition to both sides This maintains
				// backwards compatibility but may not work
				// correctly
				left_keys.push(condition.clone());
				right_keys.push(condition.clone());
			}
		}
	}

	(left_keys, right_keys)
}
