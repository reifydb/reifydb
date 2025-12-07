// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use JoinType::{Inner, Left};
use reifydb_core::{
	JoinType,
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
};

pub(crate) struct JoinCompiler {
	pub join_type: JoinType,
	pub left: Box<PhysicalPlan<'static>>,
	pub right: Box<PhysicalPlan<'static>>,
	pub on: Vec<Expression<'static>>,
	pub alias: Option<String>,
}

impl<'a> From<JoinInnerNode<'a>> for JoinCompiler {
	fn from(node: JoinInnerNode<'a>) -> Self {
		Self {
			join_type: Inner,
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
			on: to_owned_expressions(node.on),
			alias: node.alias.map(|f| f.text().to_string()),
		}
	}
}

impl<'a> From<JoinLeftNode<'a>> for JoinCompiler {
	fn from(node: JoinLeftNode<'a>) -> Self {
		Self {
			join_type: Left,
			left: Box::new(to_owned_physical_plan(*node.left)),
			right: Box::new(to_owned_physical_plan(*node.right)),
			on: to_owned_expressions(node.on),
			alias: node.alias.map(|f| f.text().to_string()),
		}
	}
}

// Extract the source name from a physical plan if it's a scan node
fn extract_source_name(plan: &PhysicalPlan) -> Option<String> {
	match plan {
		PhysicalPlan::TableScan(node) => Some(node.source.def().name.clone()),
		PhysicalPlan::ViewScan(node) => Some(node.source.def().name.clone()),
		PhysicalPlan::RingBufferScan(node) => Some(node.source.def().name.clone()),
		PhysicalPlan::DictionaryScan(node) => Some(node.source.def().name.clone()),
		// For other node types, try to recursively find the source
		PhysicalPlan::Filter(node) => extract_source_name(&node.input),
		PhysicalPlan::Map(node) => node.input.as_ref().and_then(|p| extract_source_name(p)),
		PhysicalPlan::Take(node) => extract_source_name(&node.input),
		_ => None,
	}
}

impl<T: CommandTransaction> CompileOperator<T> for JoinCompiler {
	fn compile(self, compiler: &mut FlowCompiler<T>) -> Result<FlowNodeId> {
		// Extract source name from right plan for fallback alias
		let source_name = extract_source_name(&self.right);

		let left_node = compiler.compile_plan(*self.left)?;
		let right_node = compiler.compile_plan(*self.right)?;

		let (left_keys, right_keys) = extract_join_keys(&self.on);

		// Use explicit alias, or fall back to extracted source name, or use "other"
		let effective_alias = self.alias.or(source_name).or_else(|| Some("other".to_string()));

		let node = compiler
			.build_node(FlowNodeType::Join {
				join_type: self.join_type,
				left: left_keys,
				right: right_keys,
				alias: effective_alias,
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
