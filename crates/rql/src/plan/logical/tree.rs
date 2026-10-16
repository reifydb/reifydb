// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use bumpalo::Bump;
use reifydb_value::fragment::Fragment;

use crate::{
	Result,
	bump::BumpBox,
	error::RqlError,
	plan::logical::{AppendNode, LogicalPlan},
};

#[derive(Debug)]
pub struct Tree<'bump> {
	pub plan: LogicalPlan<'bump>,
	pub input: Option<BumpBox<'bump, Tree<'bump>>>,
}

pub fn fold<'bump>(
	bump: &'bump Bump,
	plans: impl IntoIterator<Item = LogicalPlan<'bump>>,
) -> Result<Option<Tree<'bump>>> {
	let mut roots: Vec<Tree<'bump>> = Vec::new();
	for plan in plans {
		if let LogicalPlan::Pipeline(pipeline) = plan {
			roots.extend(fold(bump, pipeline.steps)?);
			continue;
		}
		let input = if takes_input(&plan) {
			roots.pop().map(|tree| BumpBox::new_in(tree, bump))
		} else {
			None
		};
		roots.push(Tree {
			plan,
			input,
		});
	}
	if roots.len() > 1 {
		return Err(RqlError::MissingSemicolon {
			fragment: Fragment::internal("compile_physical"),
			count: roots.len(),
		}
		.into());
	}
	Ok(roots.pop())
}

fn takes_input(plan: &LogicalPlan<'_>) -> bool {
	match plan {
		LogicalPlan::Aggregate(_)
		| LogicalPlan::Assert(_)
		| LogicalPlan::Filter(_)
		| LogicalPlan::Gate(_)
		| LogicalPlan::JoinInner(_)
		| LogicalPlan::JoinLeft(_)
		| LogicalPlan::JoinNatural(_)
		| LogicalPlan::Lookup(_)
		| LogicalPlan::Order(_)
		| LogicalPlan::Distinct(_)
		| LogicalPlan::Map(_)
		| LogicalPlan::Extend(_)
		| LogicalPlan::Patch(_)
		| LogicalPlan::Apply(_)
		| LogicalPlan::Take(_)
		| LogicalPlan::Window(_)
		| LogicalPlan::Append(AppendNode::Query {
			..
		}) => true,
		LogicalPlan::DeleteTable(node) => node.input.is_none(),
		LogicalPlan::DeleteRingBuffer(node) => node.input.is_none(),
		LogicalPlan::DeleteSeries(node) => node.input.is_none(),
		LogicalPlan::Update(node) => node.input.is_none(),
		LogicalPlan::UpdateRingBuffer(node) => node.input.is_none(),
		LogicalPlan::UpdateSeries(node) => node.input.is_none(),
		_ => false,
	}
}
