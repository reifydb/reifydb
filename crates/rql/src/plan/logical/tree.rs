// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::iter::once;

use bumpalo::Bump;
use reifydb_core::{
	common::JoinType,
	error::diagnostic::operation::lookup_block_not_bare_from,
	expression::Expression,
	operator_with::{JoinWith, LookupWith},
};
use reifydb_value::{error, error::Error, fragment::Fragment};

use crate::{
	Result,
	ast::identifier::{
		MaybeQualifiedRingBufferIdentifier, MaybeQualifiedSeriesIdentifier, MaybeQualifiedTableIdentifier,
	},
	bump::{BumpBox, BumpFragment},
	error::RqlError,
	plan::logical::{
		AggregateNode, AppendNode, ApplyNode, AssertNode, DistinctNode, EnvironmentNode, ExtendNode,
		FilterNode, GateNode, GeneratorNode, InlineDataNode, LogicalPlan, MapNode, ObjectScanNode, OrderNode,
		PatchNode, RemoteScanNode, TakeNode, VariableSourceNode, query::window::WindowNode,
	},
};

type Input<'bump> = BumpBox<'bump, QueryTree<'bump>>;

#[derive(Debug)]
pub enum QueryTree<'bump> {
	SourceScan {
		node: ObjectScanNode,
	},
	RemoteScan {
		node: RemoteScanNode,
	},
	InlineData {
		node: InlineDataNode,
	},
	Generator {
		node: GeneratorNode<'bump>,
	},
	VariableSource {
		node: VariableSourceNode<'bump>,
	},
	Environment {
		node: EnvironmentNode,
	},
	Aggregate {
		node: AggregateNode,
		input: Input<'bump>,
	},
	Filter {
		node: FilterNode,
		input: Input<'bump>,
	},
	Gate {
		node: GateNode,
		input: Input<'bump>,
	},
	Order {
		node: OrderNode,
		input: Input<'bump>,
	},
	Distinct {
		node: DistinctNode<'bump>,
		input: Input<'bump>,
	},
	Take {
		node: TakeNode,
		input: Input<'bump>,
	},
	Assert {
		node: AssertNode,
		input: Option<Input<'bump>>,
	},
	Map {
		node: MapNode,
		input: Option<Input<'bump>>,
	},
	Extend {
		node: ExtendNode,
		input: Option<Input<'bump>>,
	},
	Patch {
		node: PatchNode,
		input: Option<Input<'bump>>,
	},
	Apply {
		node: ApplyNode<'bump>,
		input: Option<Input<'bump>>,
	},
	Window {
		node: WindowNode,
		input: Option<Input<'bump>>,
	},
	JoinInner {
		on: Vec<Expression>,
		alias: Option<BumpFragment<'bump>>,
		with: JoinWith,
		rql: String,
		input: Input<'bump>,
		right: Input<'bump>,
	},
	JoinLeft {
		on: Vec<Expression>,
		alias: Option<BumpFragment<'bump>>,
		with: JoinWith,
		rql: String,
		input: Input<'bump>,
		right: Input<'bump>,
	},
	JoinNatural {
		join_type: JoinType,
		fragment: BumpFragment<'bump>,
		alias: Option<BumpFragment<'bump>>,
		with: JoinWith,
		rql: String,
		input: Input<'bump>,
		right: Input<'bump>,
	},
	Lookup {
		join_type: JoinType,
		on: Vec<Expression>,
		alias: BumpFragment<'bump>,
		with: LookupWith,
		input: Input<'bump>,
		right: Input<'bump>,
	},
	AppendQuery {
		fragment: BumpFragment<'bump>,
		input: Input<'bump>,
		right: Input<'bump>,
	},
	DeleteTable {
		target: Option<MaybeQualifiedTableIdentifier<'bump>>,
		returning: Option<Vec<Expression>>,
		input: Option<Input<'bump>>,
	},
	DeleteRingBuffer {
		target: MaybeQualifiedRingBufferIdentifier<'bump>,
		returning: Option<Vec<Expression>>,
		input: Option<Input<'bump>>,
	},
	DeleteSeries {
		target: MaybeQualifiedSeriesIdentifier<'bump>,
		returning: Option<Vec<Expression>>,
		input: Option<Input<'bump>>,
	},
	Update {
		target: Option<MaybeQualifiedTableIdentifier<'bump>>,
		returning: Option<Vec<Expression>>,
		input: Input<'bump>,
	},
	UpdateRingBuffer {
		target: MaybeQualifiedRingBufferIdentifier<'bump>,
		returning: Option<Vec<Expression>>,
		input: Input<'bump>,
	},
	UpdateSeries {
		target: MaybeQualifiedSeriesIdentifier<'bump>,
		returning: Option<Vec<Expression>>,
		input: Input<'bump>,
	},
	Statement(LogicalPlan<'bump>),
}

pub fn fold<'bump>(
	bump: &'bump Bump,
	plans: impl IntoIterator<Item = LogicalPlan<'bump>>,
) -> Result<Option<QueryTree<'bump>>> {
	let mut roots: Vec<QueryTree<'bump>> = Vec::new();
	for plan in plans {
		if let LogicalPlan::Pipeline(pipeline) = plan {
			roots.extend(fold(bump, pipeline.steps)?);
			continue;
		}
		let tree = build(bump, plan, &mut roots)?;
		roots.push(tree);
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

fn build<'bump>(
	bump: &'bump Bump,
	plan: LogicalPlan<'bump>,
	roots: &mut Vec<QueryTree<'bump>>,
) -> Result<QueryTree<'bump>> {
	let mut optional = || roots.pop().map(|tree| BumpBox::new_in(tree, bump));
	Ok(match plan {
		LogicalPlan::SourceScan(node) => QueryTree::SourceScan {
			node,
		},
		LogicalPlan::RemoteScan(node) => QueryTree::RemoteScan {
			node,
		},
		LogicalPlan::InlineData(node) => QueryTree::InlineData {
			node,
		},
		LogicalPlan::Generator(node) => QueryTree::Generator {
			node,
		},
		LogicalPlan::VariableSource(node) => QueryTree::VariableSource {
			node,
		},
		LogicalPlan::Environment(node) => QueryTree::Environment {
			node,
		},
		LogicalPlan::Aggregate(node) => QueryTree::Aggregate {
			node,
			input: optional().ok_or_else(|| missing_input("aggregate"))?,
		},
		LogicalPlan::Filter(node) => QueryTree::Filter {
			node,
			input: optional().ok_or_else(|| missing_input("filter"))?,
		},
		LogicalPlan::Gate(node) => QueryTree::Gate {
			node,
			input: optional().ok_or_else(|| missing_input("gate"))?,
		},
		LogicalPlan::Order(node) => QueryTree::Order {
			node,
			input: optional().ok_or_else(|| missing_input("sort"))?,
		},
		LogicalPlan::Distinct(node) => QueryTree::Distinct {
			node,
			input: optional().ok_or_else(|| missing_input("distinct"))?,
		},
		LogicalPlan::Take(node) => QueryTree::Take {
			node,
			input: optional().ok_or_else(|| missing_input("take"))?,
		},
		LogicalPlan::Assert(node) => QueryTree::Assert {
			node,
			input: optional(),
		},
		LogicalPlan::Map(node) => QueryTree::Map {
			node,
			input: optional(),
		},
		LogicalPlan::Extend(node) => QueryTree::Extend {
			node,
			input: optional(),
		},
		LogicalPlan::Patch(node) => QueryTree::Patch {
			node,
			input: optional(),
		},
		LogicalPlan::Apply(node) => QueryTree::Apply {
			node,
			input: optional(),
		},
		LogicalPlan::Window(node) => QueryTree::Window {
			node,
			input: optional(),
		},
		LogicalPlan::JoinInner(node) => QueryTree::JoinInner {
			input: optional().ok_or_else(|| missing_input("join"))?,
			right: subtree(bump, node.subquery, "join")?,
			on: node.on,
			alias: node.alias,
			with: node.with,
			rql: node.rql,
		},
		LogicalPlan::JoinLeft(node) => QueryTree::JoinLeft {
			input: optional().ok_or_else(|| missing_input("join"))?,
			right: subtree(bump, node.subquery, "join")?,
			on: node.on,
			alias: node.alias,
			with: node.with,
			rql: node.rql,
		},
		LogicalPlan::JoinNatural(node) => QueryTree::JoinNatural {
			input: optional().ok_or_else(|| missing_input("join"))?,
			right: subtree(bump, node.subquery, "join")?,
			join_type: node.join_type,
			fragment: node.fragment,
			alias: node.alias,
			with: node.with,
			rql: node.rql,
		},
		LogicalPlan::Lookup(node) => {
			let input = optional().ok_or_else(|| missing_input("lookup"))?;
			let Some(right) = fold(bump, node.subquery)? else {
				return Err(error!(lookup_block_not_bare_from(node.alias.to_owned())));
			};
			QueryTree::Lookup {
				input,
				right: BumpBox::new_in(right, bump),
				join_type: node.join_type,
				on: node.on,
				alias: node.alias,
				with: node.with,
			}
		}
		LogicalPlan::Append(AppendNode::Query {
			fragment,
			with,
		}) => QueryTree::AppendQuery {
			input: optional().ok_or_else(|| missing_input("append"))?,
			right: subtree(bump, with, "append")?,
			fragment,
		},
		LogicalPlan::DeleteTable(node) => QueryTree::DeleteTable {
			input: own_or_pipeline(bump, node.input, optional, "delete")?,
			target: node.target,
			returning: node.returning,
		},
		LogicalPlan::DeleteRingBuffer(node) => QueryTree::DeleteRingBuffer {
			input: own_or_pipeline(bump, node.input, optional, "delete")?,
			target: node.target,
			returning: node.returning,
		},
		LogicalPlan::DeleteSeries(node) => QueryTree::DeleteSeries {
			input: own_or_pipeline(bump, node.input, optional, "delete")?,
			target: node.target,
			returning: node.returning,
		},
		LogicalPlan::Update(node) => QueryTree::Update {
			input: own_or_pipeline(bump, node.input, optional, "update")?
				.ok_or_else(|| missing_input("update"))?,
			target: node.target,
			returning: node.returning,
		},
		LogicalPlan::UpdateRingBuffer(node) => QueryTree::UpdateRingBuffer {
			input: own_or_pipeline(bump, node.input, optional, "update")?
				.ok_or_else(|| missing_input("update"))?,
			target: node.target,
			returning: node.returning,
		},
		LogicalPlan::UpdateSeries(node) => QueryTree::UpdateSeries {
			input: own_or_pipeline(bump, node.input, optional, "update")?
				.ok_or_else(|| missing_input("update"))?,
			target: node.target,
			returning: node.returning,
		},
		plan => QueryTree::Statement(plan),
	})
}

fn subtree<'bump>(
	bump: &'bump Bump,
	plans: impl IntoIterator<Item = LogicalPlan<'bump>>,
	operator: &'static str,
) -> Result<Input<'bump>> {
	let tree = fold(bump, plans)?.ok_or_else(|| missing_input(operator))?;
	Ok(BumpBox::new_in(tree, bump))
}

fn own_or_pipeline<'bump>(
	bump: &'bump Bump,
	own: Option<BumpBox<'bump, LogicalPlan<'bump>>>,
	pipeline: impl FnOnce() -> Option<Input<'bump>>,
	operator: &'static str,
) -> Result<Option<Input<'bump>>> {
	match own {
		Some(own) => Ok(Some(subtree(bump, once(BumpBox::into_inner(own)), operator)?)),
		None => Ok(pipeline()),
	}
}

fn missing_input(operator: &'static str) -> Error {
	RqlError::MissingInput {
		operator,
		fragment: Fragment::internal(operator),
	}
	.into()
}
