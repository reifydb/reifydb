// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

pub mod enforce;
pub mod error;
pub mod evaluate;

use bumpalo::{Bump, collections::Vec as BumpVec};
use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::{
	catalog::policy::{DataOp, Policy, PolicyOperation, PolicyTargetType},
	resolved::ResolvedObject,
};
use reifydb_rql::{
	ast::parse_str,
	bump::BumpBox,
	expression::{ConstantExpression, Expression},
	plan::logical::{
		AppendNode, AppendSourcePlan, AssignValue, ElseIfBranch, FilterNode, LetValue, LogicalPlan,
		ObjectScanNode, PipelineNode, compile_logical, function::ReturnValue,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, fragment::Fragment};
use tracing::instrument;

#[instrument(name = "policy::inject", level = "debug", skip_all)]
pub fn inject_from_policies<'a>(
	plans: BumpVec<'a, LogicalPlan<'a>>,
	bump: &'a Bump,
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
) -> Result<BumpVec<'a, LogicalPlan<'a>>> {
	let identity = tx.identity();

	if identity.is_privileged() {
		return Ok(plans);
	}

	inject_plans(plans, bump, catalog, tx)
}

fn inject_plans<'a>(
	plans: BumpVec<'a, LogicalPlan<'a>>,
	bump: &'a Bump,
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
) -> Result<BumpVec<'a, LogicalPlan<'a>>> {
	let mut result = BumpVec::with_capacity_in(plans.len() + 4, bump);
	for plan in plans {
		match plan {
			scan @ LogicalPlan::SourceScan(_) => {
				inject_scan_with_policies(scan, &mut result, bump, catalog, tx)?
			}
			other => result.push(inject_plan(other, bump, catalog, tx)?),
		}
	}
	Ok(result)
}

fn inject_plan<'a>(
	plan: LogicalPlan<'a>,
	bump: &'a Bump,
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
) -> Result<LogicalPlan<'a>> {
	match plan {
		scan @ LogicalPlan::SourceScan(_) => {
			let mut steps = BumpVec::with_capacity_in(4, bump);
			inject_scan_with_policies(scan, &mut steps, bump, catalog, tx)?;
			Ok(LogicalPlan::Pipeline(PipelineNode {
				steps,
			}))
		}
		LogicalPlan::Pipeline(pipeline) => {
			let steps = inject_plans(pipeline.steps, bump, catalog, tx)?;
			Ok(LogicalPlan::Pipeline(PipelineNode {
				steps,
			}))
		}
		LogicalPlan::Declare(mut node) => {
			node.value = match node.value {
				LetValue::Statement(plans) => {
					LetValue::Statement(inject_plans(plans, bump, catalog, tx)?)
				}
				value => value,
			};
			Ok(LogicalPlan::Declare(node))
		}
		LogicalPlan::Assign(mut node) => {
			node.value = match node.value {
				AssignValue::Statement(plans) => {
					AssignValue::Statement(inject_plans(plans, bump, catalog, tx)?)
				}
				value => value,
			};
			Ok(LogicalPlan::Assign(node))
		}
		LogicalPlan::Append(AppendNode::IntoVariable {
			target,
			source: AppendSourcePlan::Statement(plans),
		}) => Ok(LogicalPlan::Append(AppendNode::IntoVariable {
			target,
			source: AppendSourcePlan::Statement(inject_plans(plans, bump, catalog, tx)?),
		})),
		LogicalPlan::Append(AppendNode::Query {
			fragment,
			with,
		}) => Ok(LogicalPlan::Append(AppendNode::Query {
			fragment,
			with: inject_plans(with, bump, catalog, tx)?,
		})),
		LogicalPlan::Conditional(mut node) => {
			node.then_branch = inject_boxed(node.then_branch, bump, catalog, tx)?;
			node.else_ifs = node
				.else_ifs
				.into_iter()
				.map(|branch| {
					Ok(ElseIfBranch {
						condition: branch.condition,
						then_branch: inject_boxed(branch.then_branch, bump, catalog, tx)?,
					})
				})
				.collect::<Result<_>>()?;
			node.else_branch =
				node.else_branch.map(|branch| inject_boxed(branch, bump, catalog, tx)).transpose()?;
			Ok(LogicalPlan::Conditional(node))
		}
		LogicalPlan::Loop(mut node) => {
			node.body = inject_bodies(node.body, bump, catalog, tx)?;
			Ok(LogicalPlan::Loop(node))
		}
		LogicalPlan::While(mut node) => {
			node.body = inject_bodies(node.body, bump, catalog, tx)?;
			Ok(LogicalPlan::While(node))
		}
		LogicalPlan::For(mut node) => {
			node.iterable = inject_plans(node.iterable, bump, catalog, tx)?;
			node.body = inject_bodies(node.body, bump, catalog, tx)?;
			Ok(LogicalPlan::For(node))
		}
		LogicalPlan::DefineFunction(mut node) => {
			node.body = inject_bodies(node.body, bump, catalog, tx)?;
			Ok(LogicalPlan::DefineFunction(node))
		}
		LogicalPlan::DefineClosure(mut node) => {
			node.body = inject_bodies(node.body, bump, catalog, tx)?;
			Ok(LogicalPlan::DefineClosure(node))
		}
		LogicalPlan::Return(mut node) => {
			node.value = match node.value {
				Some(ReturnValue::Statement(plans)) => {
					Some(ReturnValue::Statement(inject_plans(plans, bump, catalog, tx)?))
				}
				value => value,
			};
			Ok(LogicalPlan::Return(node))
		}
		LogicalPlan::Scalarize(mut node) => {
			node.input = inject_boxed(node.input, bump, catalog, tx)?;
			Ok(LogicalPlan::Scalarize(node))
		}
		LogicalPlan::JoinInner(mut node) => {
			node.with = inject_plans(node.with, bump, catalog, tx)?;
			Ok(LogicalPlan::JoinInner(node))
		}
		LogicalPlan::JoinLeft(mut node) => {
			node.with = inject_plans(node.with, bump, catalog, tx)?;
			Ok(LogicalPlan::JoinLeft(node))
		}
		LogicalPlan::JoinNatural(mut node) => {
			node.with = inject_plans(node.with, bump, catalog, tx)?;
			Ok(LogicalPlan::JoinNatural(node))
		}
		LogicalPlan::InsertTable(mut node) => {
			node.source = inject_boxed(node.source, bump, catalog, tx)?;
			Ok(LogicalPlan::InsertTable(node))
		}
		LogicalPlan::InsertRingBuffer(mut node) => {
			node.source = inject_boxed(node.source, bump, catalog, tx)?;
			Ok(LogicalPlan::InsertRingBuffer(node))
		}
		LogicalPlan::InsertQueue(mut node) => {
			node.source = inject_boxed(node.source, bump, catalog, tx)?;
			Ok(LogicalPlan::InsertQueue(node))
		}
		LogicalPlan::InsertDictionary(mut node) => {
			node.source = inject_boxed(node.source, bump, catalog, tx)?;
			Ok(LogicalPlan::InsertDictionary(node))
		}
		LogicalPlan::InsertSeries(mut node) => {
			node.source = inject_boxed(node.source, bump, catalog, tx)?;
			Ok(LogicalPlan::InsertSeries(node))
		}
		other => Ok(other),
	}
}

fn inject_bodies<'a>(
	bodies: Vec<BumpVec<'a, LogicalPlan<'a>>>,
	bump: &'a Bump,
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
) -> Result<Vec<BumpVec<'a, LogicalPlan<'a>>>> {
	bodies.into_iter().map(|plans| inject_plans(plans, bump, catalog, tx)).collect()
}

fn inject_boxed<'a>(
	plan: BumpBox<'a, LogicalPlan<'a>>,
	bump: &'a Bump,
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
) -> Result<BumpBox<'a, LogicalPlan<'a>>> {
	Ok(BumpBox::new_in(inject_plan(BumpBox::into_inner(plan), bump, catalog, tx)?, bump))
}

fn inject_scan_with_policies<'a>(
	step: LogicalPlan<'a>,
	result: &mut BumpVec<'a, LogicalPlan<'a>>,
	bump: &'a Bump,
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
) -> Result<()> {
	let LogicalPlan::SourceScan(scan) = &step else {
		unreachable!("inject_scan_with_policies called with non-SourceScan");
	};
	let target_type = policy_target_type_for_scan(scan);
	let target_ns = scan.source.namespace().unwrap().name().to_string();
	let target_obj = scan.source.name().to_string();

	result.push(step);

	let policies = catalog.list_all_policies(tx)?;
	let mut found_policy = false;
	for policy in &policies {
		if !policy_matches_scan(policy, target_type, &target_ns, &target_obj) {
			continue;
		}
		let ops = catalog.list_policy_operations(tx, policy.id)?;
		for op in &ops {
			if !is_from_op_with_body(op) {
				continue;
			}
			compile_and_push_from_op(op, result, bump, catalog, tx)?;
			found_policy = true;
		}
	}
	if !found_policy {
		result.push(default_deny_filter());
	}
	Ok(())
}

#[inline]
fn policy_target_type_for_scan(scan: &ObjectScanNode) -> PolicyTargetType {
	match &scan.source {
		ResolvedObject::Table(_) | ResolvedObject::TableVirtual(_) => PolicyTargetType::Table,
		ResolvedObject::View(_) | ResolvedObject::DeferredView(_) | ResolvedObject::TransactionalView(_) => {
			PolicyTargetType::View
		}
		ResolvedObject::RingBuffer(_) => PolicyTargetType::RingBuffer,
		ResolvedObject::Series(_) => PolicyTargetType::Series,
		ResolvedObject::Queue(_) => PolicyTargetType::Queue,
		ResolvedObject::Dictionary(_) => PolicyTargetType::Dictionary,
	}
}

#[inline]
fn policy_matches_scan(policy: &Policy, target_type: PolicyTargetType, target_ns: &str, target_obj: &str) -> bool {
	policy.enabled && policy.target_type == target_type && scope_matches(policy, target_ns, target_obj)
}

#[inline]
fn is_from_op_with_body(op: &PolicyOperation) -> bool {
	DataOp::parse(&op.operation) == Some(DataOp::From) && !op.body_source.is_empty()
}

fn compile_and_push_from_op<'a>(
	op: &PolicyOperation,
	result: &mut BumpVec<'a, LogicalPlan<'a>>,
	bump: &'a Bump,
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
) -> Result<()> {
	let statements = parse_str(bump, bump.alloc_str(&op.body_source))?;
	for stmt in statements {
		let logical = compile_logical(bump, catalog, tx, stmt)?;
		for logical_step in logical {
			push_policy_step(result, logical_step);
		}
	}
	Ok(())
}

#[inline]
fn default_deny_filter<'a>() -> LogicalPlan<'a> {
	LogicalPlan::Filter(FilterNode {
		condition: Expression::Constant(ConstantExpression::Bool {
			fragment: Fragment::internal("false"),
		}),
		rql: String::new(),
	})
}

fn scope_matches(policy: &Policy, target_ns: &str, target_obj: &str) -> bool {
	match (&policy.target_namespace, &policy.target_object) {
		(None, None) => true,
		(Some(ns), None) => {
			target_ns == ns
				|| target_ns.strip_prefix(ns.as_str()).is_some_and(|rest| rest.starts_with("::"))
		}
		(Some(ns), Some(obj)) => ns == target_ns && obj == target_obj,
		(None, Some(_)) => false,
	}
}

pub fn resolve_write_policies(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	target_namespace: &str,
	target_object: &str,
	operation: &str,
	target_type: PolicyTargetType,
) -> Result<Vec<(Policy, PolicyOperation)>> {
	let identity = tx.identity();
	if identity.is_privileged() {
		return Ok(vec![]);
	}

	let policies = catalog.list_all_policies(tx)?;
	let mut result = Vec::new();

	for policy in policies {
		if !policy.enabled {
			continue;
		}
		if policy.target_type != target_type {
			continue;
		}
		if !scope_matches(&policy, target_namespace, target_object) {
			continue;
		}

		let ops = catalog.list_policy_operations(tx, policy.id)?;
		for op in ops {
			if op.operation != operation {
				continue;
			}
			if op.body_source.is_empty() {
				continue;
			}
			result.push((policy.clone(), op));
		}
	}

	Ok(result)
}

fn push_policy_step<'a>(result: &mut BumpVec<'a, LogicalPlan<'a>>, step: LogicalPlan<'a>) {
	match step {
		LogicalPlan::Pipeline(p) => {
			for s in p.steps {
				result.push(s);
			}
		}
		other => {
			result.push(other);
		}
	}
}
