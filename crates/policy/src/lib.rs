// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
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
	resolved::ResolvedShape,
};
use reifydb_rql::{
	ast::parse_str,
	expression::{ConstantExpression, Expression},
	plan::logical::{FilterNode, LogicalPlan, PipelineNode, ShapeScanNode, compile_logical},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, fragment::Fragment};

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

	let has_scan = plans.iter().any(|p| matches!(p, LogicalPlan::PrimitiveScan(_)));
	if has_scan {
		let injected = inject_pipeline(plans, bump, catalog, tx)?;
		return Ok(injected);
	}

	let mut result = BumpVec::with_capacity_in(plans.len(), bump);
	for plan in plans {
		result.push(inject_plan(plan, bump, catalog, tx)?);
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
		LogicalPlan::Pipeline(pipeline) => {
			let steps = inject_pipeline(pipeline.steps, bump, catalog, tx)?;
			Ok(LogicalPlan::Pipeline(PipelineNode {
				steps,
			}))
		}
		other => Ok(other),
	}
}

fn inject_pipeline<'a>(
	steps: BumpVec<'a, LogicalPlan<'a>>,
	bump: &'a Bump,
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
) -> Result<BumpVec<'a, LogicalPlan<'a>>> {
	let mut result = BumpVec::with_capacity_in(steps.len() + 4, bump);
	for step in steps {
		match &step {
			LogicalPlan::PrimitiveScan(_) => {
				inject_scan_with_policies(step, &mut result, bump, catalog, tx)?
			}
			_ => result.push(step),
		}
	}
	Ok(result)
}

fn inject_scan_with_policies<'a>(
	step: LogicalPlan<'a>,
	result: &mut BumpVec<'a, LogicalPlan<'a>>,
	bump: &'a Bump,
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
) -> Result<()> {
	let LogicalPlan::PrimitiveScan(scan) = &step else {
		unreachable!("inject_scan_with_policies called with non-PrimitiveScan");
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
fn policy_target_type_for_scan(scan: &ShapeScanNode) -> PolicyTargetType {
	match &scan.source {
		ResolvedShape::Table(_) | ResolvedShape::TableVirtual(_) => PolicyTargetType::Table,
		ResolvedShape::View(_) | ResolvedShape::DeferredView(_) | ResolvedShape::TransactionalView(_) => {
			PolicyTargetType::View
		}
		ResolvedShape::RingBuffer(_) => PolicyTargetType::RingBuffer,
		ResolvedShape::Series(_) => PolicyTargetType::Series,
		ResolvedShape::Dictionary(_) => PolicyTargetType::Dictionary,
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
	match (&policy.target_namespace, &policy.target_shape) {
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
	target_shape: &str,
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
		if !scope_matches(&policy, target_namespace, target_shape) {
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
