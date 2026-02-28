// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod enforce;
pub mod error;
pub mod evaluate;

use bumpalo::{Bump, collections::Vec as BumpVec};
use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::{
	catalog::policy::{PolicyDef, PolicyOperationDef, PolicyTargetType},
	resolved::ResolvedPrimitive,
};
use reifydb_rql::{
	ast::parse_str,
	expression::{ConstantExpression, Expression},
	plan::logical::{FilterNode, LogicalPlan, PipelineNode, compile_logical},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, fragment::Fragment, value::identity::IdentityId};

/// Inject read policies into logical plans.
///
/// - Root identity bypasses all policies (returns plans unchanged).
/// - For non-root identities, finds Pipeline nodes with PrimitiveScan(Table), looks up enabled read policies for that
///   table, compiles their body_source into logical plan steps, and inserts them after the scan.
/// - If no policies match a table, inserts a `Filter(false)` for default-deny.
/// - Multiple policies are chained sequentially (AND composition).
pub fn inject_read_policies<'a>(
	plans: BumpVec<'a, LogicalPlan<'a>>,
	bump: &'a Bump,
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	identity: IdentityId,
) -> Result<BumpVec<'a, LogicalPlan<'a>>> {
	// Root bypasses all policies
	if identity.is_root() {
		return Ok(plans);
	}

	// If the top-level plans contain PrimitiveScan directly (not inside a Pipeline),
	// wrap them into a pipeline for injection, then unwrap.
	let has_scan = plans.iter().any(|p| matches!(p, LogicalPlan::PrimitiveScan(_)));
	if has_scan {
		// Treat the entire plans vec as a pipeline
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
			LogicalPlan::PrimitiveScan(scan) => {
				// Determine target type, namespace, and object name
				let target_type = match &scan.source {
					ResolvedPrimitive::Table(_) | ResolvedPrimitive::TableVirtual(_) => {
						PolicyTargetType::Table
					}
					ResolvedPrimitive::View(_)
					| ResolvedPrimitive::DeferredView(_)
					| ResolvedPrimitive::TransactionalView(_) => PolicyTargetType::View,
					ResolvedPrimitive::RingBuffer(_) => PolicyTargetType::RingBuffer,
					ResolvedPrimitive::Series(_) => PolicyTargetType::Series,
					ResolvedPrimitive::Dictionary(_) => PolicyTargetType::Dictionary,
					ResolvedPrimitive::Flow(_) => PolicyTargetType::Flow,
				};
				let target_ns = scan.source.namespace().unwrap().name().to_string();
				let target_obj = scan.source.name().to_string();

				// Push the scan node first
				result.push(step);

				// Look up policies for this primitive
				let policies = catalog.list_all_policies(tx)?;
				let mut found_policy = false;

				for policy in &policies {
					if !policy.enabled {
						continue;
					}
					if policy.target_type != target_type {
						continue;
					}
					if !scope_matches(policy, &target_ns, &target_obj) {
						continue;
					}

					// Get read operations for this policy
					let ops = catalog.list_policy_operations(tx, policy.id)?;
					for op in &ops {
						if op.operation != "read" {
							continue;
						}
						if op.body_source.is_empty() {
							continue;
						}

						// Compile body_source into logical plan steps.
						// Use the outer bump so the compiled plans have the same lifetime.
						let statements = parse_str(bump, bump.alloc_str(&op.body_source))?;
						for stmt in statements {
							let logical = compile_logical(bump, catalog, tx, stmt)?;
							for logical_step in logical {
								push_policy_step(&mut result, logical_step);
								found_policy = true;
							}
						}
					}
				}

				// Default-deny: no read policy found → filter out all rows
				if !found_policy {
					result.push(LogicalPlan::Filter(FilterNode {
						condition: Expression::Constant(ConstantExpression::Bool {
							fragment: Fragment::internal("false"),
						}),
					}));
				}
			}
			_ => {
				result.push(step);
			}
		}
	}

	Ok(result)
}

/// Check if a policy's scope matches a given target namespace and object.
fn scope_matches(policy: &PolicyDef, target_ns: &str, target_obj: &str) -> bool {
	match (&policy.target_namespace, &policy.target_object) {
		(None, None) => true,                                          // Global
		(Some(ns), None) => ns == target_ns,                           // Namespace-wide
		(Some(ns), Some(obj)) => ns == target_ns && obj == target_obj, // Specific
		(None, Some(_)) => false,                                      // Invalid (defensive)
	}
}

/// Resolve write policies for a given operation on a target object.
///
/// - Root identity bypasses all policies (returns empty vec).
/// - Returns matching enabled policies and their operation definitions for the given operation.
/// - Writes are default-allow: empty result means the write is permitted.
pub fn resolve_write_policies(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	identity: IdentityId,
	target_namespace: &str,
	target_object: &str,
	operation: &str,
	target_type: PolicyTargetType,
) -> Result<Vec<(PolicyDef, PolicyOperationDef)>> {
	if identity.is_root() {
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

/// Push policy logical plan steps into the result, unwrapping Pipeline nodes.
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
