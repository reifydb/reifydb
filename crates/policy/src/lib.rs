// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use bumpalo::{Bump, collections::Vec as BumpVec};
use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::{auth::Identity, catalog::policy::PolicyTargetType, resolved::ResolvedPrimitive};
use reifydb_rql::{
	ast::parse_str,
	expression::{ConstantExpression, Expression},
	plan::logical::{FilterNode, LogicalPlan, PipelineNode, compile_logical},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, fragment::Fragment};

/// Inject read policies into logical plans.
///
/// - `Identity::System` bypasses all policies (returns plans unchanged).
/// - For non-System identities, finds Pipeline nodes with PrimitiveScan(Table), looks up enabled read policies for that
///   table, compiles their body_source into logical plan steps, and inserts them after the scan.
/// - If no policies match a table, inserts a `Filter(false)` for default-deny.
/// - Multiple policies are chained sequentially (AND composition).
pub fn inject_read_policies<'a>(
	plans: BumpVec<'a, LogicalPlan<'a>>,
	bump: &'a Bump,
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	identity: &Identity,
) -> Result<BumpVec<'a, LogicalPlan<'a>>> {
	// System bypasses all policies
	if matches!(identity, Identity::System { .. }) {
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
				// Check if this is a table scan
				let (target_ns, target_obj) = match &scan.source {
					ResolvedPrimitive::Table(t) => {
						(t.namespace().name().to_string(), t.name().to_string())
					}
					_ => {
						result.push(step);
						continue;
					}
				};

				// Push the scan node first
				result.push(step);

				// Look up policies for this table
				let policies = catalog.list_all_policies(tx)?;
				let mut found_policy = false;

				for policy in &policies {
					if !policy.enabled {
						continue;
					}
					if policy.target_type != PolicyTargetType::Table {
						continue;
					}
					// Match target: namespace and object must match
					let ns_matches =
						policy.target_namespace.as_ref().is_some_and(|ns| ns == &target_ns);
					let obj_matches =
						policy.target_object.as_ref().is_some_and(|obj| obj == &target_obj);

					if !ns_matches || !obj_matches {
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
