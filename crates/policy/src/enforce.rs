// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use bumpalo::Bump;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	expression::Expression,
	interface::catalog::policy::{Policy, PolicyOperation, PolicyTargetType},
	value::batch::empty_batch,
};
use reifydb_rql::{
	ast::{ast::Ast, parse_str},
	bump::BumpBox,
	expression::ExpressionCompiler,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, error::Error, reifydb_assertions, value::identity::IdentityId};
use tracing::instrument;

use crate::{error::PolicyError, evaluate::PolicyEvaluator, resolve_write_policies};

pub struct PolicyTarget<'a> {
	pub namespace: &'a str,
	pub object: &'a str,
	pub operation: &'a str,
	pub target_type: PolicyTargetType,
}

pub struct CompiledWritePolicies {
	conditions: Vec<CompiledCondition>,
	operation: String,
	target_name: String,
}

struct CompiledCondition {
	policy_name: String,
	expr: Expression,
	reads_row: bool,
}

impl CompiledWritePolicies {
	pub fn reads_row(&self) -> bool {
		self.conditions.iter().any(|condition| condition.reads_row)
	}

	pub fn enforce(
		&self,
		row_columns: &RecordBatch,
		identity: IdentityId,
		evaluator: &impl PolicyEvaluator,
	) -> Result<()> {
		self.enforce_matching(row_columns, identity, evaluator, |_| true)
	}

	pub fn enforce_row_dependent(
		&self,
		row_columns: &RecordBatch,
		identity: IdentityId,
		evaluator: &impl PolicyEvaluator,
	) -> Result<()> {
		self.enforce_matching(row_columns, identity, evaluator, |condition| condition.reads_row)
	}

	fn enforce_matching(
		&self,
		row_columns: &RecordBatch,
		identity: IdentityId,
		evaluator: &impl PolicyEvaluator,
		include: impl Fn(&CompiledCondition) -> bool,
	) -> Result<()> {
		let row_count = row_columns.num_rows();
		if row_count == 0 {
			return Ok(());
		}
		for condition in self.conditions.iter().filter(|condition| include(condition)) {
			if !evaluator.evaluate_condition(&condition.expr, row_columns, row_count, identity)? {
				return Err(PolicyError::PolicyDenied {
					policy_name: condition.policy_name.clone(),
					operation: self.operation.clone(),
					target: self.target_name.clone(),
				}
				.into());
			}
		}
		Ok(())
	}
}

#[instrument(name = "policy::compile_write", level = "debug", skip_all)]
pub fn compile_write_policies(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	target: &PolicyTarget<'_>,
) -> Result<Option<CompiledWritePolicies>> {
	if tx.identity().is_privileged() {
		return Ok(None);
	}
	let policies = resolve_target_policies(catalog, tx, target)?;
	if policies.is_empty() {
		return Err(no_policy_error(target));
	}
	let bump = Bump::new();
	let mut conditions = Vec::new();
	for_each_policy_condition(&policies, &bump, |policy, condition_expr| {
		conditions.push(CompiledCondition {
			policy_name: policy.name.as_deref().unwrap_or("<unnamed>").to_string(),
			reads_row: reads_row(condition_expr),
			expr: condition_expr.clone(),
		});
		Ok(())
	})?;
	Ok(Some(CompiledWritePolicies {
		conditions,
		operation: target.operation.to_string(),
		target_name: format!("{}::{}", target.namespace, target.object),
	}))
}

#[instrument(name = "policy::enforce", level = "debug", skip_all)]
pub fn enforce_write_policies(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	target: &PolicyTarget<'_>,
	row_columns: &RecordBatch,
	evaluator: &impl PolicyEvaluator,
) -> Result<()> {
	let Some(compiled) = compile_write_policies(catalog, tx, target)? else {
		return Ok(());
	};
	compiled.enforce(row_columns, tx.identity(), evaluator)
}

fn reads_row(expr: &Expression) -> bool {
	match expr {
		Expression::Column(_)
		| Expression::AccessSource(_)
		| Expression::Call(_)
		| Expression::Map(_)
		| Expression::Extend(_) => true,
		Expression::Constant(_) | Expression::Type(_) | Expression::Parameter(_) | Expression::Variable(_) => {
			false
		}
		Expression::Alias(e) => reads_row(&e.expression),
		Expression::Cast(e) => reads_row(&e.expression),
		Expression::Prefix(e) => reads_row(&e.expression),
		Expression::IsVariant(e) => reads_row(&e.expression),
		Expression::FieldAccess(e) => reads_row(&e.object),
		Expression::Add(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::Div(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::Rem(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::Mul(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::Sub(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::GreaterThan(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::GreaterThanEqual(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::LessThan(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::LessThanEqual(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::Equal(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::NotEqual(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::And(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::Or(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::Xor(e) => reads_row(&e.left) || reads_row(&e.right),
		Expression::Between(e) => reads_row(&e.value) || reads_row(&e.lower) || reads_row(&e.upper),
		Expression::In(e) => reads_row(&e.value) || reads_row(&e.list),
		Expression::Contains(e) => reads_row(&e.value) || reads_row(&e.list),
		Expression::Tuple(e) => e.expressions.iter().any(reads_row),
		Expression::List(e) => e.expressions.iter().any(reads_row),
		Expression::Record(e) => e.fields.iter().any(|(_, field)| reads_row(field)),
		Expression::If(e) => {
			reads_row(&e.condition)
				|| reads_row(&e.then_expr) || e
				.else_ifs
				.iter()
				.any(|branch| reads_row(&branch.condition) || reads_row(&branch.then_expr))
				|| e.else_expr.as_deref().is_some_and(reads_row)
		}
	}
}

pub fn enforce_session_policy(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	session_type: &str,
	default_deny: bool,
	evaluator: &impl PolicyEvaluator,
) -> Result<()> {
	if tx.identity().is_privileged() {
		return Ok(());
	}
	let policies = resolve_write_policies(catalog, tx, "", "", session_type, PolicyTargetType::Session)?;
	if policies.is_empty() {
		return session_empty_outcome(session_type, default_deny);
	}
	let identity = tx.identity();
	evaluate_session_policies(&policies, session_type, identity, evaluator)
}

#[inline]
fn session_empty_outcome(session_type: &str, default_deny: bool) -> Result<()> {
	if default_deny {
		Err(PolicyError::SessionDenied {
			session_type: session_type.to_string(),
		}
		.into())
	} else {
		Ok(())
	}
}

#[inline]
fn evaluate_session_policies(
	policies: &[(Policy, PolicyOperation)],
	session_type: &str,
	identity: IdentityId,
	evaluator: &impl PolicyEvaluator,
) -> Result<()> {
	reifydb_assertions! {
		let count = policies.len();
		assert!(
			count > 0,
			"enforce_session_policy reached per-condition evaluation with no resolved policies; \
			 for_each_policy_condition over an empty slice returns Ok and would bypass the \
			 default-deny decision, silently admitting the session (policies={count})"
		);
	}
	let bump = Bump::new();
	let empty_columns = empty_batch();
	for_each_policy_condition(policies, &bump, |_policy, condition_expr| {
		let passed = evaluator.evaluate_condition(condition_expr, &empty_columns, 1, identity)?;
		if passed {
			return Ok(());
		}
		Err(PolicyError::SessionDenied {
			session_type: session_type.to_string(),
		}
		.into())
	})
}

pub fn enforce_identity_policy(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	target: &PolicyTarget<'_>,
	evaluator: &impl PolicyEvaluator,
) -> Result<()> {
	if tx.identity().is_privileged() {
		return Ok(());
	}
	let policies = resolve_target_policies(catalog, tx, target)?;
	if policies.is_empty() {
		return Err(no_policy_error(target));
	}
	let identity = tx.identity();
	evaluate_identity_policies(&policies, target, identity, evaluator)
}

#[inline]
fn evaluate_identity_policies(
	policies: &[(Policy, PolicyOperation)],
	target: &PolicyTarget<'_>,
	identity: IdentityId,
	evaluator: &impl PolicyEvaluator,
) -> Result<()> {
	reifydb_assertions! {
		let count = policies.len();
		assert!(
			count > 0,
			"enforce_identity_policy reached per-condition evaluation with no resolved policies; \
			 for_each_policy_condition over an empty slice returns Ok and would silently admit the \
			 identity past policy enforcement (policies={count})"
		);
	}
	let bump = Bump::new();
	let target_name = format!("{}::{}", target.namespace, target.object);
	let empty_columns = empty_batch();
	for_each_policy_condition(policies, &bump, |policy, condition_expr| {
		let passed = evaluator.evaluate_condition(condition_expr, &empty_columns, 1, identity)?;
		if passed {
			return Ok(());
		}
		Err(PolicyError::PolicyDenied {
			policy_name: policy.name.as_deref().unwrap_or("<unnamed>").to_string(),
			operation: target.operation.to_string(),
			target: target_name.clone(),
		}
		.into())
	})
}

#[inline]
fn resolve_target_policies(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	target: &PolicyTarget<'_>,
) -> Result<Vec<(Policy, PolicyOperation)>> {
	resolve_write_policies(catalog, tx, target.namespace, target.object, target.operation, target.target_type)
}

#[inline]
fn no_policy_error(target: &PolicyTarget<'_>) -> Error {
	PolicyError::NoPolicyined {
		operation: target.operation.to_string(),
		target: format!("{}::{}", target.namespace, target.object),
		target_type: target.target_type.as_str().to_string(),
	}
	.into()
}

fn for_each_policy_condition<F>(policies: &[(Policy, PolicyOperation)], bump: &Bump, mut on_condition: F) -> Result<()>
where
	F: FnMut(&Policy, &Expression) -> Result<()>,
{
	for (policy, op) in policies {
		let body_source = bump.alloc_str(&op.body_source);
		let statements = parse_str(bump, body_source)?;
		for stmt in statements {
			for node in stmt.nodes {
				let Some(condition_expr) = compile_policy_condition(node)? else {
					continue;
				};
				on_condition(policy, &condition_expr)?;
			}
		}
	}
	Ok(())
}

#[inline]
fn compile_policy_condition(node: Ast<'_>) -> Result<Option<Expression>> {
	let expr = match node {
		Ast::Require(req) => ExpressionCompiler::compile(BumpBox::into_inner(req.body))?,
		Ast::Filter(filter) => ExpressionCompiler::compile(BumpBox::into_inner(filter.node))?,
		_ => return Ok(None),
	};
	Ok(Some(expr))
}

#[cfg(test)]
mod tests {
	use bumpalo::Bump;
	use reifydb_rql::ast::parse_str;

	use super::{compile_policy_condition, reads_row};

	fn condition_reads_row(body: &str) -> bool {
		let bump = Bump::new();
		let source = bump.alloc_str(body);
		let statements = parse_str(&bump, source).expect("policy body parses");
		let node = statements.into_iter().flat_map(|stmt| stmt.nodes).next().expect("one condition node");
		let expr = compile_policy_condition(node).expect("condition compiles").expect("node is a condition");
		reads_row(&expr)
	}

	#[test]
	fn a_condition_on_identity_only_does_not_read_the_row() {
		// the old-row check is skipped exactly for these, so a false here must mean the row is never read.
		assert!(!condition_reads_row("filter { $identity.kind == 'anonymous' }"));
		assert!(!condition_reads_row("filter { true }"));
		assert!(!condition_reads_row("require { $identity.kind != 'service' and true }"));
	}

	#[test]
	fn a_condition_naming_a_column_reads_the_row() {
		// a missed column would skip the old-row check and let a writer take over rows it does not own.
		assert!(condition_reads_row("filter { owner == $identity.id }"));
		assert!(condition_reads_row("require { status != 'archived' }"));
	}

	#[test]
	fn a_column_nested_under_identity_logic_still_reads_the_row() {
		// the walk must reach every child, otherwise one identity term hides the column next to it.
		assert!(condition_reads_row("filter { $identity.kind == 'anonymous' and owner == $identity.id }"));
		assert!(condition_reads_row("filter { $identity.kind == 'anonymous' or status == 'open' }"));
		assert!(condition_reads_row("filter { not (owner == $identity.id) }"));
	}

	#[test]
	fn a_function_call_is_treated_as_reading_the_row() {
		// a call may be non-deterministic or read context, so skipping the old-row check must never apply.
		assert!(condition_reads_row("filter { math::abs(1) == 1 }"));
	}
}
