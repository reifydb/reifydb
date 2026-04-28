// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use bumpalo::Bump;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	interface::catalog::policy::{Policy, PolicyOperation, PolicyTargetType},
	value::column::columns::Columns,
};
use reifydb_rql::{
	ast::{ast::Ast, parse_str},
	bump::BumpBox,
	expression::{Expression, ExpressionCompiler},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, error::Error};

use crate::{error::PolicyError, evaluate::PolicyEvaluator, resolve_write_policies};

/// Identifies the target of a policy enforcement check.
pub struct PolicyTarget<'a> {
	pub namespace: &'a str,
	pub shape: &'a str,
	pub operation: &'a str,
	pub target_type: PolicyTargetType,
}

/// Enforce write policies for a DML operation (insert, update, delete).
///
/// - Root bypasses all policies.
/// - If no write policies match, the write is denied (default-deny).
/// - For each matching policy, the `require` condition is evaluated against each row.
/// - If any row fails any policy condition, the operation is denied with an error.
pub fn enforce_write_policies(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	target: &PolicyTarget<'_>,
	row_columns: &Columns,
	evaluator: &impl PolicyEvaluator,
) -> Result<()> {
	if tx.identity().is_privileged() {
		return Ok(());
	}
	let policies = resolve_target_policies(catalog, tx, target)?;
	if policies.is_empty() {
		return Err(no_policy_error(target));
	}

	let bump = Bump::new();
	let target_name = format!("{}::{}", target.namespace, target.shape);
	let identity = tx.identity();
	for_each_policy_condition(&policies, &bump, |policy, condition_expr| {
		let row_count = row_columns.row_count();
		if row_count == 0 {
			return Ok(());
		}
		let passed = evaluator.evaluate_condition(condition_expr, row_columns, row_count, identity)?;
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

/// Enforce session-level access control for admin/command/query operations.
///
/// - Root bypasses all policies.
/// - If no session policies match, uses `default_deny` to decide:
///   - `true` -> deny (e.g., admin for non-root)
///   - `false` -> allow (e.g., command/query for non-root)
/// - If policies found, evaluates filter/require conditions against identity.
/// - If any condition denies, returns `SessionDenied` error.
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
		return if default_deny {
			Err(PolicyError::SessionDenied {
				session_type: session_type.to_string(),
			}
			.into())
		} else {
			Ok(())
		};
	}

	let bump = Bump::new();
	let empty_columns = Columns::empty();
	let identity = tx.identity();
	for_each_policy_condition(&policies, &bump, |_policy, condition_expr| {
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

/// Enforce identity-only policies (no row data) for operations like procedure calls.
///
/// - Root bypasses all policies.
/// - If no policies match, the operation is denied (default-deny).
/// - For each matching policy, the `require` condition is evaluated with identity in scope but no row data
///   (row_count=1, empty columns).
/// - If the condition evaluates to false, the operation is denied.
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

	let bump = Bump::new();
	let target_name = format!("{}::{}", target.namespace, target.shape);
	let empty_columns = Columns::empty();
	let identity = tx.identity();
	for_each_policy_condition(&policies, &bump, |policy, condition_expr| {
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
	resolve_write_policies(catalog, tx, target.namespace, target.shape, target.operation, target.target_type)
}

#[inline]
fn no_policy_error(target: &PolicyTarget<'_>) -> Error {
	PolicyError::NoPolicyined {
		operation: target.operation.to_string(),
		target: format!("{}::{}", target.namespace, target.shape),
		target_type: target.target_type.as_str().to_string(),
	}
	.into()
}

/// Walk every `Require`/`Filter` condition in every policy body and invoke `on_condition`.
/// The closure may return an error to abort enforcement.
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
