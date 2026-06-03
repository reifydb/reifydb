// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
use reifydb_value::{Result, error::Error, reifydb_assertions, value::identity::IdentityId};

use crate::{error::PolicyError, evaluate::PolicyEvaluator, resolve_write_policies};

pub struct PolicyTarget<'a> {
	pub namespace: &'a str,
	pub shape: &'a str,
	pub operation: &'a str,
	pub target_type: PolicyTargetType,
}

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
	let identity = tx.identity();
	evaluate_row_policies(&policies, target, row_columns, identity, evaluator)
}

#[inline]
fn evaluate_row_policies(
	policies: &[(Policy, PolicyOperation)],
	target: &PolicyTarget<'_>,
	row_columns: &Columns,
	identity: IdentityId,
	evaluator: &impl PolicyEvaluator,
) -> Result<()> {
	reifydb_assertions! {
		let count = policies.len();
		assert!(
			count > 0,
			"enforce_write_policies reached per-condition evaluation with no resolved policies; \
			 for_each_policy_condition over an empty slice returns Ok and would silently allow an \
			 unprotected write past policy enforcement (policies={count})"
		);
	}
	let bump = Bump::new();
	let target_name = format!("{}::{}", target.namespace, target.shape);
	for_each_policy_condition(policies, &bump, |policy, condition_expr| {
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
	let empty_columns = Columns::empty();
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
	let target_name = format!("{}::{}", target.namespace, target.shape);
	let empty_columns = Columns::empty();
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
