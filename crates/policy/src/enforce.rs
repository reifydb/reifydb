// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{interface::catalog::policy::PolicyTargetType, value::column::columns::Columns};
use reifydb_rql::{
	ast::{ast::Ast, parse_str},
	bump::BumpBox,
	expression::ExpressionCompiler,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, value::identity::IdentityId};

use crate::{error::PolicyError, evaluate::PolicyEvaluator, resolve_write_policies};

/// Enforce write policies for a DML operation (insert, update, delete).
///
/// - Root bypasses all policies.
/// - If no write policies match, the write is denied (default-deny).
/// - For each matching policy, the `require` condition is evaluated against each row.
/// - If any row fails any policy condition, the operation is denied with an error.
pub fn enforce_write_policies(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	identity: IdentityId,
	target_namespace: &str,
	target_object: &str,
	operation: &str,
	row_columns: &Columns,
	target_type: PolicyTargetType,
	evaluator: &impl PolicyEvaluator,
) -> Result<()> {
	if identity.is_root() {
		return Ok(());
	}

	let target_type_str = target_type.as_str().to_string();
	let policies =
		resolve_write_policies(catalog, tx, identity, target_namespace, target_object, operation, target_type)?;

	if policies.is_empty() {
		return Err(PolicyError::NoPolicyDefined {
			operation: operation.to_string(),
			target: format!("{}::{}", target_namespace, target_object),
			target_type: target_type_str,
		}
		.into());
	}

	let bump = bumpalo::Bump::new();
	let target = format!("{}::{}", target_namespace, target_object);

	for (policy, op) in &policies {
		let policy_name = policy.name.as_deref().unwrap_or("<unnamed>");

		let body_source = bump.alloc_str(&op.body_source);
		let statements = parse_str(&bump, body_source)?;

		for stmt in statements {
			for node in stmt.nodes {
				let condition_expr = match node {
					Ast::Require(req) => {
						let body = BumpBox::into_inner(req.body);
						ExpressionCompiler::compile(body)?
					}
					Ast::Filter(filter) => {
						let body = BumpBox::into_inner(filter.node);
						ExpressionCompiler::compile(body)?
					}
					_ => continue,
				};

				let row_count = row_columns.row_count();
				if row_count == 0 {
					continue;
				}

				let passed = evaluator.evaluate_condition(
					&condition_expr,
					row_columns,
					row_count,
					identity,
				)?;

				if !passed {
					return Err(PolicyError::PolicyDenied {
						policy_name: policy_name.to_string(),
						operation: operation.to_string(),
						target: target.clone(),
					}
					.into());
				}
			}
		}
	}

	Ok(())
}

/// Enforce session-level access control for admin/command/query operations.
///
/// - Root bypasses all policies.
/// - If no session policies match, uses `default_deny` to decide:
///   - `true` → deny (e.g., admin for non-root)
///   - `false` → allow (e.g., command/query for non-root)
/// - If policies found, evaluates filter/require conditions against identity.
/// - If any condition denies, returns `SessionDenied` error.
pub fn enforce_session_policy(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	identity: IdentityId,
	session_type: &str,
	default_deny: bool,
	evaluator: &impl PolicyEvaluator,
) -> Result<()> {
	if identity.is_root() {
		return Ok(());
	}

	let policies = resolve_write_policies(catalog, tx, identity, "", "", session_type, PolicyTargetType::Session)?;

	if policies.is_empty() {
		if default_deny {
			return Err(PolicyError::SessionDenied {
				session_type: session_type.to_string(),
			}
			.into());
		}
		return Ok(());
	}

	let bump = bumpalo::Bump::new();
	let empty_columns = Columns::empty();

	for (_policy, op) in &policies {
		let body_source = bump.alloc_str(&op.body_source);
		let statements = parse_str(&bump, body_source)?;

		for stmt in statements {
			for node in stmt.nodes {
				let condition_expr = match node {
					Ast::Require(req) => {
						let body = BumpBox::into_inner(req.body);
						ExpressionCompiler::compile(body)?
					}
					Ast::Filter(filter) => {
						let body = BumpBox::into_inner(filter.node);
						ExpressionCompiler::compile(body)?
					}
					_ => continue,
				};

				let passed =
					evaluator.evaluate_condition(&condition_expr, &empty_columns, 1, identity)?;

				if !passed {
					return Err(PolicyError::SessionDenied {
						session_type: session_type.to_string(),
					}
					.into());
				}
			}
		}
	}

	Ok(())
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
	identity: IdentityId,
	target_namespace: &str,
	target_object: &str,
	operation: &str,
	target_type: PolicyTargetType,
	evaluator: &impl PolicyEvaluator,
) -> Result<()> {
	if identity.is_root() {
		return Ok(());
	}

	let target_type_str = target_type.as_str().to_string();
	let policies =
		resolve_write_policies(catalog, tx, identity, target_namespace, target_object, operation, target_type)?;

	if policies.is_empty() {
		return Err(PolicyError::NoPolicyDefined {
			operation: operation.to_string(),
			target: format!("{}::{}", target_namespace, target_object),
			target_type: target_type_str,
		}
		.into());
	}

	let bump = bumpalo::Bump::new();
	let target = format!("{}::{}", target_namespace, target_object);
	let empty_columns = Columns::empty();

	for (policy, op) in &policies {
		let policy_name = policy.name.as_deref().unwrap_or("<unnamed>");

		let body_source = bump.alloc_str(&op.body_source);
		let statements = parse_str(&bump, body_source)?;

		for stmt in statements {
			for node in stmt.nodes {
				let condition_expr = match node {
					Ast::Require(req) => {
						let body = BumpBox::into_inner(req.body);
						ExpressionCompiler::compile(body)?
					}
					Ast::Filter(filter) => {
						let body = BumpBox::into_inner(filter.node);
						ExpressionCompiler::compile(body)?
					}
					_ => continue,
				};

				let passed =
					evaluator.evaluate_condition(&condition_expr, &empty_columns, 1, identity)?;

				if !passed {
					return Err(PolicyError::PolicyDenied {
						policy_name: policy_name.to_string(),
						operation: operation.to_string(),
						target: target.clone(),
					}
					.into());
				}
			}
		}
	}

	Ok(())
}
