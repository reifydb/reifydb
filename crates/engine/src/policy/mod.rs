// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::policy::PolicyTargetType,
	value::column::{columns::Columns, data::ColumnData},
};
use reifydb_rql::{
	ast::{ast::Ast, parse_str},
	expression::ExpressionCompiler,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::identity::IdentityId;

use crate::{
	error::EngineError,
	expression::{
		compile::compile_expression,
		context::{CompileContext, EvalContext},
	},
	vm::{services::Services, stack::SymbolTable},
};

/// Enforce write policies for a DML operation (insert, update, delete).
///
/// - Root bypasses all policies.
/// - If no write policies match, the write is denied (default-deny).
/// - For each matching policy, the `require` condition is evaluated against each row.
/// - If any row fails any policy condition, the operation is denied with an error.
pub fn enforce_write_policies(
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
	identity: IdentityId,
	target_namespace: &str,
	target_object: &str,
	operation: &str,
	row_columns: &Columns,
	symbol_table: &SymbolTable,
	target_type: PolicyTargetType,
) -> crate::Result<()> {
	if identity.is_root() {
		return Ok(());
	}

	let policies = reifydb_policy::resolve_write_policies(
		&services.catalog,
		tx,
		identity,
		target_namespace,
		target_object,
		operation,
		target_type,
	)?;

	if policies.is_empty() {
		return Err(EngineError::NoPolicyDefined {
			operation: operation.to_string(),
			target: format!("{}::{}", target_namespace, target_object),
		}
		.into());
	}

	let bump = bumpalo::Bump::new();
	let target = format!("{}::{}", target_namespace, target_object);

	for (policy, op) in &policies {
		let policy_name = policy.name.as_deref().unwrap_or("<unnamed>");

		// Parse body_source into AST
		let body_source = bump.alloc_str(&op.body_source);
		let statements = parse_str(&bump, body_source)?;

		// Extract require/filter conditions from the AST
		for stmt in statements {
			for node in stmt.nodes {
				let condition_expr = match node {
					Ast::Require(req) => {
						let body = reifydb_rql::bump::BumpBox::into_inner(req.body);
						ExpressionCompiler::compile(body)?
					}
					Ast::Filter(filter) => {
						let body = reifydb_rql::bump::BumpBox::into_inner(filter.node);
						ExpressionCompiler::compile(body)?
					}
					_ => continue,
				};

				// Compile and evaluate against row_columns
				let compile_ctx = CompileContext {
					functions: &services.functions,
					symbol_table,
				};
				let compiled = compile_expression(&compile_ctx, &condition_expr)?;

				let row_count = row_columns.row_count();
				if row_count == 0 {
					continue;
				}

				let eval_ctx = EvalContext {
					target: None,
					columns: row_columns.clone(),
					row_count,
					take: None,
					params: &reifydb_type::params::Params::None,
					symbol_table,
					is_aggregate_context: false,
					functions: &services.functions,
					clock: &services.clock,
					arena: None,
					identity,
				};

				let result = compiled.execute(&eval_ctx)?;

				let denied = match result.data() {
					ColumnData::Bool(container) => (0..row_count)
						.any(|i| !container.is_defined(i) || !container.data().get(i)),
					ColumnData::Option {
						inner,
						bitvec,
					} => match inner.as_ref() {
						ColumnData::Bool(container) => (0..row_count).any(|i| {
							let defined = i < bitvec.len() && bitvec.get(i);
							let valid = defined && container.is_defined(i);
							!(valid && container.data().get(i))
						}),
						_ => true,
					},
					_ => true,
				};

				if denied {
					return Err(EngineError::PolicyDenied {
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
