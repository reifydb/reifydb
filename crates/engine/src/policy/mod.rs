// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::policy::{CallableOp, DataOp, PolicyTargetType, SessionOp},
	value::column::{buffer::ColumnBuffer, columns::Columns},
};
use reifydb_policy::{
	enforce::{PolicyTarget, enforce_identity_policy, enforce_session_policy, enforce_write_policies},
	evaluate::PolicyEvaluator as PolicyEvaluatorTrait,
};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, params::Params, value::identity::IdentityId};

use crate::{
	expression::{
		compile::compile_expression,
		context::{CompileContext, EvalContext},
	},
	vm::{services::Services, stack::SymbolTable},
};

/// Engine-side implementation of the policy evaluator trait.
///
/// Holds references to `Services` (for functions/clock) and `SymbolTable`
/// (for variable resolution), and compiles+evaluates RQL expressions.
pub struct PolicyEvaluator<'a> {
	services: &'a Arc<Services>,
	symbols: &'a SymbolTable,
}

impl<'a> PolicyEvaluator<'a> {
	pub fn new(services: &'a Arc<Services>, symbols: &'a SymbolTable) -> Self {
		Self {
			services,
			symbols,
		}
	}

	pub fn enforce_write_policies(
		&self,
		tx: &mut Transaction<'_>,
		target_namespace: &str,
		target_shape: &str,
		operation: DataOp,
		row_columns: &Columns,
		target_type: PolicyTargetType,
	) -> Result<()> {
		let target = PolicyTarget {
			namespace: target_namespace,
			shape: target_shape,
			operation: operation.as_str(),
			target_type,
		};
		enforce_write_policies(&self.services.catalog, tx, &target, row_columns, self)
	}

	pub fn enforce_session_policy(
		&self,
		tx: &mut Transaction<'_>,
		session_type: SessionOp,
		default_deny: bool,
	) -> Result<()> {
		enforce_session_policy(&self.services.catalog, tx, session_type.as_str(), default_deny, self)
	}

	pub fn enforce_identity_policy(
		&self,
		tx: &mut Transaction<'_>,
		target_namespace: &str,
		target_shape: &str,
		operation: CallableOp,
		target_type: PolicyTargetType,
	) -> Result<()> {
		let target = PolicyTarget {
			namespace: target_namespace,
			shape: target_shape,
			operation: operation.as_str(),
			target_type,
		};
		enforce_identity_policy(&self.services.catalog, tx, &target, self)
	}
}

impl PolicyEvaluatorTrait for PolicyEvaluator<'_> {
	fn evaluate_condition(
		&self,
		expr: &Expression,
		columns: &Columns,
		row_count: usize,
		identity: IdentityId,
	) -> Result<bool> {
		let compile_ctx = CompileContext {
			functions: &self.services.functions,
			symbols: self.symbols,
		};
		let compiled = compile_expression(&compile_ctx, expr)?;

		let base = EvalContext {
			params: &Params::None,
			symbols: self.symbols,
			functions: &self.services.functions,
			runtime_context: &self.services.runtime_context,
			arena: None,
			identity,
			is_aggregate_context: false,
			columns: Columns::empty(),
			row_count: 1,
			target: None,
			take: None,
		};
		let eval_ctx = base.with_eval(columns.clone(), row_count);

		let result = compiled.execute(&eval_ctx)?;

		let denied = match result.data() {
			ColumnBuffer::Bool(container) => {
				(0..row_count).any(|i| !container.is_defined(i) || !container.data().get(i))
			}
			ColumnBuffer::Option {
				inner,
				bitvec,
			} => match inner.as_ref() {
				ColumnBuffer::Bool(container) => (0..row_count).any(|i| {
					let defined = i < bitvec.len() && bitvec.get(i);
					let valid = defined && container.is_defined(i);
					!(valid && container.data().get(i))
				}),
				_ => true,
			},
			_ => true,
		};

		Ok(!denied)
	}
}
