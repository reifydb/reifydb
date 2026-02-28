// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	interface::catalog::policy::PolicyTargetType,
	value::column::{columns::Columns, data::ColumnData},
};
use reifydb_policy::evaluate::PolicyEvaluator as PolicyEvaluatorTrait;
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, value::identity::IdentityId};

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
	symbol_table: &'a SymbolTable,
}

impl<'a> PolicyEvaluator<'a> {
	pub fn new(services: &'a Arc<Services>, symbol_table: &'a SymbolTable) -> Self {
		Self {
			services,
			symbol_table,
		}
	}

	pub fn enforce_write_policies(
		&self,
		tx: &mut Transaction<'_>,
		identity: IdentityId,
		target_namespace: &str,
		target_object: &str,
		operation: &str,
		row_columns: &Columns,
		target_type: PolicyTargetType,
	) -> Result<()> {
		reifydb_policy::enforce::enforce_write_policies(
			&self.services.catalog,
			tx,
			identity,
			target_namespace,
			target_object,
			operation,
			row_columns,
			target_type,
			self,
		)
	}

	pub fn enforce_session_policy(
		&self,
		tx: &mut Transaction<'_>,
		identity: IdentityId,
		session_type: &str,
		default_deny: bool,
	) -> Result<()> {
		reifydb_policy::enforce::enforce_session_policy(
			&self.services.catalog,
			tx,
			identity,
			session_type,
			default_deny,
			self,
		)
	}

	pub fn enforce_identity_policy(
		&self,
		tx: &mut Transaction<'_>,
		identity: IdentityId,
		target_namespace: &str,
		target_object: &str,
		operation: &str,
		target_type: PolicyTargetType,
	) -> Result<()> {
		reifydb_policy::enforce::enforce_identity_policy(
			&self.services.catalog,
			tx,
			identity,
			target_namespace,
			target_object,
			operation,
			target_type,
			self,
		)
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
			symbol_table: self.symbol_table,
		};
		let compiled = compile_expression(&compile_ctx, expr)?;

		let eval_ctx = EvalContext {
			target: None,
			columns: columns.clone(),
			row_count,
			take: None,
			params: &reifydb_type::params::Params::None,
			symbol_table: self.symbol_table,
			is_aggregate_context: false,
			functions: &self.services.functions,
			clock: &self.services.clock,
			arena: None,
			identity,
		};

		let result = compiled.execute(&eval_ctx)?;

		let denied = match result.data() {
			ColumnData::Bool(container) => {
				(0..row_count).any(|i| !container.is_defined(i) || !container.data().get(i))
			}
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

		Ok(!denied)
	}
}
