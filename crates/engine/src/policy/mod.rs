// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_core::{
	expression::Expression,
	interface::catalog::policy::{CallableOp, DataOp, PolicyTargetType, SessionOp},
	value::batch::empty_batch,
};
use reifydb_evaluate::{
	expression::{
		compile::compile_expression,
		context::{CompileContext, EvalContext},
	},
	stack::SymbolTable,
};
use reifydb_policy::{
	enforce::{PolicyTarget, enforce_identity_policy, enforce_session_policy, enforce_write_policies},
	evaluate::PolicyEvaluator as PolicyEvaluatorTrait,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result,
	params::Params,
	value::{
		column_view::{ColumnView, ViewData},
		identity::IdentityId,
	},
};

use crate::vm::services::Services;

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
		target_object: &str,
		operation: DataOp,
		row_columns: &RecordBatch,
		target_type: PolicyTargetType,
	) -> Result<()> {
		let target = PolicyTarget {
			namespace: target_namespace,
			object: target_object,
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
		target_object: &str,
		operation: CallableOp,
		target_type: PolicyTargetType,
	) -> Result<()> {
		let target = PolicyTarget {
			namespace: target_namespace,
			object: target_object,
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
		batch: &RecordBatch,
		row_count: usize,
		identity: IdentityId,
	) -> Result<bool> {
		let compile_ctx = CompileContext {
			symbols: self.symbols,
		};
		let compiled = compile_expression(&compile_ctx, expr)?;

		let base = EvalContext {
			params: &Params::None,
			symbols: self.symbols,
			routines: &self.services.routines,
			runtime_context: &self.services.runtime_context,
			identity,
			is_aggregate_context: false,
			batch: empty_batch(),
			row_count: 1,
			target: None,
			take: None,
		};
		let eval_ctx = base.with_eval(batch.clone(), row_count);

		let result = compiled.execute(&eval_ctx)?;
		let view = ColumnView::try_from(&result)?;

		let denied = match &view.data {
			ViewData::Bool(container) => {
				(0..row_count).any(|i| !(view.is_defined(i) && container.value(i)))
			}
			_ => true,
		};

		Ok(!denied)
	}
}
