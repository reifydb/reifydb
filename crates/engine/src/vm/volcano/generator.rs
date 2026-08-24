// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{ColumnWithName, columns::Columns, headers::ColumnHeaders};
use reifydb_evaluate::expression::{context::EvalContext, eval::evaluate};
use reifydb_routine_abi::{Function, Procedure, context::FunctionContext};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{fragment::Fragment, params::Params, value::Value};
use tracing::instrument;

use crate::{
	Result,
	error::EngineError,
	vm::{
		callable::{CallSite, ProcedureCall, invoke_procedure_routine},
		volcano::query::{QueryContext, QueryNode, eval_context_from_query},
	},
};

enum GeneratorImpl {
	Function(Arc<dyn Function>),
	Procedure(Arc<dyn Procedure>),
}

pub(crate) struct GeneratorNode {
	function_name: Fragment,
	expressions: Vec<Expression>,
	context: Option<Arc<QueryContext>>,
	exhausted: bool,
	generator: Option<GeneratorImpl>,
}

impl GeneratorNode {
	pub fn new(function_name: Fragment, parameter_expressions: Vec<Expression>) -> Self {
		Self {
			function_name,
			expressions: parameter_expressions,
			context: None,
			exhausted: false,
			generator: None,
		}
	}

	#[instrument(level = "trace", skip_all, name = "volcano::generator::eval_params")]
	fn eval_params(&self, session: &EvalContext<'_>) -> Result<Vec<ColumnWithName>> {
		let evaluation_ctx = session.with_eval_empty();

		let mut evaluated_columns = Vec::new();
		for expr in &self.expressions {
			let column = evaluate(&evaluation_ctx, expr)?;
			evaluated_columns.push(column);
		}
		Ok(evaluated_columns)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::generator::invoke")]
	fn invoke<'a>(
		&self,
		txn: &mut Transaction<'a>,
		stored_ctx: &QueryContext,
		evaluated_columns: Vec<ColumnWithName>,
	) -> Result<Columns> {
		match self.generator.as_ref().unwrap() {
			GeneratorImpl::Function(generator) => {
				let evaluated_params = Columns::new(evaluated_columns);
				let mut fn_ctx = FunctionContext {
					fragment: self.function_name.clone(),
					identity: stored_ctx.identity,
					row_count: evaluated_params.row_count(),
					runtime_context: &stored_ctx.services.runtime_context,
				};
				Ok(generator.call(&mut fn_ctx, &evaluated_params)?)
			}
			GeneratorImpl::Procedure(procedure) => {
				let values: Vec<Value> =
					evaluated_columns.iter().map(|col| col.data().get_value(0)).collect();
				let params = Params::Positional(Arc::new(values));
				invoke_procedure_routine(
					&stored_ctx.services,
					&stored_ctx.symbols,
					txn,
					ProcedureCall {
						routine: procedure,
						fragment: &self.function_name,
						target: self.function_name.text(),
						params: &params,
					},
					CallSite::Named,
				)
			}
		}
	}
}

impl QueryNode for GeneratorNode {
	#[instrument(level = "trace", skip_all, name = "volcano::generator::initialize")]
	fn initialize<'a>(&mut self, _txn: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.context = Some(Arc::new(ctx.clone()));

		let name = self.function_name.text();
		if let Some(func) = ctx.services.routines.get_generator_function(name) {
			self.generator = Some(GeneratorImpl::Function(func));
		} else if let Some(proc) = ctx.services.routines.get_procedure(name) {
			self.generator = Some(GeneratorImpl::Procedure(proc));
		} else {
			return Err(EngineError::GeneratorNotFound {
				name: name.to_string(),
				fragment: self.function_name.clone(),
			}
			.into());
		}

		self.exhausted = false;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::generator::next")]
	fn next<'a>(&mut self, txn: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		if self.exhausted {
			return Ok(None);
		}

		let stored_ctx = self.context.as_ref().unwrap().clone();

		let session = eval_context_from_query(&stored_ctx);
		let evaluated_columns = self.eval_params(&session)?;

		let columns = self.invoke(txn, &stored_ctx, evaluated_columns)?;

		self.exhausted = true;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
