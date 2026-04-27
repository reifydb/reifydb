// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_routine::routine::{
	Function, Procedure,
	context::{FunctionContext, ProcedureContext},
};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, params::Params, value::Value};

use crate::{
	Result,
	error::EngineError,
	expression::{context::EvalContext, eval::evaluate},
	vm::volcano::query::{QueryContext, QueryNode},
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
}

impl QueryNode for GeneratorNode {
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

	fn next<'a>(&mut self, txn: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		if self.exhausted {
			return Ok(None);
		}

		let stored_ctx = self.context.as_ref().unwrap();

		let session = EvalContext::from_query(stored_ctx);
		let evaluation_ctx = session.with_eval_empty();

		// Evaluate all parameter expressions into columns
		let mut evaluated_columns = Vec::new();
		for expr in &self.expressions {
			let column = evaluate(&evaluation_ctx, expr)?;
			evaluated_columns.push(column);
		}

		let columns = match self.generator.as_ref().unwrap() {
			GeneratorImpl::Function(generator) => {
				let evaluated_params = Columns::new(evaluated_columns);
				let mut fn_ctx = FunctionContext {
					fragment: self.function_name.clone(),
					identity: stored_ctx.identity,
					row_count: evaluated_params.row_count(),
					runtime_context: &stored_ctx.services.runtime_context,
				};
				generator.call(&mut fn_ctx, &evaluated_params)?
			}
			GeneratorImpl::Procedure(procedure) => {
				let values: Vec<Value> =
					evaluated_columns.iter().map(|col| col.data().get_value(0)).collect();
				let params = Params::Positional(Arc::new(values));
				let mut proc_ctx = ProcedureContext {
					fragment: self.function_name.clone(),
					identity: stored_ctx.identity,
					row_count: 1,
					runtime_context: &stored_ctx.services.runtime_context,
					tx: txn,
					params: &params,
					catalog: &stored_ctx.services.catalog,
					ioc: &stored_ctx.services.ioc,
				};
				let empty = Columns::empty();
				procedure.call(&mut proc_ctx, &empty)?
			}
		};

		self.exhausted = true;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
