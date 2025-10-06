// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{mem::transmute, sync::Arc};

use reifydb_core::{
	error,
	interface::{ColumnEvaluationContext, Params, expression::Expression},
	value::column::{Columns, headers::ColumnHeaders},
};
use reifydb_type::{Fragment, diagnostic::function::generator_not_found};

use crate::{
	StandardTransaction,
	evaluate::column::evaluate,
	execute::{Batch, ExecutionContext, QueryNode},
	function::{GeneratorContext, GeneratorFunction},
};

pub(crate) struct GeneratorNode<'a> {
	function_name: Fragment<'a>,
	expressions: Vec<Expression<'a>>,
	context: Option<Arc<ExecutionContext<'a>>>,
	exhausted: bool,
	generator: Option<Box<dyn GeneratorFunction>>,
}

impl<'a> GeneratorNode<'a> {
	pub fn new(function_name: Fragment<'a>, parameter_expressions: Vec<Expression<'a>>) -> Self {
		Self {
			function_name,
			expressions: parameter_expressions,
			context: None,
			exhausted: false,
			generator: None,
		}
	}
}

impl<'a> QueryNode<'a> for GeneratorNode<'a> {
	fn initialize(&mut self, _txn: &mut StandardTransaction<'a>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));

		let generator = ctx
			.executor
			.functions
			.get_generator(self.function_name.text())
			.ok_or_else(|| error!(generator_not_found(self.function_name.clone())))?;

		self.exhausted = false;
		self.generator = Some(generator);
		Ok(())
	}

	fn next(&mut self, txn: &mut StandardTransaction<'a>) -> crate::Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		let ctx = self.context.as_ref().unwrap();
		let generator = self.generator.as_ref().unwrap();

		let evaluation_ctx = ColumnEvaluationContext {
			target: None,
			columns: Columns::empty(), // No input columns for generator functions
			row_count: 1,              // Single evaluation context
			take: None,
			params: unsafe { transmute::<&Params, &'a Params>(&ctx.params) },
		};

		// Evaluate all parameter expressions into columns
		let mut evaluated_columns = Vec::new();
		for expr in &self.expressions {
			let column = evaluate(&evaluation_ctx, expr)?;
			evaluated_columns.push(column);
		}
		let evaluated_params = Columns::new(evaluated_columns);

		let cloned_ctx = ctx.as_ref().clone();

		let columns = generator.generate(
			txn,
			GeneratorContext {
				params: evaluated_params,
				execution: cloned_ctx.clone(),
				executor: cloned_ctx.executor.clone(),
			},
		)?;

		self.exhausted = true;

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		None
	}
}
