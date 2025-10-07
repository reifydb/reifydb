// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_core::{
	error,
	value::column::{Columns, headers::ColumnHeaders},
};
use reifydb_rql::expression::Expression;
use reifydb_type::{Fragment, diagnostic::function::generator_not_found};

use crate::{
	StandardTransaction,
	evaluate::{ColumnEvaluationContext, column::evaluate},
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

	fn next(
		&mut self,
		txn: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext<'a>,
	) -> crate::Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		// Use the passed context parameter directly
		let generator = self.generator.as_ref().unwrap();

		let evaluation_ctx = ColumnEvaluationContext {
			target: None,
			columns: Columns::empty(), // No input columns for generator functions
			row_count: 1,              // Single evaluation context
			take: None,
			params: unsafe { std::mem::transmute(&ctx.params) },
			stack: unsafe { std::mem::transmute(&ctx.stack) },
		};

		// Evaluate all parameter expressions into columns
		let mut evaluated_columns = Vec::new();
		for expr in &self.expressions {
			let column = evaluate(&evaluation_ctx, expr)?;
			evaluated_columns.push(column);
		}
		let evaluated_params = Columns::new(evaluated_columns);

		let cloned_ctx = ctx.clone();

		let columns = generator.generate(
			txn,
			GeneratorContext {
				params: evaluated_params,
				execution: cloned_ctx.clone(),
				executor: cloned_ctx.executor.clone(),
			},
		)?;

		self.exhausted = true;

		// Transmute the columns to extend their lifetime
		// SAFETY: The columns come from generator.generate() which returns Columns<'a>
		// so they genuinely have lifetime 'a through the query execution
		let columns = unsafe { std::mem::transmute::<Columns<'_>, Columns<'a>>(columns) };

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		None
	}
}
