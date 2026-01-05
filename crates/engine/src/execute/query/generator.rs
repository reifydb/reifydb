// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	error,
	value::column::{Columns, headers::ColumnHeaders},
};
use reifydb_function::{GeneratorContext, GeneratorFunction};
use reifydb_rql::expression::Expression;
use reifydb_type::{Fragment, Params, diagnostic::function::generator_not_found};

use crate::{
	StandardTransaction,
	evaluate::{ColumnEvaluationContext, column::evaluate},
	execute::{Batch, ExecutionContext, QueryNode},
};

pub(crate) struct GeneratorNode {
	function_name: Fragment,
	expressions: Vec<Expression>,
	context: Option<Arc<ExecutionContext>>,
	exhausted: bool,
	generator: Option<Box<dyn GeneratorFunction>>,
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

#[async_trait]
impl QueryNode for GeneratorNode {
	async fn initialize<'a>(
		&mut self,
		_txn: &mut StandardTransaction<'a>,
		ctx: &ExecutionContext,
	) -> crate::Result<()> {
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

	async fn next<'a>(
		&mut self,
		txn: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		if self.exhausted {
			return Ok(None);
		}

		// Use the passed context parameter directly
		let generator = self.generator.as_ref().unwrap();

		let stored_ctx = self.context.as_ref().unwrap();
		let evaluation_ctx = ColumnEvaluationContext {
			target: None,
			columns: Columns::empty(), // No input columns for generator functions
			row_count: 1,              // Single evaluation context
			take: None,
			params: unsafe { std::mem::transmute::<&Params, &'a Params>(&stored_ctx.params) },
			stack: unsafe {
				std::mem::transmute::<&crate::stack::Stack, &'a crate::stack::Stack>(&stored_ctx.stack)
			},
			is_aggregate_context: false,
		};

		// Evaluate all parameter expressions into columns
		let mut evaluated_columns = Vec::new();
		for expr in &self.expressions {
			let column = evaluate(&evaluation_ctx, expr)?;
			evaluated_columns.push(column);
		}
		let evaluated_params = Columns::new(evaluated_columns);

		let columns = generator
			.generate(GeneratorContext {
				params: evaluated_params,
				txn: unsafe {
					std::mem::transmute::<&mut StandardTransaction, &'a mut StandardTransaction<'a>>(
						txn,
					)
				},
			})
			.await?;

		self.exhausted = true;

		Ok(Some(Batch {
			columns,
		}))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
