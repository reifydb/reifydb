// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_function::{GeneratorContext, GeneratorFunction};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{error::Error, fragment::Fragment, params::Params};

use crate::{
	Result,
	error::EngineError,
	expression::{context::EvalContext, eval::evaluate},
	vm::{
		stack::SymbolTable,
		volcano::query::{QueryContext, QueryNode},
	},
};

pub(crate) struct GeneratorNode {
	function_name: Fragment,
	expressions: Vec<Expression>,
	context: Option<Arc<QueryContext>>,
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

impl QueryNode for GeneratorNode {
	fn initialize<'a>(&mut self, _txn: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.context = Some(Arc::new(ctx.clone()));

		let generator =
			ctx.services.functions.get_generator(self.function_name.text()).ok_or_else(|| -> Error {
				EngineError::GeneratorNotFound {
					name: self.function_name.text().to_string(),
					fragment: self.function_name.clone(),
				}
				.into()
			})?;

		self.exhausted = false;
		self.generator = Some(generator);
		Ok(())
	}

	fn next<'a>(&mut self, txn: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		if self.exhausted {
			return Ok(None);
		}

		// Use the passed context parameter directly
		let generator = self.generator.as_ref().unwrap();

		let stored_ctx = self.context.as_ref().unwrap();
		let evaluation_ctx = EvalContext {
			target: None,
			columns: Columns::empty(), // No input columns for generator functions
			row_count: 1,              // Single evaluation context
			take: None,
			params: unsafe { std::mem::transmute::<&Params, &'a Params>(&stored_ctx.params) },
			symbol_table: unsafe {
				std::mem::transmute::<&SymbolTable, &'a SymbolTable>(&stored_ctx.stack)
			},
			is_aggregate_context: false,
			functions: &stored_ctx.services.functions,
			clock: &stored_ctx.services.clock,
			arena: None,
			identity: stored_ctx.identity,
		};

		// Evaluate all parameter expressions into columns
		let mut evaluated_columns = Vec::new();
		for expr in &self.expressions {
			let column = evaluate(
				&evaluation_ctx,
				expr,
				&stored_ctx.services.functions,
				&stored_ctx.services.clock,
			)?;
			evaluated_columns.push(column);
		}
		let evaluated_params = Columns::new(evaluated_columns);

		let columns = generator.generate(GeneratorContext {
			fragment: self.function_name.clone(),
			params: evaluated_params,
			txn: unsafe { std::mem::transmute::<&mut Transaction, &'a mut Transaction<'a>>(txn) },
			catalog: unsafe { std::mem::transmute::<&Catalog, &'a Catalog>(&stored_ctx.services.catalog) },
			identity: stored_ctx.identity,
		})?;

		self.exhausted = true;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
