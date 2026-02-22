// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{columns::Columns, data::ColumnData, headers::ColumnHeaders};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::{
	error::EngineError,
	expression::{context::EvalContext, eval::evaluate},
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct AssertNode {
	input: Box<dyn QueryNode>,
	expressions: Vec<Expression>,
	message: Option<String>,
	context: Option<Arc<QueryContext>>,
}

impl AssertNode {
	pub fn new(input: Box<dyn QueryNode>, expressions: Vec<Expression>, message: Option<String>) -> Self {
		Self {
			input,
			expressions,
			message,
			context: None,
		}
	}
}

impl QueryNode for AssertNode {
	#[instrument(level = "trace", skip_all, name = "volcano::assert::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::assert::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "AssertNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if let Some(columns) = self.input.next(rx, ctx)? {
			let row_count = columns.row_count();

			// Evaluate each assert expression
			for assert_expr in &self.expressions {
				let eval_ctx = EvalContext {
					target: None,
					columns: columns.clone(),
					row_count,
					take: None,
					params: &stored_ctx.params,
					symbol_table: &stored_ctx.stack,
					is_aggregate_context: false,
					functions: &stored_ctx.services.functions,
					clock: &stored_ctx.services.clock,
					arena: None,
				};

				let result = evaluate(
					&eval_ctx,
					assert_expr,
					&stored_ctx.services.functions,
					&stored_ctx.services.clock,
				)?;

				let frag = assert_expr.full_fragment_owned();
				match result.data() {
					ColumnData::Bool(container) => {
						for i in 0..row_count {
							let valid = container.is_defined(i);
							let value = container.data().get(i);
							if !valid || !value {
								return Err(EngineError::AssertionFailed {
									fragment: frag.clone(),
									message: self
										.message
										.clone()
										.unwrap_or_default(),
									expression: Some(frag.text().to_string()),
								}
								.into());
							}
						}
					}
					_ => {
						return Err(EngineError::AssertionFailed {
							fragment: frag.clone(),
							message: "assert expression must evaluate to a boolean"
								.to_string(),
							expression: Some(frag.text().to_string()),
						}
						.into());
					}
				}
			}

			// Passthrough: return the original columns unchanged
			Ok(Some(columns))
		} else {
			Ok(None)
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

pub(crate) struct AssertWithoutInputNode {
	expressions: Vec<Expression>,
	message: Option<String>,
	context: Option<Arc<QueryContext>>,
	done: bool,
}

impl AssertWithoutInputNode {
	pub fn new(expressions: Vec<Expression>, message: Option<String>) -> Self {
		Self {
			expressions,
			message,
			context: None,
			done: false,
		}
	}
}

impl QueryNode for AssertWithoutInputNode {
	#[instrument(level = "trace", skip_all, name = "volcano::assert::noinput::initialize")]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, ctx: &QueryContext) -> crate::Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::assert::noinput::next")]
	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> crate::Result<Option<Columns>> {
		if self.done {
			return Ok(None);
		}
		self.done = true;

		debug_assert!(self.context.is_some(), "AssertWithoutInputNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		for assert_expr in &self.expressions {
			let eval_ctx = EvalContext {
				target: None,
				columns: Columns::empty(),
				row_count: 1,
				take: None,
				params: &stored_ctx.params,
				symbol_table: &stored_ctx.stack,
				is_aggregate_context: false,
				functions: &stored_ctx.services.functions,
				clock: &stored_ctx.services.clock,
				arena: None,
			};

			let result = evaluate(
				&eval_ctx,
				assert_expr,
				&stored_ctx.services.functions,
				&stored_ctx.services.clock,
			)?;

			let frag = assert_expr.full_fragment_owned();
			match result.data() {
				ColumnData::Bool(container) => {
					let valid = container.is_defined(0);
					let value = container.data().get(0);
					if !valid || !value {
						return Err(EngineError::AssertionFailed {
							fragment: frag.clone(),
							message: self.message.clone().unwrap_or_default(),
							expression: Some(frag.text().to_string()),
						}
						.into());
					}
				}
				_ => {
					return Err(EngineError::AssertionFailed {
						fragment: frag.clone(),
						message: "assert expression must evaluate to a boolean".to_string(),
						expression: Some(frag.text().to_string()),
					}
					.into());
				}
			}
		}

		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
