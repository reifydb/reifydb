// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{buffer::ColumnBuffer, columns::Columns, headers::ColumnHeaders};
use reifydb_rql::expression::Expression;
use reifydb_transaction::transaction::Transaction;
use tracing::instrument;

use crate::{
	Result,
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
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::assert::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		debug_assert!(self.context.is_some(), "AssertNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();

		if let Some(columns) = self.input.next(rx, ctx)? {
			let row_count = columns.row_count();
			let session = EvalContext::from_query(stored_ctx);

			// Evaluate each assert expression
			for assert_expr in &self.expressions {
				let eval_ctx = session.with_eval(columns.clone(), row_count);

				let result = evaluate(&eval_ctx, assert_expr)?;

				let frag = assert_expr.full_fragment_owned();
				match result.data() {
					ColumnBuffer::Bool(container) => {
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
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::assert::noinput::next")]
	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		if self.done {
			return Ok(None);
		}
		self.done = true;

		debug_assert!(self.context.is_some(), "AssertWithoutInputNode::next() called before initialize()");
		let stored_ctx = self.context.as_ref().unwrap();
		let session = EvalContext::from_query(stored_ctx);

		for assert_expr in &self.expressions {
			let eval_ctx = session.with_eval_empty();

			let result = evaluate(&eval_ctx, assert_expr)?;

			let frag = assert_expr.full_fragment_owned();
			match result.data() {
				ColumnBuffer::Bool(container) => {
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
