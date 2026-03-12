// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	internal_error,
	value::column::{columns::Columns, headers::ColumnHeaders},
};
use reifydb_rql::expression::{ConstantExpression, Expression};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::Value};

use crate::{
	Result, testing,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct CallFunctionQueryNode {
	name: Fragment,
	arguments: Vec<Expression>,
	context: Arc<QueryContext>,
	executed: bool,
}

impl CallFunctionQueryNode {
	pub fn new(name: Fragment, arguments: Vec<Expression>, context: Arc<QueryContext>) -> Self {
		Self {
			name,
			arguments,
			context,
			executed: false,
		}
	}
}

impl QueryNode for CallFunctionQueryNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		Ok(())
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		if self.executed {
			return Ok(None);
		}
		self.executed = true;

		let func_name = self.name.text();

		let mut args = Vec::new();
		for arg in &self.arguments {
			match arg {
				Expression::Constant(ConstantExpression::Text {
					fragment,
				}) => {
					args.push(Value::Utf8(fragment.text().to_string()));
				}
				Expression::Constant(ConstantExpression::None {
					..
				}) => {
					args.push(Value::none());
				}
				Expression::Constant(ConstantExpression::Bool {
					fragment,
				}) => {
					args.push(Value::Boolean(fragment.text() == "true"));
				}
				_ => {
					return Err(internal_error!(
						"testing::* function arguments must be constant values"
					));
				}
			}
		}

		let columns = testing::handle_testing_call(
			func_name,
			&args,
			&self.context.testing,
			&self.context.services.ioc,
			rx,
		)?;

		Ok(Some(columns))
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
