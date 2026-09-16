// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::value::column::{columns::Columns, headers::ColumnHeaders};
use reifydb_evaluate::{error::EvaluateError, stack::Variable};
use reifydb_rql::expression::VariableExpression;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::{RuntimeErrorKind, TypeError},
	reifydb_assertions,
};
use tracing::instrument;

use crate::{
	Result,
	vm::volcano::query::{QueryContext, QueryNode},
};

pub(crate) struct VariableNode {
	variable_expr: VariableExpression,
	context: Option<Arc<QueryContext>>,
	executed: bool,
}

impl VariableNode {
	pub fn new(variable_expr: VariableExpression) -> Self {
		Self {
			variable_expr,
			context: None,
			executed: false,
		}
	}
}

impl QueryNode for VariableNode {
	#[instrument(level = "trace", skip_all, name = "volcano::variable::initialize")]
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		self.context = Some(Arc::new(ctx.clone()));
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::variable::next")]
	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<Columns>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "VariableNode::next() called before initialize()");
		}

		if self.executed {
			return Ok(None);
		}

		let variable_name = self.variable_expr.name();

		match ctx.symbols.get(variable_name) {
			Some(Variable::Columns {
				columns,
			}) => {
				self.executed = true;
				Ok(Some(columns.clone()))
			}
			Some(Variable::ForIterator {
				columns,
				..
			}) => {
				self.executed = true;

				Ok(Some(columns.clone()))
			}
			Some(Variable::Closure(_)) => Err(TypeError::Runtime {
				kind: RuntimeErrorKind::VariableIsClosure {
					fragment: self.variable_expr.fragment.clone(),
				},
				message: format!(
					"Variable '{}' holds a closure and cannot be read as rows",
					variable_name
				),
			}
			.into()),
			None => Err(EvaluateError::VariableNotFound {
				name: variable_name.to_string(),
			}
			.into()),
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
