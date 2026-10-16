// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{str::FromStr, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{FieldRef, Schema};
use reifydb_core::{
	expression::{CallExpression, Expression, name::display_label},
	internal_error,
	value::column::factory::rename,
};
use reifydb_routine_abi::Function;
use reifydb_value::{error::Error, fragment::Fragment, value::value_type::ValueType};

use crate::{
	Result,
	expression::{call::call_builtin, compare::length_mismatch, compile::type_column, context::EvalContext},
	lower::{LoweredExpr, variant::children},
};

enum Argument {
	Lowered(LoweredExpr),
	Type(ValueType, Fragment),
}

pub(super) struct Direct {
	call: CallExpression,
	function: Arc<dyn Function>,
	label: String,
	alias: Option<String>,
	arguments: Vec<Argument>,
}

impl Direct {
	pub(super) fn new(ctx: &EvalContext, operator: &'static str, expression: &Expression) -> Result<Self> {
		let (call, alias) = match expression {
			Expression::Call(call) => (call, None),
			Expression::Alias(alias) => match alias.expression.as_ref() {
				Expression::Call(call) => (call, Some(alias.alias.name().to_string())),
				_ => return Err(nested(operator)),
			},
			_ => return Err(nested(operator)),
		};
		let function = ctx
			.routines
			.get_scalar_function(call.func.0.text())
			.ok_or_else(|| internal_error!("{}: row-changing routine vanished", operator))?;
		let positions = function.type_argument_positions();
		let arguments = call
			.args
			.iter()
			.enumerate()
			.map(|(index, arg)| match arg {
				Expression::Column(column) if positions.contains(&index) => {
					match ValueType::from_str(column.0.name.text()) {
						Ok(ty) => Argument::Type(ty, column.0.name.clone()),
						Err(_) => Argument::Lowered(LoweredExpr::new(arg.clone(), operator)),
					}
				}
				_ => Argument::Lowered(LoweredExpr::new(arg.clone(), operator)),
			})
			.collect();
		Ok(Self {
			call: call.clone(),
			function,
			label: display_label(&Expression::Call(call.clone())).text().to_string(),
			alias,
			arguments,
		})
	}

	pub(super) fn evaluate(&self, ctx: &EvalContext) -> Result<(FieldRef, ArrayRef)> {
		self.function.arity().check(&self.call.func.0, self.call.args.len())?;
		let mut columns = Vec::with_capacity(self.arguments.len());
		for argument in &self.arguments {
			columns.push(match argument {
				Argument::Lowered(lowered) => lowered.evaluate(ctx)?,
				Argument::Type(ty, fragment) => {
					type_column(ctx.take.unwrap_or(ctx.row_count), ty, fragment)
				}
			});
		}
		let result = call_builtin(ctx, &self.call, Some(&self.function), &self.label, &columns)?;
		Ok(match &self.alias {
			Some(alias) => rename(result, alias),
			None => result,
		})
	}
}

fn nested(operator: &str) -> Error {
	internal_error!("{}: a routine that changes the row count must be the whole expression", operator)
}

pub(super) fn splice_key(call: &CallExpression) -> String {
	format!("__row_changing {call:?}")
}

pub(super) struct Spliced {
	calls: Vec<(String, Fragment, Direct)>,
	inner: LoweredExpr,
}

impl Spliced {
	pub(super) fn new(ctx: &EvalContext, operator: &'static str, expression: &Expression) -> Result<Self> {
		let mut found = Vec::new();
		collect(ctx, expression, &mut found);
		let mut calls = Vec::with_capacity(found.len());
		for call in found {
			let direct = Direct::new(ctx, operator, &Expression::Call(call.clone()))?;
			calls.push((splice_key(&call), call.func.0.clone(), direct));
		}
		Ok(Self {
			calls,
			inner: LoweredExpr::new(expression.clone(), operator),
		})
	}

	pub(super) fn evaluate(&self, ctx: &EvalContext) -> Result<(FieldRef, ArrayRef)> {
		let mut columns: Vec<(FieldRef, ArrayRef)> = ctx
			.batch
			.schema_ref()
			.fields()
			.iter()
			.cloned()
			.zip(ctx.batch.columns().iter().cloned())
			.collect();
		for (key, fragment, direct) in &self.calls {
			let (field, array) = direct.evaluate(ctx)?;
			if array.len() != ctx.row_count {
				return Err(length_mismatch(array.len(), ctx.row_count, fragment));
			}
			columns.push(rename((field, array), key));
		}
		let (fields, arrays): (Vec<FieldRef>, Vec<ArrayRef>) = columns.into_iter().unzip();
		let batch = RecordBatch::try_new_with_options(
			Arc::new(Schema::new(fields)),
			arrays,
			&RecordBatchOptions::new().with_row_count(Some(ctx.row_count)),
		)
		.map_err(|err| internal_error!("splice: {}", err))?;
		let mut spliced = ctx.with_eval(batch, ctx.row_count);
		spliced.target = ctx.target.clone();
		spliced.take = ctx.take;
		self.inner.evaluate(&spliced)
	}
}

fn collect(ctx: &EvalContext, expression: &Expression, found: &mut Vec<CallExpression>) {
	if let Expression::Call(call) = expression
		&& ctx.routines
			.get_scalar_function(call.func.0.text())
			.is_some_and(|function| function.changes_row_count())
	{
		found.push(call.clone());
		return;
	}
	for child in children(expression) {
		collect(ctx, child, found);
	}
}
