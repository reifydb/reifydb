// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use datafusion_common::{ScalarValue, metadata::FieldMetadata};
use datafusion_expr::Expr;
use reifydb_core::expression::{ConstantExpression, VariableExpression};

use crate::{
	Result,
	expression::{
		compile::{variable_column, variable_field_column},
		constant::constant_value,
		context::EvalContext,
	},
	lower::{Node, bool_field, error::from_datafusion},
};

pub(super) fn constant(operator: &str, constant: &ConstantExpression, label: &str) -> Result<Node> {
	let (field, array) = constant_value(constant, label, 1)?;
	row_zero(operator, &field, &array)
}

pub(super) fn variable(ctx: &EvalContext, operator: &str, variable: &VariableExpression) -> Result<Node> {
	let (field, array) = variable_column(ctx, variable, 1)?;
	row_zero(operator, &field, &array)
}

pub(super) fn variable_field(
	ctx: &EvalContext,
	operator: &str,
	variable: &VariableExpression,
	field_name: &str,
) -> Result<Node> {
	let (field, array) = variable_field_column(ctx, variable, field_name, 1)?;
	row_zero(operator, &field, &array)
}

pub(super) fn row_zero(operator: &str, field: &FieldRef, array: &ArrayRef) -> Result<Node> {
	let value = ScalarValue::try_from_array(array.as_ref(), 0).map_err(|err| from_datafusion(operator, err))?;
	Ok(Node {
		expr: Expr::Literal(value, Some(FieldMetadata::from(field.metadata()))),
		field: field.clone(),
	})
}

pub(super) fn none_bool() -> Expr {
	let field = bool_field("none", true);
	Expr::Literal(ScalarValue::Boolean(None), Some(FieldMetadata::from(field.metadata())))
}
