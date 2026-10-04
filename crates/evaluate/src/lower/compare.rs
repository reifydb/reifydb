// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DataFusionError, internal_err};
use datafusion_expr::{
	BinaryExpr, ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use reifydb_core::expression::{BetweenExpression, Expression, name::display_label};
use reifydb_value::{
	error::{BinaryOp, Diagnostic, Error, IntoDiagnostic, LogicalOp, TypeError},
	fragment::Fragment,
	value::value_type::ValueType,
};

use crate::{
	Result,
	expression::{
		compare::{GreaterThanEqual, LessThanEqual, cast_to, compare_columns, compare_target},
		context::EvalContext,
		logic::execute_logical_op,
	},
	lower::{
		Lowered, Node, bool_field, error::into_external, inner_type, is_untyped_none, literal::none_bool,
		lower_node, typed_field, udf, volatile,
	},
};

type DfResult<T> = std::result::Result<T, DataFusionError>;

pub(super) fn lower_compare<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	(left, right): (&'e Expression, &'e Expression),
	fragment: Fragment,
	(binary_op, df_op): (BinaryOp, Operator),
) -> Lowered<'e, Node> {
	let left = lower_node(ctx, operator, left)?;
	let right = lower_node(ctx, operator, right)?;
	let field = bool_field(display_label(expression).text(), left.field.is_nullable() || right.field.is_nullable());
	if is_untyped_none(&left.field)? || is_untyped_none(&right.field)? {
		return Ok(Node {
			expr: none_bool(),
			field,
		});
	}
	let (left_type, right_type) = (inner_type(&left.field)?, inner_type(&right.field)?);
	let Some(target) = compare_target(&left_type, &right_type) else {
		return Err(not_applicable(binary_op, left_type, right_type, fragment).into());
	};
	Ok(Node {
		expr: Expr::BinaryExpr(BinaryExpr::new(
			Box::new(cast_side(left, &left_type, &target, &fragment)),
			df_op,
			Box::new(cast_side(right, &right_type, &target, &fragment)),
		)),
		field,
	})
}

pub(super) fn lower_between<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	between: &'e BetweenExpression,
) -> Lowered<'e, Node> {
	let value = lower_node(ctx, operator, &between.value)?;
	let lower = lower_node(ctx, operator, &between.lower)?;
	let upper = lower_node(ctx, operator, &between.upper)?;
	check_between(&value, &lower, &between.fragment)?;
	check_between(&value, &upper, &between.fragment)?;
	let nullable = [&value, &lower, &upper].iter().any(|node| node.field.is_nullable());
	Ok(Node {
		expr: udf(
			BetweenUdf {
				fragment: between.fragment.clone(),
				signature: volatile(),
			},
			vec![value.expr, lower.expr, upper.expr],
		),
		field: bool_field(display_label(expression).text(), nullable),
	})
}

fn check_between(value: &Node, bound: &Node, fragment: &Fragment) -> Result<()> {
	if is_untyped_none(&value.field)? || is_untyped_none(&bound.field)? {
		return Ok(());
	}
	let (value_type, bound_type) = (inner_type(&value.field)?, inner_type(&bound.field)?);
	match compare_target(&value_type, &bound_type) {
		Some(_) => Ok(()),
		None => Err(not_applicable(BinaryOp::Between, value_type, bound_type, fragment.clone())),
	}
}

fn cast_side(node: Node, value_type: &ValueType, target: &ValueType, fragment: &Fragment) -> Expr {
	if value_type == target {
		return node.expr;
	}
	udf(
		CompareCast {
			target: target.clone(),
			fragment: fragment.clone(),
			signature: volatile(),
		},
		vec![node.expr],
	)
}

fn not_applicable(operator: BinaryOp, left: ValueType, right: ValueType, fragment: Fragment) -> Error {
	TypeError::BinaryOperatorNotApplicable {
		operator,
		left,
		right,
		fragment,
	}
	.into()
}

fn between_error(fragment: Fragment, left: ValueType, right: ValueType) -> Diagnostic {
	TypeError::BinaryOperatorNotApplicable {
		operator: BinaryOp::Between,
		left,
		right,
		fragment,
	}
	.into_diagnostic()
}

#[derive(Debug, PartialEq, Hash)]
struct CompareCast {
	target: ValueType,
	fragment: Fragment,
	signature: Signature,
}

impl Eq for CompareCast {}

impl ScalarUDFImpl for CompareCast {
	fn name(&self) -> &str {
		"compare_cast"
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("compare_cast types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
		Ok(typed_field(self.name(), self.target.clone(), args.arg_fields[0].is_nullable()))
	}

	fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
		let column = (args.arg_fields[0].clone(), args.args[0].clone().into_array(args.number_rows)?);
		Ok(ColumnarValue::Array(cast_to(&column, &self.target).map_err(into_external)?))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
}

#[derive(Debug, PartialEq, Hash)]
struct BetweenUdf {
	fragment: Fragment,
	signature: Signature,
}

impl Eq for BetweenUdf {}

impl ScalarUDFImpl for BetweenUdf {
	fn name(&self) -> &str {
		"between"
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("between types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
		Ok(bool_field(self.name(), args.arg_fields.iter().any(|field| field.is_nullable())))
	}

	fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
		let ScalarFunctionArgs {
			args: values,
			arg_fields,
			number_rows,
			..
		} = args;
		let columns = arg_fields
			.into_iter()
			.zip(values)
			.map(|(field, value)| Ok((field, value.into_array(number_rows)?)))
			.collect::<DfResult<Vec<(FieldRef, ArrayRef)>>>()?;
		let ge = compare_columns::<GreaterThanEqual>(
			&columns[0],
			&columns[1],
			self.fragment.clone(),
			between_error,
		)
		.map_err(into_external)?;
		let le = compare_columns::<LessThanEqual>(
			&columns[0],
			&columns[2],
			self.fragment.clone(),
			between_error,
		)
		.map_err(into_external)?;
		let (_, result) =
			execute_logical_op(&ge, &le, &self.fragment, LogicalOp::And).map_err(into_external)?;
		Ok(ColumnarValue::Array(result))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

	use super::{BetweenUdf, CompareCast};
	use crate::lower::volatile;

	fn cast_at(column: u32) -> CompareCast {
		CompareCast {
			target: ValueType::Int8,
			fragment: Fragment::statement("a", 1, column),
			signature: volatile(),
		}
	}

	fn between_at(column: u32) -> BetweenUdf {
		BetweenUdf {
			fragment: Fragment::statement("between", 1, column),
			signature: volatile(),
		}
	}

	#[test]
	fn two_compare_casts_at_different_spans_are_not_equal() {
		// CSE merges calls that compare equal, so a cast blind to its span would lose its error position.
		assert_ne!(cast_at(1), cast_at(9));
		assert_eq!(cast_at(1), cast_at(1));
	}

	#[test]
	fn two_betweens_at_different_spans_are_not_equal() {
		// CSE merges calls that compare equal, so a between blind to its span would lose its error position.
		assert_ne!(between_at(1), between_at(9));
		assert_eq!(between_at(1), between_at(1));
	}
}
