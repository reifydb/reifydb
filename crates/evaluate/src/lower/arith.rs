// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	hash::{Hash, Hasher},
	sync::Arc,
};

use arrow_array::ArrayRef;
use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DataFusionError, internal_err};
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use reifydb_core::{
	expression::{Expression, name::display_label},
	interface::catalog::property::ColumnSaturationStrategy,
};
use reifydb_value::{
	fragment::Fragment,
	reifydb_assertions,
	value::value_type::field::{FieldType, from_field, to_field},
};

use crate::{
	Result,
	expression::{
		arith::{
			ArithOp, add::add_columns, arith_target, div::div_columns, mul::mul_columns, rem::rem_columns,
			sub::sub_columns,
		},
		context::{ArithContext, EvalContext},
	},
	lower::{
		Lowered, Node, error::into_external, inner_type, is_untyped_none, lower_node, typed_field, udf,
		volatile,
	},
};

type DfResult<T> = std::result::Result<T, DataFusionError>;

pub(super) fn lower_arith<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	(left, right): (&'e Expression, &'e Expression),
	fragment: Fragment,
	op: ArithOp,
) -> Lowered<'e, Node> {
	let left = lower_node(ctx, operator, left)?;
	let right = lower_node(ctx, operator, right)?;
	let arith = ctx.arith();
	let field = arith_field(display_label(expression).text(), op, &arith, &left.field, &right.field)?;
	Ok(Node {
		expr: udf(
			ArithUdf {
				op,
				arith,
				fragment,
				signature: volatile(),
			},
			vec![left.expr, right.expr],
		),
		field,
	})
}

fn arith_field(name: &str, op: ArithOp, arith: &ArithContext, left: &FieldRef, right: &FieldRef) -> Result<FieldRef> {
	match (is_untyped_none(left)?, is_untyped_none(right)?) {
		(true, true) => Ok(Arc::new(to_field(name, &FieldType::default()))),
		(true, false) => Ok(typed_field(name, inner_type(right)?, true)),
		(false, true) => Ok(typed_field(name, inner_type(left)?, true)),
		(false, false) => Ok(typed_field(
			name,
			arith_target(op, inner_type(left)?, inner_type(right)?),
			left.is_nullable()
				|| right.is_nullable()
				|| matches!(arith.saturation_policy(), ColumnSaturationStrategy::None),
		)),
	}
}

#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
fn assert_kernel_field(op: ArithOp, fragment: &Fragment, kernel: &FieldRef, declared: &FieldRef) {
	reifydb_assertions! {
		let inner = |field: &FieldRef| {
			from_field(field)
				.expect("an arithmetic field always carries a parsable type")
				.value_type
				.map(|value_type| value_type.inner_type().clone())
		};
		assert_eq!(
			inner(kernel),
			inner(declared),
			"{op:?} at {:?}: the kernel answered {kernel:?}, the plan declared {declared:?}",
			fragment.text()
		);
	}
}

#[derive(Debug)]
struct ArithUdf {
	op: ArithOp,
	arith: ArithContext,
	fragment: Fragment,
	signature: Signature,
}

impl PartialEq for ArithUdf {
	fn eq(&self, other: &Self) -> bool {
		self.op == other.op && self.fragment == other.fragment
	}
}

impl Eq for ArithUdf {}

impl Hash for ArithUdf {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.op.hash(state);
		self.fragment.hash(state);
	}
}

impl ScalarUDFImpl for ArithUdf {
	fn name(&self) -> &str {
		"arith"
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("arith types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
		arith_field(self.name(), self.op, &self.arith, &args.arg_fields[0], &args.arg_fields[1])
			.map_err(into_external)
	}

	fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
		let ScalarFunctionArgs {
			args: values,
			arg_fields,
			number_rows,
			return_field,
			..
		} = args;
		let columns = arg_fields
			.into_iter()
			.zip(values)
			.map(|(field, value)| Ok((field, value.into_array(number_rows)?)))
			.collect::<DfResult<Vec<(FieldRef, ArrayRef)>>>()?;
		let (left, right) = (&columns[0], &columns[1]);
		let fragment = || self.fragment.clone();
		let (field, array) = match self.op {
			ArithOp::Add => add_columns(&self.arith, left, right, fragment),
			ArithOp::Sub => sub_columns(&self.arith, left, right, fragment),
			ArithOp::Mul => mul_columns(&self.arith, left, right, fragment),
			ArithOp::Div => div_columns(&self.arith, left, right, fragment),
			ArithOp::Rem => rem_columns(&self.arith, left, right, fragment),
		}
		.map_err(into_external)?;
		assert_kernel_field(self.op, &self.fragment, &field, &return_field);
		Ok(ColumnarValue::Array(array))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::fragment::Fragment;

	use super::ArithUdf;
	use crate::{
		expression::{arith::ArithOp, context::ArithContext},
		lower::volatile,
	};

	fn add_at(column: u32) -> ArithUdf {
		ArithUdf {
			op: ArithOp::Add,
			arith: ArithContext {
				target: None,
			},
			fragment: Fragment::statement("a+b", 1, column),
			signature: volatile(),
		}
	}

	#[test]
	fn two_arith_calls_at_different_spans_are_not_equal() {
		// CSE merges calls that compare equal, so an arith call blind to its span would lose its error
		// position.
		assert_ne!(add_at(1), add_at(9));
		assert_eq!(add_at(1), add_at(1));
	}
}
