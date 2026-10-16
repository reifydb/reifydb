// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::hash::{Hash, Hasher};

use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DataFusionError, internal_err};
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use reifydb_core::expression::{Expression, XorExpression, name::display_label};
use reifydb_value::{error::LogicalOp, fragment::Fragment};

use crate::{
	expression::{context::EvalContext, logic::execute_logical_op},
	lower::{Lowered, Node, bool_field, error::into_external, logic_probe, lower_node, udf, volatile},
};

type DfResult<T> = std::result::Result<T, DataFusionError>;

pub(super) fn lower_xor<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	xor: &'e XorExpression,
) -> Lowered<Node> {
	let left = lower_node(ctx, operator, &xor.left)?;
	let right = lower_node(ctx, operator, &xor.right)?;
	let fragment = xor.full_fragment_owned();
	execute_logical_op(&logic_probe(&left.field)?, &logic_probe(&right.field)?, &fragment, LogicalOp::Xor)?;
	let nullable = left.field.is_nullable() || right.field.is_nullable();
	Ok(Node {
		expr: udf(
			XorUdf {
				fragment,
				signature: volatile(),
			},
			vec![left.expr, right.expr],
		),
		field: bool_field(display_label(expression).text(), nullable),
	})
}

#[derive(Debug)]
struct XorUdf {
	fragment: Fragment,
	signature: Signature,
}

impl PartialEq for XorUdf {
	fn eq(&self, other: &Self) -> bool {
		self.fragment == other.fragment
	}
}

impl Eq for XorUdf {}

impl Hash for XorUdf {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.fragment.hash(state);
	}
}

impl ScalarUDFImpl for XorUdf {
	fn name(&self) -> &str {
		"xor"
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("xor types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
		Ok(bool_field("xor", args.arg_fields.iter().any(|field| field.is_nullable())))
	}

	fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
		let left = (args.arg_fields[0].clone(), args.args[0].clone().into_array(args.number_rows)?);
		let right = (args.arg_fields[1].clone(), args.args[1].clone().into_array(args.number_rows)?);
		let (_, array) =
			execute_logical_op(&left, &right, &self.fragment, LogicalOp::Xor).map_err(into_external)?;
		Ok(ColumnarValue::Array(array))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
}
