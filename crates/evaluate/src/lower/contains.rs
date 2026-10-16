// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::hash::{Hash, Hasher};

use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DataFusionError, internal_err};
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use reifydb_core::{
	expression::{ContainsExpression, Expression},
	value::column::factory,
};
use reifydb_value::fragment::Fragment;

use crate::{
	expression::{
		compile::{combine_bool_columns, list_contains_element},
		context::EvalContext,
	},
	lower::{Lowered, Node, bool_field, error::into_external, in_list::items, literal, lower_node, udf, volatile},
};

type DfResult<T> = std::result::Result<T, DataFusionError>;

pub(super) fn lower_contains<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	_expression: &'e Expression,
	contains: &'e ContainsExpression,
) -> Lowered<'e, Node> {
	let name = contains.fragment.text();
	let value = lower_node(ctx, operator, &contains.value)?;
	let items = items(&contains.list);
	if items.is_empty() {
		let (field, array) = factory::bool(name, [true]);
		return Ok(literal::row_zero(operator, &field, &array)?);
	}
	let mut args = vec![value.expr];
	for item in items {
		args.push(lower_node(ctx, operator, item)?.expr);
	}
	Ok(Node {
		expr: udf(
			ContainsUdf {
				fragment: contains.fragment.clone(),
				signature: volatile(),
			},
			args,
		),
		field: bool_field(name, false),
	})
}

#[derive(Debug)]
struct ContainsUdf {
	fragment: Fragment,
	signature: Signature,
}

impl PartialEq for ContainsUdf {
	fn eq(&self, other: &Self) -> bool {
		self.fragment == other.fragment
	}
}

impl Eq for ContainsUdf {}

impl Hash for ContainsUdf {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.fragment.hash(state);
	}
}

impl ScalarUDFImpl for ContainsUdf {
	fn name(&self) -> &str {
		"contains"
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("contains types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, _args: ReturnFieldArgs) -> DfResult<FieldRef> {
		Ok(bool_field("contains", false))
	}

	fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
		let mut columns = Vec::with_capacity(args.args.len());
		for (field, value) in args.arg_fields.iter().zip(args.args) {
			columns.push((field.clone(), value.into_array(args.number_rows)?));
		}
		let value = &columns[0];
		let mut result = list_contains_element(value, &columns[1], &self.fragment).map_err(into_external)?;
		for item in &columns[2..] {
			let next = list_contains_element(value, item, &self.fragment).map_err(into_external)?;
			result = combine_bool_columns(result, next, self.fragment.clone(), |l, r| l && r)
				.map_err(into_external)?;
		}
		Ok(ColumnarValue::Array(result.1))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
}
