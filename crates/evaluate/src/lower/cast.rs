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
	expression::{CastExpression, Expression, name::display_label},
	interface::evaluate::TargetColumn,
	value::column::cast::{cast_column_data, convert::TargetConvert},
};
use reifydb_value::{
	fragment::Fragment,
	reifydb_assertions,
	value::{
		column_view::ColumnView,
		value_type::{ValueType, field::from_field},
	},
};

use crate::{
	expression::{compile::wrap_cast_error, context::EvalContext},
	lower::{Lowered, Node, empty_of, error::into_external, lower_node, typed_field, udf, volatile},
};

type DfResult<T> = std::result::Result<T, DataFusionError>;

pub(super) fn lower_cast<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	cast: &'e CastExpression,
) -> Lowered<'e, Node> {
	let inner = lower_node(ctx, operator, &cast.expression)?;
	let label = display_label(expression);
	let to = cast.to.ty.clone();
	if matches!(cast.expression.as_ref(), Expression::Constant(_))
		&& ColumnView::try_from(&empty_of(&inner.field))?.get_type() == to
	{
		return Ok(Node {
			expr: inner.expr,
			field: Arc::new(inner.field.as_ref().clone().with_name(label.text())),
		});
	}
	let field = cast_field(label.text(), &to, &inner.field);
	Ok(Node {
		expr: udf(
			CastUdf {
				to,
				target: ctx.target.clone(),
				fragment: cast.expression.full_fragment_owned(),
				signature: volatile(),
			},
			vec![inner.expr],
		),
		field,
	})
}

fn cast_field(name: &str, to: &ValueType, input: &FieldRef) -> FieldRef {
	typed_field(name, to.inner_type().clone(), input.is_nullable() || to.is_option())
}

#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
fn assert_cast_field(fragment: &Fragment, kernel: &FieldRef, declared: &FieldRef, array: &ArrayRef) {
	reifydb_assertions! {
		let inner = |field: &FieldRef| {
			from_field(field)
				.expect("a cast field always carries a parsable type")
				.value_type
				.map(|value_type| value_type.inner_type().clone())
		};
		assert_eq!(
			inner(kernel),
			inner(declared),
			"cast at {:?}: the kernel answered {kernel:?}, the plan declared {declared:?}",
			fragment.text()
		);
		assert_eq!(
			kernel.is_nullable(),
			declared.is_nullable() || array.logical_null_count() > 0,
			"cast at {:?}: the kernel answered nullable {kernel:?}, the plan declared {declared:?}",
			fragment.text()
		);
	}
}

#[derive(Debug)]
struct CastUdf {
	to: ValueType,
	target: Option<TargetColumn>,
	fragment: Fragment,
	signature: Signature,
}

impl PartialEq for CastUdf {
	fn eq(&self, other: &Self) -> bool {
		self.to == other.to && self.fragment == other.fragment
	}
}

impl Eq for CastUdf {}

impl Hash for CastUdf {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.to.hash(state);
		self.fragment.hash(state);
	}
}

impl ScalarUDFImpl for CastUdf {
	fn name(&self) -> &str {
		"cast"
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("cast types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
		Ok(cast_field(self.name(), &self.to, &args.arg_fields[0]))
	}

	fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
		let ScalarFunctionArgs {
			args: values,
			arg_fields,
			number_rows,
			return_field,
			..
		} = args;
		let array = values[0].clone().into_array(number_rows)?;
		let column = (arg_fields[0].clone(), array);
		let view = ColumnView::try_from(&column).map_err(into_external)?;
		let target = TargetConvert {
			target: self.target.as_ref(),
		};
		let (field, array) = cast_column_data(target, &view, self.to.clone(), &|| self.fragment.clone())
			.map_err(|error| into_external(wrap_cast_error(error, self.fragment.clone(), &self.to)))?;
		assert_cast_field(&self.fragment, &field, &return_field, &array);
		Ok(ColumnarValue::Array(array))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
}
