// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	hash::{Hash, Hasher},
	sync::Arc,
};

use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DataFusionError, internal_err};
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use reifydb_core::expression::{Expression, PrefixExpression, PrefixOperator, name::display_label};
use reifydb_value::{fragment::Fragment, reifydb_assertions};

use crate::{
	Result,
	expression::{context::EvalContext, prefix::prefix_apply},
	lower::{
		Lowered, Node, empty_of, error::into_external, inner_type, is_untyped_none, lower_node, typed_field,
		udf, volatile,
	},
};

type DfResult<T> = std::result::Result<T, DataFusionError>;

pub(super) fn lower_prefix<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	prefix: &'e PrefixExpression,
) -> Lowered<Node> {
	let inner = lower_node(ctx, operator, &prefix.expression)?;
	let label = display_label(expression);
	if is_untyped_none(&inner.field)? {
		return Ok(Node {
			expr: inner.expr,
			field: Arc::new(inner.field.as_ref().clone().with_name(label.text())),
		});
	}
	let fragment = prefix.full_fragment_owned();
	let field = prefix_field(label.text(), &inner.field, &prefix.operator, &fragment)?;
	Ok(Node {
		expr: udf(
			PrefixUdf {
				operator: prefix.operator.clone(),
				fragment,
				signature: volatile(),
			},
			vec![inner.expr],
		),
		field,
	})
}

fn prefix_field(name: &str, input: &FieldRef, operator: &PrefixOperator, fragment: &Fragment) -> Result<FieldRef> {
	let (probe, _) = prefix_apply(&empty_of(input), operator, fragment)?;
	Ok(typed_field(name, inner_type(&probe)?, input.is_nullable()))
}

#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
fn assert_prefix_field(fragment: &Fragment, kernel: &FieldRef, declared: &FieldRef) {
	reifydb_assertions! {
		let inner = |field: &FieldRef| {
			reifydb_value::value::value_type::field::from_field(field)
				.expect("a prefix field always carries a parsable type")
				.value_type
				.map(|value_type| value_type.inner_type().clone())
		};
		assert_eq!(
			inner(kernel),
			inner(declared),
			"prefix at {:?}: the kernel answered {kernel:?}, the plan declared {declared:?}",
			fragment.text()
		);
	}
}

#[derive(Debug)]
struct PrefixUdf {
	operator: PrefixOperator,
	fragment: Fragment,
	signature: Signature,
}

impl PartialEq for PrefixUdf {
	fn eq(&self, other: &Self) -> bool {
		self.fragment == other.fragment
	}
}

impl Eq for PrefixUdf {}

impl Hash for PrefixUdf {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.fragment.hash(state);
	}
}

impl ScalarUDFImpl for PrefixUdf {
	fn name(&self) -> &str {
		"prefix"
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("prefix types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
		prefix_field(self.name(), &args.arg_fields[0], &self.operator, &self.fragment).map_err(into_external)
	}

	fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
		let ScalarFunctionArgs {
			args: values,
			arg_fields,
			number_rows,
			return_field,
			..
		} = args;
		let column = (arg_fields[0].clone(), values[0].clone().into_array(number_rows)?);
		let (field, array) = prefix_apply(&column, &self.operator, &self.fragment).map_err(into_external)?;
		assert_prefix_field(&self.fragment, &field, &return_field);
		Ok(ColumnarValue::Array(array))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::expression::PrefixOperator;
	use reifydb_value::fragment::Fragment;

	use super::PrefixUdf;
	use crate::lower::volatile;

	fn negate_at(column: u32) -> PrefixUdf {
		PrefixUdf {
			operator: PrefixOperator::Minus(Fragment::statement("-", 1, column)),
			fragment: Fragment::statement("-", 1, column),
			signature: volatile(),
		}
	}

	#[test]
	fn two_negations_at_different_spans_are_not_equal() {
		// CSE merges calls that compare equal, so a negation blind to its span would lose its error position.
		assert_ne!(negate_at(1), negate_at(9));
		assert_eq!(negate_at(1), negate_at(1));
	}
}
