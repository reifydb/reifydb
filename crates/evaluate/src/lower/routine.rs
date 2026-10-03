// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	fmt,
	hash::{Hash, Hasher},
	str::FromStr,
	sync::Arc,
};

use arrow_array::ArrayRef;
use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DataFusionError, internal_err};
use datafusion_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature};
use reifydb_core::{
	expression::{CallExpression, Expression, name::display_label},
	value::column::nulls::split_nulls,
};
use reifydb_routine_abi::{Function, context::FunctionContext};
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	error::Error,
	fragment::Fragment,
	reifydb_assertions,
	value::{
		column_view::ColumnView,
		identity::IdentityId,
		value_type::{ValueType, field::from_field},
	},
};

use crate::{
	Result,
	error::EvaluateError,
	expression::{compile::type_column, context::EvalContext},
	lower::{Lowered, Node, empty_of, error::into_external, literal, lower_node, typed_field, udf, volatile},
};

type DfResult<T> = std::result::Result<T, DataFusionError>;

pub(super) fn lower_call<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	call: &'e CallExpression,
) -> Lowered<'e, Node> {
	let name = call.func.0.text();
	assert!(ctx.symbols.get_function(name).is_none(), "UDF '{}' should have been hoisted to UdfEvalNode", name);
	let function = ctx.routines.get_scalar_function(name).ok_or_else(|| -> Error {
		EvaluateError::UnknownFunction {
			name: name.to_string(),
			fragment: call.func.0.clone(),
		}
		.into()
	})?;
	function.arity().check(&call.func.0, call.args.len()).map_err(Error::from)?;
	let type_positions = function.type_argument_positions();
	let args = call
		.args
		.iter()
		.enumerate()
		.map(|(index, arg)| match arg {
			Expression::Column(column) if type_positions.contains(&index) => {
				match ValueType::from_str(column.0.name.text()) {
					Ok(ty) => {
						let (field, array) = type_column(1, &ty, &column.0.name);
						Ok(literal::row_zero(operator, &field, &array)?)
					}
					Err(_) => lower_node(ctx, operator, arg),
				}
			}
			_ => lower_node(ctx, operator, arg),
		})
		.collect::<Lowered<'e, Vec<Node>>>()?;
	let fields: Vec<FieldRef> = args.iter().map(|node| node.field.clone()).collect();
	let field = routine_field(display_label(expression).text(), function.as_ref(), &fields)?;
	Ok(Node {
		expr: udf(
			RoutineUdf {
				function,
				fragment: call.func.0.clone(),
				identity: ctx.identity,
				runtime_context: ctx.runtime_context.clone(),
				signature: volatile(),
			},
			args.into_iter().map(|node| node.expr).collect(),
		),
		field,
	})
}

fn routine_field(name: &str, function: &dyn Function, args: &[FieldRef]) -> Result<FieldRef> {
	let types = args
		.iter()
		.map(|field| Ok(ColumnView::try_from(&split_nulls(empty_of(field))?.0)?.get_type()))
		.collect::<Result<Vec<ValueType>>>()?;
	let value_type = function.return_type(&types);
	let nullable = (function.propagates_options() && args.iter().any(|field| field.is_nullable()))
		|| value_type.is_option();
	Ok(typed_field(name, value_type.inner_type().clone(), nullable))
}

#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
fn assert_routine_field(fragment: &Fragment, actual: &FieldRef, declared: &FieldRef) {
	reifydb_assertions! {
		let inner = |field: &FieldRef| {
			from_field(field)
				.expect("a routine field always carries a parsable type")
				.value_type
				.map(|value_type| value_type.inner_type().clone())
		};
		assert_eq!(
			inner(actual),
			inner(declared),
			"routine {:?}: the routine answered {actual:?}, the plan declared {declared:?}",
			fragment.text()
		);
	}
}

struct RoutineUdf {
	function: Arc<dyn Function>,
	fragment: Fragment,
	identity: IdentityId,
	runtime_context: RuntimeContext,
	signature: Signature,
}

impl fmt::Debug for RoutineUdf {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RoutineUdf")
			.field("function", &self.function.info().name)
			.field("fragment", &self.fragment)
			.finish()
	}
}

impl PartialEq for RoutineUdf {
	fn eq(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.function, &other.function) && self.fragment == other.fragment
	}
}

impl Eq for RoutineUdf {}

impl Hash for RoutineUdf {
	fn hash<H: Hasher>(&self, state: &mut H) {
		Arc::as_ptr(&self.function).cast::<()>().hash(state);
		self.fragment.hash(state);
	}
}

impl ScalarUDFImpl for RoutineUdf {
	fn name(&self) -> &str {
		&self.function.info().name
	}

	fn signature(&self) -> &Signature {
		&self.signature
	}

	fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
		internal_err!("routine types its output through return_field_from_args")
	}

	fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
		routine_field(self.name(), self.function.as_ref(), args.arg_fields).map_err(into_external)
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
		let mut function_ctx = FunctionContext {
			fragment: self.fragment.clone(),
			identity: self.identity,
			row_count: number_rows,
			runtime_context: &self.runtime_context,
		};
		let (field, array) = self
			.function
			.call(&mut function_ctx, &columns)
			.map_err(|error| into_external(error.with_context(self.fragment.clone(), false)))?;
		assert_routine_field(&self.fragment, &field, &return_field);
		Ok(ColumnarValue::Array(array))
	}

	fn coerce_types(&self, arg_types: &[DataType]) -> DfResult<Vec<DataType>> {
		Ok(arg_types.to_vec())
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_routine::function::default_in_process_functions;
	use reifydb_routine_abi::{Function, registry::Routines};
	use reifydb_runtime::context::{RuntimeContext, clock::Clock};
	use reifydb_value::{fragment::Fragment, value::identity::IdentityId};

	use super::RoutineUdf;
	use crate::lower::volatile;

	fn abs() -> Arc<dyn Function> {
		default_in_process_functions(Routines::builder()).configure().get_scalar_function("math::abs").unwrap()
	}

	fn abs_at(function: &Arc<dyn Function>, column: u32) -> RoutineUdf {
		RoutineUdf {
			function: function.clone(),
			fragment: Fragment::statement("math::abs", 1, column),
			identity: IdentityId::system(),
			runtime_context: RuntimeContext::with_clock(Clock::Real),
			signature: volatile(),
		}
	}

	#[test]
	fn two_routine_calls_at_different_spans_are_not_equal() {
		// CSE merges calls that compare equal, so equality must include the span or a merged call loses its
		// error position.
		let function = abs();
		assert_ne!(abs_at(&function, 1), abs_at(&function, 9));
		assert_eq!(abs_at(&function, 1), abs_at(&function, 1));
	}
}
