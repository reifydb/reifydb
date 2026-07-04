// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::result::Result as StdResult;

use reifydb_core::value::column::{
	buffer::ColumnBuffer,
	cast::{
		cast_column_data,
		convert::{Convert, TargetConvert},
	},
};
use reifydb_value::{
	Result,
	fragment::Fragment,
	value::{
		number::safe::convert::SafeConvert,
		value_type::{ValueType, get::GetType},
	},
};

use crate::routine::{context::FunctionContext, error::RoutineError};

#[derive(Clone, Copy)]
pub(crate) enum CoercePolicy {
	Error,
	None,
}

#[derive(Clone, Copy)]
pub(crate) struct NonePolicyConvert;

impl Convert for NonePolicyConvert {
	fn convert<From, To>(&self, from: From, _fragment: impl Into<Fragment>) -> Result<Option<To>>
	where
		From: SafeConvert<To> + GetType,
		To: GetType,
	{
		Ok(from.checked_convert())
	}
}

pub(crate) fn coerce_column(
	ctx: &FunctionContext,
	data: &ColumnBuffer,
	target: ValueType,
	policy: CoercePolicy,
) -> StdResult<ColumnBuffer, RoutineError> {
	let fragment = &ctx.fragment;
	let cast = match policy {
		CoercePolicy::Error => cast_column_data(
			TargetConvert {
				target: None,
			},
			data,
			target,
			fragment,
		)?,
		CoercePolicy::None => cast_column_data(NonePolicyConvert, data, target, fragment)?,
	};
	Ok(cast)
}

pub(crate) fn all_rows_none(col: &ColumnBuffer) -> bool {
	let (inner, bv) = col.unwrap_option();
	(0..inner.len()).all(|i| !(inner.is_defined(i) && bv.is_none_or(|b| b.get(i))))
}

pub(crate) fn promote_pair(left: ValueType, right: ValueType) -> ValueType {
	match (left, right) {
		(ValueType::Any, other) => other,
		(other, ValueType::Any) => other,
		(left, right) => ValueType::promote(left, right),
	}
}

pub(crate) fn promote_all(types: impl IntoIterator<Item = ValueType>) -> ValueType {
	types.into_iter().reduce(promote_pair).unwrap_or(ValueType::Float8)
}

#[cfg(test)]
mod tests {
	use std::sync::LazyLock;

	use reifydb_core::value::column::{
		buffer::ColumnBuffer,
		cast::convert::{Convert, TargetConvert},
	};
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_value::{
		error::IntoDiagnostic,
		fragment::Fragment,
		util::bitvec::BitVec,
		value::{identity::IdentityId, value_type::ValueType},
	};

	use super::{CoercePolicy, NonePolicyConvert, coerce_column, promote_all};
	use crate::routine::context::FunctionContext;

	fn ctx() -> FunctionContext<'static> {
		static RUNTIME: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::testing(0, 0));
		FunctionContext {
			fragment: Fragment::internal("coerce_test"),
			identity: IdentityId::root(),
			row_count: 0,
			runtime_context: &RUNTIME,
		}
	}

	// The None policy must behave exactly like TargetConvert's None arm:
	// checked_convert failure becomes Ok(None) instead of an error.
	#[test]
	fn none_policy_matches_targetconvert_none_arm() {
		let out: Option<i8> = NonePolicyConvert.convert(300i16, Fragment::internal("300")).unwrap();
		assert_eq!(out, None);
		let out: Option<i8> = NonePolicyConvert.convert(100i16, Fragment::internal("100")).unwrap();
		assert_eq!(out, Some(100));
		// TargetConvert with the default (Error) policy errors on the same input.
		let err = TargetConvert {
			target: None,
		}
		.convert::<i16, i8>(300i16, Fragment::internal("300"));
		assert!(err.is_err());
	}

	// Error policy: out-of-range values raise NUMBER_002 through the house cast.
	#[test]
	fn error_policy_raises_number_out_of_range() {
		let ctx = ctx();
		let data = ColumnBuffer::int2([300]);
		let err = coerce_column(&ctx, &data, ValueType::Int1, CoercePolicy::Error).unwrap_err();
		assert_eq!(err.into_diagnostic().code, "NUMBER_002");
	}

	// None policy: the same out-of-range value becomes an undefined row.
	#[test]
	fn none_policy_turns_overflow_into_none() {
		let ctx = ctx();
		let data = ColumnBuffer::int2([300, 100]);
		let cast = coerce_column(&ctx, &data, ValueType::Int1, CoercePolicy::None).unwrap();
		assert!(!cast.is_defined(0));
		assert!(cast.is_defined(1));
	}

	// Option-shaped input keeps its shape and per-row nones after coercion.
	#[test]
	fn option_shape_and_nones_are_preserved() {
		let ctx = ctx();
		let inner = ColumnBuffer::int2([1, 2, 3]);
		let data = ColumnBuffer::Option {
			inner: Box::new(inner),
			bitvec: BitVec::from_slice(&[true, false, true]),
		};
		let cast = coerce_column(&ctx, &data, ValueType::Int4, CoercePolicy::Error).unwrap();
		assert_eq!(cast.get_type(), ValueType::Option(Box::new(ValueType::Int4)));
		assert!(cast.is_defined(0));
		assert!(!cast.is_defined(1));
		assert!(cast.is_defined(2));
	}

	#[test]
	fn promote_all_folds_canonically() {
		assert_eq!(
			promote_all([ValueType::Int1, ValueType::Int1]),
			ValueType::promote(ValueType::Int1, ValueType::Int1)
		);
		assert_eq!(promote_all([ValueType::Float4, ValueType::Float8]), ValueType::Float8);
		assert_eq!(promote_all(Vec::<ValueType>::new()), ValueType::Float8);
	}
}
