// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::CoreError,
	value::column::{Column, data::ColumnData},
};
use reifydb_rql::expression::PrefixOperator;
use reifydb_type::{
	error::{LogicalOp, OperandCategory, TypeError},
	fragment::Fragment,
	value::{decimal::Decimal, int::Int, uint::Uint},
};

use crate::{Result, expression::option::unary_op_unwrap_option};

/// Macro for signed integer prefix arms (Int1, Int2, Int4, Int8, Int16).
/// Each arm negates with `-*val`, plus with `*val`, and Not returns an error.
macro_rules! prefix_signed_int {
	($column:expr, $container:expr, $operator:expr, $fragment:expr, $variant:ident) => {{
		let mut result = Vec::with_capacity($container.data().len());
		for (idx, val) in $container.data().iter().enumerate() {
			if $container.is_defined(idx) {
				result.push(match $operator {
					PrefixOperator::Minus(_) => -*val,
					PrefixOperator::Plus(_) => *val,
					PrefixOperator::Not(_) => {
						return Err(TypeError::LogicalOperatorNotApplicable {
							operator: LogicalOp::Not,
							operand_category: OperandCategory::Number,
							fragment: $fragment,
						}
						.into());
					}
				});
			} else {
				result.push(0);
			}
		}
		let new_data = ColumnData::$variant(result);
		Ok($column.with_new_data(new_data))
	}};
}

/// Macro for unsigned integer prefix arms (Uint1, Uint2, Uint4, Uint8, Uint16).
/// Each arm converts to a signed type, then negates/plus, and Not returns an error.
macro_rules! prefix_unsigned_int {
	($column:expr, $container:expr, $operator:expr, $fragment:expr, $signed_ty:ty, $constructor:ident) => {{
		let mut result = Vec::with_capacity($container.data().len());
		for val in $container.data().iter() {
			let signed = *val as $signed_ty;
			result.push(match $operator {
				PrefixOperator::Minus(_) => -signed,
				PrefixOperator::Plus(_) => signed,
				PrefixOperator::Not(_) => {
					return Err(TypeError::LogicalOperatorNotApplicable {
						operator: LogicalOp::Not,
						operand_category: OperandCategory::Number,
						fragment: $fragment,
					}
					.into());
				}
			});
		}
		let new_data = ColumnData::$constructor(result);
		Ok($column.with_new_data(new_data))
	}};
}

/// Macro for float prefix arms (Float4, Float8).
/// Same pattern as signed ints but with float zero defaults.
macro_rules! prefix_float {
	($column:expr, $container:expr, $operator:expr, $fragment:expr, $zero:expr, $constructor:ident) => {{
		let mut result = Vec::with_capacity($container.data().len());
		for (idx, val) in $container.data().iter().enumerate() {
			if $container.is_defined(idx) {
				result.push(match $operator {
					PrefixOperator::Minus(_) => -*val,
					PrefixOperator::Plus(_) => *val,
					PrefixOperator::Not(_) => {
						return Err(TypeError::LogicalOperatorNotApplicable {
							operator: LogicalOp::Not,
							operand_category: OperandCategory::Number,
							fragment: $fragment,
						}
						.into());
					}
				});
			} else {
				result.push($zero);
			}
		}
		let new_data = ColumnData::$constructor(result);
		Ok($column.with_new_data(new_data))
	}};
}

/// Macro for types that only support `Not` returning an error and `unimplemented!()` for arithmetic.
macro_rules! prefix_not_error {
	($operator:expr, $fragment:expr, $category:expr) => {
		match $operator {
			PrefixOperator::Not(_) => Err(TypeError::LogicalOperatorNotApplicable {
				operator: LogicalOp::Not,
				operand_category: $category,
				fragment: $fragment,
			}
			.into()),
			_ => unimplemented!(),
		}
	};
}

/// Applies a prefix operator to an already-evaluated column, without re-evaluating
/// the inner expression. This avoids recompilation when the column has already been
/// computed.
pub(crate) fn prefix_apply(column: &Column, operator: &PrefixOperator, fragment: &Fragment) -> Result<Column> {
	unary_op_unwrap_option(column, |column| match column.data() {
		ColumnData::Bool(container) => match operator {
			PrefixOperator::Not(_) => {
				let mut result = Vec::with_capacity(container.data().len());
				for (idx, val) in container.data().iter().enumerate() {
					if container.is_defined(idx) {
						result.push(!val);
					} else {
						result.push(false);
					}
				}

				let new_data = ColumnData::bool(result);
				Ok(column.with_new_data(new_data))
			}
			_ => Err(CoreError::FrameError {
				message: "Cannot apply arithmetic prefix operator to bool".to_string(),
			}
			.into()),
		},

		ColumnData::Float4(container) => {
			prefix_float!(column, container, operator, fragment.clone(), 0.0f32, float4)
		}

		ColumnData::Float8(container) => {
			prefix_float!(column, container, operator, fragment.clone(), 0.0f64, float8)
		}

		ColumnData::Int1(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int1)
		}

		ColumnData::Int2(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int2)
		}

		ColumnData::Int4(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int4)
		}

		ColumnData::Int8(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int8)
		}

		ColumnData::Int16(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int16)
		}

		ColumnData::Utf8 {
			container: _,
			..
		} => match operator {
			PrefixOperator::Not(_) => Err(TypeError::LogicalOperatorNotApplicable {
				operator: LogicalOp::Not,
				operand_category: OperandCategory::Text,
				fragment: fragment.clone(),
			}
			.into()),
			_ => Err(CoreError::FrameError {
				message: "Cannot apply arithmetic prefix operator to text".to_string(),
			}
			.into()),
		},

		ColumnData::Uint1(container) => {
			prefix_unsigned_int!(column, container, operator, fragment.clone(), i8, int1)
		}

		ColumnData::Uint2(container) => {
			prefix_unsigned_int!(column, container, operator, fragment.clone(), i16, int2)
		}

		ColumnData::Uint4(container) => {
			prefix_unsigned_int!(column, container, operator, fragment.clone(), i32, int4)
		}

		ColumnData::Uint8(container) => {
			prefix_unsigned_int!(column, container, operator, fragment.clone(), i64, int8)
		}

		ColumnData::Uint16(container) => {
			prefix_unsigned_int!(column, container, operator, fragment.clone(), i128, int16)
		}

		ColumnData::Date(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal)
		}
		ColumnData::DateTime(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal)
		}
		ColumnData::Time(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal)
		}
		ColumnData::Duration(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal)
		}
		ColumnData::IdentityId(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Uuid)
		}
		ColumnData::Uuid4(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Uuid)
		}
		ColumnData::Uuid7(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Uuid)
		}

		ColumnData::Blob {
			container: _,
			..
		} => match operator {
			PrefixOperator::Not(_) => Err(CoreError::FrameError {
				message: "Cannot apply NOT operator to BLOB".to_string(),
			}
			.into()),
			_ => Err(CoreError::FrameError {
				message: "Cannot apply arithmetic prefix operator to BLOB".to_string(),
			}
			.into()),
		},
		ColumnData::Int {
			container,
			..
		} => {
			let mut result = Vec::with_capacity(container.data().len());
			for (idx, val) in container.data().iter().enumerate() {
				if container.is_defined(idx) {
					result.push(match operator {
						PrefixOperator::Minus(_) => Int(-val.0.clone()),
						PrefixOperator::Plus(_) => val.clone(),
						PrefixOperator::Not(_) => {
							return Err(TypeError::LogicalOperatorNotApplicable {
								operator: LogicalOp::Not,
								operand_category: OperandCategory::Number,
								fragment: fragment.clone(),
							}
							.into());
						}
					});
				} else {
					result.push(Int::zero());
				}
			}
			let new_data = ColumnData::int(result);
			Ok(column.with_new_data(new_data))
		}
		ColumnData::Uint {
			container,
			..
		} => match operator {
			PrefixOperator::Minus(_) => {
				let mut result = Vec::with_capacity(container.data().len());
				for (idx, val) in container.data().iter().enumerate() {
					if container.is_defined(idx) {
						let negated = -val.0.clone();
						result.push(Int::from(negated));
					} else {
						result.push(Int::zero());
					}
				}
				let new_data = ColumnData::int(result);
				Ok(column.with_new_data(new_data))
			}
			PrefixOperator::Plus(_) => {
				let mut result = Vec::with_capacity(container.data().len());
				for (idx, val) in container.data().iter().enumerate() {
					if container.is_defined(idx) {
						result.push(val.clone());
					} else {
						result.push(Uint::zero());
					}
				}
				let new_data = ColumnData::uint(result);
				Ok(column.with_new_data(new_data))
			}
			PrefixOperator::Not(_) => Err(TypeError::LogicalOperatorNotApplicable {
				operator: LogicalOp::Not,
				operand_category: OperandCategory::Number,
				fragment: fragment.clone(),
			}
			.into()),
		},
		ColumnData::Decimal {
			container,
			..
		} => {
			let mut result = Vec::with_capacity(container.data().len());
			for (idx, val) in container.data().iter().enumerate() {
				if container.is_defined(idx) {
					result.push(match operator {
						PrefixOperator::Minus(_) => val.clone().negate(),
						PrefixOperator::Plus(_) => val.clone(),
						PrefixOperator::Not(_) => {
							return Err(TypeError::LogicalOperatorNotApplicable {
								operator: LogicalOp::Not,
								operand_category: OperandCategory::Number,
								fragment: fragment.clone(),
							}
							.into());
						}
					});
				} else {
					result.push(Decimal::from(0));
				}
			}
			let new_data = ColumnData::decimal(result);
			Ok(column.with_new_data(new_data))
		}
		ColumnData::DictionaryId(_) => match operator {
			PrefixOperator::Not(_) => Err(CoreError::FrameError {
				message: "Cannot apply NOT operator to DictionaryId type".to_string(),
			}
			.into()),
			_ => Err(CoreError::FrameError {
				message: "Cannot apply arithmetic prefix operator to DictionaryId type".to_string(),
			}
			.into()),
		},
		ColumnData::Any(_) => match operator {
			PrefixOperator::Not(_) => Err(CoreError::FrameError {
				message: "Cannot apply NOT operator to Any type".to_string(),
			}
			.into()),
			_ => Err(CoreError::FrameError {
				message: "Cannot apply arithmetic prefix operator to Any type".to_string(),
			}
			.into()),
		},
		ColumnData::Option {
			..
		} => unreachable!("nested Option after unwrap"),
	})
}
