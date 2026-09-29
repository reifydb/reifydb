// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_arith::boolean::not;
use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{error::CoreError, expression::PrefixOperator, value::column::factory};
use reifydb_value::{
	error::{LogicalOp, OperandCategory, TypeError},
	fragment::Fragment,
	value::{
		column_view::{ColumnView, ViewData},
		container::{decimal_array::decimals, wide_int_array::wides},
		decimal::Decimal,
		value_type::ValueType,
	},
};

use crate::{
	Result,
	expression::{logic::bool_column, option::unary_op_unwrap_option},
};

macro_rules! prefix_signed_int {
	($column:expr, $values:expr, $operator:expr, $fragment:expr, $variant:ident, $value_type:expr) => {{
		let values: &[_] = $values;
		let mut result = Vec::with_capacity(values.len());
		for val in values.iter() {
			result.push(match $operator {
				PrefixOperator::Minus(_) => {
					val.checked_neg().ok_or_else(|| TypeError::NumberOutOfRange {
						target: $value_type,
						fragment: $fragment,
						descriptor: None,
					})?
				}
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
		}
		Ok(factory::$variant($column.0.name(), result))
	}};
}

macro_rules! prefix_unsigned_int {
	($column:expr, $values:expr, $operator:expr, $fragment:expr, $signed_ty:ty, $constructor:ident, $value_type:expr) => {{
		match $operator {
			PrefixOperator::Plus(_) => Ok($column.clone()),
			PrefixOperator::Not(_) => Err(TypeError::LogicalOperatorNotApplicable {
				operator: LogicalOp::Not,
				operand_category: OperandCategory::Number,
				fragment: $fragment,
			}
			.into()),
			PrefixOperator::Minus(_) => {
				let values: &[_] = $values;
				let magnitude = <$signed_ty>::MIN.unsigned_abs();
				let mut result = Vec::with_capacity(values.len());
				for val in values.iter() {
					if *val > magnitude {
						return Err(TypeError::NumberOutOfRange {
							target: $value_type,
							fragment: $fragment,
							descriptor: None,
						}
						.into());
					}
					result.push((*val as $signed_ty).wrapping_neg());
				}
				Ok(factory::$constructor($column.0.name(), result))
			}
		}
	}};
}

macro_rules! prefix_float {
	($column:expr, $container:expr, $operator:expr, $fragment:expr, $zero:expr, $constructor:ident) => {{
		let values: &[_] = $container.values();
		let mut result = Vec::with_capacity(values.len());
		for (idx, val) in values.iter().enumerate() {
			if idx < values.len() {
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
		Ok(factory::$constructor($column.0.name(), result))
	}};
}

macro_rules! prefix_not_error {
	($operator:expr, $fragment:expr, $category:expr, $what:expr) => {
		match $operator {
			PrefixOperator::Not(_) => Err(TypeError::LogicalOperatorNotApplicable {
				operator: LogicalOp::Not,
				operand_category: $category,
				fragment: $fragment,
			}
			.into()),
			_ => Err(CoreError::FrameError {
				message: format!("Cannot apply arithmetic prefix operator to {}", $what),
			}
			.into()),
		}
	};
}

pub fn prefix_apply(
	column: &(FieldRef, ArrayRef),
	operator: &PrefixOperator,
	fragment: &Fragment,
) -> Result<(FieldRef, ArrayRef)> {
	if ColumnView::try_from(column)?.is_untyped_none() && !matches!(operator, PrefixOperator::Not(_)) {
		return Ok(column.clone());
	}
	unary_op_unwrap_option(column, |column| match &ColumnView::try_from(column)?.data {
		ViewData::Bool(container) => match operator {
			PrefixOperator::Not(_) => {
				let negated = not(container).map_err(|err| CoreError::FrameError {
					message: err.to_string(),
				})?;
				Ok(bool_column(column.0.name(), negated, column.0.is_nullable()))
			}
			_ => Err(CoreError::FrameError {
				message: "Cannot apply arithmetic prefix operator to bool".to_string(),
			}
			.into()),
		},

		ViewData::Float4(container) => {
			prefix_float!(column, container, operator, fragment.clone(), 0.0f32, float4)
		}

		ViewData::Float8(container) => {
			prefix_float!(column, container, operator, fragment.clone(), 0.0f64, float8)
		}

		ViewData::Int1(container) => {
			prefix_signed_int!(
				column,
				container.values(),
				operator,
				fragment.clone(),
				int1,
				ValueType::Int1
			)
		}

		ViewData::Int2(container) => {
			prefix_signed_int!(
				column,
				container.values(),
				operator,
				fragment.clone(),
				int2,
				ValueType::Int2
			)
		}

		ViewData::Int4(container) => {
			prefix_signed_int!(
				column,
				container.values(),
				operator,
				fragment.clone(),
				int4,
				ValueType::Int4
			)
		}

		ViewData::Int8(container) => {
			prefix_signed_int!(
				column,
				container.values(),
				operator,
				fragment.clone(),
				int8,
				ValueType::Int8
			)
		}

		ViewData::Int16(container) => {
			prefix_signed_int!(
				column,
				&wides::<i128>(container),
				operator,
				fragment.clone(),
				int16,
				ValueType::Int16
			)
		}

		ViewData::Utf8 {
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

		ViewData::Uint1(container) => {
			prefix_unsigned_int!(
				column,
				container.values(),
				operator,
				fragment.clone(),
				i8,
				int1,
				ValueType::Int1
			)
		}

		ViewData::Uint2(container) => {
			prefix_unsigned_int!(
				column,
				container.values(),
				operator,
				fragment.clone(),
				i16,
				int2,
				ValueType::Int2
			)
		}

		ViewData::Uint4(container) => {
			prefix_unsigned_int!(
				column,
				container.values(),
				operator,
				fragment.clone(),
				i32,
				int4,
				ValueType::Int4
			)
		}

		ViewData::Uint8(container) => {
			prefix_unsigned_int!(
				column,
				container.values(),
				operator,
				fragment.clone(),
				i64,
				int8,
				ValueType::Int8
			)
		}

		ViewData::Uint16(container) => {
			prefix_unsigned_int!(
				column,
				&wides::<u128>(container),
				operator,
				fragment.clone(),
				i128,
				int16,
				ValueType::Int16
			)
		}

		ViewData::Date(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal, "date")
		}
		ViewData::DateTime(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal, "datetime")
		}
		ViewData::Time(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal, "time")
		}
		ViewData::Duration(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal, "duration")
		}
		ViewData::IdentityId(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Uuid, "identity id")
		}
		ViewData::Uuid4(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Uuid, "uuid4")
		}
		ViewData::Uuid7(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Uuid, "uuid7")
		}

		ViewData::None {
			..
		} => Ok(column.clone()),

		ViewData::Blob {
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
		ViewData::Decimal(container) => match operator {
			PrefixOperator::Minus(_) => {
				let result = decimals(container).iter().map(Decimal::negate).collect::<Vec<_>>();
				Ok(factory::decimal(column.0.name(), container.precision(), container.scale(), result))
			}
			PrefixOperator::Plus(_) => Ok(column.clone()),
			PrefixOperator::Not(_) => Err(TypeError::LogicalOperatorNotApplicable {
				operator: LogicalOp::Not,
				operand_category: OperandCategory::Number,
				fragment: fragment.clone(),
			}
			.into()),
		},
		ViewData::DictionaryId {
			..
		} => match operator {
			PrefixOperator::Not(_) => Err(CoreError::FrameError {
				message: "Cannot apply NOT operator to DictionaryId type".to_string(),
			}
			.into()),
			_ => Err(CoreError::FrameError {
				message: "Cannot apply arithmetic prefix operator to DictionaryId type".to_string(),
			}
			.into()),
		},
		ViewData::Any {
			..
		} => match operator {
			PrefixOperator::Not(_) => Err(CoreError::FrameError {
				message: "Cannot apply NOT operator to Any type".to_string(),
			}
			.into()),
			_ => Err(CoreError::FrameError {
				message: "Cannot apply arithmetic prefix operator to Any type".to_string(),
			}
			.into()),
		},
		ViewData::Digest {
			..
		} => match operator {
			PrefixOperator::Not(_) => Err(CoreError::FrameError {
				message: "Cannot apply NOT operator to Digest type".to_string(),
			}
			.into()),
			_ => Err(CoreError::FrameError {
				message: "Cannot apply arithmetic prefix operator to Digest type".to_string(),
			}
			.into()),
		},
	})
}
