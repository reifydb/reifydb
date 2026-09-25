// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::result::Result as StdResult;

use arrow_arith::boolean::not;
use reifydb_core::{
	error::CoreError,
	value::column::{ColumnWithName, buffer::ColumnBuffer},
};
use reifydb_rql::expression::PrefixOperator;
use reifydb_value::{
	error::{LogicalOp, OperandCategory, TypeError},
	fragment::Fragment,
	value::{
		container::decimal_array::{decimals, ints, u128s, uints},
		decimal::Decimal,
		int::Int,
		value_type::ValueType,
	},
};

use crate::{Result, expression::option::unary_op_unwrap_option};

macro_rules! prefix_signed_int {
	($column:expr, $container:expr, $operator:expr, $fragment:expr, $variant:ident, $value_type:expr) => {{
		let values: &[_] = $container.values();
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
		let new_data = ColumnBuffer::$variant(result);
		Ok($column.with_new_data(new_data))
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
				let new_data = ColumnBuffer::$constructor(result);
				Ok($column.with_new_data(new_data))
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
		let new_data = ColumnBuffer::$constructor(result);
		Ok($column.with_new_data(new_data))
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

pub fn prefix_apply(column: &ColumnWithName, operator: &PrefixOperator, fragment: &Fragment) -> Result<ColumnWithName> {
	unary_op_unwrap_option(column, |column| match column.data() {
		ColumnBuffer::Bool(container) => match operator {
			PrefixOperator::Not(_) => {
				let new_data =
					ColumnBuffer::Bool(not(container).map_err(|err| CoreError::FrameError {
						message: err.to_string(),
					})?);
				Ok(column.with_new_data(new_data))
			}
			_ => Err(CoreError::FrameError {
				message: "Cannot apply arithmetic prefix operator to bool".to_string(),
			}
			.into()),
		},

		ColumnBuffer::Float4(container) => {
			prefix_float!(column, container, operator, fragment.clone(), 0.0f32, float4)
		}

		ColumnBuffer::Float8(container) => {
			prefix_float!(column, container, operator, fragment.clone(), 0.0f64, float8)
		}

		ColumnBuffer::Int1(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int1, ValueType::Int1)
		}

		ColumnBuffer::Int2(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int2, ValueType::Int2)
		}

		ColumnBuffer::Int4(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int4, ValueType::Int4)
		}

		ColumnBuffer::Int8(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int8, ValueType::Int8)
		}

		ColumnBuffer::Int16(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int16, ValueType::Int16)
		}

		ColumnBuffer::Utf8 {
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

		ColumnBuffer::Uint1(container) => {
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

		ColumnBuffer::Uint2(container) => {
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

		ColumnBuffer::Uint4(container) => {
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

		ColumnBuffer::Uint8(container) => {
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

		ColumnBuffer::Uint16(container) => {
			prefix_unsigned_int!(
				column,
				&u128s(container),
				operator,
				fragment.clone(),
				i128,
				int16,
				ValueType::Int16
			)
		}

		ColumnBuffer::Date(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal, "date")
		}
		ColumnBuffer::DateTime(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal, "datetime")
		}
		ColumnBuffer::Time(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal, "time")
		}
		ColumnBuffer::Duration(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal, "duration")
		}
		ColumnBuffer::IdentityId(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Uuid, "identity id")
		}
		ColumnBuffer::Uuid4(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Uuid, "uuid4")
		}
		ColumnBuffer::Uuid7(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Uuid, "uuid7")
		}

		ColumnBuffer::Blob {
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
		ColumnBuffer::Int(container) => match operator {
			PrefixOperator::Minus(_) => {
				let result = ints(container).iter().map(Int::negate).collect::<Vec<_>>();
				Ok(column.with_new_data(ColumnBuffer::int(container.precision(), result)))
			}
			PrefixOperator::Plus(_) => Ok(column.clone()),
			PrefixOperator::Not(_) => Err(TypeError::LogicalOperatorNotApplicable {
				operator: LogicalOp::Not,
				operand_category: OperandCategory::Number,
				fragment: fragment.clone(),
			}
			.into()),
		},
		ColumnBuffer::Uint(container) => match operator {
			PrefixOperator::Minus(_) => {
				let result = uints(container)
					.into_iter()
					.map(|val| {
						Int::from_i256(val.to_i256().wrapping_neg()).ok_or_else(|| {
							Box::new(TypeError::NumberOutOfRange {
								target: ValueType::int(container.precision()),
								fragment: fragment.clone(),
								descriptor: None,
							})
						})
					})
					.collect::<StdResult<Vec<_>, Box<TypeError>>>()
					.map_err(|e| *e)?;
				Ok(column.with_new_data(ColumnBuffer::int(container.precision(), result)))
			}
			PrefixOperator::Plus(_) => Ok(column.clone()),
			PrefixOperator::Not(_) => Err(TypeError::LogicalOperatorNotApplicable {
				operator: LogicalOp::Not,
				operand_category: OperandCategory::Number,
				fragment: fragment.clone(),
			}
			.into()),
		},
		ColumnBuffer::Decimal(container) => match operator {
			PrefixOperator::Minus(_) => {
				let result = decimals(container).iter().map(Decimal::negate).collect::<Vec<_>>();
				Ok(column.with_new_data(ColumnBuffer::decimal(
					container.precision(),
					container.scale(),
					result,
				)))
			}
			PrefixOperator::Plus(_) => Ok(column.clone()),
			PrefixOperator::Not(_) => Err(TypeError::LogicalOperatorNotApplicable {
				operator: LogicalOp::Not,
				operand_category: OperandCategory::Number,
				fragment: fragment.clone(),
			}
			.into()),
		},
		ColumnBuffer::DictionaryId {
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
		ColumnBuffer::Any {
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
		ColumnBuffer::Digest {
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
