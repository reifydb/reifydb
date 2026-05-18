// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	error::CoreError,
	value::column::{ColumnWithName, buffer::ColumnBuffer},
};
use reifydb_rql::expression::PrefixOperator;
use reifydb_type::{
	error::{LogicalOp, OperandCategory, TypeError},
	fragment::Fragment,
	value::{decimal::Decimal, int::Int, uint::Uint},
};

use crate::{Result, expression::option::unary_op_unwrap_option};

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
		let new_data = ColumnBuffer::$variant(result);
		Ok($column.with_new_data(new_data))
	}};
}

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
		let new_data = ColumnBuffer::$constructor(result);
		Ok($column.with_new_data(new_data))
	}};
}

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
		let new_data = ColumnBuffer::$constructor(result);
		Ok($column.with_new_data(new_data))
	}};
}

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

pub(crate) fn prefix_apply(
	column: &ColumnWithName,
	operator: &PrefixOperator,
	fragment: &Fragment,
) -> Result<ColumnWithName> {
	unary_op_unwrap_option(column, |column| match column.data() {
		ColumnBuffer::Bool(container) => match operator {
			PrefixOperator::Not(_) => {
				let mut result = Vec::with_capacity(container.data().len());
				for (idx, val) in container.data().iter().enumerate() {
					if container.is_defined(idx) {
						result.push(!val);
					} else {
						result.push(false);
					}
				}

				let new_data = ColumnBuffer::bool(result);
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
			prefix_signed_int!(column, container, operator, fragment.clone(), int1)
		}

		ColumnBuffer::Int2(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int2)
		}

		ColumnBuffer::Int4(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int4)
		}

		ColumnBuffer::Int8(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int8)
		}

		ColumnBuffer::Int16(container) => {
			prefix_signed_int!(column, container, operator, fragment.clone(), int16)
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
			prefix_unsigned_int!(column, container, operator, fragment.clone(), i8, int1)
		}

		ColumnBuffer::Uint2(container) => {
			prefix_unsigned_int!(column, container, operator, fragment.clone(), i16, int2)
		}

		ColumnBuffer::Uint4(container) => {
			prefix_unsigned_int!(column, container, operator, fragment.clone(), i32, int4)
		}

		ColumnBuffer::Uint8(container) => {
			prefix_unsigned_int!(column, container, operator, fragment.clone(), i64, int8)
		}

		ColumnBuffer::Uint16(container) => {
			prefix_unsigned_int!(column, container, operator, fragment.clone(), i128, int16)
		}

		ColumnBuffer::Date(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal)
		}
		ColumnBuffer::DateTime(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal)
		}
		ColumnBuffer::Time(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal)
		}
		ColumnBuffer::Duration(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Temporal)
		}
		ColumnBuffer::IdentityId(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Uuid)
		}
		ColumnBuffer::Uuid4(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Uuid)
		}
		ColumnBuffer::Uuid7(_) => {
			prefix_not_error!(operator, fragment.clone(), OperandCategory::Uuid)
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
		ColumnBuffer::Int {
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
			let new_data = ColumnBuffer::int(result);
			Ok(column.with_new_data(new_data))
		}
		ColumnBuffer::Uint {
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
				let new_data = ColumnBuffer::int(result);
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
				let new_data = ColumnBuffer::uint(result);
				Ok(column.with_new_data(new_data))
			}
			PrefixOperator::Not(_) => Err(TypeError::LogicalOperatorNotApplicable {
				operator: LogicalOp::Not,
				operand_category: OperandCategory::Number,
				fragment: fragment.clone(),
			}
			.into()),
		},
		ColumnBuffer::Decimal {
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
			let new_data = ColumnBuffer::decimal(result);
			Ok(column.with_new_data(new_data))
		}
		ColumnBuffer::DictionaryId(_) => match operator {
			PrefixOperator::Not(_) => Err(CoreError::FrameError {
				message: "Cannot apply NOT operator to DictionaryId type".to_string(),
			}
			.into()),
			_ => Err(CoreError::FrameError {
				message: "Cannot apply arithmetic prefix operator to DictionaryId type".to_string(),
			}
			.into()),
		},
		ColumnBuffer::Any(_) => match operator {
			PrefixOperator::Not(_) => Err(CoreError::FrameError {
				message: "Cannot apply NOT operator to Any type".to_string(),
			}
			.into()),
			_ => Err(CoreError::FrameError {
				message: "Cannot apply arithmetic prefix operator to Any type".to_string(),
			}
			.into()),
		},
		ColumnBuffer::Option {
			..
		} => unreachable!("nested Option after unwrap"),
	})
}
