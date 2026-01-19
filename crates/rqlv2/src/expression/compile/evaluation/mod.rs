// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Modular evaluation functions for expressions.
//!
//! This module is organized into specialized sub-modules:
//! - `arith`: Add, Sub, Mul, Div, Rem operations
//! - `compare`: Eq, Ne, Gt, Ge, Lt, Le operations
//! - `logical`: And, Or, Xor operations
//! - `conditional`: If/Then/Else expressions
//! - `unary`: Not, Neg, Plus operations
//! - `cast`: Type conversion operations

mod arith;
mod cast;
mod compare;
mod conditional;
mod logical;
mod unary;

pub(crate) use arith::{eval_add, eval_div, eval_mul, eval_rem, eval_sub};
pub(crate) use cast::cast_column_data;
pub(crate) use compare::{eval_eq, eval_ge, eval_gt, eval_le, eval_lt, eval_ne};
pub(crate) use conditional::eval_conditional;
pub(crate) use logical::{eval_logical_and, eval_logical_or, eval_logical_xor};
pub(crate) use unary::eval_unary;

use reifydb_core::value::column::Column;

use crate::{
	expression::types::{EvalError, EvalResult},
	plan::node::expr::BinaryPlanOp,
};

/// Main dispatcher for binary operations.
pub(crate) fn eval_binary(op: BinaryPlanOp, left: &Column, right: &Column) -> EvalResult<Column> {
	let row_count = left.data().len();
	if right.data().len() != row_count {
		return Err(EvalError::RowCountMismatch {
			expected: row_count,
			actual: right.data().len(),
		});
	}

	match op {
		// Comparison operators
		BinaryPlanOp::Eq => eval_eq(left, right),
		BinaryPlanOp::Ne => eval_ne(left, right),
		BinaryPlanOp::Gt => eval_gt(left, right),
		BinaryPlanOp::Ge => eval_ge(left, right),
		BinaryPlanOp::Lt => eval_lt(left, right),
		BinaryPlanOp::Le => eval_le(left, right),

		// Logical operators
		BinaryPlanOp::And => eval_logical_and(left, right),
		BinaryPlanOp::Or => eval_logical_or(left, right),
		BinaryPlanOp::Xor => eval_logical_xor(left, right),

		// Arithmetic operators
		BinaryPlanOp::Add => eval_add(left, right),
		BinaryPlanOp::Sub => eval_sub(left, right),
		BinaryPlanOp::Mul => eval_mul(left, right),
		BinaryPlanOp::Div => eval_div(left, right),
		BinaryPlanOp::Rem => eval_rem(left, right),

		// String
		BinaryPlanOp::Concat => Err(EvalError::UnsupportedOperation {
			operation: "CONCAT".to_string(),
		}),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plan::node::expr::UnaryPlanOp;
	use reifydb_core::value::column::data::ColumnData;
	use reifydb_type::fragment::Fragment;

	fn create_bool_column(name: &str, data: Vec<bool>, bitvec: Vec<bool>) -> Column {
		Column::new(
			Fragment::internal(name),
			ColumnData::bool_with_bitvec(data, bitvec),
		)
	}

	fn create_int8_column(name: &str, data: Vec<i64>, bitvec: Vec<bool>) -> Column {
		Column::new(
			Fragment::internal(name),
			ColumnData::int8_with_bitvec(data, bitvec),
		)
	}

	fn create_float8_column(name: &str, data: Vec<f64>, bitvec: Vec<bool>) -> Column {
		Column::new(
			Fragment::internal(name),
			ColumnData::float8_with_bitvec(data, bitvec),
		)
	}

	#[test]
	fn test_xor_basic() {
		let left = create_bool_column("left", vec![true, true, false, false], vec![true, true, true, true]);
		let right = create_bool_column("right", vec![true, false, true, false], vec![true, true, true, true]);

		let result = eval_logical_xor(&left, &right).unwrap();

		if let ColumnData::Bool(container) = result.data() {
			assert_eq!(container.get(0), Some(false)); // true XOR true = false
			assert_eq!(container.get(1), Some(true)); // true XOR false = true
			assert_eq!(container.get(2), Some(true)); // false XOR true = true
			assert_eq!(container.get(3), Some(false)); // false XOR false = false
		} else {
			panic!("Expected Bool column");
		}
	}

	#[test]
	fn test_xor_with_nulls() {
		let left = create_bool_column("left", vec![true, false, true, false], vec![true, true, false, false]);
		let right = create_bool_column("right", vec![true, false, true, false], vec![true, false, true, false]);

		let result = eval_logical_xor(&left, &right).unwrap();

		if let ColumnData::Bool(container) = result.data() {
			assert_eq!(container.get(0), Some(false)); // true XOR true = false
			assert_eq!(container.get(1), None); // false XOR null = null
			assert_eq!(container.get(2), None); // null XOR true = null
			assert_eq!(container.get(3), None); // null XOR null = null
		} else {
			panic!("Expected Bool column");
		}
	}

	#[test]
	fn test_rem_int8() {
		let left = create_int8_column("left", vec![10, 17, -10, 10], vec![true, true, true, true]);
		let right = create_int8_column("right", vec![3, 5, 3, 0], vec![true, true, true, true]);

		let result = eval_rem(&left, &right).unwrap();

		// Type::promote(Int8, Int8) returns Int16 to prevent overflow
		if let ColumnData::Int16(container) = result.data() {
			assert_eq!(container.get(0), Some(&1)); // 10 % 3 = 1
			assert_eq!(container.get(1), Some(&2)); // 17 % 5 = 2
			assert_eq!(container.get(2), Some(&-1)); // -10 % 3 = -1
			assert_eq!(container.get(3), None); // 10 % 0 = null (division by zero)
		} else {
			panic!("Expected Int16 column, got {:?}", result.data().get_type());
		}
	}

	#[test]
	fn test_rem_float8() {
		let left = create_float8_column("left", vec![10.5, 17.3, -10.2, 10.0], vec![true, true, true, true]);
		let right = create_float8_column("right", vec![3.0, 5.0, 3.0, 0.0], vec![true, true, true, true]);

		let result = eval_rem(&left, &right).unwrap();

		if let ColumnData::Float8(container) = result.data() {
			assert!((container.get(0).unwrap() - 1.5).abs() < 0.0001); // 10.5 % 3.0 ≈ 1.5
			assert!((container.get(1).unwrap() - 2.3).abs() < 0.0001); // 17.3 % 5.0 ≈ 2.3
			assert!((container.get(2).unwrap() - (-1.2)).abs() < 0.0001); // -10.2 % 3.0 ≈ -1.2
			assert_eq!(container.get(3), None); // 10.0 % 0.0 = null (division by zero)
		} else {
			panic!("Expected Float8 column");
		}
	}

	#[test]
	fn test_rem_mixed_types() {
		// Int8 % Float8
		let left = create_int8_column("left", vec![10, 17], vec![true, true]);
		let right = create_float8_column("right", vec![3.0, 5.5], vec![true, true]);

		let result = eval_rem(&left, &right).unwrap();

		if let ColumnData::Float8(container) = result.data() {
			assert!((container.get(0).unwrap() - 1.0).abs() < 0.0001); // 10 % 3.0 = 1.0
			assert!((container.get(1).unwrap() - 0.5).abs() < 0.0001); // 17 % 5.5 = 0.5 (17 = 5.5*3 + 0.5)
		} else {
			panic!("Expected Float8 column");
		}

		// Float8 % Int8
		let left = create_float8_column("left", vec![10.5, 17.3], vec![true, true]);
		let right = create_int8_column("right", vec![3, 5], vec![true, true]);

		let result = eval_rem(&left, &right).unwrap();

		if let ColumnData::Float8(container) = result.data() {
			assert!((container.get(0).unwrap() - 1.5).abs() < 0.0001); // 10.5 % 3 = 1.5
			assert!((container.get(1).unwrap() - 2.3).abs() < 0.0001); // 17.3 % 5 ≈ 2.3
		} else {
			panic!("Expected Float8 column");
		}
	}

	#[test]
	fn test_unary_not() {
		let col = create_bool_column("col", vec![true, false, true, false], vec![true, true, false, false]);

		let result = eval_unary(UnaryPlanOp::Not, &col).unwrap();

		if let ColumnData::Bool(container) = result.data() {
			assert_eq!(container.get(0), Some(false)); // NOT true = false
			assert_eq!(container.get(1), Some(true)); // NOT false = true
			assert_eq!(container.get(2), None); // NOT null = null
			assert_eq!(container.get(3), None); // NOT null = null
		} else {
			panic!("Expected Bool column");
		}
	}

	#[test]
	fn test_unary_neg_int8() {
		let col = create_int8_column("col", vec![10, -20, 0, 5], vec![true, true, true, false]);

		let result = eval_unary(UnaryPlanOp::Neg, &col).unwrap();

		if let ColumnData::Int8(container) = result.data() {
			assert_eq!(container.get(0), Some(&-10)); // NEG 10 = -10
			assert_eq!(container.get(1), Some(&20)); // NEG -20 = 20
			assert_eq!(container.get(2), Some(&0)); // NEG 0 = 0
			assert_eq!(container.get(3), None); // NEG null = null
		} else {
			panic!("Expected Int8 column");
		}
	}

	#[test]
	fn test_unary_neg_float8() {
		let col = create_float8_column("col", vec![10.5, -20.3, 0.0, 5.5], vec![true, true, true, false]);

		let result = eval_unary(UnaryPlanOp::Neg, &col).unwrap();

		if let ColumnData::Float8(container) = result.data() {
			assert_eq!(container.get(0), Some(&-10.5)); // NEG 10.5 = -10.5
			assert_eq!(container.get(1), Some(&20.3)); // NEG -20.3 = 20.3
			assert_eq!(container.get(2), Some(&0.0)); // NEG 0.0 = 0.0
			assert_eq!(container.get(3), None); // NEG null = null
		} else {
			panic!("Expected Float8 column");
		}
	}

	#[test]
	fn test_unary_plus() {
		let col = create_int8_column("col", vec![10, -20, 0], vec![true, true, true]);

		let result = eval_unary(UnaryPlanOp::Plus, &col).unwrap();

		// Plus is a no-op, should return identical column
		if let ColumnData::Int8(container) = result.data() {
			assert_eq!(container.get(0), Some(&10));
			assert_eq!(container.get(1), Some(&-20));
			assert_eq!(container.get(2), Some(&0));
		} else {
			panic!("Expected Int8 column");
		}
	}
}
