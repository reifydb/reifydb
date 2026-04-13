// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::columns::Columns;
use reifydb_type::{
	error::{BinaryOp, IntoDiagnostic, LogicalOp, TypeError},
	fragment::Fragment,
};

use super::broadcast::broadcast_to_match;
use crate::{
	Result,
	expression::{
		compare::{
			CompareOp, Equal, GreaterThan, GreaterThanEqual, LessThan, LessThanEqual, NotEqual,
			compare_columns,
		},
		logic::execute_logical_op,
	},
	vm::{stack::Variable, vm::Vm},
};

impl Vm {
	/// Pop two columns, apply a columnar comparison, push the boolean result column.
	fn exec_columnar_cmp<Op: CompareOp>(&mut self, binary_op: BinaryOp) -> Result<()> {
		let right = self.pop_as_column()?;
		let left = self.pop_as_column()?;
		let (left, right) = broadcast_to_match(left, right);
		let result = compare_columns::<Op>(&left, &right, Fragment::internal("vm_cmp"), |frag, lt, rt| {
			TypeError::BinaryOperatorNotApplicable {
				operator: binary_op.clone(),
				left: lt,
				right: rt,
				fragment: frag,
			}
			.into_diagnostic()
		})?;
		self.stack.push(Variable::columns(Columns::new(vec![result])));
		Ok(())
	}

	/// Pop two columns, apply a columnar boolean logic op, push the boolean result column.
	fn exec_columnar_logic(&mut self, op: LogicalOp, bool_fn: fn(bool, bool) -> bool) -> Result<()> {
		let right = self.pop_as_column()?;
		let left = self.pop_as_column()?;
		let (left, right) = broadcast_to_match(left, right);
		let frag = Fragment::internal("vm_logic");
		let result = execute_logical_op(&left, &right, &frag, op, bool_fn)?;
		self.stack.push(Variable::columns(Columns::new(vec![result])));
		Ok(())
	}

	pub(crate) fn exec_cmp_eq(&mut self) -> Result<()> {
		self.exec_columnar_cmp::<Equal>(BinaryOp::Equal)
	}

	pub(crate) fn exec_cmp_ne(&mut self) -> Result<()> {
		self.exec_columnar_cmp::<NotEqual>(BinaryOp::NotEqual)
	}

	pub(crate) fn exec_cmp_lt(&mut self) -> Result<()> {
		self.exec_columnar_cmp::<LessThan>(BinaryOp::LessThan)
	}

	pub(crate) fn exec_cmp_le(&mut self) -> Result<()> {
		self.exec_columnar_cmp::<LessThanEqual>(BinaryOp::LessThanEqual)
	}

	pub(crate) fn exec_cmp_gt(&mut self) -> Result<()> {
		self.exec_columnar_cmp::<GreaterThan>(BinaryOp::GreaterThan)
	}

	pub(crate) fn exec_cmp_ge(&mut self) -> Result<()> {
		self.exec_columnar_cmp::<GreaterThanEqual>(BinaryOp::GreaterThanEqual)
	}

	pub(crate) fn exec_logic_and(&mut self) -> Result<()> {
		self.exec_columnar_logic(LogicalOp::And, |a, b| a && b)
	}

	pub(crate) fn exec_logic_or(&mut self) -> Result<()> {
		self.exec_columnar_logic(LogicalOp::Or, |a, b| a || b)
	}

	pub(crate) fn exec_logic_xor(&mut self) -> Result<()> {
		self.exec_columnar_logic(LogicalOp::Xor, |a, b| a != b)
	}
}
