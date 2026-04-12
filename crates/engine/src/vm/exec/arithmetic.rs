// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, columns::Columns};
use reifydb_rql::expression::PrefixOperator;
use reifydb_type::fragment::Fragment;

use super::broadcast::broadcast_to_match;
use crate::{
	Result,
	expression::{
		arith::{add::add_columns, div::div_columns, mul::mul_columns, rem::rem_columns, sub::sub_columns},
		context::{EvalContext, EvalSession},
		prefix::prefix_apply,
	},
	vm::{stack::Variable, vm::Vm},
};

impl Vm {
	fn exec_binary_column_op<F>(&mut self, op: F, frag: fn() -> Fragment) -> Result<()>
	where
		F: FnOnce(&EvalContext, &Column, &Column, fn() -> Fragment) -> Result<Column>,
	{
		let right = self.pop_as_column()?;
		let left = self.pop_as_column()?;
		let (left, right) = broadcast_to_match(left, right);
		let session = EvalSession::testing();
		let ctx = session.eval(Columns::empty(), self.batch_size);
		let result = op(&ctx, &left, &right, frag)?;
		self.stack.push(Variable::columns(Columns::new(vec![result])));
		Ok(())
	}

	pub(crate) fn exec_add(&mut self) -> Result<()> {
		self.exec_binary_column_op(add_columns, || Fragment::internal("vm_add"))
	}

	pub(crate) fn exec_sub(&mut self) -> Result<()> {
		self.exec_binary_column_op(sub_columns, || Fragment::internal("vm_sub"))
	}

	pub(crate) fn exec_mul(&mut self) -> Result<()> {
		self.exec_binary_column_op(mul_columns, || Fragment::internal("vm_mul"))
	}

	pub(crate) fn exec_div(&mut self) -> Result<()> {
		self.exec_binary_column_op(div_columns, || Fragment::internal("vm_div"))
	}

	pub(crate) fn exec_rem(&mut self) -> Result<()> {
		self.exec_binary_column_op(rem_columns, || Fragment::internal("vm_rem"))
	}

	pub(crate) fn exec_negate(&mut self) -> Result<()> {
		let col = self.pop_as_column()?;
		let frag = Fragment::internal("vm_negate");
		let result = prefix_apply(&col, &PrefixOperator::Minus(frag.clone()), &frag)?;
		self.stack.push(Variable::columns(Columns::new(vec![result])));
		Ok(())
	}

	pub(crate) fn exec_logic_not(&mut self) -> Result<()> {
		let col = self.pop_as_column()?;
		let frag = Fragment::internal("vm_not");
		let result = prefix_apply(&col, &PrefixOperator::Not(frag.clone()), &frag)?;
		self.stack.push(Variable::columns(Columns::new(vec![result])));
		Ok(())
	}
}
