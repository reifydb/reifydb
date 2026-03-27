// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::Value;

use crate::{
	Result,
	vm::{scalar, stack::Variable, vm::Vm},
};

impl Vm {
	/// Pop two values, apply a fallible binary operation, push the result.
	pub(crate) fn exec_binop(&mut self, op: fn(Value, Value) -> Result<Value>) -> Result<()> {
		let right = self.pop_value()?;
		let left = self.pop_value()?;
		self.stack.push(Variable::scalar(op(left, right)?));
		Ok(())
	}

	/// Pop one value, apply a fallible unary operation, push the result.
	pub(crate) fn exec_negate(&mut self) -> Result<()> {
		let value = self.pop_value()?;
		self.stack.push(Variable::scalar(scalar::scalar_negate(value)?));
		Ok(())
	}

	/// Pop one value, apply scalar_not (infallible), push the result.
	pub(crate) fn exec_logic_not(&mut self) -> Result<()> {
		let value = self.pop_value()?;
		self.stack.push(Variable::scalar(scalar::scalar_not(&value)));
		Ok(())
	}
}
