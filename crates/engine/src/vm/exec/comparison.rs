// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::Value;

use crate::{
	Result,
	vm::{stack::Variable, vm::Vm},
};

impl Vm {
	/// Pop two values, apply an infallible comparison/logic operation, push the result.
	pub(crate) fn exec_cmp_op(&mut self, op: fn(&Value, &Value) -> Value) -> Result<()> {
		let right = self.pop_value()?;
		let left = self.pop_value()?;
		self.stack.push(Variable::scalar(op(&left, &right)));
		Ok(())
	}
}
