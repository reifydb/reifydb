// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::{Value, r#type::Type};

use crate::{
	Result,
	vm::{scalar, stack::Variable, vm::Vm},
};

impl Vm {
	pub(crate) fn exec_between(&mut self) -> Result<()> {
		let upper = self.pop_value()?;
		let lower = self.pop_value()?;
		let value = self.pop_value()?;
		let ge = scalar::scalar_ge(&value, &lower);
		let le = scalar::scalar_le(&value, &upper);
		let result = match (ge, le) {
			(Value::Boolean(a), Value::Boolean(b)) => Value::Boolean(a && b),
			_ => Value::none(),
		};
		self.stack.push(Variable::scalar(result));
		Ok(())
	}

	pub(crate) fn exec_in_list(&mut self, count: u16, negated: bool) -> Result<()> {
		let count = count as usize;
		let mut list_items = Vec::with_capacity(count);
		for _ in 0..count {
			list_items.push(self.pop_value()?);
		}
		list_items.reverse();
		let value = self.pop_value()?;
		let has_undefined = matches!(value, Value::None { .. })
			|| list_items.iter().any(|item| matches!(item, Value::None { .. }));
		if has_undefined {
			self.stack.push(Variable::scalar(Value::none()));
		} else {
			let found = list_items
				.iter()
				.any(|item| matches!(scalar::scalar_eq(&value, item), Value::Boolean(true)));
			let result = if negated {
				!found
			} else {
				found
			};
			self.stack.push(Variable::scalar(Value::Boolean(result)));
		}
		Ok(())
	}

	pub(crate) fn exec_cast(&mut self, target: &Type) -> Result<()> {
		let value = self.pop_value()?;
		self.stack.push(Variable::scalar(scalar::scalar_cast(value, target.clone())?));
		Ok(())
	}
}
