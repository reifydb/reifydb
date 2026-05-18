// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	internal_error,
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_type::{fragment::Fragment, value::r#type::Type};

use crate::{
	Result,
	vm::{stack::Variable, vm::Vm},
};

impl<'a> Vm<'a> {
	pub(crate) fn exec_for_init(&mut self, variable_name: &Fragment) -> Result<()> {
		let columns = match self.stack.pop()? {
			Variable::Columns {
				columns: c,
				..
			}
			| Variable::ForIterator {
				columns: c,
				..
			} => c,
			Variable::Closure(_) => {
				return Err(internal_error!("ForInit expects Columns on data stack, got Scalar"));
			}
		};
		let var_name = variable_name.text();
		let iter_key = format!("__for_{}", var_name);
		self.symbols.set(
			iter_key,
			Variable::ForIterator {
				columns,
				index: 0,
			},
			true,
		)?;
		Ok(())
	}

	pub(crate) fn exec_for_next(&mut self, variable_name: &Fragment, end_addr: usize) -> Result<bool> {
		let var_name = variable_name.text();
		let clean_name = var_name.strip_prefix('$').unwrap_or(var_name);
		let iter_key = format!("__for_{}", var_name);

		let (columns, index) = match self.symbols.get(&iter_key) {
			Some(Variable::ForIterator {
				columns,
				index,
			}) => (columns.clone(), *index),
			_ => {
				self.ip = end_addr;
				return Ok(true);
			}
		};

		if index >= columns.row_count() {
			self.ip = end_addr;
			return Ok(true);
		}

		if columns.len() == 1 {
			let value = columns.columns[0].get_value(index);
			self.symbols.set(clean_name.to_string(), Variable::scalar(value), true)?;
		} else {
			let mut row_columns = Vec::new();
			for (name, col) in columns.names.iter().zip(columns.columns.iter()) {
				let value = col.get_value(index);
				let mut data = ColumnBuffer::none_typed(Type::Boolean, 0);
				data.push_value(value);
				row_columns.push(ColumnWithName::new(name.clone(), data));
			}
			let row_frame = Columns::new(row_columns);
			self.symbols.set(clean_name.to_string(), Variable::columns(row_frame), true)?;
		}

		self.symbols.reassign(
			iter_key,
			Variable::ForIterator {
				columns,
				index: index + 1,
			},
		)?;
		Ok(false)
	}
}
