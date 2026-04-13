// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	internal_error,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_type::{
	error::{RuntimeErrorKind, TypeError},
	fragment::Fragment,
	value::{Value, frame::frame::Frame, r#type::Type},
};

use crate::{
	Result,
	vm::{stack::Variable, vm::Vm},
};

impl Vm {
	pub(crate) fn exec_push_const(&mut self, value: &Value) {
		if self.batch_size > 1 {
			let mut data = ColumnData::with_capacity(value.get_type(), self.batch_size);
			for _ in 0..self.batch_size {
				data.push_value(value.clone());
			}
			let col = Column::new(Fragment::internal("const"), data);
			self.stack.push(Variable::Columns {
				columns: Columns::new(vec![col]),
				is_scalar: false,
			});
		} else {
			self.stack.push(Variable::scalar(value.clone()));
		}
	}

	pub(crate) fn exec_push_none(&mut self) {
		if self.batch_size > 1 {
			let data = ColumnData::none_typed(Type::Any, self.batch_size);
			let col = Column::new(Fragment::internal("none"), data);
			self.stack.push(Variable::Columns {
				columns: Columns::new(vec![col]),
				is_scalar: false,
			});
		} else {
			self.stack.push(Variable::scalar(Value::none()));
		}
	}

	pub(crate) fn exec_pop(&mut self) -> Result<()> {
		self.stack.pop()?;
		Ok(())
	}

	pub(crate) fn exec_dup(&mut self) -> Result<()> {
		let value = self.stack.pop()?;
		let cloned = value.clone();
		self.stack.push(value);
		self.stack.push(cloned);
		Ok(())
	}

	pub(crate) fn exec_emit(&mut self, result: &mut Vec<Frame>) {
		let Some(value) = self.stack.pop().ok() else {
			return;
		};
		match value {
			Variable::Columns {
				columns: c,
				..
			}
			| Variable::ForIterator {
				columns: c,
				..
			} => {
				result.push(Frame::from(c));
			}
			Variable::Closure(_) => {
				result.push(Frame::from(Columns::scalar(Value::none())));
			}
		}
	}

	pub(crate) fn exec_append(&mut self, target: &Fragment) -> Result<()> {
		let clean_name = strip_dollar_prefix(target.text());
		let mut columns = match self.stack.pop()? {
			Variable::Columns {
				columns: cols,
				..
			} => cols,
			_ => {
				return Err(internal_error!("APPEND requires columns/frame data on stack"));
			}
		};

		// Drop rows that are masked out in the current execution context.
		// Without this, an APPEND inside a vectorized IF/WHILE would
		// unconditionally write every row, ignoring the branch condition.
		if self.batch_size > 1 && (self.active_mask.is_some() || !self.mask_stack.is_empty()) {
			let mask = self.effective_mask();
			for col in columns.columns.make_mut().iter_mut() {
				col.data_mut().filter(&mask)?;
			}
		}

		match self.symbols.get(clean_name) {
			Some(Variable::Columns {
				is_scalar: false,
				..
			}) => {
				let mut existing = match self.symbols.get(clean_name).unwrap() {
					Variable::Columns {
						columns: f,
						..
					} => f.clone(),
					_ => unreachable!(),
				};
				existing.append_columns(columns)?;
				self.symbols.reassign(clean_name.to_string(), Variable::columns(existing))?;
			}
			None => {
				self.symbols.set(clean_name.to_string(), Variable::columns(columns), true)?;
			}
			_ => {
				return Err(TypeError::Runtime {
					kind: RuntimeErrorKind::AppendTargetNotFrame {
						name: clean_name.to_string(),
					},
					message: format!(
						"Cannot APPEND to variable '{}' because it is not a Frame",
						clean_name
					),
				}
				.into());
			}
		}
		Ok(())
	}
}

pub(crate) fn strip_dollar_prefix(name: &str) -> &str {
	name.strip_prefix('$').unwrap_or(name)
}
