// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{internal_error, value::column::columns::Columns};
use reifydb_type::{
	error::{RuntimeErrorKind, TypeError},
	fragment::Fragment,
	value::{Value, frame::frame::Frame},
};

use crate::{
	Result,
	vm::{stack::Variable, vm::Vm},
};

impl Vm {
	pub(crate) fn exec_push_const(&mut self, value: &Value) {
		self.stack.push(Variable::scalar(value.clone()));
	}

	pub(crate) fn exec_push_none(&mut self) {
		self.stack.push(Variable::scalar(Value::none()));
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
			Variable::Columns(c)
			| Variable::ForIterator {
				columns: c,
				..
			} => {
				result.push(Frame::from(c));
			}
			Variable::Scalar(c) => {
				result.push(Frame::from(c));
			}
			Variable::Closure(_) => {
				result.push(Frame::from(Columns::scalar(Value::none())));
			}
		}
	}

	pub(crate) fn exec_append(&mut self, target: &Fragment) -> Result<()> {
		let clean_name = strip_dollar_prefix(target.text());
		let columns = match self.stack.pop()? {
			Variable::Columns(cols) => cols,
			_ => {
				return Err(internal_error!("APPEND requires columns/frame data on stack"));
			}
		};

		match self.symbols.get(clean_name) {
			Some(Variable::Columns(_)) => {
				let mut existing = match self.symbols.get(clean_name).unwrap() {
					Variable::Columns(f) => f.clone(),
					_ => unreachable!(),
				};
				existing.append_columns(columns)?;
				self.symbols.reassign(clean_name.to_string(), Variable::Columns(existing))?;
			}
			None => {
				self.symbols.set(clean_name.to_string(), Variable::Columns(columns), true)?;
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
