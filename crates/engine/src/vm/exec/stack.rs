// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	internal_error,
	value::{
		batch::{append, batch, single_row},
		column::{builder::ColumnBuilder, factory},
	},
};
use reifydb_evaluate::stack::{Variable, strip_dollar_prefix};
use reifydb_value::{
	error::{RuntimeErrorKind, TypeError},
	fragment::Fragment,
	value::{Value, frame::frame::Frame, system_columns::user_columns},
};

use crate::{Result, vm::vm::Vm};

impl<'a> Vm<'a> {
	pub(crate) fn exec_push_const(&mut self, value: &Value) -> Result<()> {
		if self.batch_size != 1 {
			let mut data = ColumnBuilder::with_capacity(value.get_type(), self.batch_size);
			for _ in 0..self.batch_size {
				data.push_value(value.clone());
			}
			self.stack.push(Variable::columns(batch(vec![data.finish("const")])?));
		} else {
			self.stack.push(Variable::scalar(value.clone()));
		}
		Ok(())
	}

	pub(crate) fn exec_push_none(&mut self) -> Result<()> {
		if self.batch_size != 1 {
			self.stack.push(Variable::columns(batch(vec![factory::none("none", self.batch_size)])?));
		} else {
			self.stack.push(Variable::scalar(Value::none()));
		}
		Ok(())
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

	pub(crate) fn exec_emit(&mut self, result: &mut Vec<Frame>) -> Result<()> {
		let Some(value) = self.stack.pop().ok() else {
			return Ok(());
		};
		match value {
			Variable::Columns {
				batch: c,
				..
			}
			| Variable::ForIterator {
				batch: c,
				..
			} => {
				result.push(Frame::from(c));
			}
			Variable::Closure(_) => {
				result.push(Frame::from(single_row([("value", Value::none())])?));
			}
		}
		Ok(())
	}

	pub(crate) fn exec_append(&mut self, target: &Fragment) -> Result<()> {
		let clean_name = strip_dollar_prefix(target.text());
		let columns = match self.stack.pop()? {
			Variable::Columns {
				batch: cols,
				..
			} => cols,
			_ => {
				return Err(internal_error!("APPEND requires columns/frame data on stack"));
			}
		};

		match self.symbols.get(clean_name) {
			Some(Variable::Columns {
				batch: existing,
			}) => {
				let existing_names: Vec<String> =
					user_columns(existing).map(|(field, _)| field.name().clone()).collect();
				let incoming_names: Vec<String> =
					user_columns(&columns).map(|(field, _)| field.name().clone()).collect();
				if existing_names != incoming_names {
					return Err(TypeError::Runtime {
						kind: RuntimeErrorKind::AppendColumnMismatch {
							name: clean_name.to_string(),
							existing: existing_names.clone(),
							incoming: incoming_names.clone(),
							fragment: target.clone(),
						},
						message: format!(
							"Cannot APPEND to '${}': existing column {} does not match incoming column {}.",
							clean_name,
							format_column_list(&existing_names),
							format_column_list(&incoming_names),
						),
					}
					.into());
				}
				let appended = append(existing, &columns)?;
				self.symbols.reassign(clean_name.to_string(), Variable::columns(appended))?;
			}
			None => {
				self.symbols.set(clean_name.to_string(), Variable::columns(columns), true)?;
			}
			Some(Variable::Closure(_))
			| Some(Variable::ForIterator {
				..
			}) => {
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

fn format_column_list(names: &[String]) -> String {
	format!("[{}]", names.join(", "))
}
