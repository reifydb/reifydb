// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	internal_error,
	value::{
		batch::{batch, from_rows, is_scalar, scalar_value},
		column::{builder::ColumnBuilder, factory},
	},
};
use reifydb_evaluate::stack::Variable;
use reifydb_value::{
	fragment::Fragment,
	value::{Value, column_view::ColumnView, system_columns::user_columns},
};

use crate::{Result, vm::vm::Vm};

impl<'a> Vm<'a> {
	pub(crate) fn exec_for_init(&mut self, variable_name: &Fragment) -> Result<()> {
		let columns = match self.stack.pop()? {
			Variable::Columns {
				batch: c,
				..
			}
			| Variable::ForIterator {
				batch: c,
				..
			} => c,
			Variable::Closure(_) => {
				return Err(internal_error!("ForInit expects Columns on data stack, got Scalar"));
			}
		};
		let columns = if is_scalar(&columns) {
			let mut value = scalar_value(&columns)?;
			while let Value::Any(inner) = value {
				value = *inner;
			}
			match value {
				Value::List(items) => {
					let rows: Vec<Vec<Value>> = items.into_iter().map(|item| vec![item]).collect();
					let name = user_columns(&columns)
						.next()
						.map(|(field, _)| field.name().clone())
						.ok_or_else(|| internal_error!("ForInit scalar has no user column"))?;
					from_rows(&[name.as_str()], &rows)?
				}
				_ => columns,
			}
		} else {
			columns
		};
		let var_name = variable_name.text();
		let iter_key = format!("__for_{}", var_name);
		self.symbols.set(
			iter_key,
			Variable::ForIterator {
				batch: columns,
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
				batch,
				index,
			}) => (batch.clone(), *index),
			_ => {
				self.ip = end_addr;
				return Ok(true);
			}
		};

		if index >= columns.num_rows() {
			self.ip = end_addr;
			return Ok(true);
		}

		let user: Vec<_> = user_columns(&columns).collect();
		if user.len() == 1 {
			let (field, array) = user[0];
			let value = ColumnView::try_from((array, field.as_ref()))?.get_value(index);
			self.symbols.set(clean_name.to_string(), Variable::scalar(value), true)?;
		} else {
			let mut row_columns = Vec::new();
			for (field, array) in user {
				let value = ColumnView::try_from((array, field.as_ref()))?.get_value(index);
				let column = match value {
					Value::None {
						..
					} => factory::from_many(field.name(), value, 1),
					_ => {
						let mut data = ColumnBuilder::with_capacity(value.get_type(), 1);
						data.push_value(value);
						data.finish(field.name())
					}
				};
				row_columns.push(column);
			}
			let row_frame = batch(row_columns)?;
			self.symbols.set(clean_name.to_string(), Variable::columns(row_frame), true)?;
		}

		self.symbols.reassign(
			iter_key,
			Variable::ForIterator {
				batch: columns,
				index: index + 1,
			},
		)?;
		Ok(false)
	}
}
