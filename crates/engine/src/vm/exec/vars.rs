// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	internal_error,
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_type::{
	error::{RuntimeErrorKind, TypeError},
	fragment::Fragment,
};

use super::stack::strip_dollar_prefix;
use crate::{
	Result,
	vm::{stack::Variable, vm::Vm},
};

impl<'a> Vm<'a> {
	pub(crate) fn exec_load_var(&mut self, fragment: &Fragment) -> Result<()> {
		let name = strip_dollar_prefix(fragment.text());
		match self.symbols.get(name) {
			Some(Variable::Columns {
				columns: c,
			}) if c.is_scalar() => {
				if self.batch_size > 1 {
					// Broadcast scalar to batch_size rows
					let value = c.scalar_value();
					let mut data = ColumnData::with_capacity(value.get_type(), self.batch_size);
					for _ in 0..self.batch_size {
						data.push_value(value.clone());
					}
					let col = Column::new(Fragment::internal(name), data);
					self.stack.push(Variable::columns(Columns::new(vec![col])));
				} else {
					self.stack.push(Variable::columns(c.clone()));
				}
			}
			Some(Variable::Closure(c)) => {
				self.stack.push(Variable::Closure(c.clone()));
			}
			Some(Variable::Columns {
				columns: c,
			}) => {
				if self.batch_size > 1 {
					// In columnar mode, Columns variables are valid on the stack
					self.stack.push(Variable::columns(c.clone()));
				} else {
					return Err(TypeError::Runtime {
						kind: RuntimeErrorKind::VariableIsDataframe {
							name: name.to_string(),
						},
						message: format!(
							"Variable '{}' contains a dataframe and cannot be used directly in scalar expressions",
							name
						),
					}
					.into());
				}
			}
			Some(Variable::ForIterator {
				..
			}) => {
				return Err(internal_error!("Cannot load a FOR iterator as a value"));
			}
			None => {
				return Err(TypeError::Runtime {
					kind: RuntimeErrorKind::VariableNotFound {
						name: name.to_string(),
					},
					message: format!("Variable '{}' is not defined", name),
				}
				.into());
			}
		}
		Ok(())
	}

	pub(crate) fn exec_store_var(&mut self, fragment: &Fragment) -> Result<()> {
		let name = strip_dollar_prefix(fragment.text());
		let value = self.pop_value()?;
		self.symbols.reassign(name.to_string(), Variable::scalar_named(name, value))?;
		Ok(())
	}

	pub(crate) fn exec_declare_var(&mut self, fragment: &Fragment) -> Result<()> {
		let name = strip_dollar_prefix(fragment.text());
		let sv = self.stack.pop()?;
		let variable = match sv {
			Variable::Closure(c) => Variable::Closure(c),
			Variable::Columns {
				columns: mut c,
			}
			| Variable::ForIterator {
				columns: mut c,
				..
			} => {
				if c.is_scalar() {
					c.columns.make_mut()[0].name = Fragment::internal(name);
				}
				Variable::columns(c)
			}
		};
		self.symbols.set(name.to_string(), variable, true)?;
		Ok(())
	}

	pub(crate) fn exec_field_access(&mut self, object: &Fragment, field: &Fragment) -> Result<()> {
		let var_name = strip_dollar_prefix(object.text());
		let field_name = field.text();
		match self.symbols.get(var_name) {
			Some(Variable::Columns {
				columns,
			}) if !columns.is_scalar() => {
				let col = columns.columns.iter().find(|c| c.name.text() == field_name);
				match col {
					Some(col) => {
						let value = col.data.get_value(0);
						self.stack.push(Variable::scalar(value));
					}
					None => {
						let available: Vec<String> = columns
							.columns
							.iter()
							.map(|c| c.name.text().to_string())
							.collect();
						return Err(TypeError::Runtime {
							kind: RuntimeErrorKind::FieldNotFound {
								variable: var_name.to_string(),
								field: field_name.to_string(),
								available: available.clone(),
							},
							message: format!(
								"Field '{}' not found on variable '{}'",
								field_name, var_name
							),
						}
						.into());
					}
				}
			}
			Some(Variable::Columns {
				..
			})
			| Some(Variable::Closure(_)) => {
				return Err(TypeError::Runtime {
					kind: RuntimeErrorKind::FieldNotFound {
						variable: var_name.to_string(),
						field: field_name.to_string(),
						available: vec![],
					},
					message: format!("Field '{}' not found on variable '{}'", field_name, var_name),
				}
				.into());
			}
			Some(Variable::ForIterator {
				..
			}) => {
				return Err(TypeError::Runtime {
					kind: RuntimeErrorKind::VariableIsDataframe {
						name: var_name.to_string(),
					},
					message: format!(
						"Variable '{}' contains a dataframe and cannot be used directly in scalar expressions",
						var_name
					),
				}
				.into());
			}
			None => {
				return Err(TypeError::Runtime {
					kind: RuntimeErrorKind::VariableNotFound {
						name: var_name.to_string(),
					},
					message: format!("Variable '{}' is not defined", var_name),
				}
				.into());
			}
		}
		Ok(())
	}
}
