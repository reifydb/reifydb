// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::internal_error;
use reifydb_type::{
	error::{RuntimeErrorKind, TypeError},
	fragment::Fragment,
};

use super::stack::strip_dollar_prefix;
use crate::{
	Result,
	vm::{stack::Variable, vm::Vm},
};

impl Vm {
	pub(crate) fn exec_load_var(&mut self, fragment: &Fragment) -> Result<()> {
		let name = strip_dollar_prefix(fragment.text());
		match self.symbols.get(name) {
			Some(Variable::Scalar(c)) => {
				self.stack.push(Variable::Scalar(c.clone()));
			}
			Some(Variable::Closure(c)) => {
				self.stack.push(Variable::Closure(c.clone()));
			}
			Some(Variable::Columns(_)) => {
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
		self.symbols.reassign(name.to_string(), Variable::scalar(value))?;
		Ok(())
	}

	pub(crate) fn exec_declare_var(&mut self, fragment: &Fragment) -> Result<()> {
		let name = strip_dollar_prefix(fragment.text());
		let sv = self.stack.pop()?;
		let variable = match sv {
			Variable::Scalar(c) => Variable::Scalar(c),
			Variable::Closure(c) => Variable::Closure(c),
			Variable::Columns(c)
			| Variable::ForIterator {
				columns: c,
				..
			} => {
				if c.len() == 1 && c.row_count() == 1 {
					Variable::Scalar(c)
				} else {
					Variable::Columns(c)
				}
			}
		};
		self.symbols.set(name.to_string(), variable, true)?;
		Ok(())
	}

	pub(crate) fn exec_field_access(&mut self, object: &Fragment, field: &Fragment) -> Result<()> {
		let var_name = strip_dollar_prefix(object.text());
		let field_name = field.text();
		match self.symbols.get(var_name) {
			Some(Variable::Columns(columns)) => {
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
			Some(Variable::Scalar(_)) | Some(Variable::Closure(_)) => {
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
