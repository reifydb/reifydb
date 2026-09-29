// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_core::{
	internal_error,
	value::{
		batch::{batch, is_scalar, scalar_value},
		column::{builder::ColumnBuilder, factory},
	},
};
use reifydb_evaluate::stack::{Variable, strip_dollar_prefix};
use reifydb_value::{
	error::{RuntimeErrorKind, TypeError},
	fragment::Fragment,
	value::{
		column_view::ColumnView,
		system_columns::{is_system_field, user_columns},
	},
};

use crate::{Result, vm::vm::Vm};

impl<'a> Vm<'a> {
	pub(crate) fn exec_load_var(&mut self, fragment: &Fragment) -> Result<()> {
		let name = strip_dollar_prefix(fragment.text());
		match self.symbols.get(name) {
			Some(Variable::Columns {
				batch: c,
			}) if is_scalar(c) => {
				if self.batch_size != 1 {
					let value = scalar_value(c)?;
					let mut data = ColumnBuilder::with_capacity(value.get_type(), self.batch_size);
					for _ in 0..self.batch_size {
						data.push_value(value.clone());
					}
					self.stack.push(Variable::columns(batch(vec![data.finish(name)])?));
				} else {
					self.stack.push(Variable::columns(c.clone()));
				}
			}
			Some(Variable::Closure(c)) => {
				self.stack.push(Variable::Closure(c.clone()));
			}
			Some(Variable::Columns {
				batch: c,
			}) => {
				if self.batch_size != 1 {
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
						fragment: fragment.clone(),
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
		let sv = self.stack.pop()?;
		let variable = match sv {
			Variable::Columns {
				batch: c,
			} if is_scalar(&c) => Variable::scalar_named(name, scalar_value(&c)?),
			Variable::Columns {
				batch: c,
			}
			| Variable::ForIterator {
				batch: c,
				..
			} => Variable::columns(c),
			Variable::Closure(c) => Variable::Closure(c),
		};
		self.symbols.reassign(name.to_string(), variable)?;
		Ok(())
	}

	pub(crate) fn exec_declare_var(&mut self, fragment: &Fragment) -> Result<()> {
		let name = strip_dollar_prefix(fragment.text());
		if name == "identity" {
			return Err(TypeError::Runtime {
				kind: RuntimeErrorKind::VariableIsImmutable {
					name: name.to_string(),
				},
				message: "'$identity' is reserved and cannot be declared or shadowed".to_string(),
			}
			.into());
		}
		let sv = self.stack.pop()?;
		let variable = match sv {
			Variable::Closure(c) => Variable::Closure(c),
			Variable::Columns {
				batch: c,
			}
			| Variable::ForIterator {
				batch: c,
				..
			} => {
				if is_scalar(&c) {
					Variable::columns(rename_user_columns(&c, name)?)
				} else {
					Variable::columns(c)
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
			Some(Variable::Columns {
				batch: columns,
			}) if !is_scalar(columns) => {
				let found = user_columns(columns).find(|(field, _)| field.name() == field_name);
				match found {
					Some((field, array)) => {
						let value = ColumnView::try_from((array, field.as_ref()))?.get_value(0);
						self.stack.push(Variable::scalar(value));
					}
					None => {
						let available: Vec<String> = user_columns(columns)
							.map(|(field, _)| field.name().clone())
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
						fragment: object.clone(),
					},
					message: format!("Variable '{}' is not defined", var_name),
				}
				.into());
			}
		}
		Ok(())
	}
}

fn rename_user_columns(scalar: &RecordBatch, name: &str) -> Result<RecordBatch> {
	let columns = scalar
		.schema_ref()
		.fields()
		.iter()
		.zip(scalar.columns())
		.map(|(field, array)| {
			let column = (field.clone(), array.clone());
			if is_system_field(field) {
				column
			} else {
				factory::rename(column, name)
			}
		})
		.collect();
	batch(columns)
}
