// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeStringArray, RecordBatch, UInt8Array};
use arrow_schema::FieldRef;
use reifydb_core::{
	internal_error,
	value::{
		batch::{append, batch, batch_with, single_row},
		column::{builder::ColumnBuilder, factory},
	},
};
use reifydb_evaluate::stack::{Variable, strip_dollar_prefix};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	error::{RuntimeErrorKind, TypeError},
	fragment::Fragment,
	value::{
		Value,
		frame::frame::Frame,
		sumtype::SumTypeId,
		system_columns::user_columns,
		value_type::{
			ValueType,
			field::{FieldType, from_field, to_field},
		},
	},
};

use crate::{
	Result,
	vm::{services::Services, vm::Vm},
};

impl<'a> Vm<'a> {
	pub(crate) fn exec_make_list(&mut self, count: usize) -> Result<()> {
		let items = self.pop_scalars(count)?;
		self.exec_push_const(&Value::List(items))
	}

	pub(crate) fn exec_make_record(&mut self, fields: &[String]) -> Result<()> {
		let values = self.pop_scalars(fields.len())?;
		self.exec_push_const(&Value::Record(fields.iter().cloned().zip(values).collect()))
	}

	fn pop_scalars(&mut self, count: usize) -> Result<Vec<Value>> {
		if self.batch_size != 1 {
			return Err(internal_error!(
				"list and record construction requires a scalar context, got a batch of {}",
				self.batch_size
			));
		}
		let mut values = Vec::with_capacity(count);
		for _ in 0..count {
			let mut value = self.pop_value()?;
			while let Value::Any(inner) = value {
				value = *inner;
			}
			values.push(value);
		}
		values.reverse();
		Ok(values)
	}

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

	pub(crate) fn exec_emit(
		&mut self,
		services: &Arc<Services>,
		tx: &mut Transaction<'_>,
		result: &mut Vec<Frame>,
	) -> Result<()> {
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
				result.push(Frame::from(rebuild_enums(c, services, tx)?));
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

fn rebuild_enums(batch: RecordBatch, services: &Arc<Services>, tx: &mut Transaction<'_>) -> Result<RecordBatch> {
	let schema = batch.schema();
	let mut columns = Vec::with_capacity(batch.num_columns());
	for (field, array) in schema.fields().iter().zip(batch.columns()) {
		let column = match from_field(field)? {
			FieldType {
				sumtype: Some(id),
				value_type,
				..
			} => enum_names(field, array, id, value_type, services, tx)?,
			_ => (field.clone(), array.clone()),
		};
		columns.push(column);
	}
	batch_with(&schema, columns, batch.num_rows())
}

fn enum_names(
	field: &FieldRef,
	array: &ArrayRef,
	id: SumTypeId,
	value_type: Option<ValueType>,
	services: &Arc<Services>,
	tx: &mut Transaction<'_>,
) -> Result<(FieldRef, ArrayRef)> {
	let sumtype = services.catalog.get_sumtype(tx, id)?;
	let namespace = services.catalog.get_namespace(tx, sumtype.namespace)?;
	let tags = array
		.as_any()
		.downcast_ref::<UInt8Array>()
		.ok_or_else(|| internal_error!("sumtype tag column {} is not uint8", field.name()))?;
	let names = tags
		.iter()
		.map(|tag| {
			tag.map(|tag| {
				sumtype.variants
					.iter()
					.find(|variant| variant.tag == tag)
					.map(|variant| variant.name.as_str())
					.ok_or_else(|| {
						internal_error!("tag {tag} of {} is outside its variants", field.name())
					})
			})
			.transpose()
		})
		.collect::<Result<Vec<_>>>()?;
	let value_type = match value_type {
		Some(ValueType::Option(_)) => ValueType::Option(Box::new(ValueType::Utf8)),
		_ => ValueType::Utf8,
	};
	let field_type = FieldType {
		enum_name: Some(format!("{}::{}", namespace.name(), sumtype.name)),
		..FieldType::from(value_type)
	};
	let name = field.name().strip_suffix("_tag").unwrap_or(field.name());
	Ok((Arc::new(to_field(name, &field_type)), Arc::new(LargeStringArray::from(names))))
}
