// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, iter::once, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{FieldRef, Schema, SchemaRef};
use reifydb_core::{
	interface::{
		catalog::{column::Column, dictionary::Dictionary, flow::OperatorId, series::Series},
		change::{Change, Diff},
	},
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_value::{
	Result,
	error::Error,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		dictionary::{DictionaryEntryId, DictionaryId},
	},
};

use crate::{error::FlowSinkError, txn::Resolve};

pub struct SourceOperator {
	operator: OperatorId,
	schema: SchemaRef,
}

impl SourceOperator {
	pub fn from_columns(operator: OperatorId, columns: &[Column]) -> Self {
		Self {
			operator,
			schema: catalog_schema(columns),
		}
	}

	pub fn from_series(operator: OperatorId, series: &Series) -> Self {
		let key = series.key_column_data(vec![]).0;
		let data = series
			.data_columns()
			.map(|col| ColumnBuilder::with_capacity(col.constraint.get_type(), 0).finish(&col.name).0);
		Self {
			operator,
			schema: Arc::new(Schema::new(once(key).chain(data).collect::<Vec<_>>())),
		}
	}

	pub fn id(&self) -> OperatorId {
		self.operator
	}

	pub fn output_schema(&self) -> Option<SchemaRef> {
		Some(self.schema.clone())
	}

	pub fn apply<T: Resolve + ?Sized>(&self, txn: &mut T, change: Change) -> Result<Change> {
		let mut decoded_diffs = Vec::with_capacity(change.diffs.len());
		for diff in change.diffs {
			decoded_diffs.push(match diff {
				Diff::Insert {
					post,
					..
				} => {
					let mut decoded = post;
					decode_dictionary_columns(&mut decoded, txn)?;
					Diff::insert(decoded)
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let mut decoded_pre = pre;
					let mut decoded_post = post;
					decode_dictionary_columns(&mut decoded_pre, txn)?;
					decode_dictionary_columns(&mut decoded_post, txn)?;
					Diff::update(decoded_pre, decoded_post)
				}
				Diff::Remove {
					pre,
					..
				} => {
					let mut decoded = pre;
					decode_dictionary_columns(&mut decoded, txn)?;
					Diff::remove(decoded)
				}
			});
		}
		Ok(Change::from_flow(self.operator, change.version, decoded_diffs, change.changed_at))
	}
}

pub fn catalog_schema(columns: &[Column]) -> SchemaRef {
	Arc::new(Schema::new(
		columns.iter()
			.map(|column| {
				ColumnBuilder::with_capacity(column.constraint.get_type(), 0).finish(&column.name).0
			})
			.collect::<Vec<_>>(),
	))
}

pub fn decode_dictionary_columns<T: Resolve + ?Sized>(columns: &mut RecordBatch, txn: &mut T) -> Result<()> {
	let dict_columns: Vec<(usize, Dictionary)> = {
		let mut ids: Vec<(usize, DictionaryId)> = Vec::new();
		for (pos, (field, array)) in columns.schema_ref().fields().iter().zip(columns.columns()).enumerate() {
			if let ViewData::DictionaryId {
				dictionary_id: Some(id),
				..
			} = ColumnView::try_from((array, field.as_ref()))?.data
			{
				ids.push((pos, id));
			}
		}
		ids.into_iter()
			.map(|(pos, id)| {
				let dictionary = txn.dictionary(id)?.ok_or_else(|| {
					Error::from(FlowSinkError::DictionaryNotFound {
						dictionary_id: format!("{:?}", id),
						column: columns.schema_ref().field(pos).name().clone(),
					})
				})?;
				Ok((pos, dictionary))
			})
			.collect::<Result<Vec<_>>>()?
	};

	if dict_columns.is_empty() {
		return Ok(());
	}

	let mut decoded: Vec<(FieldRef, ArrayRef)> =
		columns.schema_ref().fields().iter().cloned().zip(columns.columns().iter().cloned()).collect();
	let mut resolved: HashMap<(DictionaryId, DictionaryEntryId), Value> = HashMap::new();
	for (col_pos, dictionary) in &dict_columns {
		let (field, array) = &decoded[*col_pos];
		let column = ColumnView::try_from((array, field.as_ref()))?;
		let row_count = column.len();
		let mut new_data = ColumnBuilder::with_capacity(dictionary.value_type.clone(), row_count);

		for row_idx in 0..row_count {
			let id_value = column.get_value(row_idx);
			let value = match DictionaryEntryId::from_value(&id_value) {
				Some(entry_id) => match resolved.get(&(dictionary.id, entry_id)) {
					Some(value) => value.clone(),
					None => match txn.resolve(dictionary, entry_id)? {
						Some(value) => {
							resolved.insert((dictionary.id, entry_id), value.clone());
							value
						}
						None => Value::none(),
					},
				},
				None => Value::none(),
			};
			new_data.push_value(value);
		}

		let name = field.name().clone();
		decoded[*col_pos] = new_data.finish(&name);
	}

	*columns = batch(decoded)?;
	Ok(())
}
