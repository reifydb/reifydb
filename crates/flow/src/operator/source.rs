// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, iter::once, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{FieldRef, Schema, SchemaRef};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{column::Column, dictionary::Dictionary, flow::OperatorId, series::Series, sumtype::SumType},
		change::{Change, Diff},
	},
	value::{
		batch::{batch, mark_field, mark_tag_columns},
		column::{builder::ColumnBuilder, dictionary::decode_dictionary_ids},
	},
};
use reifydb_value::{
	Result,
	error::Error,
	value::{
		column_view::{ColumnView, ViewData},
		constraint::Constraint,
		dictionary::DictionaryId,
		sumtype::SumTypeId,
		system_columns::{SystemColumn, with_system_column},
		value_type::{ValueType, field::from_field},
	},
};

use crate::{error::FlowSinkError, txn::Resolve};

pub struct SourceOperator {
	operator: OperatorId,
	schema: SchemaRef,
	sumtypes: HashMap<String, SumTypeId>,
}

impl SourceOperator {
	pub fn from_columns(
		operator: OperatorId,
		columns: &[Column],
		sumtypes: HashMap<String, SumTypeId>,
	) -> Result<Self> {
		Ok(Self {
			operator,
			schema: mark_schema(&catalog_schema(columns), &sumtypes)?,
			sumtypes,
		})
	}

	pub fn from_series(operator: OperatorId, series: &Series) -> Self {
		let key = series.key_column_data(vec![]).0;
		let data = series.data_columns().map(|col| column_field(col).0);
		Self {
			operator,
			schema: Arc::new(Schema::new(once(key).chain(data).collect::<Vec<_>>())),
			sumtypes: HashMap::new(),
		}
	}

	pub fn id(&self) -> OperatorId {
		self.operator
	}

	pub fn output_schema(&self) -> Option<SchemaRef> {
		Some(self.schema.clone())
	}

	pub fn apply(&self, change: Change) -> Result<Change> {
		let commit = change.version.commit;
		let diffs = change
			.diffs
			.into_iter()
			.map(|diff| stamp_commit_version(diff, commit, &self.sumtypes))
			.collect::<Result<Vec<_>>>()?;
		Ok(Change::from_flow(self.operator, change.version, diffs, change.changed_at))
	}
}

fn stamp_commit_version(diff: Diff, commit: CommitVersion, sumtypes: &HashMap<String, SumTypeId>) -> Result<Diff> {
	let stamp = |batch: RecordBatch| {
		let versions: ArrayRef = Arc::new(UInt64Array::from(vec![commit.0; batch.num_rows()]));
		with_system_column(mark_tag_columns(batch, sumtypes)?, SystemColumn::CommitVersion, versions)
	};
	Ok(match diff {
		Diff::Insert {
			post,
			origin,
		} => Diff::Insert {
			post: stamp(post)?,
			origin,
		},
		Diff::Update {
			pre,
			post,
			origin,
		} => Diff::Update {
			pre: stamp(pre)?,
			post: stamp(post)?,
			origin,
		},
		Diff::Remove {
			pre,
			origin,
		} => Diff::Remove {
			pre: stamp(pre)?,
			origin,
		},
	})
}

pub fn decode_change<T: Resolve + ?Sized>(txn: &mut T, change: Change) -> Result<Change> {
	decode_change_keeping(txn, change, &[])
}

pub fn decode_change_keeping<T: Resolve + ?Sized>(txn: &mut T, mut change: Change, kept: &[Column]) -> Result<Change> {
	for diff in change.diffs.iter_mut() {
		match diff {
			Diff::Insert {
				post,
				..
			} => decode_dictionary_columns_where(post, txn, &keeps(kept))?,
			Diff::Update {
				pre,
				post,
				..
			} => {
				decode_dictionary_columns_where(pre, txn, &keeps(kept))?;
				decode_dictionary_columns_where(post, txn, &keeps(kept))?;
			}
			Diff::Remove {
				pre,
				..
			} => decode_dictionary_columns_where(pre, txn, &keeps(kept))?,
		}
	}
	Ok(change)
}

pub fn decoded_schema(
	schema: &SchemaRef,
	mut dictionary: impl FnMut(DictionaryId) -> Result<Option<Dictionary>>,
) -> Result<SchemaRef> {
	let mut fields = Vec::with_capacity(schema.fields().len());
	for field in schema.fields() {
		let field_type = from_field(field)?;
		let (optional, bare) = match field_type.value_type {
			Some(ValueType::Option(inner)) => (true, *inner),
			Some(bare) => (false, bare),
			None => {
				fields.push(field.clone());
				continue;
			}
		};
		let (ValueType::DictionaryId, Some(id)) = (bare, field_type.dictionary_id) else {
			fields.push(field.clone());
			continue;
		};
		let dictionary = dictionary(id)?.ok_or_else(|| {
			Error::from(FlowSinkError::DictionaryNotFound {
				dictionary_id: format!("{:?}", id),
				column: field.name().clone(),
			})
		})?;
		let value_type = match optional {
			true => ValueType::Option(Box::new(dictionary.value_type)),
			false => dictionary.value_type,
		};
		fields.push(ColumnBuilder::with_capacity(value_type, 0).finish(field.name()).0);
	}
	Ok(Arc::new(Schema::new(fields)))
}

pub fn unit_sumtype_tags(
	columns: &[Column],
	mut sumtype: impl FnMut(SumTypeId) -> Result<SumType>,
) -> Result<HashMap<String, SumTypeId>> {
	let mut tags = HashMap::new();
	for column in columns {
		if let Some(Constraint::SumType(id)) = column.constraint.constraint()
			&& sumtype(*id)?.variants.iter().all(|variant| variant.fields.is_empty())
		{
			tags.insert(column.name.clone(), *id);
		}
	}
	Ok(tags)
}

pub fn mark_schema(schema: &SchemaRef, sumtypes: &HashMap<String, SumTypeId>) -> Result<SchemaRef> {
	let fields = schema
		.fields()
		.iter()
		.map(|field| match sumtypes.get(field.name()) {
			Some(id) => mark_field(field, *id),
			None => Ok(field.clone()),
		})
		.collect::<Result<Vec<_>>>()?;
	Ok(Arc::new(Schema::new(fields)))
}

pub fn schema_sumtypes(schema: &SchemaRef) -> Result<HashMap<String, SumTypeId>> {
	let mut sumtypes = HashMap::new();
	for field in schema.fields() {
		if let Some(id) = from_field(field)?.sumtype {
			sumtypes.insert(field.name().clone(), id);
		}
	}
	Ok(sumtypes)
}

pub fn mark_diffs(diffs: &mut [Diff], sumtypes: &HashMap<String, SumTypeId>) -> Result<()> {
	if sumtypes.is_empty() {
		return Ok(());
	}
	for diff in diffs {
		for batch in diff.batches_mut() {
			*batch = mark_tag_columns(batch.clone(), sumtypes)?;
		}
	}
	Ok(())
}

pub fn catalog_schema(columns: &[Column]) -> SchemaRef {
	Arc::new(Schema::new(columns.iter().map(|column| column_field(column).0).collect::<Vec<_>>()))
}

fn column_field(column: &Column) -> (FieldRef, ArrayRef) {
	let Some(id) = column.dictionary_id else {
		return ColumnBuilder::with_capacity(column.constraint.get_type(), 0).finish(&column.name);
	};
	let value_type = match column.constraint.get_type() {
		ValueType::Option(_) => ValueType::Option(Box::new(ValueType::DictionaryId)),
		_ => ValueType::DictionaryId,
	};
	let mut builder = ColumnBuilder::with_capacity(value_type, 0);
	builder.set_dictionary_id(id);
	builder.finish(&column.name)
}

pub fn decode_dictionary_columns<T: Resolve + ?Sized>(columns: &mut RecordBatch, txn: &mut T) -> Result<()> {
	decode_dictionary_columns_where(columns, txn, &|_, _| false)
}

pub fn decode_dictionary_columns_except<T: Resolve + ?Sized>(
	columns: &mut RecordBatch,
	txn: &mut T,
	kept: &[(String, DictionaryId)],
) -> Result<()> {
	decode_dictionary_columns_where(columns, txn, &|name, id| kept.iter().any(|(n, d)| n == name && *d == id))
}

fn keeps(kept: &[Column]) -> impl Fn(&str, DictionaryId) -> bool + '_ {
	move |name, id| kept.iter().any(|column| column.name == name && column.dictionary_id == Some(id))
}

fn decode_dictionary_columns_where<T: Resolve + ?Sized>(
	columns: &mut RecordBatch,
	txn: &mut T,
	keep: &dyn Fn(&str, DictionaryId) -> bool,
) -> Result<()> {
	let dict_columns: Vec<(usize, Dictionary)> = {
		let mut ids: Vec<(usize, DictionaryId)> = Vec::new();
		for (pos, (field, array)) in columns.schema_ref().fields().iter().zip(columns.columns()).enumerate() {
			if let ViewData::DictionaryId {
				dictionary_id: Some(id),
				..
			} = ColumnView::try_from((array, field.as_ref()))?.data
				&& !keep(field.name(), id)
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
	for (col_pos, dictionary) in &dict_columns {
		let (field, array) = &decoded[*col_pos];
		decoded[*col_pos] = decode_dictionary_ids(field, array, dictionary.value_type.clone(), |ids| {
			txn.resolve(dictionary, ids)
		})?;
	}

	*columns = batch(decoded)?;
	Ok(())
}
