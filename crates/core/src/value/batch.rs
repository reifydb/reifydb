// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, result::Result as StdResult, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UInt64Array, new_null_array};
use arrow_buffer::BooleanBuffer;
use arrow_schema::{ArrowError, FieldRef, Schema};
use arrow_select::{
	concat::{concat as concat_arrays, concat_batches},
	take::take,
};
use reifydb_codec::row::{
	bytes::EncodedBytes,
	shape::{RowFamily, RowShape, RowShapeField},
};
use reifydb_value::{
	Result,
	error::{Diagnostic, Error},
	util::kernel,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		constraint::Constraint,
		container::{
			dictionary_array,
			digest_array::push_digest,
			temporal_array::{
				date_to_native, datetime_array, datetime_to_native, duration_to_native, time_to_native,
			},
		},
		date::Date,
		datetime::DateTime,
		dictionary::DictionaryEntryId,
		duration::Duration,
		identity::IdentityId,
		row_number::RowNumber,
		system_columns::{SystemColumn, column_view, is_system_field, system_column, with_system_column},
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::{
			ValueType,
			field::{from_field, to_field},
		},
	},
};

use crate::{
	error::CoreError,
	interface::catalog::column::Column as CatalogColumn,
	internal_err,
	metrics::heap::HeapSize,
	row::Row,
	value::column::{
		builder::{ColumnBuilder, TypedBuilder, append_fixed},
		factory::from_many,
		nulls::none_filler,
		view::group_by::{GroupKeyDict, GroupRows, group_rows},
	},
};

pub fn batch(columns: Vec<(FieldRef, ArrayRef)>) -> Result<RecordBatch> {
	let row_count = columns.first().map_or(0, |(_, array)| array.len());
	assemble(columns, HashMap::new(), row_count)
}

pub fn empty_batch() -> RecordBatch {
	RecordBatch::new_empty(Arc::new(Schema::empty()))
}

pub fn single_row<'a>(values: impl IntoIterator<Item = (&'a str, Value)>) -> Result<RecordBatch> {
	batch(values.into_iter().map(|(name, value)| from_many(name, value, 1)).collect())
}

pub fn from_rows(names: &[&str], rows: &[Vec<Value>]) -> Result<RecordBatch> {
	let mut builders: Vec<ColumnBuilder> = (0..names.len())
		.map(|i| match rows.iter().map(|row| &row[i]).find(|value| !matches!(value, Value::None { .. })) {
			Some(value) => ColumnBuilder::with_capacity(value.get_type(), rows.len()),
			None => ColumnBuilder::untyped_none(),
		})
		.collect();

	for row in rows {
		assert_eq!(row.len(), names.len(), "row length does not match column count");
		for (builder, value) in builders.iter_mut().zip(row) {
			builder.push_value(value.clone());
		}
	}

	batch(names.iter().zip(builders).map(|(name, builder)| builder.finish(name)).collect())
}

pub fn try_from_records(param: &str, records: &[Value]) -> Result<RecordBatch> {
	let Some(Value::Record(first_fields)) = records.first() else {
		return Ok(empty_batch());
	};
	let names: Vec<String> = first_fields.iter().map(|(name, _)| name.clone()).collect();

	let mut rows = Vec::with_capacity(records.len());
	for (row, record) in records.iter().enumerate() {
		let Value::Record(fields) = record else {
			return Err(record_shape_error(param, row, "is not a record"));
		};
		if let Some((extra, _)) = fields.iter().find(|(name, _)| !names.contains(name)) {
			return Err(record_shape_error(param, row, &format!("has unexpected field `{extra}`")));
		}
		let mut ordered = Vec::with_capacity(names.len());
		for name in &names {
			let value = fields
				.iter()
				.find(|(field_name, _)| field_name == name)
				.ok_or_else(|| record_shape_error(param, row, &format!("is missing field `{name}`")))?
				.1
				.clone();
			ordered.push(value);
		}
		rows.push(ordered);
	}

	let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
	from_rows(&name_refs, &rows)
}

pub fn from_row(row: &Row) -> Result<RecordBatch> {
	let shape = &row.shape;
	let mut columns = Vec::with_capacity(shape.fields().len());
	for (idx, field) in shape.fields().iter().enumerate() {
		let value = shape.get_value(&row.encoded, idx);

		let column_type = match value {
			Value::None {
				..
			} => field.constraint.get_type(),
			Value::Decimal(_) => field.constraint.get_type().inner_type().clone(),
			_ => value.get_type(),
		};

		let mut builder = ColumnBuilder::with_capacity(column_type, 1);
		builder.push_value(value);

		if let Some(Constraint::Dictionary(dict_id, _)) = field.constraint.constraint() {
			builder.set_dictionary_id(*dict_id);
		}

		let name = shape.get_field_name(idx).expect("RowShape missing name for field");
		columns.push(builder.finish(name));
	}

	let mut out = assemble(columns, HashMap::new(), 1)?;
	out = with_system_column(out, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![row.number.0])))?;
	if !matches!(shape.family(), RowFamily::Pod | RowFamily::Operator) {
		let created_at = datetime_array([shape.created_at(&row.encoded)]);
		let updated_at = datetime_array([shape.updated_at(&row.encoded)]);
		out = with_system_column(out, SystemColumn::CreatedAt, Arc::new(created_at))?;
		out = with_system_column(out, SystemColumn::UpdatedAt, Arc::new(updated_at))?;
	}
	if let Some(time) = shape.time(&row.encoded) {
		out = with_system_column(out, SystemColumn::Time, Arc::new(datetime_array([time])))?;
	}
	Ok(out)
}

pub fn from_encoded_bytes(shape: &RowShape, ids: &[RowNumber], rows: &[EncodedBytes]) -> Result<RecordBatch> {
	assert_eq!(ids.len(), rows.len(), "ids length must match rows length");

	let mut columns = Vec::with_capacity(shape.field_count());
	for (index, field) in shape.fields().iter().enumerate() {
		let mut builder = ColumnBuilder::with_capacity(field.constraint.get_type(), rows.len());
		if let Some(Constraint::Dictionary(dict_id, _)) = field.constraint.constraint() {
			builder.set_dictionary_id(*dict_id);
		}
		for row in rows {
			builder.push_value(shape.get_value(row, index));
		}
		columns.push(builder.finish(&field.name));
	}

	let out = assemble(columns, HashMap::new(), rows.len())?;
	stamps(shape, rows, ids)?
		.into_iter()
		.try_fold(out, |out, (column, array)| with_system_column(out, column, array))
}

pub fn empty_for(columns: &[CatalogColumn]) -> Result<RecordBatch> {
	batch(columns
		.iter()
		.map(|column| ColumnBuilder::with_capacity(column.constraint.get_type(), 0).finish(&column.name))
		.collect())
}

pub fn append(left: &RecordBatch, right: &RecordBatch) -> Result<RecordBatch> {
	concat(&[left.clone(), right.clone()])
}

pub fn concat(batches: &[RecordBatch]) -> Result<RecordBatch> {
	let mut parts: Vec<&RecordBatch> = Vec::with_capacity(batches.len());
	for batch in batches {
		match parts.first() {
			None => parts.push(batch),
			Some(_) if batch.num_rows() == 0 => {}
			Some(lead) if lead.num_columns() == 0 => parts = vec![batch],
			Some(lead) => {
				check_joinable(lead, batch)?;
				parts.push(batch);
			}
		}
	}
	let Some((lead, rest)) = parts.split_first() else {
		return Ok(empty_batch());
	};
	if rest.is_empty() {
		return Ok((*lead).clone());
	}
	let schema = lead.schema_ref();
	if rest.iter().all(|part| part.schema_ref() == schema) {
		return concat_batches(schema, parts.iter().copied()).map_err(frame_error);
	}
	let mut columns = Vec::with_capacity(lead.num_columns());
	for index in 0..lead.num_columns() {
		let views = parts
			.iter()
			.map(|part| ColumnView::try_from((part.column(index), part.schema_ref().field(index))))
			.collect::<Result<Vec<_>>>()?;
		columns.push(unify(schema.field(index).name(), &views)?);
	}
	assemble(columns, schema.metadata().clone(), parts.iter().map(|part| part.num_rows()).sum())
}

pub fn concat_columns(columns: &[(FieldRef, ArrayRef)]) -> Result<(FieldRef, ArrayRef)> {
	let Some(((field, array), rest)) = columns.split_first() else {
		return internal_err!("concat_columns needs at least one column");
	};
	if rest.is_empty() {
		return Ok((field.clone(), array.clone()));
	}
	if rest.iter().all(|(other, _)| other == field) {
		let arrays: Vec<&dyn Array> = columns.iter().map(|(_, array)| array.as_ref()).collect();
		return Ok((field.clone(), concat_arrays(&arrays).map_err(frame_error)?));
	}
	let views = columns.iter().map(ColumnView::try_from).collect::<Result<Vec<_>>>()?;
	unify(field.name(), &views)
}

pub fn head(batch: &RecordBatch, n: usize) -> RecordBatch {
	batch.slice(0, n.min(batch.num_rows()))
}

pub fn filter(batch: &RecordBatch, mask: &BooleanBuffer) -> Result<RecordBatch> {
	let predicate = kernel::shared_predicate(mask, batch.num_rows());
	let columns = batch
		.columns()
		.iter()
		.map(|column| predicate.filter(column.as_ref()))
		.collect::<StdResult<Vec<_>, _>>()
		.map_err(frame_error)?;
	RecordBatch::try_new_with_options(
		batch.schema(),
		columns,
		&RecordBatchOptions::new().with_row_count(Some(predicate.count())),
	)
	.map_err(frame_error)
}

pub fn append_rows(
	batch: RecordBatch,
	shape: &RowShape,
	rows: impl IntoIterator<Item = impl Into<EncodedBytes>>,
	row_numbers: Vec<RowNumber>,
) -> Result<RecordBatch> {
	let schema = batch.schema();
	let user: Vec<usize> = schema
		.fields()
		.iter()
		.enumerate()
		.filter(|(_, field)| !is_system_field(field))
		.map(|(index, _)| index)
		.collect();
	if user.len() != shape.field_count() {
		return Err(CoreError::FrameError {
			message: format!(
				"mismatched column count: expected {}, got {}",
				user.len(),
				shape.field_count()
			),
		}
		.into());
	}

	let rows: Vec<EncodedBytes> = rows.into_iter().map(Into::into).collect();
	if !row_numbers.is_empty() && row_numbers.len() != rows.len() {
		return Err(CoreError::FrameError {
			message: format!(
				"row_numbers length {} does not match rows length {}",
				row_numbers.len(),
				rows.len()
			),
		}
		.into());
	}

	let names: Vec<&str> = user.iter().map(|&index| schema.field(index).name().as_str()).collect();
	let mut builders = Vec::with_capacity(user.len());
	for (&index, field) in user.iter().zip(shape.fields()) {
		let view = ColumnView::try_from((batch.column(index), schema.field(index)))?;
		builders.push(retyped(&view, field, rows.len()));
	}

	for row in &rows {
		match (0..shape.field_count()).all(|index| shape.is_defined(row, index)) {
			true => append_all_defined(&names, &mut builders, shape, row)?,
			false => append_fallback(&names, &mut builders, shape, row)?,
		}
	}

	let columns = names.iter().zip(builders).map(|(name, builder)| builder.finish(name)).collect();
	let out = assemble(columns, schema.metadata().clone(), batch.num_rows() + rows.len())?;
	merge_system_columns(out, &batch, stamps(shape, &rows, &row_numbers)?, rows.len())
}

pub fn take_rows(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
	kernel::rows_in_range(indices, batch.num_rows())?;
	let indices = UInt64Array::from_iter_values(indices.iter().map(|&index| index as u64));
	let columns = batch
		.columns()
		.iter()
		.map(|column| take(column.as_ref(), &indices, None))
		.collect::<StdResult<Vec<_>, _>>()
		.map_err(frame_error)?;
	RecordBatch::try_new_with_options(
		batch.schema(),
		columns,
		&RecordBatchOptions::new().with_row_count(Some(indices.len())),
	)
	.map_err(frame_error)
}

pub(crate) fn gather(sources: &[&RecordBatch], picks: &[(usize, usize)]) -> Result<RecordBatch> {
	let Some(lead) = sources.first() else {
		return Ok(empty_batch());
	};
	let schema = lead.schema_ref();
	if sources.iter().all(|source| source.schema_ref() == schema) {
		let columns = (0..lead.num_columns())
			.map(|index| {
				let arrays: Vec<&dyn Array> =
					sources.iter().map(|source| source.column(index).as_ref()).collect();
				kernel::picked(&arrays, picks)
			})
			.collect();
		return RecordBatch::try_new_with_options(
			schema.clone(),
			columns,
			&RecordBatchOptions::new().with_row_count(Some(picks.len())),
		)
		.map_err(frame_error);
	}
	let mut rows_of: Vec<Vec<usize>> = vec![Vec::new(); sources.len()];
	let mut slot_of: Vec<(usize, usize)> = Vec::with_capacity(picks.len());
	for &(source, row) in picks {
		slot_of.push((source, rows_of[source].len()));
		rows_of[source].push(row);
	}
	let mut offsets: Vec<usize> = Vec::with_capacity(sources.len());
	let mut parts: Vec<RecordBatch> = Vec::with_capacity(sources.len());
	let mut next = 0;
	for (source, rows) in sources.iter().zip(&rows_of) {
		offsets.push(next);
		next += rows.len();
		if !rows.is_empty() {
			parts.push(take_rows(source, rows)?);
		}
	}
	let glued = concat(&parts)?;
	let order: Vec<usize> = slot_of.iter().map(|&(source, at)| offsets[source] + at).collect();
	take_rows(&glued, &order)
}

pub fn take_rows_or_none(batch: &RecordBatch, picks: &[Option<usize>]) -> Result<RecordBatch> {
	kernel::rows_in_range(&picks.iter().flatten().copied().collect::<Vec<_>>(), batch.num_rows())?;
	let pairs: Vec<(usize, usize)> = picks.iter().map(|pick| pick.map_or((1, 0), |index| (0, index))).collect();
	let schema = batch.schema_ref();
	let mut columns = Vec::with_capacity(batch.num_columns());
	for (field, array) in schema.fields().iter().zip(batch.columns()) {
		let mut field_type = from_field(field)?;
		let filler = match &field_type.value_type {
			Some(ty) => none_filler(ty, array.data_type()),
			None => new_null_array(array.data_type(), 1),
		};
		let taken = kernel::picked(&[array.as_ref(), filler.as_ref()], &pairs);
		let field = match taken.logical_null_count() > 0 && !field.is_nullable() {
			true => {
				field_type.value_type =
					field_type.value_type.map(|value_type| ValueType::Option(Box::new(value_type)));
				Arc::new(to_field(field.name(), &field_type))
			}
			false => field.clone(),
		};
		columns.push((field, taken));
	}
	assemble(columns, schema.metadata().clone(), picks.len())
}

pub fn scalar_value(batch: &RecordBatch) -> Result<Value> {
	let user = user_views(batch)?;
	if user.len() != 1 {
		return internal_err!("scalar_value() requires exactly 1 column, got {}", user.len());
	}
	if batch.num_rows() != 1 {
		return internal_err!("scalar_value() requires exactly 1 row, got {}", batch.num_rows());
	}
	Ok(user[0].get_value(0))
}

pub fn is_scalar(batch: &RecordBatch) -> bool {
	batch.schema_ref().fields().iter().filter(|field| !is_system_field(field)).count() == 1 && batch.num_rows() == 1
}

pub fn group_by(batch: &RecordBatch, keys: &[&str], dict: &mut GroupKeyDict) -> Result<GroupRows> {
	let schema = batch.schema_ref();
	let mut key_views = Vec::with_capacity(keys.len());
	for &key in keys {
		let index = schema.fields().iter().position(|field| field.name() == key).ok_or_else(|| {
			Error::from(CoreError::FrameError {
				message: format!("Column '{}' not found", key),
			})
		})?;
		key_views.push(ColumnView::try_from((batch.column(index), schema.field(index)))?);
	}
	group_rows(&key_views, batch.num_rows(), dict)
}

pub fn reattach_dictionary_ids(batch: RecordBatch, from: &RecordBatch) -> Result<RecordBatch> {
	let schema = batch.schema();
	let mut columns = Vec::with_capacity(batch.num_columns());
	for (field, array) in schema.fields().iter().zip(batch.columns()) {
		let view = ColumnView::try_from((array, field.as_ref()))?;
		let source = match view.data {
			ViewData::DictionaryId {
				dictionary_id: None,
				..
			} => column_view(from, field.name())?,
			_ => None,
		};
		let field = match source.map(|source| source.data) {
			Some(ViewData::DictionaryId {
				dictionary_id: source_id,
				..
			}) => {
				let mut field_type = from_field(field)?;
				field_type.dictionary_id = source_id;
				Arc::new(to_field(field.name(), &field_type))
			}
			_ => field.clone(),
		};
		columns.push((field, array.clone()));
	}
	assemble(columns, schema.metadata().clone(), batch.num_rows())
}

pub fn heap_size(batch: &RecordBatch) -> Result<usize> {
	let data: usize = views(batch)?.iter().map(|view| view.heap_size()).sum();
	let names: usize = batch.schema_ref().fields().iter().map(|field| field.name().len()).sum();
	Ok(data + names)
}

pub fn views(batch: &RecordBatch) -> Result<Vec<ColumnView<'_>>> {
	batch.schema_ref()
		.fields()
		.iter()
		.zip(batch.columns())
		.map(|(field, array)| ColumnView::try_from((array, field.as_ref())))
		.collect()
}

pub(crate) fn frame_error(error: ArrowError) -> Error {
	CoreError::FrameError {
		message: error.to_string(),
	}
	.into()
}

fn user_views(batch: &RecordBatch) -> Result<Vec<ColumnView<'_>>> {
	Ok(views(batch)?.into_iter().filter(|view| !is_system_field(view.field)).collect())
}

fn assemble(
	columns: Vec<(FieldRef, ArrayRef)>,
	metadata: HashMap<String, String>,
	row_count: usize,
) -> Result<RecordBatch> {
	let (fields, arrays): (Vec<FieldRef>, Vec<ArrayRef>) = columns.into_iter().unzip();
	RecordBatch::try_new_with_options(
		Arc::new(Schema::new_with_metadata(fields, metadata)),
		arrays,
		&RecordBatchOptions::new().with_row_count(Some(row_count)),
	)
	.map_err(frame_error)
}

fn check_joinable(lead: &RecordBatch, part: &RecordBatch) -> Result<()> {
	if lead.num_columns() != part.num_columns() {
		return Err(CoreError::FrameError {
			message: "mismatched column count".to_string(),
		}
		.into());
	}
	for (index, (lead_field, part_field)) in
		lead.schema_ref().fields().iter().zip(part.schema_ref().fields()).enumerate()
	{
		if lead_field.name() != part_field.name() {
			return Err(CoreError::FrameError {
				message: format!(
					"column name mismatch at index {}: '{}' vs '{}'",
					index,
					lead_field.name(),
					part_field.name(),
				),
			}
			.into());
		}
	}
	Ok(())
}

fn unify(name: &str, views: &[ColumnView]) -> Result<(FieldRef, ArrayRef)> {
	let Some((lead, rest)) = views.split_first() else {
		return internal_err!("column {} has no parts to concat", name);
	};
	let mut builder = ColumnBuilder::from_view(lead);
	for view in rest {
		builder.extend(view)?;
	}
	Ok(builder.finish(name))
}

fn record_shape_error(param: &str, row: usize, detail: &str) -> Error {
	Error(Box::new(Diagnostic {
		code: "PARAM_001".to_string(),
		message: format!("parameter `{param}` row {row} {detail}"),
		label: Some("row shape mismatch".to_string()),
		help: Some("every row in a list-of-records parameter must share the same fields as row 0".to_string()),
		..Default::default()
	}))
}

fn stamps(shape: &RowShape, rows: &[EncodedBytes], row_numbers: &[RowNumber]) -> Result<Vec<(SystemColumn, ArrayRef)>> {
	let mut columns: Vec<(SystemColumn, ArrayRef)> = Vec::new();
	if !row_numbers.is_empty() {
		let values = UInt64Array::from_iter_values(row_numbers.iter().map(|row_number| row_number.0));
		columns.push((SystemColumn::RowNumbers, Arc::new(values)));
	}
	if rows.is_empty() {
		return Ok(columns);
	}
	if !matches!(shape.family(), RowFamily::Pod) {
		let created_at = datetime_array(rows.iter().map(|row| shape.created_at(row)));
		let updated_at = datetime_array(rows.iter().map(|row| shape.updated_at(row)));
		columns.push((SystemColumn::CreatedAt, Arc::new(created_at)));
		columns.push((SystemColumn::UpdatedAt, Arc::new(updated_at)));
	}
	let time: Vec<DateTime> = rows.iter().filter_map(|row| shape.time(row)).collect();
	match time.len() {
		0 => {}
		stamped if stamped == rows.len() => columns.push((SystemColumn::Time, Arc::new(datetime_array(time)))),
		stamped => {
			return internal_err!(
				"{} of {} rows carry a {} stamp",
				stamped,
				rows.len(),
				SystemColumn::Time
			);
		}
	}
	Ok(columns)
}

fn merge_system_columns(
	mut out: RecordBatch,
	batch: &RecordBatch,
	fresh: Vec<(SystemColumn, ArrayRef)>,
	appended: usize,
) -> Result<RecordBatch> {
	if let Some(field) = batch
		.schema_ref()
		.fields()
		.iter()
		.find(|field| is_system_field(field) && SystemColumn::from_name(field.name()).is_none())
	{
		return internal_err!("unknown system column {}", field.name());
	}
	for column in SystemColumn::ALL {
		let new = fresh.iter().find(|(fresh_column, _)| *fresh_column == column).map(|(_, array)| array);
		let merged = match (system_column(batch, column), new) {
			(Some(old), Some(new)) => concat_arrays(&[old.as_ref(), new.as_ref()]).map_err(frame_error)?,
			(Some(old), None) if appended == 0 => old.clone(),
			(Some(_), None) if batch.num_rows() == 0 => continue,
			(None, Some(new)) if batch.num_rows() == 0 => new.clone(),
			(None, None) => continue,
			_ => {
				return Err(CoreError::AppendSystemColumnMismatch {
					column: column.to_string(),
				}
				.into());
			}
		};
		out = with_system_column(out, column, merged)?;
	}
	Ok(out)
}

fn retyped(view: &ColumnView, field: &RowShapeField, appended: usize) -> ColumnBuilder {
	let target = field.constraint.get_type();
	let mut builder = match view.is_untyped_none() && !matches!(target, ValueType::Option(_)) {
		true => {
			let mut builder = ColumnBuilder::with_capacity(target, view.len() + appended);
			for _ in 0..view.len() {
				builder.push_none();
			}
			builder
		}
		false => ColumnBuilder::from_view(view),
	};
	if builder.dictionary_id().is_none()
		&& let Some(Constraint::Dictionary(dict_id, _)) = field.constraint.constraint()
	{
		builder.set_dictionary_id(*dict_id);
	}
	builder
}

fn append_all_defined(
	names: &[&str],
	builders: &mut [ColumnBuilder],
	shape: &RowShape,
	bytes: &EncodedBytes,
) -> Result<()> {
	for (index, (builder, field)) in builders.iter_mut().zip(shape.fields()).enumerate() {
		if builder.optional {
			builder.push_value(shape.get_value(bytes, index));
			continue;
		}
		let column_type = builder.get_type();
		let value_type = field.constraint.get_type();
		if !append_encoded(&mut builder.inner, &value_type, shape, bytes, index) {
			return Err(CoreError::FrameError {
				message: format!(
					"type mismatch for column '{}'({}): incompatible with value {}",
					names[index], column_type, value_type
				),
			}
			.into());
		}
	}
	Ok(())
}

fn append_fallback(
	names: &[&str],
	builders: &mut [ColumnBuilder],
	shape: &RowShape,
	bytes: &EncodedBytes,
) -> Result<()> {
	for (index, (builder, field)) in builders.iter_mut().zip(shape.fields()).enumerate() {
		if !shape.is_defined(bytes, index) {
			builder.push_none();
			continue;
		}
		if builder.optional {
			builder.push_value(shape.get_value(bytes, index));
			continue;
		}
		let column_type = builder.get_type();
		let value_type = field.constraint.get_type();
		if !append_encoded(&mut builder.inner, &value_type, shape, bytes, index) {
			return Err(CoreError::FrameError {
				message: format!(
					"type mismatch for column '{}'({}): incompatible with value {}",
					names[index], column_type, value_type
				),
			}
			.into());
		}
	}
	Ok(())
}

fn append_encoded(
	builder: &mut TypedBuilder,
	value_type: &ValueType,
	shape: &RowShape,
	bytes: &EncodedBytes,
	index: usize,
) -> bool {
	match (builder, value_type) {
		(TypedBuilder::Bool(builder), ValueType::Boolean) => {
			builder.append_value(shape.get::<bool>(bytes, index));
		}
		(TypedBuilder::Float4(builder), ValueType::Float4) => {
			builder.append_value(shape.get::<f32>(bytes, index));
		}
		(TypedBuilder::Float8(builder), ValueType::Float8) => {
			builder.append_value(shape.get::<f64>(bytes, index));
		}
		(TypedBuilder::Int1(builder), ValueType::Int1) => {
			builder.append_value(shape.get::<i8>(bytes, index));
		}
		(TypedBuilder::Int2(builder), ValueType::Int2) => {
			builder.append_value(shape.get::<i16>(bytes, index));
		}
		(TypedBuilder::Int4(builder), ValueType::Int4) => {
			builder.append_value(shape.get::<i32>(bytes, index));
		}
		(TypedBuilder::Int8(builder), ValueType::Int8) => {
			builder.append_value(shape.get::<i64>(bytes, index));
		}
		(TypedBuilder::Int16(builder), ValueType::Int16) => {
			builder.append_value(shape.get::<i128>(bytes, index));
		}
		(
			TypedBuilder::Utf8 {
				builder,
				..
			},
			ValueType::Utf8,
		) => {
			builder.append_value(shape.get_utf8(bytes, index));
		}
		(TypedBuilder::Uint1(builder), ValueType::Uint1) => {
			builder.append_value(shape.get::<u8>(bytes, index));
		}
		(TypedBuilder::Uint2(builder), ValueType::Uint2) => {
			builder.append_value(shape.get::<u16>(bytes, index));
		}
		(TypedBuilder::Uint4(builder), ValueType::Uint4) => {
			builder.append_value(shape.get::<u32>(bytes, index));
		}
		(TypedBuilder::Uint8(builder), ValueType::Uint8) => {
			builder.append_value(shape.get::<u64>(bytes, index));
		}
		(TypedBuilder::Uint16(builder), ValueType::Uint16) => {
			builder.append_value(shape.get::<u128>(bytes, index));
		}
		(TypedBuilder::Date(builder), ValueType::Date) => {
			builder.append_value(date_to_native(shape.get::<Date>(bytes, index)));
		}
		(TypedBuilder::DateTime(builder), ValueType::DateTime) => {
			builder.append_value(datetime_to_native(shape.get::<DateTime>(bytes, index)));
		}
		(TypedBuilder::Time(builder), ValueType::Time) => {
			builder.append_value(time_to_native(shape.get::<Time>(bytes, index)));
		}
		(TypedBuilder::Duration(builder), ValueType::Duration) => {
			builder.append_value(duration_to_native(shape.get::<Duration>(bytes, index)));
		}
		(TypedBuilder::Uuid4(builder), ValueType::Uuid4) => {
			append_fixed(builder, shape.get::<Uuid4>(bytes, index).as_bytes());
		}
		(TypedBuilder::Uuid7(builder), ValueType::Uuid7) => {
			append_fixed(builder, shape.get::<Uuid7>(bytes, index).as_bytes());
		}
		(TypedBuilder::IdentityId(builder), ValueType::IdentityId) => {
			append_fixed(builder, shape.get::<IdentityId>(bytes, index).as_bytes());
		}
		(
			TypedBuilder::Blob {
				builder,
				..
			},
			ValueType::Blob,
		) => {
			builder.append_value(shape.get_blob_slice(bytes, index));
		}
		(
			TypedBuilder::Decimal(builder),
			ValueType::Decimal {
				..
			},
		) => {
			builder.push(&shape.get_decimal(bytes, index));
		}
		(
			TypedBuilder::DictionaryId {
				builder,
				..
			},
			ValueType::DictionaryId,
		) => match shape.get_value(bytes, index) {
			Value::DictionaryId(id) => append_fixed(builder, &dictionary_array::encode(id)),
			_ => append_fixed(builder, &dictionary_array::encode(DictionaryEntryId::default())),
		},
		(
			TypedBuilder::Digest {
				builder,
				inner,
				accuracy,
			},
			ValueType::Digest {
				inner: field_inner,
				accuracy: field_accuracy,
			},
		) if *inner == **field_inner && *accuracy == *field_accuracy => {
			push_digest(builder, &shape.get_digest(bytes, index));
		}
		_ => return false,
	}
	true
}

#[cfg(test)]
pub mod tests {
	use std::{str::FromStr, sync::Arc};

	use arrow_array::{Array, ArrayRef, Int32Array, LargeStringArray, RecordBatch};
	use arrow_buffer::BooleanBuffer;
	use arrow_schema::FieldRef;
	use reifydb_value::value::{
		Value,
		blob::Blob,
		column_view::{ColumnView, ViewData},
		constraint::{bytes::MaxBytes, precision::Precision, scale::Scale},
		date::Date,
		datetime::DateTime,
		decimal::Decimal,
		dictionary::{DictionaryEntryId, DictionaryId},
		duration::Duration,
		identity::IdentityId,
		row_number::RowNumber,
		system_columns::{self, SystemColumn, column_view, with_system_column},
		time::Time,
		uuid::{Uuid4, Uuid7},
		value_type::{
			ValueType,
			field::{FieldType, from_field, named},
		},
	};
	use uuid::{Timestamp, Uuid};

	use super::{append, batch, gather, heap_size, single_row, take_rows};
	use crate::value::column::{builder::ColumnBuilder, factory};

	fn one_row_append_chain(sources: &[&RecordBatch], picks: &[(usize, usize)]) -> RecordBatch {
		// Must mirror consolidation before gather: one 1-row take per pick, appended in pick order.
		let mut parts = picks.iter().map(|&(source, row)| take_rows(sources[source], &[row]).unwrap());
		let first = parts.next().unwrap();
		parts.fold(first, |merged, part| append(&merged, &part).unwrap())
	}

	fn cells(batch: &RecordBatch) -> Vec<Vec<Value>> {
		// Reads every cell through its own field, so a wrong type or row shows up as a different value.
		(0..batch.num_rows())
			.map(|row| {
				(0..batch.num_columns())
					.map(|index| {
						ColumnView::try_from((
							batch.column(index),
							batch.schema_ref().field(index),
						))
						.unwrap()
						.get_value(row)
					})
					.collect()
			})
			.collect()
	}

	fn field_types(batch: &RecordBatch) -> Vec<FieldType> {
		batch.schema_ref().fields().iter().map(|field| from_field(field).unwrap()).collect()
	}

	#[test]
	fn gather_keeps_pick_order_across_sources_of_one_schema() {
		// A pick must land on its own source and row, otherwise consolidation emits a neighbour's row.
		let first = batch(vec![factory::int4("v", [1, 2, 3])]).unwrap();
		let second = batch(vec![factory::int4("v", [4, 5])]).unwrap();
		let third = batch(vec![factory::int4("v", [6])]).unwrap();
		let gathered = gather(&[&first, &second, &third], &[(1, 1), (0, 2), (2, 0), (0, 0)]).unwrap();
		assert_eq!(
			cells(&gathered),
			vec![vec![Value::Int4(5)], vec![Value::Int4(3)], vec![Value::Int4(6)], vec![Value::Int4(1)]]
		);
		assert!(
			Arc::ptr_eq(gathered.schema_ref(), first.schema_ref()),
			"one shared schema must be reused, never rebuilt"
		);
	}

	#[test]
	fn gather_over_mixed_nullability_equals_the_one_row_append_chain() {
		// Sources that differ only in nullability must come out typed and ordered exactly as the 1-row chain
		// did.
		let optional = batch(vec![factory::int4_optional("v", [Some(4), None, Some(6)])]).unwrap();
		let plain = batch(vec![factory::int4("v", [1, 2, 3])]).unwrap();
		let sources = [&optional, &plain];
		let picks = [(0, 1), (1, 2), (0, 0), (1, 0)];
		let gathered = gather(&sources, &picks).unwrap();
		let chained = one_row_append_chain(&sources, &picks);
		assert_eq!(field_types(&gathered), field_types(&chained));
		assert_eq!(cells(&gathered), cells(&chained));
	}

	#[test]
	fn gather_over_mixed_decimals_widens_only_for_picked_rows() {
		// A row that is not picked must never widen the type, or a cancelled row would change the output
		// schema.
		let decimal = |text: &str| Decimal::from_str(text).unwrap();
		let narrow = batch(vec![factory::decimal(
			"d",
			Precision::new(5),
			Scale::new(2),
			[decimal("1.25"), decimal("2.50")],
		)])
		.unwrap();
		let wide = batch(vec![factory::decimal(
			"d",
			Precision::new(20),
			Scale::new(4),
			[decimal("3.1250"), decimal("1234567890123456.0001")],
		)])
		.unwrap();
		let sources = [&narrow, &wide];
		let picks = [(0, 1), (1, 0), (0, 0)];
		let gathered = gather(&sources, &picks).unwrap();
		let chained = one_row_append_chain(&sources, &picks);
		assert_eq!(field_types(&gathered), field_types(&chained));
		assert_eq!(cells(&gathered), cells(&chained));
	}

	#[test]
	fn gather_over_any_columns_with_nones_equals_the_one_row_append_chain() {
		// An Any column's none typing depends on the rows it holds, so gather must see the same rows as the
		// chain.
		let first = batch(vec![factory::any_optional("x", [Some(Value::Int4(5)), None])]).unwrap();
		let second =
			batch(vec![factory::any_optional("x", [None, Some(Value::Utf8("w".to_string()))])]).unwrap();
		let sources = [&first, &second];
		let picks = [(0, 1), (1, 0), (1, 1)];
		let gathered = gather(&sources, &picks).unwrap();
		let chained = one_row_append_chain(&sources, &picks);
		assert_eq!(field_types(&gathered), field_types(&chained));
		assert_eq!(cells(&gathered), cells(&chained));
	}

	pub(super) fn column(batch: &RecordBatch, index: usize) -> (FieldRef, ArrayRef) {
		(batch.schema_ref().fields()[index].clone(), batch.column(index).clone())
	}

	pub(super) fn view<'a>(batch: &'a RecordBatch, name: &str) -> ColumnView<'a> {
		column_view(batch, name).unwrap().unwrap()
	}

	fn retag(column: (FieldRef, ArrayRef), edit: impl FnOnce(&mut FieldType)) -> (FieldRef, ArrayRef) {
		let (field, array) = column;
		let mut field_type = from_field(&field).unwrap();
		edit(&mut field_type);
		named(field.name(), field_type, array)
	}

	fn uuid7_at(a: u64, b: u16) -> Uuid7 {
		Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian_time(a, b)))
	}

	fn assert_extract_preserves_values(column: (FieldRef, ArrayRef), indices: &[usize]) {
		// Each extracted value must equal its source row, covering every column type without hand-built values.
		let original = batch(vec![column]).unwrap();
		let extracted = take_rows(&original, indices).unwrap();

		assert_eq!(extracted.num_columns(), 1, "column count must be preserved");
		assert_eq!(extracted.num_rows(), indices.len(), "row count must equal number of indices");

		let src = view(&original, "c");
		let dst = view(&extracted, "c");
		assert_eq!(dst.get_type(), src.get_type(), "value type must be preserved");
		for (j, &idx) in indices.iter().enumerate() {
			assert_eq!(
				dst.get_value(j),
				src.get_value(idx),
				"value at extracted row {j} must equal source row {idx}"
			);
		}
	}

	#[test]
	fn extract_by_indices_preserves_bool_values() {
		assert_extract_preserves_values(factory::bool("c", [true, false, true, false]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_float4_values() {
		assert_extract_preserves_values(factory::float4("c", [1.0f32, 2.5, -3.0, 4.25]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_float8_values() {
		assert_extract_preserves_values(factory::float8("c", [1.0f64, 2.5, -3.0, 4.25]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_int1_values() {
		assert_extract_preserves_values(factory::int1("c", [-1i8, 2, -3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_int2_values() {
		assert_extract_preserves_values(factory::int2("c", [-1i16, 2, -3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_int4_values() {
		assert_extract_preserves_values(factory::int4("c", [-1i32, 2, -3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_int8_values() {
		assert_extract_preserves_values(factory::int8("c", [-1i64, 2, -3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_int16_values() {
		assert_extract_preserves_values(factory::int16("c", [-1i128, 2, -3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uint1_values() {
		assert_extract_preserves_values(factory::uint1("c", [1u8, 2, 3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uint2_values() {
		assert_extract_preserves_values(factory::uint2("c", [1u16, 2, 3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uint4_values() {
		assert_extract_preserves_values(factory::uint4("c", [1u32, 2, 3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uint8_values() {
		assert_extract_preserves_values(factory::uint8("c", [1u64, 2, 3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uint16_values() {
		assert_extract_preserves_values(factory::uint16("c", [1u128, 2, 3, 4]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_utf8_values() {
		assert_extract_preserves_values(factory::utf8("c", ["a", "bb", "ccc", "dddd"]), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_date_values() {
		let data = [
			Date::from_ymd(2025, 1, 1).unwrap(),
			Date::from_ymd(2025, 6, 15).unwrap(),
			Date::from_ymd(2024, 12, 31).unwrap(),
			Date::from_ymd(2000, 2, 29).unwrap(),
		];
		assert_extract_preserves_values(factory::date("c", data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_datetime_values() {
		let data = [
			DateTime::from_epoch_secs(1000).unwrap(),
			DateTime::from_epoch_secs(2000).unwrap(),
			DateTime::from_epoch_secs(3000).unwrap(),
			DateTime::from_epoch_secs(4000).unwrap(),
		];
		assert_extract_preserves_values(factory::datetime("c", data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_time_values() {
		let data = [
			Time::from_hms(0, 0, 0).unwrap(),
			Time::from_hms(12, 30, 45).unwrap(),
			Time::from_hms(23, 59, 59).unwrap(),
			Time::from_hms(6, 15, 0).unwrap(),
		];
		assert_extract_preserves_values(factory::time("c", data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_duration_values() {
		let data = [
			Duration::from_days(1).unwrap(),
			Duration::from_days(7).unwrap(),
			Duration::from_days(30).unwrap(),
			Duration::from_days(365).unwrap(),
		];
		assert_extract_preserves_values(factory::duration("c", data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_identity_id_values() {
		let data = [IdentityId::root(), IdentityId::system(), IdentityId::anonymous(), IdentityId::root()];
		assert_extract_preserves_values(factory::identity_id("c", data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uuid4_values() {
		let data = [Uuid4::generate(), Uuid4::generate(), Uuid4::generate(), Uuid4::generate()];
		assert_extract_preserves_values(factory::uuid4("c", data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_uuid7_values() {
		let data = [uuid7_at(1, 1), uuid7_at(1, 2), uuid7_at(2, 1), uuid7_at(2, 2)];
		assert_extract_preserves_values(factory::uuid7("c", data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_blob_values() {
		let data = [
			Blob::new(vec![1]),
			Blob::new(vec![2, 3]),
			Blob::new(vec![4, 5, 6]),
			Blob::new(vec![7, 8, 9, 10]),
		];
		assert_extract_preserves_values(factory::blob("c", data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_decimal_values() {
		let data = [
			Decimal::from_str("1.50").unwrap(),
			Decimal::from_str("2.25").unwrap(),
			Decimal::from_str("-3.75").unwrap(),
			Decimal::from_str("4.00").unwrap(),
		];
		assert_extract_preserves_values(factory::decimal("c", Precision::MAX, Scale::new(2), data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_any_values() {
		let data =
			[Some(Value::Int4(1)), Some(Value::Utf8("two".to_string())), Some(Value::Boolean(true)), None];
		assert_extract_preserves_values(factory::any_optional("c", data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_dictionary_id_values() {
		let data = [
			DictionaryEntryId::U2(10),
			DictionaryEntryId::U2(20),
			DictionaryEntryId::U2(30),
			DictionaryEntryId::U2(40),
		];
		assert_extract_preserves_values(factory::dictionary_id("c", data), &[3, 1, 2]);
	}

	#[test]
	fn extract_by_indices_preserves_option_values_including_none() {
		let mut builder = ColumnBuilder::with_capacity(ValueType::Option(Box::new(ValueType::Int4)), 0);
		builder.push_value(Value::Int4(1));
		builder.push_value(Value::none());
		builder.push_value(Value::Int4(3));
		builder.push_value(Value::none());
		let column = builder.finish("c");
		assert_extract_preserves_values(column, &[3, 1, 2, 0]);
	}

	#[test]
	fn extract_by_indices_empty_indices_yields_empty_columns() {
		let original = batch(vec![factory::int4("c", [1, 2, 3])]).unwrap();
		let extracted = take_rows(&original, &[]).unwrap();
		assert_eq!(extracted.num_rows(), 0);
		assert_eq!(extracted.schema(), original.schema());
	}

	#[test]
	fn extract_by_indices_full_identity_reproduces_all_rows() {
		assert_extract_preserves_values(factory::int4("c", [10, 20, 30, 40]), &[0, 1, 2, 3]);
	}

	#[test]
	fn heap_size_grows_with_row_count() {
		let small = heap_size(&batch(vec![factory::int4("c", [1i32, 2, 3, 4])]).unwrap()).unwrap();
		let large = heap_size(&batch(vec![factory::int4("c", 0..4000i32)]).unwrap()).unwrap();
		assert!(
			large > small + 4000,
			"heap_size must scale with the number of buffered rows (small={}, large={})",
			small,
			large
		);
	}

	#[test]
	fn heap_size_counts_utf8_payload_not_just_row_count() {
		// Same row count with bigger strings must report more, otherwise wide strings blow past the cap.
		let short = batch(vec![factory::utf8("c", ["a", "b", "c"])]).unwrap();
		let long_value = "x".repeat(4096);
		let long =
			batch(vec![factory::utf8("c", [long_value.clone(), long_value.clone(), long_value.clone()])])
				.unwrap();
		assert_eq!(short.num_rows(), long.num_rows(), "same row count is the point of the test");
		let (short, long) = (heap_size(&short).unwrap(), heap_size(&long).unwrap());
		assert!(
			long >= short + 3 * 4096,
			"heap_size must account for utf8 payload bytes (short={}, long={})",
			short,
			long
		);
	}

	#[test]
	fn extract_by_indices_duplicate_index_duplicates_row() {
		let original = batch(vec![factory::int4("c", [10, 20, 30])]).unwrap();
		let extracted = take_rows(&original, &[1, 1, 1]).unwrap();
		assert_eq!(extracted.num_rows(), 3);
		assert_eq!(view(&extracted, "c").get_value(0), Value::Int4(20));
		assert_eq!(view(&extracted, "c").get_value(1), Value::Int4(20));
		assert_eq!(view(&extracted, "c").get_value(2), Value::Int4(20));
	}

	#[test]
	fn extract_by_indices_extracts_multiple_columns_consistently() {
		let original = batch(vec![
			factory::int4("id", [1, 2, 3, 4]),
			factory::utf8("name", ["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()]),
			factory::bool("flag", [true, false, true, false]),
		])
		.unwrap();
		let extracted = take_rows(&original, &[2, 0]).unwrap();

		assert_eq!(extracted.num_columns(), 3);
		assert_eq!(extracted.num_rows(), 2);
		assert_eq!(view(&extracted, "id").get_value(0), Value::Int4(3));
		assert_eq!(view(&extracted, "id").get_value(1), Value::Int4(1));
		assert_eq!(view(&extracted, "name").get_value(0), Value::Utf8("c".to_string()));
		assert_eq!(view(&extracted, "name").get_value(1), Value::Utf8("a".to_string()));
		assert_eq!(view(&extracted, "flag").get_value(0), Value::Boolean(true));
		assert_eq!(view(&extracted, "flag").get_value(1), Value::Boolean(true));
	}

	#[test]
	fn extract_by_indices_extracts_system_columns_in_order() {
		let row_numbers = [RowNumber::from(1), RowNumber::from(2), RowNumber::from(3), RowNumber::from(4)];
		let created_at = vec![
			DateTime::from_epoch_secs(1000).unwrap(),
			DateTime::from_epoch_secs(2000).unwrap(),
			DateTime::from_epoch_secs(3000).unwrap(),
			DateTime::from_epoch_secs(4000).unwrap(),
		];
		let updated_at = vec![
			DateTime::from_epoch_secs(1100).unwrap(),
			DateTime::from_epoch_secs(2200).unwrap(),
			DateTime::from_epoch_secs(3300).unwrap(),
			DateTime::from_epoch_secs(4400).unwrap(),
		];
		let time = created_at.clone();
		let mut original = batch(vec![factory::int4("id", [10, 20, 30, 40])]).unwrap();
		let row_numbers = factory::uint8("", row_numbers.iter().map(|row_number| row_number.0)).1;
		original = with_system_column(original, SystemColumn::RowNumbers, row_numbers).unwrap();
		original = with_system_column(original, SystemColumn::CreatedAt, factory::datetime("", created_at).1)
			.unwrap();
		original = with_system_column(original, SystemColumn::UpdatedAt, factory::datetime("", updated_at).1)
			.unwrap();
		original = with_system_column(original, SystemColumn::Time, factory::datetime("", time).1).unwrap();

		let extracted = take_rows(&original, &[3, 0]).unwrap();

		let rns: Vec<RowNumber> = system_columns::row_numbers(&extracted).unwrap().to_vec();
		assert_eq!(rns, vec![RowNumber::from(4), RowNumber::from(1)], "row_numbers must follow indices");
		assert_eq!(
			system_columns::created_at(&extracted).unwrap().to_vec(),
			vec![DateTime::from_epoch_secs(4000).unwrap(), DateTime::from_epoch_secs(1000).unwrap()],
			"created_at must follow indices"
		);
		assert_eq!(
			system_columns::updated_at(&extracted).unwrap().to_vec(),
			vec![DateTime::from_epoch_secs(4400).unwrap(), DateTime::from_epoch_secs(1100).unwrap()],
			"updated_at must follow indices"
		);
	}

	#[test]
	fn extract_by_indices_preserves_dictionary_id_metadata() {
		// A dropped dictionary_id leaves a deferred view unable to decode, silently losing inserts.
		let column = retag(
			factory::dictionary_id(
				"token",
				[DictionaryEntryId::U2(10), DictionaryEntryId::U2(20), DictionaryEntryId::U2(30)],
			),
			|field_type| field_type.dictionary_id = Some(DictionaryId(42)),
		);

		let original = batch(vec![column]).unwrap();
		let extracted = take_rows(&original, &[2, 0]).unwrap();

		let extracted = view(&extracted, "token");
		match &extracted.data {
			ViewData::DictionaryId {
				dictionary_id,
				..
			} => {
				assert_eq!(
					*dictionary_id,
					Some(DictionaryId(42)),
					"dictionary_id metadata must survive extraction"
				);
			}
			_ => panic!("expected DictionaryId buffer, got {:?}", extracted.get_type()),
		}
	}

	#[test]
	fn extract_by_indices_preserves_utf8_max_bytes_metadata() {
		let column = retag(factory::utf8("c", ["a", "bb", "ccc"]), |field_type| {
			field_type.max_bytes = Some(MaxBytes::new(255))
		});

		let original = batch(vec![column]).unwrap();
		let extracted = take_rows(&original, &[2, 0]).unwrap();

		let extracted = view(&extracted, "c");
		match &extracted.data {
			ViewData::Utf8 {
				max_bytes,
				..
			} => assert_eq!(*max_bytes, MaxBytes::new(255), "Utf8 max_bytes must survive extraction"),
			_ => panic!("expected Utf8 buffer, got {:?}", extracted.get_type()),
		}
	}

	#[test]
	fn extract_by_indices_preserves_blob_max_bytes_metadata() {
		let column = retag(
			factory::blob("c", [Blob::new(vec![1]), Blob::new(vec![2, 3]), Blob::new(vec![4])]),
			|field_type| field_type.max_bytes = Some(MaxBytes::new(1024)),
		);

		let original = batch(vec![column]).unwrap();
		let extracted = take_rows(&original, &[2, 0]).unwrap();

		let extracted = view(&extracted, "c");
		match &extracted.data {
			ViewData::Blob {
				max_bytes,
				..
			} => assert_eq!(*max_bytes, MaxBytes::new(1024), "Blob max_bytes must survive extraction"),
			_ => panic!("expected Blob buffer, got {:?}", extracted.get_type()),
		}
	}

	#[test]
	fn extract_by_indices_preserves_decimal_precision_and_scale_metadata() {
		let column = factory::decimal(
			"c",
			Precision::new(10),
			Scale::new(2),
			[
				Decimal::from_str("1.50").unwrap(),
				Decimal::from_str("2.25").unwrap(),
				Decimal::from_str("3.75").unwrap(),
			],
		);

		let original = batch(vec![column]).unwrap();
		let extracted = take_rows(&original, &[2, 0]).unwrap();

		match view(&extracted, "c").get_type() {
			ValueType::Decimal {
				precision,
				scale,
			} => {
				assert_eq!(precision, Precision::new(10), "Decimal precision must survive extraction");
				assert_eq!(scale, Scale::new(2), "Decimal scale must survive extraction");
			}
			other => panic!("expected Decimal buffer, got {:?}", other),
		}
	}

	#[test]
	fn test_single_row_temporal_types() {
		let date = Date::from_ymd(2025, 1, 15).unwrap();
		let datetime = DateTime::from_epoch_secs(1642694400).unwrap();
		let time = Time::from_hms(14, 30, 45).unwrap();
		let duration = Duration::from_days(30).unwrap();

		let columns = single_row([
			("date_col", Value::Date(date)),
			("datetime_col", Value::DateTime(datetime)),
			("time_col", Value::Time(time)),
			("interval_col", Value::Duration(duration)),
		])
		.unwrap();

		assert_eq!(columns.num_columns(), 4);
		assert_eq!((columns.num_rows(), columns.num_columns()), (1, 4));

		assert_eq!(view(&columns, "date_col").get_value(0), Value::Date(date));
		assert_eq!(view(&columns, "datetime_col").get_value(0), Value::DateTime(datetime));
		assert_eq!(view(&columns, "time_col").get_value(0), Value::Time(time));
		assert_eq!(view(&columns, "interval_col").get_value(0), Value::Duration(duration));
	}

	#[test]
	fn test_single_row_mixed_types() {
		let date = Date::from_ymd(2025, 7, 15).unwrap();
		let time = Time::from_hms(9, 15, 30).unwrap();

		let columns = single_row([
			("bool_col", Value::Boolean(true)),
			("int_col", Value::Int4(42)),
			("str_col", Value::Utf8("hello".to_string())),
			("date_col", Value::Date(date)),
			("time_col", Value::Time(time)),
			("none_col", Value::none()),
		])
		.unwrap();

		assert_eq!(columns.num_columns(), 6);
		assert_eq!((columns.num_rows(), columns.num_columns()), (1, 6));

		assert_eq!(view(&columns, "bool_col").get_value(0), Value::Boolean(true));
		assert_eq!(view(&columns, "int_col").get_value(0), Value::Int4(42));
		assert_eq!(view(&columns, "str_col").get_value(0), Value::Utf8("hello".to_string()));
		assert_eq!(view(&columns, "date_col").get_value(0), Value::Date(date));
		assert_eq!(view(&columns, "time_col").get_value(0), Value::Time(time));
		assert_eq!(view(&columns, "none_col").get_value(0), Value::none());
	}

	#[test]
	fn test_single_row_none_of_int4_is_int4_typed() {
		// The inner type a none carries must survive, otherwise every all-none column is silently mistyped.
		let columns = single_row([("n", Value::none_of(ValueType::Int4))]).unwrap();
		match view(&columns, "n").get_value(0) {
			Value::None {
				inner,
			} => assert_eq!(inner, ValueType::Int4),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_single_row_none_of_utf8_is_utf8_typed() {
		let columns = single_row([("n", Value::none_of(ValueType::Utf8))]).unwrap();
		match view(&columns, "n").get_value(0) {
			Value::None {
				inner,
			} => assert_eq!(inner, ValueType::Utf8),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_single_row_bare_none_is_any_typed() {
		let columns = single_row([("n", Value::none())]).unwrap();
		match view(&columns, "n").get_value(0) {
			Value::None {
				inner,
			} => assert_eq!(inner, ValueType::Any),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_single_row_none_of_nested_option_collapses_to_base_type() {
		// A nested Option(inner) none must land in a column of its base type, here Duration.
		let inner_ty = ValueType::Option(Box::new(ValueType::Duration));
		let columns = single_row([("n", Value::none_of(inner_ty))]).unwrap();
		match view(&columns, "n").get_value(0) {
			Value::None {
				inner,
			} => assert_eq!(inner, ValueType::Duration),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_single_row_none_of_boolean_is_boolean_typed() {
		// A typed none of Boolean must keep Boolean, never become the untyped none column.
		let columns = single_row([("n", Value::none_of(ValueType::Boolean))]).unwrap();
		match view(&columns, "n").get_value(0) {
			Value::None {
				inner,
			} => assert_eq!(inner, ValueType::Boolean),
			other => panic!("expected Value::None, got {other:?}"),
		}
	}

	#[test]
	fn test_single_row_normal_column_names_work() {
		let columns = single_row([("normal_column", Value::Int4(42))]).unwrap();
		assert_eq!(columns.num_columns(), 1);
		assert_eq!(view(&columns, "normal_column").get_value(0), Value::Int4(42));
	}

	#[test]
	fn with_row_numbers_leaves_an_absent_sidecar_absent() {
		// A timeless batch must stay timeless; a filled #time reads downstream as a real time zero.
		let columns = with_system_column(
			batch(vec![factory::int4("v", [1, 2, 3])]).unwrap(),
			SystemColumn::RowNumbers,
			factory::uint8("", [1u64, 2, 3]).1,
		)
		.unwrap();

		assert_eq!(system_columns::row_numbers(&columns).unwrap().len(), 3);
		assert!(system_columns::time(&columns).unwrap().is_empty(), "#time must stay absent");
		assert!(system_columns::created_at(&columns).unwrap().is_empty(), "created_at must stay absent");
		assert!(system_columns::updated_at(&columns).unwrap().is_empty(), "updated_at must stay absent");
	}

	#[test]
	fn with_row_numbers_keeps_a_populated_sidecar() {
		let stamps = vec![DateTime::from_nanos(10), DateTime::from_nanos(20)];
		let mut columns = batch(vec![factory::int4("v", [1, 2])]).unwrap();
		columns =
			with_system_column(columns, SystemColumn::RowNumbers, factory::uint8("", [7u64, 8]).1).unwrap();
		columns = with_system_column(columns, SystemColumn::Time, factory::datetime("", stamps.clone()).1)
			.unwrap();

		let columns =
			with_system_column(columns, SystemColumn::RowNumbers, factory::uint8("", [1u64, 2]).1).unwrap();

		assert_eq!(system_columns::time(&columns).unwrap(), stamps.as_slice());
		assert_eq!(system_columns::row_numbers(&columns).unwrap(), &[RowNumber(1), RowNumber(2)]);
	}

	#[test]
	fn with_row_numbers_fails_on_a_partial_sidecar() {
		// A row number column that covers only some rows must stop the write, never be padded to fit.
		let columns = batch(vec![factory::int4("v", [1, 2, 3])]).unwrap();
		let columns = with_system_column(columns, SystemColumn::RowNumbers, factory::uint8("", [1u64, 2, 3]).1)
			.unwrap();
		let columns = with_system_column(
			columns,
			SystemColumn::Time,
			factory::datetime(
				"",
				[DateTime::from_nanos(10), DateTime::from_nanos(20), DateTime::from_nanos(30)],
			)
			.1,
		)
		.unwrap();

		let short = factory::uint8("", [1u64, 2]).1;
		assert!(with_system_column(columns, SystemColumn::RowNumbers, short).is_err());
	}

	#[test]
	fn extract_by_indices_keeps_an_all_valid_column_nullable() {
		// Arrow kernels drop an all-valid null buffer, so an Option column without its field flag turns bare.
		let columns = batch(vec![named(
			"c",
			FieldType::from(ValueType::Option(Box::new(ValueType::Int4))),
			Arc::new(Int32Array::from(vec![1, 2, 3])),
		)])
		.unwrap();

		let extracted = take_rows(&columns, &[2, 0]).unwrap();

		assert!(extracted.schema_ref().field(0).is_nullable());
		assert_eq!(view(&extracted, "c").get_type(), ValueType::Option(Box::new(ValueType::Int4)));
		assert_eq!(view(&extracted, "c").get_value(0), Value::Int4(3));
	}

	#[test]
	fn extract_by_indices_writes_the_type_default_under_a_none_row() {
		// A none row must carry the type default underneath it, never the bytes left at the source row.
		let columns = batch(vec![factory::utf8_with_bitvec(
			"c",
			["keep", "hidden"],
			BooleanBuffer::from(vec![true, false]),
		)])
		.unwrap();

		let extracted = take_rows(&columns, &[1, 0]).unwrap();

		let Some(container) = extracted.column(0).as_any().downcast_ref::<LargeStringArray>() else {
			panic!("expected a utf8 column");
		};
		assert_eq!(container.value(0), "");
		assert_eq!(view(&extracted, "c").get_value(0), Value::none_of(ValueType::Utf8));
		assert_eq!(view(&extracted, "c").get_value(1), Value::Utf8("keep".to_string()));
	}

	#[test]
	fn extract_by_indices_out_of_range_index_fails() {
		// An index past the end is a bug, so it must fail naming the row and length, never read as a none row.
		let columns = batch(vec![factory::int4("c", [1, 2])]).unwrap();

		let error = take_rows(&columns, &[1, 7]).unwrap_err();

		assert!(error.diagnostic().message.contains("row index 7 out of range for a column of 2 rows"));
	}

	#[test]
	fn extract_by_indices_with_no_indices_keeps_the_schema() {
		// An empty index list must keep the schema, otherwise an emptied result loses the columns it renders.
		let columns = batch(vec![factory::int4("c", [1, 2])]).unwrap();

		let extracted = take_rows(&columns, &[]).unwrap();

		assert_eq!(extracted.schema(), columns.schema());
		assert_eq!(extracted.num_rows(), 0);
	}

	mod take {
		use arrow_buffer::NullBuffer;
		use reifydb_value::value::{Value, value_type::ValueType};

		use super::{column, view};
		use crate::value::{
			batch::{batch, head},
			column::{factory, nulls::with_nulls},
		};

		#[test]
		fn test_bool_column() {
			let test_instance = batch(vec![factory::bool_with_bitvec(
				"flag",
				[true, true, false],
				vec![false, true, true],
			)])
			.unwrap();

			let test_instance = test_instance.slice(0, 1);

			assert_eq!(column(&test_instance, 0), factory::bool_with_bitvec("flag", [true], vec![false]));
		}

		#[test]
		fn test_float4_column() {
			let test_instance =
				batch(vec![factory::float4_with_bitvec("a", [1.0, 2.0, 3.0], vec![true, false, true])])
					.unwrap();

			let test_instance = test_instance.slice(0, 2);

			assert_eq!(
				column(&test_instance, 0),
				factory::float4_with_bitvec("a", [1.0, 2.0], vec![true, false])
			);
		}

		#[test]
		fn test_float8_column() {
			let test_instance = batch(vec![factory::float8_with_bitvec(
				"a",
				[1f64, 2.0, 3.0, 4.0],
				vec![true, true, false, true],
			)])
			.unwrap();

			let test_instance = test_instance.slice(0, 2);

			assert_eq!(
				column(&test_instance, 0),
				with_nulls(factory::float8("a", [1.0, 2.0]), NullBuffer::new_valid(2)).unwrap()
			);
		}

		#[test]
		fn test_int1_column() {
			let test_instance =
				batch(vec![factory::int1_with_bitvec("a", [1, 2, 3], vec![true, false, true])])
					.unwrap();

			let test_instance = test_instance.slice(0, 2);

			assert_eq!(
				column(&test_instance, 0),
				factory::int1_with_bitvec("a", [1, 2], vec![true, false])
			);
		}

		#[test]
		fn test_int2_column() {
			let test_instance = batch(vec![factory::int2_with_bitvec(
				"a",
				[1, 2, 3, 4],
				vec![true, true, false, true],
			)])
			.unwrap();

			let test_instance = test_instance.slice(0, 2);

			assert_eq!(
				column(&test_instance, 0),
				with_nulls(factory::int2("a", [1, 2]), NullBuffer::new_valid(2)).unwrap()
			);
		}

		#[test]
		fn test_int4_column() {
			let test_instance =
				batch(vec![factory::int4_with_bitvec("a", [1, 2], vec![true, false])]).unwrap();

			let test_instance = test_instance.slice(0, 1);

			assert_eq!(
				column(&test_instance, 0),
				with_nulls(factory::int4("a", [1]), NullBuffer::new_valid(1)).unwrap()
			);
		}

		#[test]
		fn test_int8_column() {
			let test_instance =
				batch(vec![factory::int8_with_bitvec("a", [1, 2, 3], vec![false, true, true])])
					.unwrap();

			let test_instance = test_instance.slice(0, 2);

			assert_eq!(
				column(&test_instance, 0),
				factory::int8_with_bitvec("a", [1, 2], vec![false, true])
			);
		}

		#[test]
		fn test_int16_column() {
			let test_instance =
				batch(vec![factory::int16_with_bitvec("a", [1, 2], vec![true, true])]).unwrap();

			let test_instance = test_instance.slice(0, 1);

			assert_eq!(column(&test_instance, 0), factory::int16_with_bitvec("a", [1], vec![true]));
		}

		#[test]
		fn test_uint1_column() {
			let test_instance =
				batch(vec![factory::uint1_with_bitvec("a", [1, 2, 3], vec![false, false, true])])
					.unwrap();

			let test_instance = test_instance.slice(0, 2);

			assert_eq!(
				column(&test_instance, 0),
				factory::uint1_with_bitvec("a", [1, 2], vec![false, false])
			);
		}

		#[test]
		fn test_uint2_column() {
			let test_instance =
				batch(vec![factory::uint2_with_bitvec("a", [1, 2], vec![true, false])]).unwrap();

			let test_instance = test_instance.slice(0, 1);

			assert_eq!(
				column(&test_instance, 0),
				with_nulls(factory::uint2("a", [1]), NullBuffer::new_valid(1)).unwrap()
			);
		}

		#[test]
		fn test_uint4_column() {
			let test_instance =
				batch(vec![factory::uint4_with_bitvec("a", [10, 20], vec![false, true])]).unwrap();

			let test_instance = test_instance.slice(0, 1);

			assert_eq!(column(&test_instance, 0), factory::uint4_with_bitvec("a", [10], vec![false]));
		}

		#[test]
		fn test_uint8_column() {
			let test_instance =
				batch(vec![factory::uint8_with_bitvec("a", [10, 20, 30], vec![true, true, false])])
					.unwrap();

			let test_instance = test_instance.slice(0, 2);

			assert_eq!(
				column(&test_instance, 0),
				with_nulls(factory::uint8("a", [10, 20]), NullBuffer::new_valid(2)).unwrap()
			);
		}

		#[test]
		fn test_uint16_column() {
			let test_instance =
				batch(vec![factory::uint16_with_bitvec("a", [100, 200, 300], vec![true, false, true])])
					.unwrap();

			let test_instance = test_instance.slice(0, 1);

			assert_eq!(
				column(&test_instance, 0),
				with_nulls(factory::uint16("a", [100]), NullBuffer::new_valid(1)).unwrap()
			);
		}

		#[test]
		fn test_text_column() {
			let test_instance = batch(vec![factory::utf8_with_bitvec(
				"t",
				vec!["a".to_string(), "b".to_string(), "c".to_string()],
				vec![true, false, true],
			)])
			.unwrap();

			let test_instance = test_instance.slice(0, 2);

			assert_eq!(
				column(&test_instance, 0),
				factory::utf8_with_bitvec("t", ["a".to_string(), "b".to_string()], vec![true, false])
			);
		}

		#[test]
		fn test_none_column() {
			let test_instance = batch(vec![factory::none_typed("u", ValueType::Boolean, 3)]).unwrap();

			let test_instance = test_instance.slice(0, 2);

			assert_eq!(view(&test_instance, "u").len(), 2);
			assert_eq!(view(&test_instance, "u").get_value(0), Value::none_of(ValueType::Boolean));
			assert_eq!(view(&test_instance, "u").get_value(1), Value::none_of(ValueType::Boolean));
		}

		#[test]
		fn test_handles_none() {
			let test_instance = batch(vec![factory::none_typed("u", ValueType::Boolean, 5)]).unwrap();

			let test_instance = test_instance.slice(0, 3);

			assert_eq!(view(&test_instance, "u").len(), 3);
			assert_eq!(view(&test_instance, "u").get_value(0), Value::none_of(ValueType::Boolean));
		}

		#[test]
		fn test_n_larger_than_len_is_safe() {
			// Asking for more rows than the batch holds must return every row, never panic or pad.
			let test_instance =
				batch(vec![factory::int2_with_bitvec("a", [10, 20], vec![true, false])]).unwrap();

			let taken = head(&test_instance, 10);

			assert_eq!(taken, test_instance);
			assert_eq!(head(&test_instance, 1).num_rows(), 1);
		}
	}

	mod extract_rows {
		use reifydb_value::value::{Value, value_type::ValueType};

		use super::view;
		use crate::value::{
			batch::{batch, take_rows, take_rows_or_none},
			column::factory,
		};

		#[test]
		fn extract_rows_past_the_end_fails() {
			// A row past the end is a bug: it must fail naming row and length, never read as none.
			let columns = batch(vec![factory::int4("c", [1, 2])]).unwrap();

			let error = take_rows(&columns, &[0, 2]).unwrap_err();

			assert!(error.diagnostic().message.contains("row index 2 out of range for a column of 2 rows"));
		}

		#[test]
		fn extract_rows_keeps_a_none_row_at_a_valid_index() {
			// The range check must not turn a real none row into an error or a value.
			let columns = batch(vec![factory::int4_with_bitvec("c", [1, 2], vec![false, true])]).unwrap();

			let extracted = take_rows(&columns, &[1, 0]).unwrap();

			assert_eq!(view(&extracted, "c").get_value(0), Value::Int4(2));
			assert_eq!(view(&extracted, "c").get_value(1), Value::none_of(ValueType::Int4));
		}

		#[test]
		fn extract_rows_or_none_gives_a_none_row_for_no_pick() {
			// A left join row with no match must read as none, never as the row at a placeholder index.
			let columns = batch(vec![factory::int4("c", [7, 8])]).unwrap();

			let extracted = take_rows_or_none(&columns, &[Some(1), None, Some(0)]).unwrap();

			assert_eq!(extracted.num_rows(), 3);
			assert_eq!(view(&extracted, "c").get_type(), ValueType::Option(Box::new(ValueType::Int4)));
			assert_eq!(view(&extracted, "c").get_value(0), Value::Int4(8));
			assert_eq!(view(&extracted, "c").get_value(1), Value::none_of(ValueType::Int4));
			assert_eq!(view(&extracted, "c").get_value(2), Value::Int4(7));
		}

		#[test]
		fn extract_rows_or_none_fails_on_a_pick_past_the_end() {
			// A matched pick past the end is a bug, so it must fail like extract rows does.
			let columns = batch(vec![factory::int4("c", [7, 8])]).unwrap();

			let error = take_rows_or_none(&columns, &[None, Some(2)]).unwrap_err();

			assert!(error.diagnostic().message.contains("row index 2 out of range for a column of 2 rows"));
		}

		#[test]
		fn extract_rows_or_none_with_every_pick_keeps_a_bare_column_bare() {
			// Widening a column that got no none row would flip a bare type to Option for nothing.
			let columns = batch(vec![factory::int4("c", [7, 8])]).unwrap();

			let extracted = take_rows_or_none(&columns, &[Some(1), Some(0)]).unwrap();

			assert_eq!(extracted.schema(), columns.schema());
			assert_eq!(view(&extracted, "c").get_value(0), Value::Int4(8));
		}
	}

	mod filter {
		use std::sync::Arc;

		use arrow_array::{ArrayRef, Int32Array, LargeStringArray, RecordBatch};
		use arrow_buffer::BooleanBuffer;
		use arrow_schema::FieldRef;
		use reifydb_runtime::context::{
			clock::{Clock, MockClock},
			rng::Rng,
		};
		use reifydb_value::value::{
			Value,
			dictionary::DictionaryEntryId,
			identity::IdentityId,
			row_number::RowNumber,
			system_columns::{SystemColumn, row_numbers, with_system_column},
			value_type::{
				ValueType,
				field::{FieldType, named},
			},
		};

		use super::view;
		use crate::value::{
			batch::{batch, filter},
			column::factory,
		};

		fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
			let mock = MockClock::from_millis(1000);
			let clock = Clock::Mock(mock.clone());
			let rng = Rng::seeded(42);
			(mock, clock, rng)
		}

		fn filtered(column: (FieldRef, ArrayRef), mask: Vec<bool>) -> RecordBatch {
			filter(&batch(vec![column]).unwrap(), &BooleanBuffer::from(mask)).unwrap()
		}

		#[test]
		fn test_filter_bool() {
			let col = filtered(
				factory::bool("c", [true, false, true, false]),
				vec![true, false, true, false],
			);

			assert_eq!(col.num_rows(), 2);
			assert_eq!(view(&col, "c").get_value(0), Value::Boolean(true));
			assert_eq!(view(&col, "c").get_value(1), Value::Boolean(true));
		}

		#[test]
		fn test_filter_int4() {
			let col = filtered(factory::int4("c", [1, 2, 3, 4, 5]), vec![true, false, true, false, true]);

			assert_eq!(col.num_rows(), 3);
			assert_eq!(view(&col, "c").get_value(0), Value::Int4(1));
			assert_eq!(view(&col, "c").get_value(1), Value::Int4(3));
			assert_eq!(view(&col, "c").get_value(2), Value::Int4(5));
		}

		#[test]
		fn test_filter_float4() {
			let col = filtered(factory::float4("c", [1.0, 2.0, 3.0, 4.0]), vec![false, true, false, true]);

			assert_eq!(col.num_rows(), 2);
			match view(&col, "c").get_value(0) {
				Value::Float4(v) => assert_eq!(v.value(), 2.0),
				_ => panic!("Expected Float4"),
			}
			match view(&col, "c").get_value(1) {
				Value::Float4(v) => assert_eq!(v.value(), 4.0),
				_ => panic!("Expected Float4"),
			}
		}

		#[test]
		fn test_filter_string() {
			let col = filtered(factory::utf8("c", ["a", "b", "c", "d"]), vec![true, false, false, true]);

			assert_eq!(col.num_rows(), 2);
			assert_eq!(view(&col, "c").get_value(0), Value::Utf8("a".to_string()));
			assert_eq!(view(&col, "c").get_value(1), Value::Utf8("d".to_string()));
		}

		#[test]
		fn test_filter_none() {
			let col = filtered(
				factory::none_typed("c", ValueType::Boolean, 5),
				vec![true, false, true, false, false],
			);

			assert_eq!(col.num_rows(), 2);
			assert_eq!(view(&col, "c").get_value(0), Value::none_of(ValueType::Boolean));
			assert_eq!(view(&col, "c").get_value(1), Value::none_of(ValueType::Boolean));
		}

		#[test]
		fn test_filter_empty_mask() {
			let col = filtered(factory::int4("c", [1, 2, 3]), vec![false, false, false]);

			assert_eq!(col.num_rows(), 0);
		}

		#[test]
		fn test_filter_all_true_mask() {
			let col = filtered(factory::int4("c", [1, 2, 3]), vec![true, true, true]);

			assert_eq!(col.num_rows(), 3);
			assert_eq!(view(&col, "c").get_value(0), Value::Int4(1));
			assert_eq!(view(&col, "c").get_value(1), Value::Int4(2));
			assert_eq!(view(&col, "c").get_value(2), Value::Int4(3));
		}

		#[test]
		fn test_filter_identity_id() {
			let (mock, clock, rng) = test_clock_and_rng();
			let id1 = IdentityId::generate(&clock, &rng);
			mock.advance_millis(1);
			let id2 = IdentityId::generate(&clock, &rng);
			mock.advance_millis(1);
			let id3 = IdentityId::generate(&clock, &rng);
			mock.advance_millis(1);
			let id4 = IdentityId::generate(&clock, &rng);

			let col = filtered(
				factory::identity_id("c", [id1, id2, id3, id4]),
				vec![true, false, true, false],
			);

			assert_eq!(col.num_rows(), 2);
			assert_eq!(view(&col, "c").get_value(0), Value::IdentityId(id1));
			assert_eq!(view(&col, "c").get_value(1), Value::IdentityId(id3));
		}

		#[test]
		fn test_filter_dictionary_id() {
			let e1 = DictionaryEntryId::U4(10);
			let e2 = DictionaryEntryId::U4(20);
			let e3 = DictionaryEntryId::U4(30);
			let e4 = DictionaryEntryId::U4(40);

			let col =
				filtered(factory::dictionary_id("c", [e1, e2, e3, e4]), vec![true, false, true, false]);

			assert_eq!(col.num_rows(), 2);
			assert_eq!(view(&col, "c").get_value(0), Value::DictionaryId(e1));
			assert_eq!(view(&col, "c").get_value(1), Value::DictionaryId(e3));
		}

		#[test]
		fn test_filter_dictionary_id_with_undefined() {
			let e1 = DictionaryEntryId::U4(10);
			let e2 = DictionaryEntryId::U4(20);

			let col = filtered(
				factory::dictionary_id_with_bitvec(
					"c",
					[e1, DictionaryEntryId::default(), e2, DictionaryEntryId::default()],
					BooleanBuffer::from(vec![true, false, true, false]),
				),
				vec![true, true, false, true],
			);

			assert_eq!(col.num_rows(), 3);
			assert!(view(&col, "c").is_defined(0));
			assert!(!view(&col, "c").is_defined(1));
			assert!(!view(&col, "c").is_defined(2));
			assert_eq!(view(&col, "c").get_value(0), Value::DictionaryId(e1));
		}

		#[test]
		fn filter_with_a_mask_longer_than_the_column_ignores_the_extra_bits() {
			// A mask sized past the batch must select nothing beyond the end instead of failing the kernel.
			let col = filtered(factory::int4("c", [1, 2, 3]), vec![true, false, true, true, true]);

			assert_eq!(col.num_rows(), 2);
			assert_eq!(view(&col, "c").get_value(0), Value::Int4(1));
			assert_eq!(view(&col, "c").get_value(1), Value::Int4(3));
		}

		#[test]
		fn filter_with_a_mask_shorter_than_the_column_drops_the_tail() {
			// The rows past the end of the mask are unselected, never kept by default.
			let col = filtered(factory::int4("c", [1, 2, 3, 4]), vec![true, true]);

			assert_eq!(col.num_rows(), 2);
			assert_eq!(view(&col, "c").get_value(0), Value::Int4(1));
			assert_eq!(view(&col, "c").get_value(1), Value::Int4(2));
		}

		#[test]
		fn filter_keeps_an_all_valid_column_nullable() {
			// Arrow drops an all-valid null buffer, so the Option type must survive on the field.
			let column = named(
				"c",
				FieldType::from(ValueType::Option(Box::new(ValueType::Int4))),
				Arc::new(Int32Array::from(vec![1, 2, 3])),
			);

			let col = filtered(column, vec![true, false, true]);

			assert!(col.schema_ref().field(0).is_nullable());
			assert_eq!(view(&col, "c").get_type(), ValueType::Option(Box::new(ValueType::Int4)));
		}

		#[test]
		fn filter_with_an_all_false_mask_keeps_the_type_and_nullability() {
			// An all-false mask gives a fresh empty array, so the type must come from the field.
			let col = filtered(
				factory::utf8_with_bitvec("c", ["a", "b"], BooleanBuffer::from(vec![true, false])),
				vec![false, false],
			);

			assert_eq!(col.num_rows(), 0);
			assert!(col.schema_ref().field(0).is_nullable());
			assert_eq!(view(&col, "c").get_type(), ValueType::Option(Box::new(ValueType::Utf8)));
		}

		#[test]
		fn filter_keeps_the_placeholder_bytes_under_a_none_row() {
			// Encoders see the bytes under a none row, so the filter must move them unchanged.
			let col = filtered(
				factory::utf8_with_bitvec(
					"c",
					["keep", "hidden"],
					BooleanBuffer::from(vec![true, false]),
				),
				vec![true, true],
			);

			let Some(container) = col.column(0).as_any().downcast_ref::<LargeStringArray>() else {
				panic!("expected a utf8 column");
			};
			assert_eq!(container.value(1), "hidden");
			assert_eq!(view(&col, "c").get_value(1), Value::none_of(ValueType::Utf8));
		}

		#[test]
		fn filter_applies_one_mask_to_every_column_of_the_batch() {
			// A # column filtered apart from user columns would pair rows with wrong row numbers.
			let columns = batch(vec![factory::int4("a", [1, 2, 3]), factory::utf8("b", ["x", "y", "z"])])
				.unwrap();
			let columns = with_system_column(
				columns,
				SystemColumn::RowNumbers,
				factory::uint8("", [10u64, 20, 30]).1,
			)
			.unwrap();

			let col = filter(&columns, &BooleanBuffer::from(vec![false, true, true])).unwrap();

			assert_eq!(view(&col, "a").get_value(0), Value::Int4(2));
			assert_eq!(view(&col, "b").get_value(1), Value::Utf8("z".to_string()));
			assert_eq!(row_numbers(&col).unwrap(), &[RowNumber(20), RowNumber(30)]);
		}
	}

	mod reorder {
		use reifydb_runtime::context::{
			clock::{Clock, MockClock},
			rng::Rng,
		};
		use reifydb_value::value::{
			Value, dictionary::DictionaryEntryId, identity::IdentityId, value_type::ValueType,
		};

		use super::view;
		use crate::value::{
			batch::{batch, take_rows},
			column::factory,
		};

		fn test_clock_and_rng() -> (MockClock, Clock, Rng) {
			let mock = MockClock::from_millis(1000);
			let clock = Clock::Mock(mock.clone());
			let rng = Rng::seeded(42);
			(mock, clock, rng)
		}

		#[test]
		fn test_reorder_bool() {
			let col = take_rows(&batch(vec![factory::bool("c", [true, false, true])]).unwrap(), &[2, 0, 1])
				.unwrap();

			assert_eq!(col.num_rows(), 3);
			assert_eq!(view(&col, "c").get_value(0), Value::Boolean(true));
			assert_eq!(view(&col, "c").get_value(1), Value::Boolean(true));
			assert_eq!(view(&col, "c").get_value(2), Value::Boolean(false));
		}

		#[test]
		fn test_reorder_float4() {
			let col = take_rows(&batch(vec![factory::float4("c", [1.0, 2.0, 3.0])]).unwrap(), &[2, 0, 1])
				.unwrap();

			assert_eq!(col.num_rows(), 3);
			match view(&col, "c").get_value(0) {
				Value::Float4(v) => assert_eq!(v.value(), 3.0),
				_ => panic!("Expected Float4"),
			}
			match view(&col, "c").get_value(1) {
				Value::Float4(v) => assert_eq!(v.value(), 1.0),
				_ => panic!("Expected Float4"),
			}
			match view(&col, "c").get_value(2) {
				Value::Float4(v) => assert_eq!(v.value(), 2.0),
				_ => panic!("Expected Float4"),
			}
		}

		#[test]
		fn test_reorder_int4() {
			let col = take_rows(&batch(vec![factory::int4("c", [1, 2, 3])]).unwrap(), &[2, 0, 1]).unwrap();

			assert_eq!(col.num_rows(), 3);
			assert_eq!(view(&col, "c").get_value(0), Value::Int4(3));
			assert_eq!(view(&col, "c").get_value(1), Value::Int4(1));
			assert_eq!(view(&col, "c").get_value(2), Value::Int4(2));
		}

		#[test]
		fn test_reorder_string() {
			let col = take_rows(
				&batch(vec![factory::utf8("c", ["a".to_string(), "b".to_string(), "c".to_string()])])
					.unwrap(),
				&[2, 0, 1],
			)
			.unwrap();

			assert_eq!(col.num_rows(), 3);
			assert_eq!(view(&col, "c").get_value(0), Value::Utf8("c".to_string()));
			assert_eq!(view(&col, "c").get_value(1), Value::Utf8("a".to_string()));
			assert_eq!(view(&col, "c").get_value(2), Value::Utf8("b".to_string()));
		}

		#[test]
		fn test_reorder_none() {
			let col = take_rows(
				&batch(vec![factory::none_typed("c", ValueType::Boolean, 3)]).unwrap(),
				&[2, 0, 1],
			)
			.unwrap();
			assert_eq!(col.num_rows(), 3);

			let col = take_rows(&col, &[1, 0]).unwrap();
			assert_eq!(col.num_rows(), 2);
		}

		#[test]
		fn test_reorder_identity_id() {
			let (mock, clock, rng) = test_clock_and_rng();
			let id1 = IdentityId::generate(&clock, &rng);
			mock.advance_millis(1);
			let id2 = IdentityId::generate(&clock, &rng);
			mock.advance_millis(1);
			let id3 = IdentityId::generate(&clock, &rng);

			let col = take_rows(
				&batch(vec![factory::identity_id("c", [id1, id2, id3])]).unwrap(),
				&[2, 0, 1],
			)
			.unwrap();

			assert_eq!(col.num_rows(), 3);
			assert_eq!(view(&col, "c").get_value(0), Value::IdentityId(id3));
			assert_eq!(view(&col, "c").get_value(1), Value::IdentityId(id1));
			assert_eq!(view(&col, "c").get_value(2), Value::IdentityId(id2));
		}

		#[test]
		fn test_reorder_dictionary_id() {
			let e1 = DictionaryEntryId::U4(10);
			let e2 = DictionaryEntryId::U4(20);
			let e3 = DictionaryEntryId::U4(30);

			let col =
				take_rows(&batch(vec![factory::dictionary_id("c", [e1, e2, e3])]).unwrap(), &[2, 0, 1])
					.unwrap();

			assert_eq!(col.num_rows(), 3);
			assert_eq!(view(&col, "c").get_value(0), Value::DictionaryId(e3));
			assert_eq!(view(&col, "c").get_value(1), Value::DictionaryId(e1));
			assert_eq!(view(&col, "c").get_value(2), Value::DictionaryId(e2));
		}
	}

	mod concat {
		use std::{slice::from_ref, sync::Arc};

		use arrow_array::RecordBatch;
		use arrow_schema::Schema;
		use reifydb_value::value::{
			Value,
			constraint::bytes::MaxBytes,
			value_type::{ValueType, field::from_field},
		};

		use super::{retag, view};
		use crate::value::{
			batch::{append, batch, concat, concat_columns, empty_batch},
			column::factory,
		};

		#[test]
		fn concat_of_no_batches_is_the_empty_batch() {
			// A concat of nothing must give append's identity batch, never an error or panic.
			assert_eq!(concat(&[]).unwrap(), empty_batch());
		}

		#[test]
		fn concat_matches_a_chain_of_appends() {
			// Concat must unify types like repeated append, or a scan would differ by chunk count.
			let a = batch(vec![factory::int4("c", [1, 2])]).unwrap();
			let b = batch(vec![factory::int4_optional("c", [None, Some(4)])]).unwrap();
			let c = batch(vec![factory::int4("c", [5])]).unwrap();

			let merged = concat(&[a.clone(), b.clone(), c.clone()]).unwrap();

			assert_eq!(merged, append(&append(&a, &b).unwrap(), &c).unwrap());
			assert_eq!(view(&merged, "c").get_type(), ValueType::Option(Box::new(ValueType::Int4)));
			assert_eq!(view(&merged, "c").get_value(2), Value::none_of(ValueType::Int4));
			assert_eq!(view(&merged, "c").get_value(4), Value::Int4(5));
		}

		#[test]
		fn concat_skips_empty_batches_and_replaces_a_columnless_lead() {
			// An empty part must not fail the merge, and a columnless lead must not drop rows.
			let columnless = RecordBatch::new_empty(Arc::new(Schema::empty()));
			let a = batch(vec![factory::int4("c", [1])]).unwrap();
			let other = batch(vec![factory::utf8("x", Vec::<String>::new())]).unwrap();
			let b = batch(vec![factory::int4("c", [2])]).unwrap();

			let merged = concat(&[columnless, a, other, b]).unwrap();

			assert_eq!(merged.num_rows(), 2);
			assert_eq!(view(&merged, "c").get_value(1), Value::Int4(2));
		}

		#[test]
		fn concat_rejects_a_name_mismatch_in_any_part() {
			// A later part with a different column would be glued under the wrong name.
			let a = batch(vec![factory::int4("c", [1])]).unwrap();
			let b = batch(vec![factory::int4("c", [2])]).unwrap();
			let c = batch(vec![factory::int4("d", [3])]).unwrap();

			let error = concat(&[a, b, c]).unwrap_err();

			assert!(error.diagnostic().message.contains("column name mismatch at index 0: 'c' vs 'd'"));
		}

		#[test]
		fn concat_columns_of_nothing_fails() {
			// A column merge has no name or type to give an empty result, so it must fail, not panic.
			assert!(concat_columns(&[]).is_err());
		}

		#[test]
		fn concat_columns_of_one_part_is_that_part() {
			// A single chunk must come back as is, keeping its field details.
			let part = retag(factory::utf8("c", ["a"]), |field_type| {
				field_type.max_bytes = Some(MaxBytes::new(8))
			});

			assert_eq!(concat_columns(from_ref(&part)).unwrap(), part);
		}

		#[test]
		fn concat_columns_unifies_a_bare_and_an_optional_part() {
			// A bare chunk then a chunk with none rows must widen to Option, never drop the nones.
			let parts = [factory::int4("c", [1, 2]), factory::int4_optional("c", [None, Some(4)])];

			let (field, array) = concat_columns(&parts).unwrap();

			assert_eq!(field.name(), "c");
			assert!(field.is_nullable());
			let merged = batch(vec![(field, array)]).unwrap();
			assert_eq!(view(&merged, "c").get_value(1), Value::Int4(2));
			assert_eq!(view(&merged, "c").get_value(2), Value::none_of(ValueType::Int4));
			assert_eq!(view(&merged, "c").get_value(3), Value::Int4(4));
		}

		#[test]
		fn concat_columns_keeps_the_field_details_of_equal_parts() {
			// The equal-field fast path must keep max_bytes, or the merged column accepts too much.
			let tagged = |values: [&str; 1]| {
				retag(factory::utf8("c", values), |field_type| {
					field_type.max_bytes = Some(MaxBytes::new(8))
				})
			};

			let (field, array) = concat_columns(&[tagged(["a"]), tagged(["b"])]).unwrap();

			assert_eq!(from_field(&field).unwrap().max_bytes, Some(MaxBytes::new(8)));
			let merged = batch(vec![(field, array)]).unwrap();
			assert_eq!(merged.num_rows(), 2);
			assert_eq!(view(&merged, "c").get_value(1), Value::Utf8("b".to_string()));
		}
	}

	mod columns {
		use reifydb_value::value::uuid::{Uuid4, Uuid7};
		use uuid::{Timestamp, Uuid};

		use super::column;
		use crate::value::{
			batch::{append, batch},
			column::factory,
		};

		#[test]
		fn test_boolean() {
			let test_instance1 = batch(vec![factory::bool_with_bitvec("id", [true], vec![false])]).unwrap();

			let test_instance2 = batch(vec![factory::bool_with_bitvec("id", [false], vec![true])]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::bool_with_bitvec("id", [true, false], vec![false, true])
			);
		}

		#[test]
		fn test_float4() {
			let test_instance1 = batch(vec![factory::float4("id", [1.0f32, 2.0])]).unwrap();

			let test_instance2 =
				batch(vec![factory::float4_with_bitvec("id", [3.0f32, 4.0], vec![true, false])])
					.unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::float4_with_bitvec(
					"id",
					[1.0f32, 2.0, 3.0, 4.0],
					vec![true, true, true, false]
				)
			);
		}

		#[test]
		fn test_float8() {
			let test_instance1 = batch(vec![factory::float8("id", [1.0f64, 2.0])]).unwrap();

			let test_instance2 =
				batch(vec![factory::float8_with_bitvec("id", [3.0f64, 4.0], vec![true, false])])
					.unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::float8_with_bitvec(
					"id",
					[1.0f64, 2.0, 3.0, 4.0],
					vec![true, true, true, false]
				)
			);
		}

		#[test]
		fn test_int1() {
			let test_instance1 = batch(vec![factory::int1("id", [1, 2])]).unwrap();

			let test_instance2 =
				batch(vec![factory::int1_with_bitvec("id", [3, 4], vec![true, false])]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::int1_with_bitvec("id", [1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_int2() {
			let test_instance1 = batch(vec![factory::int2("id", [1, 2])]).unwrap();

			let test_instance2 =
				batch(vec![factory::int2_with_bitvec("id", [3, 4], vec![true, false])]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::int2_with_bitvec("id", [1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_int4() {
			let test_instance1 = batch(vec![factory::int4("id", [1, 2])]).unwrap();

			let test_instance2 =
				batch(vec![factory::int4_with_bitvec("id", [3, 4], vec![true, false])]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::int4_with_bitvec("id", [1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_int8() {
			let test_instance1 = batch(vec![factory::int8("id", [1, 2])]).unwrap();

			let test_instance2 =
				batch(vec![factory::int8_with_bitvec("id", [3, 4], vec![true, false])]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::int8_with_bitvec("id", [1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_int16() {
			let test_instance1 = batch(vec![factory::int16("id", [1, 2])]).unwrap();

			let test_instance2 =
				batch(vec![factory::int16_with_bitvec("id", [3, 4], vec![true, false])]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::int16_with_bitvec("id", [1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_string() {
			let test_instance1 = batch(vec![factory::utf8_with_bitvec(
				"id",
				vec!["a".to_string(), "b".to_string()],
				vec![true, true],
			)])
			.unwrap();

			let test_instance2 = batch(vec![factory::utf8_with_bitvec(
				"id",
				vec!["c".to_string(), "d".to_string()],
				vec![true, false],
			)])
			.unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::utf8_with_bitvec(
					"id",
					vec!["a".to_string(), "b".to_string(), "c".to_string(), "d".to_string()],
					vec![true, true, true, false]
				)
			);
		}

		#[test]
		fn test_uint1() {
			let test_instance1 = batch(vec![factory::uint1("id", [1, 2])]).unwrap();

			let test_instance2 =
				batch(vec![factory::uint1_with_bitvec("id", [3, 4], vec![true, false])]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::uint1_with_bitvec("id", [1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_uint2() {
			let test_instance1 = batch(vec![factory::uint2("id", [1, 2])]).unwrap();

			let test_instance2 =
				batch(vec![factory::uint2_with_bitvec("id", [3, 4], vec![true, false])]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::uint2_with_bitvec("id", [1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_uint4() {
			let test_instance1 = batch(vec![factory::uint4("id", [1, 2])]).unwrap();

			let test_instance2 =
				batch(vec![factory::uint4_with_bitvec("id", [3, 4], vec![true, false])]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::uint4_with_bitvec("id", [1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_uint8() {
			let test_instance1 = batch(vec![factory::uint8("id", [1, 2])]).unwrap();

			let test_instance2 =
				batch(vec![factory::uint8_with_bitvec("id", [3, 4], vec![true, false])]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::uint8_with_bitvec("id", [1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_uint16() {
			let test_instance1 = batch(vec![factory::uint16("id", [1, 2])]).unwrap();

			let test_instance2 =
				batch(vec![factory::uint16_with_bitvec("id", [3, 4], vec![true, false])]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::uint16_with_bitvec("id", [1, 2, 3, 4], vec![true, true, true, false])
			);
		}

		#[test]
		fn test_uuid4() {
			let uuid1 = Uuid4::from(Uuid::new_v4());
			let uuid2 = Uuid4::from(Uuid::new_v4());
			let uuid3 = Uuid4::from(Uuid::new_v4());
			let uuid4 = Uuid4::from(Uuid::new_v4());

			let test_instance1 = batch(vec![factory::uuid4("id", [uuid1, uuid2])]).unwrap();

			let test_instance2 =
				batch(vec![factory::uuid4_with_bitvec("id", [uuid3, uuid4], vec![true, false])])
					.unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::uuid4_with_bitvec(
					"id",
					[uuid1, uuid2, uuid3, uuid4],
					vec![true, true, true, false]
				)
			);
		}

		#[test]
		fn test_uuid7() {
			let uuid1 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian_time(1, 1)));
			let uuid2 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian_time(1, 2)));
			let uuid3 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian_time(2, 1)));
			let uuid4 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian_time(2, 2)));

			let test_instance1 = batch(vec![factory::uuid7("id", [uuid1, uuid2])]).unwrap();

			let test_instance2 =
				batch(vec![factory::uuid7_with_bitvec("id", [uuid3, uuid4], vec![true, false])])
					.unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::uuid7_with_bitvec(
					"id",
					[uuid1, uuid2, uuid3, uuid4],
					vec![true, true, true, false]
				)
			);
		}

		#[test]
		fn test_with_undefined_lr_promotes_correctly() {
			let test_instance1 =
				batch(vec![factory::int2_with_bitvec("id", [1, 2], vec![true, false])]).unwrap();

			let test_instance2 = batch(vec![factory::none("id", 2)]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::int2_with_bitvec("id", [1, 2, 0, 0], vec![true, false, false, false])
			);
		}

		#[test]
		fn test_with_undefined_l_promotes_correctly() {
			let test_instance1 = batch(vec![factory::none("score", 2)]).unwrap();

			let test_instance2 =
				batch(vec![factory::int2_with_bitvec("score", [10, 20], vec![true, false])]).unwrap();

			let test_instance1 = append(&test_instance1, &test_instance2).unwrap();

			assert_eq!(
				column(&test_instance1, 0),
				factory::int2_with_bitvec("score", [0, 0, 10, 20], vec![false, false, true, false])
			);
		}

		#[test]
		fn test_fails_on_column_count_mismatch() {
			let test_instance1 = batch(vec![factory::int2("id", [1])]).unwrap();

			let test_instance2 =
				batch(vec![factory::int2("id", [2]), factory::utf8("name", vec!["Bob".to_string()])])
					.unwrap();

			let result = append(&test_instance1, &test_instance2);
			assert!(result.is_err());
		}

		#[test]
		fn test_fails_on_column_name_mismatch() {
			let test_instance1 = batch(vec![factory::int2("id", [1])]).unwrap();

			let test_instance2 = batch(vec![factory::int2("wrong", [2])]).unwrap();

			let result = append(&test_instance1, &test_instance2);
			assert!(result.is_err());
		}

		#[test]
		fn test_fails_on_type_mismatch() {
			let test_instance1 = batch(vec![factory::int2("id", [1])]).unwrap();

			let test_instance2 = batch(vec![factory::utf8("id", vec!["A".to_string()])]).unwrap();

			let result = append(&test_instance1, &test_instance2);
			assert!(result.is_err());
		}
	}

	mod row {
		use arrow_array::RecordBatch;
		use arrow_buffer::BooleanBuffer;
		use reifydb_codec::row::shape::{RowFamily, RowShape, RowShapeField};
		use reifydb_value::value::{
			Value,
			blob::Blob,
			constraint::TypeConstraint,
			dictionary::{DictionaryEntryId, DictionaryId},
			identity::IdentityId,
			ordered_f32::OrderedF32,
			ordered_f64::OrderedF64,
			value_type::ValueType,
		};

		use super::{column, view};
		use crate::value::{
			batch::{append_rows, batch, empty_batch},
			column::factory,
		};

		#[test]
		fn test_before_undefined_bool() {
			let test_instance =
				batch(vec![factory::none_typed("test_col", ValueType::Boolean, 2)]).unwrap();

			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Boolean]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Boolean(true)]);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::bool_with_bitvec(
					"test_col",
					[false, false, true],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_float4() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Float4]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Float4(OrderedF32::try_from(1.5).unwrap())]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::float4_with_bitvec(
					"test_col",
					[0.0, 0.0, 1.5],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_float8() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Float8]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Float8(OrderedF64::try_from(2.25).unwrap())]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::float8_with_bitvec(
					"test_col",
					[0.0, 0.0, 2.25],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int1() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int1]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Int1(42)]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::int1_with_bitvec(
					"test_col",
					[0, 0, 42],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int2() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int2]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Int2(-1234)]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::int2_with_bitvec(
					"test_col",
					[0, 0, -1234],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int4() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int4]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Int4(56789)]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::int4_with_bitvec(
					"test_col",
					[0, 0, 56789],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int8() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int8]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Int8(-987654321)]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::int8_with_bitvec(
					"test_col",
					[0, 0, -987654321],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int16() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int16]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Int16(123456789012345678901234567890i128)]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::int16_with_bitvec(
					"test_col",
					[0, 0, 123456789012345678901234567890i128],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_string() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Utf8]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Utf8("reifydb".into())]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::utf8_with_bitvec(
					"test_col",
					["".to_string(), "".to_string(), "reifydb".to_string()],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint1() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uint1]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Uint1(255)]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::uint1_with_bitvec(
					"test_col",
					[0, 0, 255],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint2() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uint2]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Uint2(65535)]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::uint2_with_bitvec(
					"test_col",
					[0, 0, 65535],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint4() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uint4]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Uint4(4294967295)]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::uint4_with_bitvec(
					"test_col",
					[0, 0, 4294967295],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint8() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uint8]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Uint8(18446744073709551615)]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::uint8_with_bitvec(
					"test_col",
					[0, 0, 18446744073709551615],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint16() {
			let test_instance = batch(vec![factory::none("test_col", 2)]).unwrap();
			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Uint16]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::Uint16(340282366920938463463374607431768211455u128)]);
			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::uint16_with_bitvec(
					"test_col",
					[0, 0, 340282366920938463463374607431768211455u128],
					BooleanBuffer::from(vec![false, false, true])
				)
			);
		}

		#[test]
		fn test_mismatched_columns() {
			let test_instance = empty_batch();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int2]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Int2(2)]);

			let err = append_rows(test_instance, &shape, [row.freeze()], vec![]).err().unwrap();
			assert!(err.to_string().contains("mismatched column count: expected 0, got 1"));
		}

		#[test]
		fn test_ok() {
			let test_instance = test_instance_with_columns();

			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int2, ValueType::Boolean]);
			let mut row_one = shape.allocate_pod();
			shape.set_values(&mut row_one, &[Value::Int2(2), Value::Boolean(true)]);
			let mut row_two = shape.allocate_pod();
			shape.set_values(&mut row_two, &[Value::Int2(3), Value::Boolean(false)]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::int2("int2", [1, 2, 3]));
			assert_eq!(column(&test_instance, 1), factory::bool("bool", [true, true, false]));
		}

		#[test]
		fn test_all_defined_bool() {
			let test_instance = batch(vec![factory::bool("test_col", Vec::<bool>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Boolean]);
			let mut row_one = shape.allocate_table();
			shape.set::<bool>(&mut row_one, 0, true);
			let mut row_two = shape.allocate_table();
			shape.set::<bool>(&mut row_two, 0, false);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::bool("test_col", [true, false]));
		}

		#[test]
		fn test_all_defined_float4() {
			let test_instance = batch(vec![factory::float4("test_col", Vec::<f32>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Float4]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Float4(OrderedF32::try_from(1.0).unwrap())]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Float4(OrderedF32::try_from(2.0).unwrap())]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::float4("test_col", [1.0, 2.0]));
		}

		#[test]
		fn test_all_defined_float8() {
			let test_instance = batch(vec![factory::float8("test_col", Vec::<f64>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Float8]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Float8(OrderedF64::try_from(1.0).unwrap())]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Float8(OrderedF64::try_from(2.0).unwrap())]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::float8("test_col", [1.0, 2.0]));
		}

		#[test]
		fn test_all_defined_int1() {
			let test_instance = batch(vec![factory::int1("test_col", Vec::<i8>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int1]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Int1(1)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Int1(2)]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::int1("test_col", [1, 2]));
		}

		#[test]
		fn test_all_defined_int2() {
			let test_instance = batch(vec![factory::int2("test_col", Vec::<i16>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int2]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Int2(100)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Int2(200)]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::int2("test_col", [100, 200]));
		}

		#[test]
		fn test_all_defined_int4() {
			let test_instance = batch(vec![factory::int4("test_col", Vec::<i32>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int4]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Int4(1000)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Int4(2000)]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::int4("test_col", [1000, 2000]));
		}

		#[test]
		fn test_all_defined_int8() {
			let test_instance = batch(vec![factory::int8("test_col", Vec::<i64>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int8]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Int8(10000)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Int8(20000)]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::int8("test_col", [10000, 20000]));
		}

		#[test]
		fn test_all_defined_int16() {
			let test_instance = batch(vec![factory::int16("test_col", Vec::<i128>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int16]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Int16(1000)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Int16(2000)]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::int16("test_col", [1000, 2000]));
		}

		#[test]
		fn test_all_defined_string() {
			let test_instance = batch(vec![factory::utf8("test_col", Vec::<String>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Utf8]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Utf8("a".into())]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Utf8("b".into())]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::utf8("test_col", ["a".to_string(), "b".to_string()])
			);
		}

		#[test]
		fn test_all_defined_uint1() {
			let test_instance = batch(vec![factory::uint1("test_col", Vec::<u8>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint1]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Uint1(1)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Uint1(2)]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::uint1("test_col", [1, 2]));
		}

		#[test]
		fn test_all_defined_uint2() {
			let test_instance = batch(vec![factory::uint2("test_col", Vec::<u16>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint2]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Uint2(100)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Uint2(200)]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::uint2("test_col", [100, 200]));
		}

		#[test]
		fn test_all_defined_uint4() {
			let test_instance = batch(vec![factory::uint4("test_col", Vec::<u32>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint4]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Uint4(1000)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Uint4(2000)]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::uint4("test_col", [1000, 2000]));
		}

		#[test]
		fn test_all_defined_uint8() {
			let test_instance = batch(vec![factory::uint8("test_col", Vec::<u64>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint8]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Uint8(10000)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Uint8(20000)]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::uint8("test_col", [10000, 20000]));
		}

		#[test]
		fn test_all_defined_uint16() {
			let test_instance = batch(vec![factory::uint16("test_col", Vec::<u128>::new())]).unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint16]);
			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::Uint16(1000)]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::Uint16(2000)]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(column(&test_instance, 0), factory::uint16("test_col", [1000, 2000]));
		}

		#[test]
		fn test_row_with_undefined() {
			let test_instance = test_instance_with_columns();

			let shape = RowShape::testing(RowFamily::Pod, &[ValueType::Int2, ValueType::Boolean]);
			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::none(), Value::Boolean(false)]);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::int2_with_bitvec("int2", vec![1, 0], vec![true, false])
			);
			assert_eq!(
				column(&test_instance, 1),
				factory::bool_with_bitvec("bool", [true, false], vec![true, true])
			);
		}

		#[test]
		fn test_row_with_type_mismatch_fails() {
			let test_instance = test_instance_with_columns();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Boolean, ValueType::Boolean]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Boolean(true), Value::Boolean(true)]);

			let result = append_rows(test_instance, &shape, [row.freeze()], vec![]);
			assert!(result.is_err());
			assert!(result.unwrap_err().to_string().contains("type mismatch"));
		}

		#[test]
		fn test_row_wrong_length_fails() {
			let test_instance = test_instance_with_columns();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int2]);
			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::Int2(2)]);

			let result = append_rows(test_instance, &shape, [row.freeze()], vec![]);
			assert!(result.is_err());
			assert!(result.unwrap_err().to_string().contains("mismatched column count"));
		}

		#[test]
		fn test_fallback_bool() {
			let test_instance = batch(vec![
				factory::bool("test_col", Vec::<bool>::new()),
				factory::bool("none", Vec::<bool>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Boolean, ValueType::Boolean]);
			let mut row = shape.allocate_table();
			shape.set::<bool>(&mut row, 0, true);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::bool_with_bitvec("test_col", [true], vec![true])
			);
			assert_eq!(column(&test_instance, 1), factory::bool_with_bitvec("none", [false], vec![false]));
		}

		#[test]
		fn test_fallback_float4() {
			let test_instance = batch(vec![
				factory::float4("test_col", Vec::<f32>::new()),
				factory::float4("none", Vec::<f32>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Float4, ValueType::Float4]);
			let mut row = shape.allocate_table();
			shape.set::<f32>(&mut row, 0, 1.5f32);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::float4_with_bitvec("test_col", [1.5], vec![true])
			);
			assert_eq!(column(&test_instance, 1), factory::float4_with_bitvec("none", [0.0], vec![false]));
		}

		#[test]
		fn test_fallback_float8() {
			let test_instance = batch(vec![
				factory::float8("test_col", Vec::<f64>::new()),
				factory::float8("none", Vec::<f64>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Float8, ValueType::Float8]);
			let mut row = shape.allocate_table();
			shape.set::<f64>(&mut row, 0, 2.5f64);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::float8_with_bitvec("test_col", [2.5], vec![true])
			);
			assert_eq!(column(&test_instance, 1), factory::float8_with_bitvec("none", [0.0], vec![false]));
		}

		#[test]
		fn test_fallback_int1() {
			let test_instance = batch(vec![
				factory::int1("test_col", Vec::<i8>::new()),
				factory::int1("none", Vec::<i8>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int1, ValueType::Int1]);
			let mut row = shape.allocate_table();
			shape.set::<i8>(&mut row, 0, 42i8);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(column(&test_instance, 0), factory::int1_with_bitvec("test_col", [42], vec![true]));
			assert_eq!(column(&test_instance, 1), factory::int1_with_bitvec("none", [0], vec![false]));
		}

		#[test]
		fn test_fallback_int2() {
			let test_instance = batch(vec![
				factory::int2("test_col", Vec::<i16>::new()),
				factory::int2("none", Vec::<i16>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int2, ValueType::Int2]);
			let mut row = shape.allocate_table();
			shape.set::<i16>(&mut row, 0, -1234i16);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::int2_with_bitvec("test_col", [-1234], vec![true])
			);
			assert_eq!(column(&test_instance, 1), factory::int2_with_bitvec("none", [0], vec![false]));
		}

		#[test]
		fn test_fallback_int4() {
			let test_instance = batch(vec![
				factory::int4("test_col", Vec::<i32>::new()),
				factory::int4("none", Vec::<i32>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int4, ValueType::Int4]);
			let mut row = shape.allocate_table();
			shape.set::<i32>(&mut row, 0, 56789i32);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::int4_with_bitvec("test_col", [56789], vec![true])
			);
			assert_eq!(column(&test_instance, 1), factory::int4_with_bitvec("none", [0], vec![false]));
		}

		#[test]
		fn test_fallback_int8() {
			let test_instance = batch(vec![
				factory::int8("test_col", Vec::<i64>::new()),
				factory::int8("none", Vec::<i64>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int8, ValueType::Int8]);
			let mut row = shape.allocate_table();
			shape.set::<i64>(&mut row, 0, -987654321i64);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::int8_with_bitvec("test_col", [-987654321], vec![true])
			);
			assert_eq!(column(&test_instance, 1), factory::int8_with_bitvec("none", [0], vec![false]));
		}

		#[test]
		fn test_fallback_int16() {
			let test_instance = batch(vec![
				factory::int16("test_col", Vec::<i128>::new()),
				factory::int16("none", Vec::<i128>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Int16, ValueType::Int16]);
			let mut row = shape.allocate_table();
			shape.set::<i128>(&mut row, 0, 123456789012345678901234567890i128);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::int16_with_bitvec(
					"test_col",
					[123456789012345678901234567890i128],
					vec![true]
				)
			);
			assert_eq!(column(&test_instance, 1), factory::int16_with_bitvec("none", [0], vec![false]));
		}

		#[test]
		fn test_fallback_string() {
			let test_instance = batch(vec![
				factory::utf8("test_col", Vec::<String>::new()),
				factory::utf8("none", Vec::<String>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Utf8, ValueType::Utf8]);
			let mut row = shape.allocate_table();
			shape.set_utf8(&mut row, 0, "reifydb");
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::utf8_with_bitvec("test_col", ["reifydb".to_string()], vec![true])
			);
			assert_eq!(
				column(&test_instance, 1),
				factory::utf8_with_bitvec("none", ["".to_string()], vec![false])
			);
		}

		#[test]
		fn test_fallback_uint1() {
			let test_instance = batch(vec![
				factory::uint1("test_col", Vec::<u8>::new()),
				factory::uint1("none", Vec::<u8>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint1, ValueType::Uint1]);
			let mut row = shape.allocate_table();
			shape.set::<u8>(&mut row, 0, 255u8);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::uint1_with_bitvec("test_col", [255], vec![true])
			);
			assert_eq!(column(&test_instance, 1), factory::uint1_with_bitvec("none", [0], vec![false]));
		}

		#[test]
		fn test_fallback_uint2() {
			let test_instance = batch(vec![
				factory::uint2("test_col", Vec::<u16>::new()),
				factory::uint2("none", Vec::<u16>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint2, ValueType::Uint2]);
			let mut row = shape.allocate_table();
			shape.set::<u16>(&mut row, 0, 65535u16);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::uint2_with_bitvec("test_col", [65535], vec![true])
			);
			assert_eq!(column(&test_instance, 1), factory::uint2_with_bitvec("none", [0], vec![false]));
		}

		#[test]
		fn test_fallback_uint4() {
			let test_instance = batch(vec![
				factory::uint4("test_col", Vec::<u32>::new()),
				factory::uint4("none", Vec::<u32>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint4, ValueType::Uint4]);
			let mut row = shape.allocate_table();
			shape.set::<u32>(&mut row, 0, 4294967295u32);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::uint4_with_bitvec("test_col", [4294967295], vec![true])
			);
			assert_eq!(column(&test_instance, 1), factory::uint4_with_bitvec("none", [0], vec![false]));
		}

		#[test]
		fn test_fallback_uint8() {
			let test_instance = batch(vec![
				factory::uint8("test_col", Vec::<u64>::new()),
				factory::uint8("none", Vec::<u64>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint8, ValueType::Uint8]);
			let mut row = shape.allocate_table();
			shape.set::<u64>(&mut row, 0, 18446744073709551615u64);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::uint8_with_bitvec("test_col", [18446744073709551615], vec![true])
			);
			assert_eq!(column(&test_instance, 1), factory::uint8_with_bitvec("none", [0], vec![false]));
		}

		#[test]
		fn test_fallback_uint16() {
			let test_instance = batch(vec![
				factory::uint16("test_col", Vec::<u128>::new()),
				factory::uint16("none", Vec::<u128>::new()),
			])
			.unwrap();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Uint16, ValueType::Uint16]);
			let mut row = shape.allocate_table();
			shape.set::<u128>(&mut row, 0, 340282366920938463463374607431768211455u128);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(
				column(&test_instance, 0),
				factory::uint16_with_bitvec(
					"test_col",
					[340282366920938463463374607431768211455u128],
					vec![true]
				)
			);
			assert_eq!(column(&test_instance, 1), factory::uint16_with_bitvec("none", [0], vec![false]));
		}

		#[test]
		fn test_all_defined_dictionary_id() {
			let constraint = TypeConstraint::dictionary(DictionaryId::from(1u64), ValueType::Uint4);
			let shape = RowShape::new(RowFamily::Table, vec![RowShapeField::new("status", constraint)]);

			let test_instance =
				batch(vec![factory::dictionary_id("status", Vec::<DictionaryEntryId>::new())]).unwrap();

			let mut row_one = shape.allocate_table();
			shape.set_values(&mut row_one, &[Value::DictionaryId(DictionaryEntryId::U4(10))]);
			let mut row_two = shape.allocate_table();
			shape.set_values(&mut row_two, &[Value::DictionaryId(DictionaryEntryId::U4(20))]);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(
				view(&test_instance, "status").get_value(0),
				Value::DictionaryId(DictionaryEntryId::U4(10))
			);
			assert_eq!(
				view(&test_instance, "status").get_value(1),
				Value::DictionaryId(DictionaryEntryId::U4(20))
			);
		}

		#[test]
		fn test_fallback_dictionary_id() {
			let dict_constraint = TypeConstraint::dictionary(DictionaryId::from(1u64), ValueType::Uint4);
			let shape = RowShape::new(
				RowFamily::Table,
				vec![
					RowShapeField::new("dict_col", dict_constraint),
					RowShapeField::unconstrained("bool_col", ValueType::Boolean),
				],
			);

			let test_instance = batch(vec![
				factory::dictionary_id("dict_col", Vec::<DictionaryEntryId>::new()),
				factory::bool("bool_col", Vec::<bool>::new()),
			])
			.unwrap();

			let mut row = shape.allocate_table();
			shape.set_values(&mut row, &[Value::none(), Value::Boolean(true)]);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert!(!view(&test_instance, "dict_col").is_defined(0));
			assert_eq!(view(&test_instance, "bool_col").get_value(0), Value::Boolean(true));
		}

		#[test]
		fn test_before_undefined_dictionary_id() {
			let constraint = TypeConstraint::dictionary(DictionaryId::from(2u64), ValueType::Uint4);
			let shape = RowShape::new(RowFamily::Pod, vec![RowShapeField::new("tag", constraint)]);

			let test_instance = batch(vec![factory::none("tag", 2)]).unwrap();

			let mut row = shape.allocate_pod();
			shape.set_values(&mut row, &[Value::DictionaryId(DictionaryEntryId::U4(5))]);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			// The first two rows carry over from the undefined column the append promoted.
			assert!(!view(&test_instance, "tag").is_defined(0));
			assert!(!view(&test_instance, "tag").is_defined(1));
			assert!(view(&test_instance, "tag").is_defined(2));
			assert_eq!(
				view(&test_instance, "tag").get_value(2),
				Value::DictionaryId(DictionaryEntryId::U4(5))
			);
		}

		#[test]
		fn test_all_defined_identity_id() {
			let id1 = IdentityId::anonymous();
			let id2 = IdentityId::root();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::IdentityId]);
			let test_instance =
				batch(vec![factory::identity_id("id_col", Vec::<IdentityId>::new())]).unwrap();

			let mut row_one = shape.allocate_table();
			shape.set::<IdentityId>(&mut row_one, 0, id1);
			let mut row_two = shape.allocate_table();
			shape.set::<IdentityId>(&mut row_two, 0, id2);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(view(&test_instance, "id_col").get_value(0), Value::IdentityId(id1));
			assert_eq!(view(&test_instance, "id_col").get_value(1), Value::IdentityId(id2));
		}

		#[test]
		fn test_fallback_identity_id() {
			let id = IdentityId::anonymous();

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::IdentityId, ValueType::Boolean]);
			let test_instance = batch(vec![
				factory::identity_id("id_col", Vec::<IdentityId>::new()),
				factory::bool("bool_col", Vec::<bool>::new()),
			])
			.unwrap();

			let mut row = shape.allocate_table();
			shape.set::<IdentityId>(&mut row, 0, id);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(view(&test_instance, "id_col").get_value(0), Value::IdentityId(id));
			assert!(view(&test_instance, "id_col").is_defined(0));
			assert!(!view(&test_instance, "bool_col").is_defined(0));
		}

		#[test]
		fn test_all_defined_blob() {
			let blob1 = Blob::new(vec![1, 2, 3]);
			let blob2 = Blob::new(vec![4, 5]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Blob]);
			let test_instance = batch(vec![factory::blob("blob_col", Vec::<Blob>::new())]).unwrap();

			let mut row_one = shape.allocate_table();
			shape.set_blob(&mut row_one, 0, &blob1);
			let mut row_two = shape.allocate_table();
			shape.set_blob(&mut row_two, 0, &blob2);

			let test_instance =
				append_rows(test_instance, &shape, [row_one.freeze(), row_two.freeze()], vec![])
					.unwrap();

			assert_eq!(view(&test_instance, "blob_col").get_value(0), Value::Blob(blob1));
			assert_eq!(view(&test_instance, "blob_col").get_value(1), Value::Blob(blob2));
		}

		#[test]
		fn test_fallback_blob() {
			let blob = Blob::new(vec![10, 20, 30]);

			let shape = RowShape::testing(RowFamily::Table, &[ValueType::Blob, ValueType::Boolean]);
			let test_instance = batch(vec![
				factory::blob("blob_col", Vec::<Blob>::new()),
				factory::bool("bool_col", Vec::<bool>::new()),
			])
			.unwrap();

			let mut row = shape.allocate_table();
			shape.set_blob(&mut row, 0, &blob);
			shape.set_none(&mut row, 1);

			let test_instance = append_rows(test_instance, &shape, [row.freeze()], vec![]).unwrap();

			assert_eq!(view(&test_instance, "blob_col").get_value(0), Value::Blob(blob));
			assert!(view(&test_instance, "blob_col").is_defined(0));
			assert!(!view(&test_instance, "bool_col").is_defined(0));
		}

		fn test_instance_with_columns() -> RecordBatch {
			batch(vec![factory::int2("int2", vec![1]), factory::bool("bool", vec![true])]).unwrap()
		}
	}
}
