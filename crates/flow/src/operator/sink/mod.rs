// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod partition;
pub mod view;

use std::sync::LazyLock;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::FieldRef;
use reifydb_codec::row::{
	bytes::{EncodedBytes, SourceRowBuilder},
	shape::{RowFamily, RowShape},
};
use reifydb_core::{
	interface::{
		catalog::{
			column::Column as CatalogColumn,
			property::{ColumnPropertyKind, ColumnSaturationStrategy},
		},
		evaluate::TargetColumn,
	},
	value::{
		batch::empty_batch,
		column::{cast::cast_column_data, factory::none_typed, write::check_digest_write},
	},
};
use reifydb_evaluate::{expression::context::EvalContext, stack::SymbolTable};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	params::Params,
	value::{
		Value,
		column_view::ColumnView,
		identity::IdentityId,
		row_number::RowNumber,
		system_columns::{column_view, created_at, time, updated_at, user_columns},
	},
};

use crate::{error::FlowSinkError, operator::with_system_columns_of};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);
static EMPTY_ROUTINES: LazyLock<Routines> = LazyLock::new(Routines::empty);

pub fn coerce_columns(
	columns: &RecordBatch,
	target_columns: &[CatalogColumn],
	runtime_context: &RuntimeContext,
) -> Result<RecordBatch> {
	let row_count = columns.num_rows();
	if row_count == 0 {
		return Ok(empty_batch());
	}

	if target_columns.is_empty() {
		return Ok(columns.clone());
	}

	let user: Vec<(&FieldRef, &ArrayRef)> = user_columns(columns).collect();
	if user.len() == target_columns.len() {
		let mut unchanged = true;
		for ((field, array), target_col) in user.iter().zip(target_columns) {
			if field.name() != target_col.name.as_str()
				|| ColumnView::try_from((*array, field.as_ref()))?.get_type()
					!= target_col.constraint.get_type()
			{
				unchanged = false;
				break;
			}
		}
		if unchanged {
			return Ok(columns.clone());
		}
	}

	let mut result_columns = Vec::with_capacity(target_columns.len());

	// FIXME how to handle failing views ?!
	let session = EvalContext {
		params: &EMPTY_PARAMS,
		symbols: &EMPTY_SYMBOL_TABLE,
		routines: &EMPTY_ROUTINES,
		runtime_context,
		identity: IdentityId::system(),
		is_aggregate_context: false,
		batch: empty_batch(),
		row_count: 1,
		target: None,
		take: None,
	};
	let mut ctx = session.with_eval(columns.clone(), row_count);

	for target_col in target_columns {
		let target_type = target_col.constraint.get_type();

		ctx.target = Some(TargetColumn::Partial {
			source_name: None,
			column_name: Some(target_col.name.clone()),
			column_type: target_type.clone(),
			properties: vec![ColumnPropertyKind::Saturation(ColumnSaturationStrategy::None)],
		});

		if let Some(source_col) = column_view(columns, &target_col.name)? {
			check_digest_write(&source_col, &target_type, Fragment::internal(&target_col.name))?;
			let casted = cast_column_data(
				&ctx,
				&source_col,
				target_type.clone(),
				Fragment::internal(&target_col.name),
			)?;
			result_columns.push(casted);
		} else {
			result_columns.push(none_typed(&target_col.name, target_type, row_count))
		}
	}

	with_system_columns_of(result_columns, columns)
}

pub fn shape_field_columns(columns: &RecordBatch, shape: &RowShape) -> Vec<usize> {
	shape.field_names()
		.map(|field_name| {
			columns.schema_ref()
				.fields()
				.iter()
				.position(|field| field.name() == field_name)
				.unwrap_or_else(|| panic!("Column '{}' not found in the batch", field_name))
		})
		.collect()
}

pub fn encode_row_at_index(
	columns: &RecordBatch,
	row_idx: usize,
	shape: &RowShape,
	row_number: RowNumber,
	field_columns: &[usize],
) -> Result<(RowNumber, EncodedBytes)> {
	match shape.family() {
		RowFamily::Table => {
			stamp_source_row(shape.allocate_table(), columns, row_idx, shape, row_number, field_columns)
		}
		RowFamily::Series => {
			stamp_source_row(shape.allocate_series(), columns, row_idx, shape, row_number, field_columns)
		}
		RowFamily::RingBuffer => stamp_source_row(
			shape.allocate_ringbuffer(),
			columns,
			row_idx,
			shape,
			row_number,
			field_columns,
		),
		other => Err(Error::from(FlowSinkError::NotASourceFamily {
			family: format!("{:?}", other),
		})),
	}
}

pub(crate) fn value_at(columns: &RecordBatch, index: usize, row_idx: usize) -> Result<Value> {
	Ok(ColumnView::try_from((columns.column(index), columns.schema_ref().field(index)))?.get_value(row_idx))
}

fn stamp_source_row<B: SourceRowBuilder>(
	mut encoded: B,
	columns: &RecordBatch,
	row_idx: usize,
	shape: &RowShape,
	row_number: RowNumber,
	field_columns: &[usize],
) -> Result<(RowNumber, EncodedBytes)> {
	let values: Vec<Value> =
		field_columns.iter().map(|&col_idx| value_at(columns, col_idx, row_idx)).collect::<Result<Vec<_>>>()?;

	shape.set_values(&mut encoded, &values);

	let created_at = created_at(columns)?.get(row_idx).copied().ok_or_else(|| {
		Error::from(FlowSinkError::MissingSystemColumn {
			column: "created_at",
			row_idx,
		})
	})?;
	let updated_at = updated_at(columns)?.get(row_idx).copied().ok_or_else(|| {
		Error::from(FlowSinkError::MissingSystemColumn {
			column: "updated_at",
			row_idx,
		})
	})?;
	encoded.set_timestamps(created_at, updated_at);
	if let Some(time) = time(columns)?.get(row_idx).copied() {
		encoded.set_time(time);
	}

	Ok((row_number, encoded.freeze_bytes()))
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use arrow_array::UInt64Array;
	use reifydb_codec::row::{shape::RowShapeField, table::EncodedTableRow};
	use reifydb_core::value::{batch::batch, column::builder::ColumnBuilder};
	use reifydb_value::value::{
		container::temporal_array::datetime_array,
		datetime::DateTime,
		system_columns::{SystemColumn, keep_system_columns, with_system_column},
		value_type::ValueType,
	};

	use super::*;

	fn single_field_shape() -> RowShape {
		RowShape::new(RowFamily::Table, vec![RowShapeField::unconstrained("n".to_string(), ValueType::Int4)])
	}

	fn columns_with_stamps(created_at: i64, updated_at: i64, time: i64) -> RecordBatch {
		let mut builder = ColumnBuilder::with_capacity(ValueType::Int4, 1);
		builder.push_value(Value::Int4(7));
		let columns = batch(vec![builder.finish("n")]).unwrap();
		let stamps = [
			(SystemColumn::RowNumbers, Arc::new(UInt64Array::from(vec![1u64])) as ArrayRef),
			(SystemColumn::CreatedAt, Arc::new(datetime_array([DateTime::from_nanos(created_at)]))),
			(SystemColumn::UpdatedAt, Arc::new(datetime_array([DateTime::from_nanos(updated_at)]))),
			(SystemColumn::Time, Arc::new(datetime_array([DateTime::from_nanos(time)]))),
		];
		stamps.into_iter()
			.fold(columns, |columns, (column, array)| with_system_column(columns, column, array).unwrap())
	}

	#[test]
	fn a_sink_row_carries_the_time_of_the_row_it_was_built_from() {
		// The only place a sink writes the row header, so dropping #time here lands every
		// materialised row at nanos 0 and a downstream flow inherits 1970. The three stamps are
		// seeded differently because copying the wrong one is as wrong as copying none.
		let shape = single_field_shape();
		let columns = columns_with_stamps(100, 200, 300);
		let field_columns = shape_field_columns(&columns, &shape);

		let (_, encoded) = encode_row_at_index(&columns, 0, &shape, RowNumber(1), &field_columns).unwrap();
		let encoded = EncodedTableRow::view(&encoded);

		assert_eq!(
			encoded.time(),
			Some(DateTime::from_nanos(300)),
			"#time must come from the source row's own sidecar"
		);
		assert_eq!(encoded.created_at(), DateTime::from_nanos(100));
		assert_eq!(encoded.updated_at(), DateTime::from_nanos(200));
	}

	#[test]
	fn a_sink_row_without_a_time_sidecar_is_written_without_one() {
		// A source with no time domain produces rows with no #time, so a sink must materialise them
		// rather than reject them. Substituting a stamp here would give a time-less table's rows a
		// clock they never had, and rejecting them would make the view permanently empty.
		let shape = single_field_shape();
		let columns = keep_system_columns(
			&columns_with_stamps(100, 200, 300),
			&[SystemColumn::RowNumbers, SystemColumn::CreatedAt, SystemColumn::UpdatedAt],
		)
		.unwrap();
		let field_columns = shape_field_columns(&columns, &shape);

		let (_, encoded) = encode_row_at_index(&columns, 0, &shape, RowNumber(1), &field_columns).unwrap();
		let encoded = EncodedTableRow::view(&encoded);

		assert_eq!(encoded.time(), None, "the sink row must carry no #time");
		assert_eq!(encoded.created_at(), DateTime::from_nanos(100), "the wall stamps are still required");
		assert_eq!(encoded.updated_at(), DateTime::from_nanos(200));
	}
}
