// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod partition;
pub mod view;

use std::sync::LazyLock;

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
	value::column::{ColumnWithName, cast::cast_column_data, columns::Columns},
};
use reifydb_evaluate::{expression::context::EvalContext, stack::SymbolTable};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::RuntimeContext;
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	params::Params,
	value::{Value, identity::IdentityId, row_number::RowNumber},
};

use crate::error::FlowSinkError;

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);
static EMPTY_ROUTINES: LazyLock<Routines> = LazyLock::new(Routines::empty);

pub fn coerce_columns(
	columns: &Columns,
	target_columns: &[CatalogColumn],
	runtime_context: &RuntimeContext,
) -> Result<Columns> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(Columns::empty());
	}

	if target_columns.is_empty() {
		return Ok(columns.clone());
	}

	if columns.len() == target_columns.len()
		&& target_columns.iter().enumerate().all(|(i, target_col)| {
			columns.name_at(i).text() == target_col.name.as_str()
				&& columns.data_at(i).get_type() == target_col.constraint.get_type()
		}) {
		return Ok(columns.clone());
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
		columns: Columns::empty(),
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

		if let Some(source_col) = columns.column(&target_col.name) {
			source_col.data().check_digest_write(&target_type, Fragment::internal(&target_col.name))?;
			let casted = cast_column_data(
				&ctx,
				source_col.data(),
				target_type.clone(),
				Fragment::internal(&target_col.name),
			)?;
			result_columns.push(ColumnWithName::new(Fragment::internal(&target_col.name), casted));
		} else {
			result_columns.push(ColumnWithName::undefined_typed(
				Fragment::internal(&target_col.name),
				target_type,
				row_count,
			))
		}
	}

	let mut names_vec = Vec::with_capacity(result_columns.len());
	let mut buffers_vec = Vec::with_capacity(result_columns.len());
	for c in result_columns {
		names_vec.push(c.name);
		buffers_vec.push(c.data);
	}
	Ok(Columns {
		system: columns.system.clone(),
		columns: buffers_vec,
		names: names_vec,
	})
}

pub fn shape_field_columns(columns: &Columns, shape: &RowShape) -> Vec<usize> {
	shape.field_names()
		.map(|field_name| {
			columns.iter()
				.position(|col| col.name().as_ref() == field_name)
				.unwrap_or_else(|| panic!("Column '{}' not found in Columns", field_name))
		})
		.collect()
}

pub fn encode_row_at_index(
	columns: &Columns,
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

fn stamp_source_row<B: SourceRowBuilder>(
	mut encoded: B,
	columns: &Columns,
	row_idx: usize,
	shape: &RowShape,
	row_number: RowNumber,
	field_columns: &[usize],
) -> Result<(RowNumber, EncodedBytes)> {
	let values: Vec<Value> =
		field_columns.iter().map(|&col_idx| columns.data_at(col_idx).get_value(row_idx)).collect();

	shape.set_values(&mut encoded, &values);

	let created_at = columns.created_at().get(row_idx).copied().ok_or_else(|| {
		Error::from(FlowSinkError::MissingSystemColumn {
			column: "created_at",
			row_idx,
		})
	})?;
	let updated_at = columns.updated_at().get(row_idx).copied().ok_or_else(|| {
		Error::from(FlowSinkError::MissingSystemColumn {
			column: "updated_at",
			row_idx,
		})
	})?;
	encoded.set_timestamps(created_at, updated_at);
	if let Some(time) = columns.time().get(row_idx).copied() {
		encoded.set_time(time);
	}

	Ok((row_number, encoded.freeze_bytes()))
}

#[cfg(test)]
mod tests {
	use reifydb_codec::row::{shape::RowShapeField, table::EncodedTableRow};
	use reifydb_core::value::column::builder::ColumnBuilder;
	use reifydb_value::value::{datetime::DateTime, system_columns::SystemColumns, value_type::ValueType};

	use super::*;

	fn single_field_shape() -> RowShape {
		RowShape::new(RowFamily::Table, vec![RowShapeField::unconstrained("n".to_string(), ValueType::Int4)])
	}

	fn columns_with_stamps(created_at: i64, updated_at: i64, time: i64) -> Columns {
		let mut builder = ColumnBuilder::with_capacity(ValueType::Int4, 1);
		builder.push_value(Value::Int4(7));
		let buffer = builder.finish();
		Columns::with_system(
			vec![ColumnWithName::new(Fragment::internal("n"), buffer)],
			SystemColumns::new(
				vec![RowNumber(1)],
				Vec::new(),
				vec![DateTime::from_nanos(created_at)],
				vec![DateTime::from_nanos(updated_at)],
				vec![DateTime::from_nanos(time)],
				Vec::new(),
			),
		)
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
		let mut columns = columns_with_stamps(100, 200, 300);
		columns.system.set_time(Vec::new());
		let field_columns = shape_field_columns(&columns, &shape);

		let (_, encoded) = encode_row_at_index(&columns, 0, &shape, RowNumber(1), &field_columns).unwrap();
		let encoded = EncodedTableRow::view(&encoded);

		assert_eq!(encoded.time(), None, "the sink row must carry no #time");
		assert_eq!(encoded.created_at(), DateTime::from_nanos(100), "the wall stamps are still required");
		assert_eq!(encoded.updated_at(), DateTime::from_nanos(200));
	}
}
