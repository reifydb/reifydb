// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod partition;
pub mod ringbuffer_view;
pub mod series_view;
pub mod view;

use std::sync::LazyLock;

use postcard::from_bytes;
use reifydb_codec::row::{operator::EncodedOperatorRow, shape::{RowFamily, RowShape, RowShapeField}};
use reifydb_core::{
	interface::{
		catalog::{
			column::Column as CatalogColumn,
			dictionary::Dictionary,
			property::{ColumnPropertyKind, ColumnSaturationStrategy},
		},
		evaluate::TargetColumn,
	},
	value::column::{ColumnWithName, buffer::ColumnBuffer, cast::cast_column_data, columns::Columns},
};
use reifydb_evaluate::{expression::context::EvalContext, stack::SymbolTable};
use reifydb_routine_abi::registry::Routines;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_value::value::system_columns::SystemColumns;
use reifydb_value::{
	Result,
	error::Error,
	fragment::Fragment,
	params::Params,
	util::cowvec::CowVec,
	value::{Value, dictionary::DictionaryEntryId, identity::IdentityId, row_number::RowNumber},
};

use crate::flow::{error::FlowSinkError, transaction::FlowTransaction};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);
static EMPTY_ROUTINES: LazyLock<Routines> = LazyLock::new(Routines::empty);
static DEFAULT_RUNTIME_CONTEXT: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::with_clock(Clock::Real));

pub(crate) fn coerce_columns(columns: &Columns, target_columns: &[CatalogColumn]) -> Result<Columns> {
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
		runtime_context: &DEFAULT_RUNTIME_CONTEXT,
		identity: IdentityId::root(),
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

	Ok(Columns::with_system(
		result_columns,
		SystemColumns::new(
			columns.row_numbers().to_vec(),
			columns.partitions().to_vec(),
			columns.created_at().to_vec(),
			columns.updated_at().to_vec(),
			columns.time().to_vec(),
		),
	))
}

pub(crate) fn shape_field_columns(columns: &Columns, shape: &RowShape) -> Vec<usize> {
	shape.field_names()
		.map(|field_name| {
			columns.iter()
				.position(|col| col.name().as_ref() == field_name)
				.unwrap_or_else(|| panic!("Column '{}' not found in Columns", field_name))
		})
		.collect()
}

pub(crate) fn encode_row_at_index(
	columns: &Columns,
	row_idx: usize,
	shape: &RowShape,
	row_number: RowNumber,
	field_columns: &[usize],
) -> Result<(RowNumber, EncodedOperatorRow)> {
	let values: Vec<Value> =
		field_columns.iter().map(|&col_idx| columns.data_at(col_idx).get_value(row_idx)).collect();

	let mut encoded = shape.allocate_operator();
	shape.set_values(&mut encoded, &values);

	let created_at_nanos = columns
		.created_at()
		.get(row_idx)
		.ok_or_else(|| {
			Error::from(FlowSinkError::MissingSystemColumn {
				column: "created_at",
				row_idx,
			})
		})?;
	let updated_at_nanos = columns
		.updated_at()
		.get(row_idx)
		.ok_or_else(|| {
			Error::from(FlowSinkError::MissingSystemColumn {
				column: "updated_at",
				row_idx,
			})
		})?;
	encoded.set_timestamps(*created_at_nanos, *updated_at_nanos);

	Ok((row_number, encoded.freeze()))
}

pub(crate) fn decode_dictionary_columns(columns: &mut Columns, txn: &mut FlowTransaction) -> Result<()> {
	let dict_columns: Vec<(usize, Dictionary)> = {
		let catalog = txn.catalog();
		columns.iter()
			.enumerate()
			.filter_map(|(pos, col)| {
				if let ColumnBuffer::DictionaryId(container) = col.data() {
					let dict_id = container.dictionary_id()?;
					let dictionary = catalog.cache().find_dictionary(dict_id)?;
					Some((pos, dictionary))
				} else {
					None
				}
			})
			.collect()
	};

	let registry = txn.dictionary_allocators();
	for (col_pos, dictionary) in &dict_columns {
		let col = &columns[*col_pos];
		let row_count = col.len();
		let mut new_data = ColumnBuffer::with_capacity(dictionary.value_type.clone(), row_count);

		for row_idx in 0..row_count {
			let id_value = col.get_value(row_idx);
			let value = match DictionaryEntryId::from_value(&id_value) {
				Some(entry_id) => match registry.get(dictionary, entry_id.to_u128())? {
					Some(bytes) => from_bytes(&bytes).unwrap_or(Value::none()),
					None => Value::none(),
				},
				None => Value::none(),
			};
			new_data.push_value(value);
		}

		columns.columns[*col_pos] = new_data;
	}

	Ok(())
}
