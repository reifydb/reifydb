// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod ringbuffer_view;
pub mod series_view;
pub mod view;

use std::sync::LazyLock;

use postcard::from_bytes;
use reifydb_core::{
	encoded::{row::EncodedRow, shape::RowShape},
	interface::{
		catalog::{
			column::Column as CatalogColumn,
			dictionary::Dictionary,
			property::{ColumnPropertyKind, ColumnSaturationStrategy},
		},
		evaluate::TargetColumn,
	},
	internal_error,
	key::{EncodableKey, dictionary::DictionaryEntryIndexKey},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_engine::{
	expression::{cast::cast_column_data, context::EvalContext},
	vm::stack::SymbolTable,
};
use reifydb_routine::function::registry::Functions;
use reifydb_runtime::context::{RuntimeContext, clock::Clock};
use reifydb_type::{
	Result,
	fragment::Fragment,
	params::Params,
	util::cowvec::CowVec,
	value::{Value, dictionary::DictionaryEntryId, identity::IdentityId, row_number::RowNumber},
};

use crate::transaction::FlowTransaction;
// All types are accessed directly from their submodules:
// - crate::operator::sink::view::SinkTableViewOperator

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);
static EMPTY_FUNCTIONS: LazyLock<Functions> = LazyLock::new(Functions::empty);
static DEFAULT_RUNTIME_CONTEXT: LazyLock<RuntimeContext> = LazyLock::new(|| RuntimeContext::with_clock(Clock::Real));

/// Coerce columns to match target shape types
pub(crate) fn coerce_columns(columns: &Columns, target_columns: &[CatalogColumn]) -> Result<Columns> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(Columns::empty());
	}

	// If target columns are empty, use input columns as-is
	if target_columns.is_empty() {
		return Ok(columns.clone());
	}

	let mut result_columns = Vec::with_capacity(target_columns.len());

	for target_col in target_columns {
		let target_type = target_col.constraint.get_type();

		// Create context with Undefined saturation policy for this column
		// This ensures overflow during cast produces undefined instead of errors
		// FIXME how to handle failing views ?!
		let session = EvalContext {
			params: &EMPTY_PARAMS,
			symbols: &EMPTY_SYMBOL_TABLE,
			functions: &EMPTY_FUNCTIONS,
			runtime_context: &DEFAULT_RUNTIME_CONTEXT,
			arena: None,
			identity: IdentityId::root(),
			is_aggregate_context: false,
			columns: Columns::empty(),
			row_count: 1,
			target: None,
			take: None,
		};
		let mut ctx = session.with_eval(columns.clone(), row_count);
		ctx.target = Some(TargetColumn::Partial {
			source_name: None,
			column_name: Some(target_col.name.clone()),
			column_type: target_type.clone(),
			properties: vec![ColumnPropertyKind::Saturation(ColumnSaturationStrategy::None)],
		});

		if let Some(source_col) = columns.column(&target_col.name) {
			// Cast to target type
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

	// Preserve system columns
	Ok(Columns {
		row_numbers: columns.row_numbers.clone(),
		created_at: columns.created_at.clone(),
		updated_at: columns.updated_at.clone(),
		columns: CowVec::new(result_columns),
	})
}

/// Encode values at a specific row index with explicit row number
pub(crate) fn encode_row_at_index(
	columns: &Columns,
	row_idx: usize,
	shape: &RowShape,
	row_number: RowNumber,
) -> Result<(RowNumber, EncodedRow)> {
	// Use row_number parameter instead of columns.row_numbers[row_idx]

	// Collect values in SHAPE FIELD ORDER by matching column names
	// This ensures values are in the same order as shape expects
	let values: Vec<Value> = shape
		.field_names()
		.map(|field_name| {
			// Find column with matching name
			let col = columns
				.iter()
				.find(|col| col.name.as_ref() == field_name)
				.unwrap_or_else(|| panic!("Column '{}' not found in Columns", field_name));

			col.data().get_value(row_idx)
		})
		.collect();

	// Encode directly
	let mut encoded = shape.allocate();
	shape.set_values(&mut encoded, &values);

	let created_at_nanos = columns
		.created_at
		.get(row_idx)
		.ok_or_else(|| internal_error!("Row at index {} is missing created_at timestamp", row_idx))?
		.to_nanos();
	let updated_at_nanos = columns
		.updated_at
		.get(row_idx)
		.ok_or_else(|| internal_error!("Row at index {} is missing updated_at timestamp", row_idx))?
		.to_nanos();
	encoded.set_timestamps(created_at_nanos, updated_at_nanos);

	Ok((row_number, encoded))
}

/// Decode dictionary columns in-place using FlowTransaction for lookups.
///
/// For columns that store `DictionaryId` values, reads the embedded `dictionary_id`
/// from the container metadata, looks up the `Dictionary` in the catalog,
/// then resolves each dictionary entry ID to its actual value.
pub(crate) fn decode_dictionary_columns(columns: &mut Columns, txn: &mut FlowTransaction) -> Result<()> {
	// Collect (col_pos, Dictionary) for every DictionaryId column that carries a dictionary_id
	let dict_columns: Vec<(usize, Dictionary)> = {
		let catalog = txn.catalog();
		columns.iter()
			.enumerate()
			.filter_map(|(pos, col)| {
				if let ColumnBuffer::DictionaryId(container) = col.data() {
					let dict_id = container.dictionary_id()?;
					let dictionary = catalog.materialized.find_dictionary(dict_id)?;
					Some((pos, dictionary))
				} else {
					None
				}
			})
			.collect()
	};

	for (col_pos, dictionary) in &dict_columns {
		let col = &columns[*col_pos];
		let row_count = col.data().len();
		let mut new_data = ColumnBuffer::with_capacity(dictionary.value_type.clone(), row_count);

		for row_idx in 0..row_count {
			let id_value = col.data().get_value(row_idx);
			if let Some(entry_id) = DictionaryEntryId::from_value(&id_value) {
				let index_key =
					DictionaryEntryIndexKey::new(dictionary.id, entry_id.to_u128() as u64).encode();
				match txn.get(&index_key)? {
					Some(encoded) => {
						let value: Value = from_bytes(&encoded).unwrap_or(Value::none());
						new_data.push_value(value);
					}
					None => {
						new_data.push_value(Value::none());
					}
				}
			} else {
				new_data.push_value(Value::none());
			}
		}

		columns.columns.make_mut()[*col_pos] = ColumnWithName::new(columns[*col_pos].name().clone(), new_data);
	}

	Ok(())
}
