// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod subscription;
pub mod view;

use std::sync::LazyLock;

use reifydb_core::{
	encoded::{encoded::EncodedValues, schema::Schema},
	interface::{
		catalog::{
			column::ColumnDef,
			dictionary::DictionaryDef,
			property::{ColumnPropertyKind, ColumnSaturationPolicy},
			subscription::SubscriptionColumnDef,
		},
		evaluate::TargetColumn,
	},
	key::{EncodableKey, dictionary::DictionaryEntryIndexKey},
	value::column::{Column, columns::Columns, data::ColumnData},
};
use reifydb_engine::{
	expression::{cast::cast_column_data, context::EvalContext},
	vm::stack::SymbolTable,
};
use reifydb_function::registry::Functions;
use reifydb_runtime::clock::Clock;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, dictionary::DictionaryEntryId, identity::IdentityId, row_number::RowNumber},
};

use crate::transaction::FlowTransaction;
// All types are accessed directly from their submodules:
// - crate::operator::sink::subscription::SinkSubscriptionOperator
// - crate::operator::sink::view::SinkViewOperator

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

/// Coerce columns to match target schema types
pub(crate) fn coerce_columns(columns: &Columns, target_columns: &[ColumnDef]) -> reifydb_type::Result<Columns> {
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
		let ctx = EvalContext {
			target: Some(TargetColumn::Partial {
				source_name: None,
				column_name: Some(target_col.name.clone()),
				column_type: target_type.clone(),
				properties: vec![ColumnPropertyKind::Saturation(ColumnSaturationPolicy::None)],
			}),
			columns: columns.clone(),
			row_count,
			take: None,
			params: &EMPTY_PARAMS,
			symbol_table: &EMPTY_SYMBOL_TABLE,
			is_aggregate_context: false,
			functions: &Functions::empty(),
			clock: &Clock::default(),
			arena: None,
			identity: IdentityId::root(),
		};

		if let Some(source_col) = columns.column(&target_col.name) {
			// Cast to target type
			let casted = cast_column_data(
				&ctx,
				source_col.data(),
				target_type.clone(),
				Fragment::internal(&target_col.name),
			)?;
			result_columns.push(Column {
				name: Fragment::internal(&target_col.name),
				data: casted,
			});
		} else {
			result_columns.push(Column::undefined_typed(
				Fragment::internal(&target_col.name),
				target_type,
				row_count,
			))
		}
	}

	// Preserve row numbers
	let row_numbers = columns.row_numbers.iter().cloned().collect();
	Ok(Columns::with_row_numbers(result_columns, row_numbers))
}

/// Coerce columns to match subscription schema types (simpler than ColumnDef)
pub(crate) fn coerce_subscription_columns(
	columns: &Columns,
	target_columns: &[SubscriptionColumnDef],
) -> reifydb_type::Result<Columns> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(Columns::empty());
	}

	// If target columns are empty (schema-less subscription),
	// use the input columns as-is (inferred from query)
	if target_columns.is_empty() {
		return Ok(columns.clone());
	}

	let mut result_columns = Vec::with_capacity(target_columns.len());

	for target_col in target_columns {
		let target_type = target_col.ty.clone();

		// Create context with Undefined saturation policy for this column
		let ctx = EvalContext {
			target: Some(TargetColumn::Partial {
				source_name: None,
				column_name: Some(target_col.name.clone()),
				column_type: target_type.clone(),
				properties: vec![ColumnPropertyKind::Saturation(ColumnSaturationPolicy::None)],
			}),
			columns: columns.clone(),
			row_count,
			take: None,
			params: &EMPTY_PARAMS,
			symbol_table: &EMPTY_SYMBOL_TABLE,
			is_aggregate_context: false,
			functions: &Functions::empty(),
			clock: &Clock::default(),
			arena: None,
			identity: IdentityId::root(),
		};

		if let Some(source_col) = columns.column(&target_col.name) {
			// Cast to target type
			let casted = cast_column_data(
				&ctx,
				source_col.data(),
				target_type.clone(),
				Fragment::internal(&target_col.name),
			)?;
			result_columns.push(Column {
				name: Fragment::internal(&target_col.name),
				data: casted,
			});
		} else {
			result_columns.push(Column::undefined_typed(
				Fragment::internal(&target_col.name),
				target_type,
				row_count,
			))
		}
	}

	// Preserve row numbers
	let row_numbers = columns.row_numbers.iter().cloned().collect();
	Ok(Columns::with_row_numbers(result_columns, row_numbers))
}

/// Encode values at a specific row index with explicit row number
pub(crate) fn encode_row_at_index(
	columns: &Columns,
	row_idx: usize,
	schema: &Schema,
	row_number: RowNumber,
) -> (RowNumber, EncodedValues) {
	// Use row_number parameter instead of columns.row_numbers[row_idx]

	// Collect values in SCHEMA FIELD ORDER by matching column names
	// This ensures values are in the same order as schema expects
	let values: Vec<reifydb_type::value::Value> = schema
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
	let mut encoded = schema.allocate();
	schema.set_values(&mut encoded, &values);

	(row_number, encoded)
}

/// Decode dictionary columns in-place using FlowTransaction for lookups.
///
/// For columns that store `DictionaryId` values, reads the embedded `dictionary_id`
/// from the container metadata, looks up the `DictionaryDef` in the catalog,
/// then resolves each dictionary entry ID to its actual value.
pub(crate) fn decode_dictionary_columns(columns: &mut Columns, txn: &mut FlowTransaction) -> reifydb_type::Result<()> {
	// Collect (col_pos, DictionaryDef) for every DictionaryId column that carries a dictionary_id
	let dict_columns: Vec<(usize, DictionaryDef)> = {
		let catalog = txn.catalog();
		columns.iter()
			.enumerate()
			.filter_map(|(pos, col)| {
				if let ColumnData::DictionaryId(container) = col.data() {
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
		let mut new_data = ColumnData::with_capacity(dictionary.value_type.clone(), row_count);

		for row_idx in 0..row_count {
			let id_value = col.data().get_value(row_idx);
			if let Some(entry_id) = DictionaryEntryId::from_value(&id_value) {
				let index_key =
					DictionaryEntryIndexKey::new(dictionary.id, entry_id.to_u128() as u64).encode();
				match txn.get(&index_key)? {
					Some(encoded) => {
						let value: Value =
							postcard::from_bytes(&encoded).unwrap_or(Value::none());
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

		columns.columns.make_mut()[*col_pos] = Column {
			name: columns[*col_pos].name().clone(),
			data: new_data,
		};
	}

	Ok(())
}
