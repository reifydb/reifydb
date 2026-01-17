// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! DML opcodes: InsertRow, UpdateRow, DeleteRow.

use std::collections::HashMap;

use reifydb_catalog::{CatalogStore, store::sequence::row::RowSequence};
use reifydb_core::{
	encoded::{layout::EncodedValuesLayout, value::encode_value},
	key::row::RowKey,
	value::column::columns::Columns,
};
use reifydb_rqlv2::bytecode::program::DmlTargetType;
use reifydb_type::value::{Value, row_number::RowNumber};

use super::HandlerContext;
use crate::{
	error::{Result, VmError},
	pipeline,
	runtime::{dispatch::DispatchResult, operand::OperandValue},
};

/// InsertRow - insert rows into a table.
pub fn insert_row(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let target_index = ctx.read_u16()?;

	let target = ctx.vm.program.dml_targets.get(target_index as usize).ok_or(VmError::InvalidDmlTargetIndex {
		index: target_index,
	})?;

	// Clone target info to avoid borrow issues
	let target_type = target.target_type;
	let target_name = target.name.clone();

	let tx = ctx.tx.as_mut().ok_or(VmError::TransactionRequired)?;
	let cmd_tx = tx.command_mut();

	match target_type {
		DmlTargetType::Table => {
			// Parse namespace.table from name
			let parts: Vec<&str> = target_name.split('.').collect();
			let (namespace_name, table_name) = if parts.len() >= 2 {
				(parts[0], parts[1])
			} else {
				("default", parts[0])
			};

			let namespace = CatalogStore::find_namespace_by_name(cmd_tx, namespace_name)
				.map_err(|e| VmError::CatalogError {
					message: e.to_string(),
				})?
				.ok_or_else(|| VmError::CatalogError {
					message: format!("Namespace '{}' not found", namespace_name),
				})?;

			let table = CatalogStore::find_table_by_name(cmd_tx, namespace.id, table_name)
				.map_err(|e| VmError::CatalogError {
					message: e.to_string(),
				})?
				.ok_or_else(|| VmError::CatalogError {
					message: format!("Table '{}' not found", table_name),
				})?;

			// Pop the input pipeline (rows to insert)
			let input_pipeline = ctx.vm.pop_pipeline()?;
			let input_columns = pipeline::collect(input_pipeline)?;

			// Build storage layout types
			let table_types: Vec<reifydb_type::value::r#type::Type> =
				table.columns.iter().map(|c| c.constraint.get_type()).collect();
			let layout = EncodedValuesLayout::testing(&table_types);

			// Insert each row
			let row_count = input_columns.row_count();
			if row_count == 0 {
				let result = Columns::single_row([
					("namespace", Value::Utf8(namespace_name.to_string())),
					("table", Value::Utf8(table_name.to_string())),
					("inserted", Value::Uint8(0)),
				]);
				ctx.vm.push_operand(OperandValue::Frame(result))?;
				return Ok(ctx.advance_and_continue());
			}

			// Build column name to index map
			let mut column_map: HashMap<&str, usize> = HashMap::new();
			for (idx, col) in input_columns.iter().enumerate() {
				column_map.insert(col.name().text(), idx);
			}

			// Allocate row numbers in batch
			let row_numbers = RowSequence::next_row_number_batch(cmd_tx, table.id, row_count as u64)
				.map_err(|e| VmError::CatalogError {
					message: e.to_string(),
				})?;

			// Insert each row
			for row_idx in 0..row_count {
				let mut row = layout.allocate();

				for (table_idx, table_column) in table.columns.iter().enumerate() {
					let value = if let Some(&input_idx) = column_map.get(table_column.name.as_str())
					{
						input_columns[input_idx].data().get_value(row_idx)
					} else {
						Value::Undefined
					};

					encode_value(&layout, &mut row, table_idx, &value);
				}

				// Insert the row using the RowKey
				let row_key = RowKey::encoded(table.id, row_numbers[row_idx]);
				cmd_tx.set(&row_key, row).map_err(|e| VmError::CatalogError {
					message: e.to_string(),
				})?;
			}

			let result = Columns::single_row([
				("namespace", Value::Utf8(namespace_name.to_string())),
				("table", Value::Utf8(table_name.to_string())),
				("inserted", Value::Uint8(row_count as u64)),
			]);
			ctx.vm.push_operand(OperandValue::Frame(result))?;
		}
		_ => {
			return Err(VmError::UnsupportedOperation {
				operation: format!("INSERT into {:?} not yet implemented", target_type),
			});
		}
	}

	Ok(ctx.advance_and_continue())
}

/// UpdateRow - update rows in a table.
pub fn update_row(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let _target_index = ctx.read_u16()?;

	// TODO: Implement UPDATE
	Err(VmError::UnsupportedOperation {
		operation: "UPDATE not yet implemented".to_string(),
	})
}

/// DeleteRow - delete rows from a table.
pub fn delete_row(ctx: &mut HandlerContext) -> Result<DispatchResult> {
	let target_index = ctx.read_u16()?;

	let target = ctx.vm.program.dml_targets.get(target_index as usize).ok_or(VmError::InvalidDmlTargetIndex {
		index: target_index,
	})?;

	// Clone target info to avoid borrow issues
	let target_type = target.target_type;
	let target_name = target.name.clone();

	let tx = ctx.tx.as_mut().ok_or(VmError::TransactionRequired)?;
	let cmd_tx = tx.command_mut();

	match target_type {
		DmlTargetType::Table => {
			// Parse namespace.table from name
			let parts: Vec<&str> = target_name.split('.').collect();
			let (namespace_name, table_name) = if parts.len() >= 2 {
				(parts[0], parts[1])
			} else {
				("default", parts[0])
			};

			let namespace = CatalogStore::find_namespace_by_name(cmd_tx, namespace_name)
				.map_err(|e| VmError::CatalogError {
					message: e.to_string(),
				})?
				.ok_or_else(|| VmError::CatalogError {
					message: format!("Namespace '{}' not found", namespace_name),
				})?;

			let table = CatalogStore::find_table_by_name(cmd_tx, namespace.id, table_name)
				.map_err(|e| VmError::CatalogError {
					message: e.to_string(),
				})?
				.ok_or_else(|| VmError::CatalogError {
					message: format!("Table '{}' not found", table_name),
				})?;

			// Pop the input pipeline (rows to delete - should contain row numbers)
			let input_pipeline = ctx.vm.pop_pipeline()?;
			let input_columns = pipeline::collect(input_pipeline)?;

			// Find the row_number column
			let row_number_col = input_columns
				.iter()
				.find(|c| c.name().text() == "_row_number" || c.name().text() == "row_number");

			let deleted_count = if let Some(row_num_col) = row_number_col {
				let row_count = row_num_col.data().len();
				for i in 0..row_count {
					if let Value::Uint8(row_num) = row_num_col.data().get_value(i) {
						let row_key = RowKey::encoded(table.id, RowNumber::from(row_num));
						cmd_tx.remove(&row_key).map_err(|e| VmError::CatalogError {
							message: e.to_string(),
						})?;
					}
				}
				row_count
			} else {
				// If no row number column, we can't delete anything
				0
			};

			let result = Columns::single_row([
				("namespace", Value::Utf8(namespace_name.to_string())),
				("table", Value::Utf8(table_name.to_string())),
				("deleted", Value::Uint8(deleted_count as u64)),
			]);
			ctx.vm.push_operand(OperandValue::Frame(result))?;
		}
		_ => {
			return Err(VmError::UnsupportedOperation {
				operation: format!("DELETE from {:?} not yet implemented", target_type),
			});
		}
	}

	Ok(ctx.advance_and_continue())
}
