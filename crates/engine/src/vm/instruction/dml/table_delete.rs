// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::Bound::Included, sync::Arc};

use reifydb_core::{
	encoded::key::EncodedKeyRange,
	error::diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		engine,
		internal::internal,
	},
	interface::{
		catalog::id::IndexId,
		resolved::{ResolvedNamespace, ResolvedPrimitive, ResolvedTable},
	},
	key::{
		EncodableKey, EncodableKeyRange,
		index_entry::IndexEntryKey,
		row::{RowKey, RowKeyRange},
	},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::DeleteTableNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{error, fragment::Fragment, params::Params, return_error, value::Value};

use super::primary_key;
use crate::vm::{
	services::Services,
	stack::SymbolTable,
	volcano::{
		compile::compile,
		query::{QueryContext, QueryNode},
	},
};

pub(crate) fn delete<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: DeleteTableNode,
	params: Params,
) -> crate::Result<Columns> {
	// Get table from plan or infer from input pipeline
	let (namespace, table) = if let Some(target) = &plan.target {
		// Namespace and table explicitly specified
		let namespace_name = target.namespace().name();
		let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
			return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
		};

		let Some(table) = services.catalog.find_table_by_name(txn, namespace.id, target.name())? else {
			let fragment = target.identifier().clone();
			return_error!(table_not_found(fragment.clone(), namespace_name, target.name(),));
		};

		(namespace, table)
	} else {
		unimplemented!("DELETE without input requires explicit target table");
	};

	// Create resolved source for the table
	let namespace_ident = Fragment::internal(namespace.name.clone());
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace.clone());

	let table_ident = Fragment::internal(table.name.clone());
	let resolved_table = ResolvedTable::new(table_ident, resolved_namespace, table.clone());
	let resolved_source = Some(ResolvedPrimitive::Table(resolved_table));

	let mut deleted_count = 0;

	if let Some(input_plan) = plan.input {
		// Delete specific rows based on input plan
		// First collect all encoded numbers to delete
		let mut row_numbers_to_delete = Vec::new();

		let mut input_node = compile(
			*input_plan,
			txn,
			Arc::new(QueryContext {
				services: services.clone(),
				source: resolved_source.clone(),
				batch_size: 1024,
				params: params.clone(),
				stack: SymbolTable::new(),
			}),
		);

		let context = QueryContext {
			services: services.clone(),
			source: resolved_source.clone(),
			batch_size: 1024,
			params: params.clone(),
			stack: SymbolTable::new(),
		};

		// Initialize the operator before execution
		input_node.initialize(txn, &context)?;

		let mut mutable_context = context.clone();
		while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
			// Get encoded numbers from the Columns structure
			if columns.row_numbers.is_empty() {
				return_error!(engine::missing_row_number_column());
			}

			// Extract RowNumber data
			let row_numbers = &columns.row_numbers;

			for row_numberx in 0..columns.row_count() {
				let row_number = row_numbers[row_numberx];
				row_numbers_to_delete.push(row_number);
			}
		}

		// Get primary key info if table has one
		let pk_def = primary_key::get_primary_key(&services.catalog, txn, &table)?;

		for row_number in row_numbers_to_delete {
			let row_key = RowKey::encoded(table.id, row_number);

			// Get row values for metrics tracking (and for primary key encoding)
			let row_values = match txn.get(&row_key)? {
				Some(v) => v.values,
				None => continue, // Row doesn't exist, skip
			};

			// Remove primary key index entry if table has one
			if let Some(ref pk_def) = pk_def {
				// Load schema from the row data
				let fingerprint = row_values.fingerprint();
				let schema =
					services.catalog.schema.get_or_load(fingerprint, txn)?.ok_or_else(|| {
						error!(reifydb_core::error::diagnostic::internal::internal(format!(
							"Schema with fingerprint {:?} not found for table {}",
							fingerprint, table.name
						)))
					})?;
				let index_key = primary_key::encode_primary_key(pk_def, &row_values, &table, &schema)?;

				txn.remove(
					&IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), index_key).encode()
				)?;
			}

			// Now remove the row
			txn.unset(&row_key, row_values)?;
			deleted_count += 1;
		}
	} else {
		// Delete entire table - scan all rows and delete them
		let range = RowKeyRange {
			primitive: table.id.into(),
		};

		// Get primary key info if table has one
		let pk_def = primary_key::get_primary_key(&services.catalog, txn, &table)?;

		let rows: Vec<_> = txn
			.range(
				EncodedKeyRange::new(Included(range.start().unwrap()), Included(range.end().unwrap())),
				1024,
			)?
			.collect::<Result<Vec<_>, _>>()?;

		for multi in rows {
			// Remove primary key index entry if table has
			// one
			if let Some(ref pk_def) = pk_def {
				// Load schema from the row data
				let fingerprint = multi.values.fingerprint();
				let schema =
					services.catalog.schema.get_or_load(fingerprint, txn)?.ok_or_else(|| {
						error!(internal(format!(
							"Schema with fingerprint {:?} not found for table {}",
							fingerprint, table.name
						)))
					})?;
				let index_key =
					primary_key::encode_primary_key(pk_def, &multi.values, &table, &schema)?;

				txn.remove(
					&IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), index_key).encode()
				)?;
			}

			// Remove the row
			txn.unset(&multi.key, multi.values.clone())?;
			deleted_count += 1;
		}
	}

	// Return summary columns
	Ok(Columns::single_row([
		("namespace", Value::Utf8(namespace.name)),
		("table", Value::Utf8(table.name)),
		("deleted", Value::Uint8(deleted_count as u64)),
	]))
}
