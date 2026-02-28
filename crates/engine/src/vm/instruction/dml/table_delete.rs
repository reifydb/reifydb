// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::Bound::Included, sync::Arc};

use reifydb_catalog::error::{CatalogError, CatalogObjectKind};
use reifydb_core::{
	encoded::key::EncodedKeyRange,
	interface::{
		catalog::{id::IndexId, policy::PolicyTargetType},
		resolved::{ResolvedNamespace, ResolvedPrimitive, ResolvedTable},
	},
	internal_error,
	key::{
		EncodableKey, EncodableKeyRange,
		index_entry::IndexEntryKey,
		row::{RowKey, RowKeyRange},
	},
	value::column::columns::Columns,
};
use reifydb_rql::nodes::DeleteTableNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	params::Params,
	value::{Value, identity::IdentityId},
};

use super::primary_key;
use crate::{
	error::EngineError,
	policy::PolicyEvaluator,
	transaction::operation::table::TableOperations,
	vm::{
		services::Services,
		stack::SymbolTable,
		volcano::{
			compile::compile,
			query::{QueryContext, QueryNode},
		},
	},
};

pub(crate) fn delete<'a>(
	services: &Arc<Services>,
	txn: &mut Transaction<'_>,
	plan: DeleteTableNode,
	params: Params,
	identity: IdentityId,
	symbol_table_ref: &crate::vm::stack::SymbolTable,
) -> crate::Result<Columns> {
	// Get table from plan or infer from input pipeline
	let (namespace, table) = if let Some(target) = &plan.target {
		// Namespace and table explicitly specified
		let namespace_name = target.namespace().name();
		let Some(namespace) = services.catalog.find_namespace_by_name(txn, namespace_name)? else {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Namespace,
				namespace: namespace_name.to_string(),
				name: String::new(),
				fragment: Fragment::internal(namespace_name),
			}
			.into());
		};

		let Some(table) = services.catalog.find_table_by_name(txn, namespace.id, target.name())? else {
			return Err(CatalogError::NotFound {
				kind: CatalogObjectKind::Table,
				namespace: namespace_name.to_string(),
				name: target.name().to_string(),
				fragment: target.identifier().clone(),
			}
			.into());
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
				identity: IdentityId::root(),
			}),
		);

		let context = QueryContext {
			services: services.clone(),
			source: resolved_source.clone(),
			batch_size: 1024,
			params: params.clone(),
			stack: SymbolTable::new(),
			identity: IdentityId::root(),
		};

		// Initialize the operator before execution
		input_node.initialize(txn, &context)?;

		let mut mutable_context = context.clone();
		while let Some(columns) = input_node.next(txn, &mut mutable_context)? {
			// Enforce write policies before processing rows
			PolicyEvaluator::new(services, symbol_table_ref).enforce_write_policies(
				txn,
				identity,
				&namespace.name,
				&table.name,
				"delete",
				&columns,
				PolicyTargetType::Table,
			)?;

			// Get encoded numbers from the Columns structure
			if columns.row_numbers.is_empty() {
				return Err(EngineError::MissingRowNumberColumn.into());
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
						internal_error!(
							"Schema with fingerprint {:?} not found for table {}",
							fingerprint,
							table.name
						)
					})?;
				let index_key = primary_key::encode_primary_key(pk_def, &row_values, &table, &schema)?;

				txn.remove(
					&IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), index_key).encode()
				)?;
			}

			txn.remove_from_table(table.clone(), row_number)?;
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
			if let Some(ref pk_def) = pk_def {
				let fingerprint = multi.values.fingerprint();
				let schema =
					services.catalog.schema.get_or_load(fingerprint, txn)?.ok_or_else(|| {
						internal_error!(
							"Schema with fingerprint {:?} not found for table {}",
							fingerprint,
							table.name
						)
					})?;
				let index_key =
					primary_key::encode_primary_key(pk_def, &multi.values, &table, &schema)?;

				txn.remove(
					&IndexEntryKey::new(table.id, IndexId::primary(pk_def.id), index_key).encode()
				)?;
			}

			let row_key = RowKey::decode(&multi.key).expect("valid RowKey encoding");
			txn.remove_from_table(table.clone(), row_key.row)?;
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
