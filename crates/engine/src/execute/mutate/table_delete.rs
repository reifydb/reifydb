// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::Bound::Included, sync::Arc};

use futures_util::TryStreamExt;
use reifydb_catalog::CatalogStore;
use reifydb_core::{
	EncodedKeyRange,
	interface::{
		EncodableKey, EncodableKeyRange, GetEncodedRowLayout, IndexEntryKey, IndexId, Params,
		ResolvedNamespace, ResolvedPrimitive, ResolvedTable, RowKey, RowKeyRange,
	},
	value::column::Columns,
};
use reifydb_rql::plan::physical::DeleteTableNode;
use reifydb_type::{
	Fragment, Value,
	diagnostic::{
		catalog::{namespace_not_found, table_not_found},
		engine,
	},
	return_error,
};

use super::primary_key;
use crate::{
	StandardCommandTransaction, StandardTransaction,
	execute::{Batch, ExecutionContext, Executor, QueryNode, query::compile::compile},
	stack::Stack,
};

impl Executor {
	pub(crate) async fn delete<'a>(
		&self,
		txn: &mut StandardCommandTransaction,
		plan: DeleteTableNode,
		params: Params,
	) -> crate::Result<Columns> {
		// Get table from plan or infer from input pipeline
		let (namespace, table) = if let Some(target) = &plan.target {
			// Namespace and table explicitly specified
			let namespace_name = target.namespace().name();
			let Some(namespace) = CatalogStore::find_namespace_by_name(txn, namespace_name).await? else {
				return_error!(namespace_not_found(Fragment::internal(namespace_name), namespace_name));
			};

			let Some(table) = CatalogStore::find_table_by_name(txn, namespace.id, target.name()).await?
			else {
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

			let mut std_txn = StandardTransaction::from(txn);
			let mut input_node = compile(
				*input_plan,
				&mut std_txn,
				Arc::new(ExecutionContext {
					executor: self.clone(),
					source: resolved_source.clone(),
					batch_size: 1024,
					params: params.clone(),
					stack: Stack::new(),
				}),
			)
			.await;

			let context = ExecutionContext {
				executor: self.clone(),
				source: resolved_source.clone(),
				batch_size: 1024,
				params: params.clone(),
				stack: Stack::new(),
			};

			// Initialize the operator before execution
			input_node.initialize(&mut std_txn, &context).await?;

			let mut mutable_context = context.clone();
			while let Some(Batch {
				columns,
			}) = input_node.next(&mut std_txn, &mut mutable_context).await?
			{
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
			let pk_def = primary_key::get_primary_key(std_txn.command_mut(), &table).await?;

			let cmd = std_txn.command();
			for row_number in row_numbers_to_delete {
				let row_key = RowKey::encoded(table.id, row_number);

				// Remove primary key index entry if table has
				// one
				if let Some(ref pk_def) = pk_def {
					if let Some(row_data) = cmd.get(&row_key).await? {
						let row = row_data.values;
						let layout = table.get_layout();
						let index_key =
							primary_key::encode_primary_key(pk_def, &row, &table, &layout)?;

						cmd.remove(&IndexEntryKey::new(
							table.id,
							IndexId::primary(pk_def.id),
							index_key,
						)
						.encode())
							.await?;
					}
				}

				// Now remove the encoded
				cmd.remove(&row_key).await?;
				deleted_count += 1;
			}
		} else {
			// Delete entire table - scan all rows and delete them
			let range = RowKeyRange {
				primitive: table.id.into(),
			};

			// Get primary key info if table has one
			let pk_def = primary_key::get_primary_key(txn, &table).await?;

			let rows: Vec<_> = txn
				.range(
					EncodedKeyRange::new(
						Included(range.start().unwrap()),
						Included(range.end().unwrap()),
					),
					1024,
				)?
				.try_collect()
				.await?;

			for multi in rows {
				// Remove primary key index entry if table has
				// one
				if let Some(ref pk_def) = pk_def {
					let layout = table.get_layout();
					let index_key = super::primary_key::encode_primary_key(
						pk_def,
						&multi.values,
						&table,
						&layout,
					)?;

					txn.remove(&IndexEntryKey::new(
						table.id,
						IndexId::primary(pk_def.id),
						index_key,
					)
					.encode())
						.await?;
				}

				// Remove the encoded
				txn.remove(&multi.key).await?;
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
}
