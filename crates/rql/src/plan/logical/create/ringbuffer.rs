// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::ringbuffer::RingBufferColumnToCreate;
use reifydb_core::{
	error::diagnostic::catalog::{dictionary_not_found, dictionary_type_mismatch},
	interface::catalog::policy::ColumnPolicyKind,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	ast::ast::AstCreateRingBuffer,
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateRingBufferNode, LogicalPlan, convert_policy},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_create_ringbuffer(
		&self,
		ast: AstCreateRingBuffer<'bump>,
		tx: &mut Transaction<'_>,
	) -> crate::Result<LogicalPlan<'bump>> {
		let mut columns: Vec<RingBufferColumnToCreate> = vec![];

		// Get the ring buffer's namespace for dictionary resolution
		let ringbuffer_namespace_name = ast.ringbuffer.namespace.first().map(|n| n.text()).unwrap_or("default");

		for col in ast.columns.into_iter() {
			let column_name = col.name.text().to_string();
			let constraint = convert_data_type_with_constraints(&col.ty)?;
			let column_type = constraint.get_type();

			let policies = if let Some(policy_block) = &col.policies {
				policy_block.policies.iter().map(convert_policy).collect::<Vec<ColumnPolicyKind>>()
			} else {
				vec![]
			};

			let name = col.name.to_owned();
			let ty_fragment = col.ty.name_fragment().to_owned();
			let fragment = Fragment::merge_all([name.clone(), ty_fragment]);

			// Resolve dictionary if specified
			let dictionary_id = if let Some(ref dict_ident) = col.dictionary {
				// Get the dictionary's namespace (uses column's namespace or ring buffer's namespace)
				let dict_namespace_name = dict_ident
					.namespace
					.first()
					.map(|n| n.text())
					.unwrap_or(ringbuffer_namespace_name);
				let dict_name = dict_ident.name.text();

				// Find the namespace
				let Some(namespace) = self.catalog.find_namespace_by_name(tx, dict_namespace_name)?
				else {
					return_error!(dictionary_not_found(
						dict_ident.name.to_owned(),
						dict_namespace_name,
						dict_name,
					));
				};

				// Find the dictionary
				let Some(dictionary) =
					self.catalog.find_dictionary_by_name(tx, namespace.id, dict_name)?
				else {
					return_error!(dictionary_not_found(
						dict_ident.name.to_owned(),
						dict_namespace_name,
						dict_name,
					));
				};

				// Validate type compatibility: column type must match dictionary's value_type
				if column_type != dictionary.value_type {
					return_error!(dictionary_type_mismatch(
						col.name.to_owned(),
						&column_name,
						column_type,
						dict_name,
						dictionary.value_type,
					));
				}

				Some(dictionary.id)
			} else {
				None
			};

			columns.push(RingBufferColumnToCreate {
				name,
				fragment,
				constraint,
				policies,
				auto_increment: col.auto_increment,
				dictionary_id,
			});
		}

		// Use the ring buffer identifier directly from AST
		let ringbuffer = ast.ringbuffer;

		// Convert AST primary key to logical plan primary key
		let primary_key = ast.primary_key.map(|pk| {
			use crate::plan::logical::{PrimaryKeyColumn, PrimaryKeyDef};

			PrimaryKeyDef {
				columns: pk
					.columns
					.into_iter()
					.map(|col| PrimaryKeyColumn {
						column: col.column.name,
						order: col.order,
					})
					.collect(),
			}
		});

		Ok(LogicalPlan::CreateRingBuffer(CreateRingBufferNode {
			ringbuffer,
			if_not_exists: false,
			columns,
			capacity: ast.capacity,
			primary_key,
		}))
	}
}
