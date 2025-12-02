// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::{CatalogQueryTransaction, store::ring_buffer::create::RingBufferColumnToCreate};
use reifydb_core::{interface::ColumnPolicyKind, return_error};
use reifydb_type::{
	Fragment,
	diagnostic::catalog::{dictionary_not_found, dictionary_type_mismatch},
};

use crate::{
	ast::AstCreateRingBuffer,
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateRingBufferNode, LogicalPlan, convert_policy},
};

impl Compiler {
	pub(crate) fn compile_create_ring_buffer<'a, T: CatalogQueryTransaction>(
		ast: AstCreateRingBuffer<'a>,
		tx: &mut T,
	) -> crate::Result<LogicalPlan<'a>> {
		let mut columns: Vec<RingBufferColumnToCreate> = vec![];

		// Get the ring buffer's namespace for dictionary resolution
		let ring_buffer_namespace_name =
			ast.ring_buffer.namespace.as_ref().map(|n| n.text()).unwrap_or("default");

		for col in ast.columns.into_iter() {
			let column_name = col.name.text().to_string();
			let constraint = convert_data_type_with_constraints(&col.ty)?;
			let column_type = constraint.get_type();

			let policies = if let Some(policy_block) = &col.policies {
				policy_block.policies.iter().map(convert_policy).collect::<Vec<ColumnPolicyKind>>()
			} else {
				vec![]
			};

			let ty_fragment = match &col.ty {
				crate::ast::AstDataType::Unconstrained(fragment) => fragment.clone(),
				crate::ast::AstDataType::Constrained {
					name,
					..
				} => name.clone(),
			};

			let fragment = Some(Fragment::merge_all([col.name.clone(), ty_fragment]).into_owned());

			// Resolve dictionary if specified
			let dictionary_id = if let Some(ref dict_ident) = col.dictionary {
				// Get the dictionary's namespace (uses column's namespace or ring buffer's namespace)
				let dict_namespace_name = dict_ident
					.namespace
					.as_ref()
					.map(|n| n.text())
					.unwrap_or(ring_buffer_namespace_name);
				let dict_name = dict_ident.name.text();

				// Find the namespace
				let Some(namespace) = tx.find_namespace_by_name(dict_namespace_name)? else {
					return_error!(dictionary_not_found(
						dict_ident.name.clone(),
						dict_namespace_name,
						dict_name,
					));
				};

				// Find the dictionary
				let Some(dictionary) = tx.find_dictionary_by_name(namespace.id, dict_name)? else {
					return_error!(dictionary_not_found(
						dict_ident.name.clone(),
						dict_namespace_name,
						dict_name,
					));
				};

				// Validate type compatibility: column type must match dictionary's value_type
				if column_type != dictionary.value_type {
					return_error!(dictionary_type_mismatch(
						col.name.clone(),
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
				name: column_name,
				constraint,
				policies,
				auto_increment: col.auto_increment,
				fragment,
				dictionary_id,
			});
		}

		// Use the ring buffer identifier directly from AST
		let ring_buffer = ast.ring_buffer;

		Ok(LogicalPlan::CreateRingBuffer(CreateRingBufferNode {
			ring_buffer,
			if_not_exists: false,
			columns,
			capacity: ast.capacity,
		}))
	}
}
