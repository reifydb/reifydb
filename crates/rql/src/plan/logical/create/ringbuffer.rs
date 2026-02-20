// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::ringbuffer::RingBufferColumnToCreate;
use reifydb_core::error::diagnostic::catalog::{dictionary_not_found, dictionary_type_mismatch};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, return_error};

use crate::{
	ast::ast::{AstColumnProperty, AstCreateRingBuffer},
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateRingBufferNode, LogicalPlan},
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

			let name = col.name.to_owned();
			let ty_fragment = col.ty.name_fragment().to_owned();
			let fragment = Fragment::merge_all([name.clone(), ty_fragment]);

			let mut auto_increment = false;
			let mut dictionary_id = None;
			let policies = vec![];

			for property in &col.properties {
				match property {
					AstColumnProperty::AutoIncrement => auto_increment = true,
					AstColumnProperty::Dictionary(dict_ident) => {
						let dict_namespace_name = dict_ident
							.namespace
							.first()
							.map(|n| n.text())
							.unwrap_or(ringbuffer_namespace_name);
						let dict_name = dict_ident.name.text();

						let Some(namespace) =
							self.catalog.find_namespace_by_name(tx, dict_namespace_name)?
						else {
							return_error!(dictionary_not_found(
								dict_ident.name.to_owned(),
								dict_namespace_name,
								dict_name,
							));
						};

						let Some(dictionary) = self.catalog.find_dictionary_by_name(
							tx,
							namespace.id,
							dict_name,
						)?
						else {
							return_error!(dictionary_not_found(
								dict_ident.name.to_owned(),
								dict_namespace_name,
								dict_name,
							));
						};

						if column_type != dictionary.value_type {
							return_error!(dictionary_type_mismatch(
								col.name.to_owned(),
								&column_name,
								column_type,
								dict_name,
								dictionary.value_type,
							));
						}

						dictionary_id = Some(dictionary.id);
					}
					AstColumnProperty::Saturation(_) => {
						// TODO: inline saturation policy
					}
					AstColumnProperty::Default(_) => {
						// TODO: inline default policy
					}
				}
			}

			columns.push(RingBufferColumnToCreate {
				name,
				fragment,
				constraint,
				policies,
				auto_increment,
				dictionary_id,
			});
		}

		// Use the ring buffer identifier directly from AST
		let ringbuffer = ast.ringbuffer;

		Ok(LogicalPlan::CreateRingBuffer(CreateRingBufferNode {
			ringbuffer,
			if_not_exists: false,
			columns,
			capacity: ast.capacity,
		}))
	}
}
