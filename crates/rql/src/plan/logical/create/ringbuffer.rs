// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::{
	catalog::ringbuffer::RingBufferColumnToCreate,
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	value::constraint::{Constraint, TypeConstraint},
};

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

		let ringbuffer_namespace_name = ast.ringbuffer.namespace.first().map(|n| n.text()).unwrap_or("default");

		for col in ast.columns.into_iter() {
			let column_name = col.name.text().to_string();
			let mut constraint = convert_data_type_with_constraints(&col.ty)?;
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
							return Err(CatalogError::NotFound {
								kind: CatalogObjectKind::Dictionary,
								namespace: dict_namespace_name.to_string(),
								name: dict_name.to_string(),
								fragment: dict_ident.name.to_owned(),
							}
							.into());
						};

						let Some(dictionary) = self.catalog.find_dictionary_by_name(
							tx,
							namespace.id,
							dict_name,
						)?
						else {
							return Err(CatalogError::NotFound {
								kind: CatalogObjectKind::Dictionary,
								namespace: dict_namespace_name.to_string(),
								name: dict_name.to_string(),
								fragment: dict_ident.name.to_owned(),
							}
							.into());
						};

						if column_type != dictionary.value_type {
							return Err(CatalogError::DictionaryTypeMismatch {
								column: column_name.clone(),
								column_type,
								dictionary: dict_name.to_string(),
								dictionary_value_type: dictionary.value_type,
								fragment: col.name.to_owned(),
							}
							.into());
						}

						dictionary_id = Some(dictionary.id);
						// Embed dictionary constraint so the TypeConstraint carries id_type
						// info
						constraint = TypeConstraint::with_constraint(
							constraint.get_type(),
							Constraint::Dictionary(dictionary.id, dictionary.id_type),
						);
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

		let ringbuffer = ast.ringbuffer;

		Ok(LogicalPlan::CreateRingBuffer(CreateRingBufferNode {
			ringbuffer,
			if_not_exists: false,
			columns,
			capacity: ast.capacity,
		}))
	}
}
