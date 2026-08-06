// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{
	catalog::view::ViewColumnToCreate,
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	value::constraint::{Constraint, TypeConstraint},
};

use crate::{
	Result,
	ast::ast::{AstColumnProperty, AstCreateTransactionalView, AstViewStorageKind},
	bump::BumpVec,
	convert_data_type_with_constraints,
	plan::logical::{Compiler, CreateTransactionalViewNode, LogicalPlan},
};

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_transactional_view(
		&self,
		ast: AstCreateTransactionalView<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let mut columns: Vec<ViewColumnToCreate> = vec![];

		let view_ns_segments: Vec<&str> = ast.view.namespace.iter().map(|n| n.text()).collect();

		for col in ast.columns.into_iter() {
			let column_name = col.name.text().to_string();
			let mut constraint = convert_data_type_with_constraints(&col.ty)?;
			let column_type = constraint.get_type();

			let name = col.name.to_owned();
			let ty_fragment = col.ty.name_fragment().to_owned();
			let fragment = Fragment::merge_all([name.clone(), ty_fragment]);

			let mut dictionary_id = None;

			for property in &col.properties {
				if let AstColumnProperty::Dictionary(dict_ident) = property {
					let dict_ns_segments: Vec<&str> = if dict_ident.namespace.is_empty() {
						view_ns_segments.clone()
					} else {
						dict_ident.namespace.iter().map(|n| n.text()).collect()
					};
					let dict_name = dict_ident.name.text();

					let Some(namespace) =
						self.catalog.find_namespace_by_segments(tx, &dict_ns_segments)?
					else {
						return Err(CatalogError::NotFound {
							kind: CatalogObjectKind::Dictionary,
							namespace: dict_ns_segments.join("::"),
							name: dict_name.to_string(),
							fragment: dict_ident.name.to_owned(),
						}
						.into());
					};

					let Some(dictionary) =
						self.catalog.find_dictionary_by_name(tx, namespace.id(), dict_name)?
					else {
						return Err(CatalogError::NotFound {
							kind: CatalogObjectKind::Dictionary,
							namespace: dict_ns_segments.join("::"),
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

					constraint = TypeConstraint::with_constraint(
						constraint.get_type(),
						Constraint::Dictionary(dictionary.id, dictionary.id_type),
					);
				}
			}

			columns.push(ViewColumnToCreate {
				name,
				fragment,
				constraint,
				dictionary_id,
			});
		}

		let partition_by: &[String] = match &ast.storage_kind {
			AstViewStorageKind::Table {
				partition_by,
			} => partition_by,
			AstViewStorageKind::RingBuffer {
				partition_by,
				..
			} => partition_by,
			AstViewStorageKind::Series {
				partition_by,
				..
			} => partition_by,
		};

		for pb_col in partition_by {
			if !columns.iter().any(|c| c.name.text() == pb_col.as_str()) {
				return Err(CatalogError::NotFound {
					kind: CatalogObjectKind::Column,
					namespace: view_ns_segments.join("::"),
					name: pb_col.clone(),
					fragment: Fragment::internal(pb_col.as_str()),
				}
				.into());
			}
		}

		let view = ast.view;

		let with = if let Some(as_statement) = ast.as_clause {
			self.compile(as_statement, tx)?
		} else {
			BumpVec::new_in(self.bump)
		};

		let (ttl, persistent) = match ast.settings {
			Some(settings) => (
				settings.ttl.map(Self::compile_ttl).transpose()?,
				settings.persistent.is_none_or(|p| p.value),
			),
			None => (None, true),
		};

		Ok(LogicalPlan::CreateTransactionalView(CreateTransactionalViewNode {
			view,
			if_not_exists: false,
			columns,
			as_clause: with,
			storage_kind: ast.storage_kind,
			ttl,
			persistent,
		}))
	}
}
