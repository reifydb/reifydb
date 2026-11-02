// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{
	catalog::{queue::QueueColumnToCreate, view::ViewColumnToCreate},
	error::{CatalogError, CatalogObjectKind},
};
use reifydb_core::interface::catalog::{ringbuffer::EvictionVisibility, view::QueueView};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	value::{
		constraint::{Constraint, TypeConstraint},
		value_type::ValueType,
	},
};

use crate::{
	Result,
	ast::ast::{AstColumnProperty, AstColumnToCreate, AstCreateDeferredView, AstViewStorageKind},
	bump::BumpVec,
	convert_data_type_with_constraints,
	plan::logical::{
		Compiler, CreateDeferredViewNode, LogicalPlan,
		create::{
			check_view_partition,
			queue::{compile_dispatch, compile_retention, compile_retry},
			reject_dictionary_on_decimal,
		},
		reserved::reject_reserved_column_name,
	},
};

fn queue_view_columns(columns: &[ViewColumnToCreate]) -> Vec<QueueColumnToCreate> {
	columns.iter()
		.map(|column| QueueColumnToCreate {
			name: column.name.clone(),
			fragment: column.fragment.clone(),
			constraint: column.constraint.clone(),
			properties: vec![],
			auto_increment: false,
			dictionary_id: column.dictionary_id,
		})
		.collect()
}

fn change_column() -> ViewColumnToCreate {
	ViewColumnToCreate {
		name: Fragment::internal(QueueView::CHANGE_COLUMN),
		fragment: Fragment::internal(QueueView::CHANGE_COLUMN),
		constraint: TypeConstraint::unconstrained(ValueType::Utf8),
		dictionary_id: None,
	}
}

impl<'bump> Compiler<'bump> {
	pub(crate) fn compile_deferred_view(
		&self,
		ast: AstCreateDeferredView<'bump>,
		tx: &mut Transaction<'_>,
	) -> Result<LogicalPlan<'bump>> {
		let view_ns_segments: Vec<&str> = ast.view.namespace.iter().map(|n| n.text()).collect();
		let mut columns = self.compile_view_columns(ast.columns, &view_ns_segments, tx)?;

		check_view_partition(&columns, &ast.storage_kind, &view_ns_segments)?;

		let storage_kind = match ast.queue {
			Some(options) => {
				if let Some(column) = columns.iter().find(|c| c.name.text() == QueueView::CHANGE_COLUMN)
				{
					return Err(CatalogError::InvalidDefinition {
						kind: CatalogObjectKind::View,
						name: ast.view.name.text().to_string(),
						field: "column change",
						reason: "is reserved on a queue view: the sink writes it on every item"
							.to_string(),
						fragment: column.name.clone(),
					}
					.into());
				}
				let queue_columns = queue_view_columns(&columns);
				let partition_by = match ast.storage_kind {
					AstViewStorageKind::Queue {
						partition_by,
						..
					} => partition_by,
					_ => unreachable!("queue options come only from a queue view"),
				};
				columns.push(change_column());
				AstViewStorageKind::Queue {
					partition_by,
					dispatch: compile_dispatch(
						&options.dispatch,
						&queue_columns,
						&view_ns_segments,
					)?,
					retention: compile_retention(options.retention.as_ref())?,
					retry: compile_retry(options.retry.as_ref())?,
				}
			}
			None => ast.storage_kind,
		};

		let view = ast.view;

		let with = if let Some(as_statement) = ast.as_clause {
			self.compile(as_statement, tx)?
		} else {
			BumpVec::new_in(self.bump)
		};

		let (ttl, persistent, ttl_eviction) = match ast.settings {
			Some(settings) => (
				settings.ttl.map(Self::compile_ttl).transpose()?,
				settings.persistent.is_none_or(|p| p.value),
				settings.ttl_eviction,
			),
			None => (None, true, EvictionVisibility::Silent),
		};

		Ok(LogicalPlan::CreateDeferredView(CreateDeferredViewNode {
			view,
			if_not_exists: false,
			columns,
			as_clause: with,
			storage_kind,
			ttl,
			persistent,
			ttl_eviction,
		}))
	}

	pub(crate) fn compile_view_columns(
		&self,
		ast_columns: Vec<AstColumnToCreate<'bump>>,
		view_ns_segments: &[&str],
		tx: &mut Transaction<'_>,
	) -> Result<Vec<ViewColumnToCreate>> {
		let mut columns: Vec<ViewColumnToCreate> = vec![];
		for col in ast_columns.into_iter() {
			reject_reserved_column_name(&col.name.to_owned())?;
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
						view_ns_segments.to_vec()
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

					reject_dictionary_on_decimal(&column_name, &column_type, col.name.to_owned())?;

					if column_type.inner_type() != &dictionary.value_type {
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

		Ok(columns)
	}
}
