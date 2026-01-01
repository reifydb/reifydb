// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Catalog resolution methods.

use bumpalo::collections::Vec as BumpVec;
use reifydb_core::interface::ColumnDef;
use reifydb_transaction::IntoStandardTransaction;

use super::core::{DEFAULT_NAMESPACE, PlanError, PlanErrorKind, Planner, Result};
use crate::{
	plan::{CatalogColumn, Dictionary, Namespace, Primitive, RingBuffer, Table, View},
	token::Span,
};

impl<'bump, 'cat, T: IntoStandardTransaction> Planner<'bump, 'cat, T> {
	/// Resolve a namespace by name, defaulting to "default" if not specified.
	pub(super) async fn resolve_namespace(
		&mut self,
		name: Option<&str>,
		span: Span,
	) -> Result<&'bump Namespace<'bump>> {
		let ns_name = name.unwrap_or(DEFAULT_NAMESPACE);
		let ns_def = self.catalog.find_namespace_by_name(self.tx, ns_name).await.map_err(|e| PlanError {
			kind: PlanErrorKind::Unsupported(format!("catalog error: {}", e)),
			span,
		})?;

		match ns_def {
			Some(def) => Ok(self.bump.alloc(Namespace {
				id: def.id,
				name: self.bump.alloc_str(&def.name),
				span,
			})),
			None => Err(PlanError {
				kind: PlanErrorKind::NamespaceNotFound(ns_name.to_string()),
				span,
			}),
		}
	}

	/// Resolve a primitive data source (table, view, ring buffer, dictionary).
	pub(super) async fn resolve_primitive(
		&mut self,
		namespace: Option<&str>,
		name: &str,
		span: Span,
	) -> Result<Primitive<'bump>> {
		let ns = self.resolve_namespace(namespace, span).await?;

		// Try table first
		if let Some(table_def) =
			self.catalog.find_table_by_name(self.tx, ns.id, name).await.map_err(|e| PlanError {
				kind: PlanErrorKind::Unsupported(format!("catalog error: {}", e)),
				span,
			})? {
			let columns = self.resolve_columns(&table_def.columns, span);
			let table = self.bump.alloc(Table {
				id: table_def.id,
				namespace: ns,
				name: self.bump.alloc_str(&table_def.name),
				columns,
				span,
			});
			return Ok(Primitive::Table(table));
		}

		// Try ring buffer
		if let Some(rb_def) =
			self.catalog.find_ringbuffer_by_name(self.tx, ns.id, name).await.map_err(|e| PlanError {
				kind: PlanErrorKind::Unsupported(format!("catalog error: {}", e)),
				span,
			})? {
			let columns = self.resolve_columns(&rb_def.columns, span);
			let rb = self.bump.alloc(RingBuffer {
				id: rb_def.id,
				namespace: ns,
				name: self.bump.alloc_str(&rb_def.name),
				columns,
				capacity: rb_def.capacity,
				span,
			});
			return Ok(Primitive::RingBuffer(rb));
		}

		// Try view
		if let Some(view_def) =
			self.catalog.find_view_by_name(self.tx, ns.id, name).await.map_err(|e| PlanError {
				kind: PlanErrorKind::Unsupported(format!("catalog error: {}", e)),
				span,
			})? {
			let columns = self.resolve_columns(&view_def.columns, span);
			let view = self.bump.alloc(View {
				id: view_def.id,
				namespace: ns,
				name: self.bump.alloc_str(&view_def.name),
				columns,
				span,
			});
			return Ok(Primitive::View(view));
		}

		// Try dictionary
		if let Some(dict_def) =
			self.catalog.find_dictionary_by_name(self.tx, ns.id, name).await.map_err(|e| PlanError {
				kind: PlanErrorKind::Unsupported(format!("catalog error: {}", e)),
				span,
			})? {
			let dict = self.bump.alloc(Dictionary {
				id: dict_def.id,
				namespace: ns,
				name: self.bump.alloc_str(&dict_def.name),
				key_type: dict_def.id_type,
				value_type: dict_def.value_type,
				span,
			});
			return Ok(Primitive::Dictionary(dict));
		}

		Err(PlanError {
			kind: PlanErrorKind::TableNotFound(name.to_string()),
			span,
		})
	}

	/// Resolve columns from column definitions.
	pub(super) fn resolve_columns(&self, columns: &[ColumnDef], span: Span) -> &'bump [CatalogColumn<'bump>] {
		let mut resolved = BumpVec::with_capacity_in(columns.len(), self.bump);
		for (idx, col) in columns.iter().enumerate() {
			resolved.push(CatalogColumn {
				id: col.id,
				name: self.bump.alloc_str(&col.name),
				column_type: col.constraint.get_type(),
				column_index: idx as u32,
				span,
			});
		}
		resolved.into_bump_slice()
	}

	pub(super) async fn resolve_table(
		&mut self,
		namespace: Option<&str>,
		name: &str,
		span: Span,
	) -> Result<&'bump Table<'bump>> {
		let ns = self.resolve_namespace(namespace, span).await?;

		let table_def = self
			.catalog
			.find_table_by_name(self.tx, ns.id, name)
			.await
			.map_err(|e| PlanError {
				kind: PlanErrorKind::Unsupported(format!("catalog error: {}", e)),
				span,
			})?
			.ok_or_else(|| PlanError {
				kind: PlanErrorKind::TableNotFound(name.to_string()),
				span,
			})?;

		let columns = self.resolve_columns(&table_def.columns, span);
		Ok(self.bump.alloc(Table {
			id: table_def.id,
			namespace: ns,
			name: self.bump.alloc_str(&table_def.name),
			columns,
			span,
		}))
	}
}
