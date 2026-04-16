// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::{
	catalog::{
		id::NamespaceId,
		view::ViewKind,
		vtable::{VTable, VTableId},
	},
	resolved::{
		ResolvedDeferredView, ResolvedDictionary, ResolvedNamespace, ResolvedRingBuffer, ResolvedSeries,
		ResolvedShape, ResolvedTable, ResolvedTableVirtual, ResolvedTransactionalView,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, fragment::Fragment};

use crate::{
	ast::identifier::UnresolvedShapeIdentifier,
	error::{IdentifierError, ShapeNotFoundError},
};

pub enum ResolvedSource {
	Shape(ResolvedShape),
	Remote {
		address: String,
		token: Option<String>,
		local_namespace: String,
		remote_name: String,
	},
}

/// Default namespace for unqualified identifiers
pub const DEFAULT_NAMESPACE: &str = "default";

/// Resolve an unresolved source identifier to a ResolvedSource.
/// Returns `ResolvedSource::Remote` for remote namespaces, or
/// `ResolvedSource::Shape` for local sources.
pub fn resolve_unresolved_source(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	unresolved: &UnresolvedShapeIdentifier,
) -> Result<ResolvedSource> {
	let namespace_str = if !unresolved.namespace.is_empty() {
		unresolved.namespace.iter().map(|s| s.text()).collect::<Vec<_>>().join("::")
	} else {
		DEFAULT_NAMESPACE.to_string()
	};
	let name_str = unresolved.name.text();

	let ns_def = if !unresolved.namespace.is_empty() {
		let ns_fragment = unresolved.namespace[0].to_owned().with_text(&namespace_str);
		catalog.get_namespace_by_name(tx, ns_fragment)?
	} else {
		catalog.get_namespace_by_name(tx, DEFAULT_NAMESPACE)?
	};

	// Check if this is a remote namespace
	if let Some(address) = ns_def.address() {
		return Ok(ResolvedSource::Remote {
			address: address.to_string(),
			token: ns_def.token().map(|s| s.to_string()),
			local_namespace: namespace_str.to_string(),
			remote_name: name_str.to_string(),
		});
	}

	let namespace_fragment = Fragment::internal(ns_def.name().to_string());
	let namespace = ResolvedNamespace::new(namespace_fragment, ns_def.clone());
	let name_fragment = Fragment::internal(name_str.to_string());
	let _alias_fragment = unresolved.alias.as_ref().map(|a| Fragment::internal(a.text()));

	// Check for user-defined virtual tables first (in any namespace)
	if let Some(virtual_def) = catalog.find_vtable_user_by_name(tx, ns_def.id(), name_str) {
		return Ok(ResolvedSource::Shape(ResolvedShape::TableVirtual(ResolvedTableVirtual::new(
			name_fragment,
			namespace,
			(*virtual_def).clone(),
		))));
	}

	// Check if it's a system-managed virtual table namespace (e.g. `system`,
	// `system::metrics::storage`, `system::metrics::cdc`). The downstream
	// dispatch in `compile/vtable.rs` matches by namespace id + leaf name.
	if matches!(
		ns_def.id(),
		NamespaceId::SYSTEM
			| NamespaceId::SYSTEM_METRICS_STORAGE
			| NamespaceId::SYSTEM_METRICS_CDC
			| NamespaceId::SYSTEM_PROCEDURES
	) {
		let def = VTable {
			id: VTableId(0), // Placeholder ID - compile.rs handles actual lookup
			namespace: ns_def.id(),
			name: name_str.to_string(),
			columns: vec![], // Columns are populated at execution time
		};

		return Ok(ResolvedSource::Shape(ResolvedShape::TableVirtual(ResolvedTableVirtual::new(
			name_fragment,
			namespace,
			def,
		))));
	}

	// Try table first
	if let Some(table) = catalog.find_table_by_name(tx, ns_def.id(), name_str)? {
		// ResolvedTable doesn't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		return Ok(ResolvedSource::Shape(ResolvedShape::Table(ResolvedTable::new(
			name_fragment,
			namespace,
			table,
		))));
	}

	// Try ring buffer
	if let Some(ringbuffer) = catalog.find_ringbuffer_by_name(tx, ns_def.id(), name_str)? {
		// ResolvedRingBuffer doesn't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		return Ok(ResolvedSource::Shape(ResolvedShape::RingBuffer(ResolvedRingBuffer::new(
			name_fragment,
			namespace,
			ringbuffer,
		))));
	}

	// Try views FIRST (deferred views share name with their flow)
	if let Some(view) = catalog.find_view_by_name(tx, ns_def.id(), name_str)? {
		// Check view type to create appropriate resolved view
		// ResolvedView types don't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		let shape = match view.kind() {
			ViewKind::Deferred => {
				ResolvedShape::DeferredView(ResolvedDeferredView::new(name_fragment, namespace, view))
			}
			ViewKind::Transactional => ResolvedShape::TransactionalView(ResolvedTransactionalView::new(
				name_fragment,
				namespace,
				view,
			)),
		};
		return Ok(ResolvedSource::Shape(shape));
	}

	// Try dictionaries
	if let Some(dictionary) = catalog.find_dictionary_by_name(tx, ns_def.id(), name_str)? {
		return Ok(ResolvedSource::Shape(ResolvedShape::Dictionary(ResolvedDictionary::new(
			name_fragment,
			namespace,
			dictionary,
		))));
	}

	// Try series
	if let Some(series) = catalog.find_series_by_name(tx, ns_def.id(), name_str)? {
		return Ok(ResolvedSource::Shape(ResolvedShape::Series(ResolvedSeries::new(
			name_fragment,
			namespace,
			series,
		))));
	}

	// Not found
	Err(IdentifierError::SourceNotFound(ShapeNotFoundError {
		namespace: namespace_str.to_string(),
		name: name_str.to_string(),
		fragment: unresolved.name.to_owned(),
	})
	.into())
}
