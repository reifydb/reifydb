// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::{
	catalog::{
		id::NamespaceId,
		view::ViewKind,
		vtable::{VTableDef, VTableId},
	},
	resolved::{
		ResolvedDeferredView, ResolvedDictionary, ResolvedFlow, ResolvedNamespace, ResolvedPrimitive,
		ResolvedRingBuffer, ResolvedSeries, ResolvedTable, ResolvedTableVirtual, ResolvedTransactionalView,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{Result, fragment::Fragment};

use crate::{
	ast::identifier::UnresolvedPrimitiveIdentifier,
	error::{IdentifierError, PrimitiveNotFoundError},
};

/// Default namespace for unqualified identifiers
pub const DEFAULT_NAMESPACE: &str = "default";

/// Resolve an unresolved source identifier to a ResolvedPrimitive
/// This is used when processing From clauses and joins
pub fn resolve_unresolved_source(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	unresolved: &UnresolvedPrimitiveIdentifier,
) -> Result<ResolvedPrimitive> {
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

	let namespace_fragment = Fragment::internal(ns_def.name.clone());
	let namespace = ResolvedNamespace::new(namespace_fragment, ns_def.clone());
	let name_fragment = Fragment::internal(name_str.to_string());
	let _alias_fragment = unresolved.alias.as_ref().map(|a| Fragment::internal(a.text()));

	// Check for user-defined virtual tables first (in any namespace)
	if let Some(virtual_def) = catalog.find_vtable_user_by_name(tx, ns_def.id, name_str) {
		return Ok(ResolvedPrimitive::TableVirtual(ResolvedTableVirtual::new(
			name_fragment,
			namespace,
			(*virtual_def).clone(),
		)));
	}

	// Check if it's a system table (namespace = "system")
	// TODO: This should use proper system table definitions from the catalog
	if namespace_str == "system" {
		let def = VTableDef {
			id: VTableId(0),           // Placeholder ID - compile.rs handles actual lookup
			namespace: NamespaceId(1), // System namespace ID
			name: name_str.to_string(),
			columns: vec![], // Columns are populated at execution time
		};

		return Ok(ResolvedPrimitive::TableVirtual(ResolvedTableVirtual::new(name_fragment, namespace, def)));
	}

	// Try table first
	if let Some(table) = catalog.find_table_by_name(tx, ns_def.id, name_str)? {
		// ResolvedTable doesn't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		return Ok(ResolvedPrimitive::Table(ResolvedTable::new(name_fragment, namespace, table)));
	}

	// Try ring buffer
	if let Some(ringbuffer) = catalog.find_ringbuffer_by_name(tx, ns_def.id, name_str)? {
		// ResolvedRingBuffer doesn't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		return Ok(ResolvedPrimitive::RingBuffer(ResolvedRingBuffer::new(
			name_fragment,
			namespace,
			ringbuffer,
		)));
	}

	// Try views FIRST (deferred views share name with their flow)
	if let Some(view) = catalog.find_view_by_name(tx, ns_def.id, name_str)? {
		// Check view type to create appropriate resolved view
		// ResolvedView types don't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		let resolved_source = match view.kind {
			ViewKind::Deferred => ResolvedPrimitive::DeferredView(ResolvedDeferredView::new(
				name_fragment,
				namespace,
				view,
			)),
			ViewKind::Transactional => ResolvedPrimitive::TransactionalView(
				ResolvedTransactionalView::new(name_fragment, namespace, view),
			),
		};
		return Ok(resolved_source);
	}

	// Try dictionaries
	if let Some(dictionary) = catalog.find_dictionary_by_name(tx, ns_def.id, name_str)? {
		return Ok(ResolvedPrimitive::Dictionary(ResolvedDictionary::new(
			name_fragment,
			namespace,
			dictionary,
		)));
	}

	// Try series
	if let Some(series) = catalog.find_series_by_name(tx, ns_def.id, name_str)? {
		return Ok(ResolvedPrimitive::Series(ResolvedSeries::new(name_fragment, namespace, series)));
	}

	// Try flows (after views, since deferred views take precedence)
	if let Some(flow) = catalog.find_flow_by_name(tx, ns_def.id, name_str)? {
		// ResolvedFlow doesn't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		return Ok(ResolvedPrimitive::Flow(ResolvedFlow::new(name_fragment, namespace, flow)));
	}

	// Not found
	Err(IdentifierError::SourceNotFound(PrimitiveNotFoundError {
		namespace: namespace_str.to_string(),
		name: name_str.to_string(),
		fragment: unresolved.name.to_owned(),
	})
	.into())
}
