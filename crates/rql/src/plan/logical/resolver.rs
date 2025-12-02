// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::{
	Result,
	interface::{
		TableVirtualDef, ViewKind,
		resolved::{
			ResolvedDeferredView, ResolvedDictionary, ResolvedFlow, ResolvedNamespace, ResolvedRingBuffer,
			ResolvedSource, ResolvedTable, ResolvedTableVirtual, ResolvedTransactionalView,
		},
	},
};
use reifydb_type::Fragment;

use crate::ast::identifier::UnresolvedSourceIdentifier;

/// Default namespace for unqualified identifiers
pub const DEFAULT_NAMESPACE: &str = "default";

/// Resolve an unresolved source identifier to a ResolvedSource
/// This is used when processing From clauses and joins
pub fn resolve_unresolved_source(
	tx: &mut impl CatalogQueryTransaction,
	unresolved: &UnresolvedSourceIdentifier,
) -> Result<ResolvedSource<'static>> {
	let namespace_str = if let Some(ref ns) = unresolved.namespace {
		ns.text()
	} else {
		DEFAULT_NAMESPACE
	};
	let name_str = unresolved.name.text();

	// Get namespace
	let ns_def = if let Some(ref ns_fragment) = unresolved.namespace {
		tx.get_namespace_by_name(ns_fragment.clone())?
	} else {
		tx.get_namespace_by_name(DEFAULT_NAMESPACE)?
	};

	let namespace_fragment = Fragment::owned_internal(ns_def.name.clone());
	let namespace = ResolvedNamespace::new(namespace_fragment, ns_def.clone());
	let name_fragment = Fragment::owned_internal(name_str.to_string());
	let _alias_fragment = unresolved.alias.as_ref().map(|a| Fragment::owned_internal(a.text()));

	// Check if it's a system table
	// FIXME this is broken
	if namespace_str == "system" {
		// For system tables, we use a placeholder TableVirtualDef
		// In a real implementation, this would come from the system catalog
		use reifydb_core::interface::{NamespaceId, TableVirtualId};
		let def = TableVirtualDef {
			id: TableVirtualId(0),     // Placeholder ID
			namespace: NamespaceId(0), // System namespace ID
			name: name_str.to_string(),
			columns: vec![], // Would be populated with actual columns
		};

		// ResolvedTableVirtual doesn't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		return Ok(ResolvedSource::TableVirtual(ResolvedTableVirtual::new(name_fragment, namespace, def)));
	}

	// Try table first
	if let Some(table) = tx.find_table_by_name(ns_def.id, name_str)? {
		// ResolvedTable doesn't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		return Ok(ResolvedSource::Table(ResolvedTable::new(name_fragment, namespace, table)));
	}

	// Try ring buffer
	if let Some(ring_buffer) = tx.find_ring_buffer_by_name(ns_def.id, name_str)? {
		// ResolvedRingBuffer doesn't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		return Ok(ResolvedSource::RingBuffer(ResolvedRingBuffer::new(name_fragment, namespace, ring_buffer)));
	}

	// Try flows
	if let Some(flow) = tx.find_flow_by_name(ns_def.id, name_str)? {
		// ResolvedFlow doesn't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		return Ok(ResolvedSource::Flow(ResolvedFlow::new(name_fragment, namespace, flow)));
	}

	// Try dictionaries
	if let Some(dictionary) = tx.find_dictionary_by_name(ns_def.id, name_str)? {
		return Ok(ResolvedSource::Dictionary(ResolvedDictionary::new(name_fragment, namespace, dictionary)));
	}

	// Try views
	if let Some(view) = tx.find_view_by_name(ns_def.id, name_str)? {
		// Check view type to create appropriate resolved view
		// ResolvedView types don't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		let resolved_source = match view.kind {
			ViewKind::Deferred => {
				ResolvedSource::DeferredView(ResolvedDeferredView::new(name_fragment, namespace, view))
			}
			ViewKind::Transactional => ResolvedSource::TransactionalView(ResolvedTransactionalView::new(
				name_fragment,
				namespace,
				view,
			)),
		};
		return Ok(resolved_source);
	}

	// Not found
	Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
		namespace: namespace_str.to_string(),
		name: name_str.to_string(),
		fragment: unresolved.name.clone().into_owned(),
	})
	.into())
}
