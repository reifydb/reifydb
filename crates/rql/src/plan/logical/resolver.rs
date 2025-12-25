// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_catalog::CatalogQueryTransaction;
use reifydb_core::{
	Result,
	interface::{
		TableVirtualDef, ViewKind,
		resolved::{
			ResolvedDeferredView, ResolvedDictionary, ResolvedFlow, ResolvedNamespace, ResolvedPrimitive,
			ResolvedRingBuffer, ResolvedTable, ResolvedTableVirtual, ResolvedTransactionalView,
		},
	},
};
use reifydb_type::Fragment;

use crate::ast::identifier::UnresolvedPrimitiveIdentifier;

/// Default namespace for unqualified identifiers
pub const DEFAULT_NAMESPACE: &str = "default";

/// Resolve an unresolved source identifier to a ResolvedPrimitive
/// This is used when processing From clauses and joins
pub async fn resolve_unresolved_source(
	tx: &mut impl CatalogQueryTransaction,
	unresolved: &UnresolvedPrimitiveIdentifier,
) -> Result<ResolvedPrimitive> {
	let namespace_str = if let Some(ref ns) = unresolved.namespace {
		ns.text()
	} else {
		DEFAULT_NAMESPACE
	};
	let name_str = unresolved.name.text();

	// Get namespace
	let ns_def = if let Some(ref ns_fragment) = unresolved.namespace {
		tx.get_namespace_by_name(ns_fragment.clone()).await?
	} else {
		tx.get_namespace_by_name(DEFAULT_NAMESPACE).await?
	};

	let namespace_fragment = Fragment::internal(ns_def.name.clone());
	let namespace = ResolvedNamespace::new(namespace_fragment, ns_def.clone());
	let name_fragment = Fragment::internal(name_str.to_string());
	let _alias_fragment = unresolved.alias.as_ref().map(|a| Fragment::internal(a.text()));

	// Check for user-defined virtual tables first (in any namespace)
	if let Some(virtual_def) = tx.find_table_virtual_user_by_name(ns_def.id, name_str) {
		return Ok(ResolvedPrimitive::TableVirtual(ResolvedTableVirtual::new(
			name_fragment,
			namespace,
			(*virtual_def).clone(),
		)));
	}

	// Check if it's a system table (namespace = "system")
	// TODO: This should use proper system table definitions from the catalog
	if namespace_str == "system" {
		use reifydb_core::interface::{NamespaceId, TableVirtualId};
		let def = TableVirtualDef {
			id: TableVirtualId(0),     // Placeholder ID - compile.rs handles actual lookup
			namespace: NamespaceId(1), // System namespace ID
			name: name_str.to_string(),
			columns: vec![], // Columns are populated at execution time
		};

		return Ok(ResolvedPrimitive::TableVirtual(ResolvedTableVirtual::new(name_fragment, namespace, def)));
	}

	// Try table first
	if let Some(table) = tx.find_table_by_name(ns_def.id, name_str).await? {
		// ResolvedTable doesn't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		return Ok(ResolvedPrimitive::Table(ResolvedTable::new(name_fragment, namespace, table)));
	}

	// Try ring buffer
	if let Some(ringbuffer) = tx.find_ringbuffer_by_name(ns_def.id, name_str).await? {
		// ResolvedRingBuffer doesn't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		return Ok(ResolvedPrimitive::RingBuffer(ResolvedRingBuffer::new(
			name_fragment,
			namespace,
			ringbuffer,
		)));
	}

	// Try views FIRST (deferred views share name with their flow)
	if let Some(view) = tx.find_view_by_name(ns_def.id, name_str).await? {
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
	if let Some(dictionary) = tx.find_dictionary_by_name(ns_def.id, name_str).await? {
		return Ok(ResolvedPrimitive::Dictionary(ResolvedDictionary::new(
			name_fragment,
			namespace,
			dictionary,
		)));
	}

	// Try flows (after views, since deferred views take precedence)
	if let Some(flow) = tx.find_flow_by_name(ns_def.id, name_str).await? {
		// ResolvedFlow doesn't support aliases, so we'll need to handle this differently
		// For now, just create without alias
		return Ok(ResolvedPrimitive::Flow(ResolvedFlow::new(name_fragment, namespace, flow)));
	}

	// Not found
	Err(crate::error::IdentifierError::SourceNotFound(crate::error::PrimitiveNotFoundError {
		namespace: namespace_str.to_string(),
		name: name_str.to_string(),
		fragment: unresolved.name.clone(),
	})
	.into())
}
