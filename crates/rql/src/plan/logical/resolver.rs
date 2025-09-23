// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use reifydb_catalog::{CatalogQueryTransaction, system::SystemCatalog};
use reifydb_core::{
	Result,
	interface::{
		TableVirtualDef, ViewKind,
		identifier::{
			NamespaceIdentifier, RingBufferIdentifier, SourceIdentifier, TableIdentifier,
			TableVirtualIdentifier, UnresolvedSourceIdentifier, ViewIdentifier,
		},
		resolved::{
			ResolvedDeferredView, ResolvedNamespace, ResolvedRingBuffer, ResolvedSource, ResolvedTable,
			ResolvedTableVirtual, ResolvedTransactionalView, ResolvedView,
		},
	},
};
use reifydb_type::{Fragment, OwnedFragment};

use crate::ast::identifier::{
	MaybeQualifiedDeferredViewIdentifier, MaybeQualifiedRingBufferIdentifier, MaybeQualifiedTableIdentifier,
	MaybeQualifiedTransactionalViewIdentifier, MaybeQualifiedViewIdentifier,
};

/// Default namespace for unqualified identifiers
pub const DEFAULT_NAMESPACE: &str = "default";

// Helper methods

/// Create a SourceIdentifier from resolved components and source kind
// Helper method to create a source identifier from an unresolved source
// by determining its type from the catalog
fn create_source_identifier_from_catalog(
	tx: &mut impl CatalogQueryTransaction,
	namespace: Fragment<'static>,
	name: Fragment<'static>,
	alias: Option<Fragment<'static>>,
) -> Result<SourceIdentifier<'static>> {
	let namespace_str = namespace.text();
	let name_str = name.text();

	// Check if it's a system table (virtual table in system
	// namespace)
	if namespace_str == "system" {
		// Check if it's a known system table
		if is_system_table(name_str) {
			let mut t = TableVirtualIdentifier::new(namespace, name);
			if let Some(a) = alias {
				t = t.with_alias(a);
			}
			return Ok(SourceIdentifier::TableVirtual(t));
		}
	}

	// Get the namespace ID
	let ns = tx.get_namespace_by_name(namespace_str)?;

	// Check for regular table
	if tx.find_table_by_name(ns.id, name_str)?.is_some() {
		let mut t = TableIdentifier::new(namespace, name);
		if let Some(a) = alias {
			t = t.with_alias(a);
		}
		return Ok(SourceIdentifier::Table(t));
	}

	// Check for ring buffer
	if tx.find_ring_buffer_by_name(ns.id, name_str)?.is_some() {
		let mut rb = RingBufferIdentifier::new(namespace, name);
		if let Some(a) = alias {
			rb = rb.with_alias(a);
		}
		return Ok(SourceIdentifier::RingBuffer(rb));
	}

	// Check for view and determine its type
	if let Some(view) = tx.find_view_by_name(ns.id, name_str)? {
		match view.kind {
			ViewKind::Deferred => {
				let mut v = ViewIdentifier::new(namespace, name);
				if let Some(a) = alias {
					v = v.with_alias(a);
				}
				Ok(SourceIdentifier::View(v))
			}
			ViewKind::Transactional => {
				let mut v = ViewIdentifier::new(namespace, name);
				if let Some(a) = alias {
					v = v.with_alias(a);
				}
				Ok(SourceIdentifier::View(v))
			}
		}
	} else {
		// Source not found
		Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
			namespace: namespace_str.to_string(),
			name: name_str.to_string(),
			fragment: name.clone().into_owned(),
		})
		.into())
	}
}

// Resolution methods that return resolved types

/// Build a resolved namespace
pub fn build_resolved_namespace<'a>(
	tx: &mut impl CatalogQueryTransaction,
	ident: NamespaceIdentifier<'a>,
) -> Result<ResolvedNamespace<'a>> {
	let namespace_name = ident.name.text();

	// Lookup in catalog - get_namespace_by_name returns
	// Result<NamespaceDef>
	let def = tx.get_namespace_by_name(namespace_name)?;

	let resolved = ResolvedNamespace::new(ident, def);

	Ok(resolved)
}

/// Build a resolved table
pub fn build_resolved_table<'a>(
	tx: &mut impl CatalogQueryTransaction,
	ident: SourceIdentifier<'a>,
) -> Result<ResolvedTable<'a>> {
	// Extract the TableIdentifier from the enum
	let table_ident = match ident {
		SourceIdentifier::Table(t) => t,
		_ => {
			// Create a TableIdentifier from other variants
			TableIdentifier {
				namespace: ident.namespace().clone(),
				name: ident.name().clone(),
				alias: ident.alias().cloned(),
			}
		}
	};

	// Resolve namespace first
	let namespace_ident = NamespaceIdentifier {
		name: table_ident.namespace.clone(),
	};
	let namespace = build_resolved_namespace(tx, namespace_ident)?;

	// Lookup table in catalog
	let table_name = table_ident.name.text();
	let def = tx.find_table_by_name(namespace.def().id, table_name)?.ok_or_else(|| -> reifydb_core::Error {
		// Return an error instead of panicking
		crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
			namespace: namespace.def().name.clone(),
			name: table_name.to_string(),
			fragment: table_ident.name.clone().into_owned(),
		})
		.into()
	})?;

	Ok(ResolvedTable::new(table_ident, namespace, def))
}

/// Build a resolved ring buffer
pub fn build_resolved_ring_buffer<'a>(
	tx: &mut impl CatalogQueryTransaction,
	ident: SourceIdentifier<'a>,
) -> Result<ResolvedRingBuffer<'a>> {
	// Extract the RingBufferIdentifier from the enum
	let ring_buffer_ident = match ident {
		SourceIdentifier::RingBuffer(rb) => rb,
		_ => {
			// Create a RingBufferIdentifier from other variants
			RingBufferIdentifier {
				namespace: ident.namespace().clone(),
				name: ident.name().clone(),
				alias: ident.alias().cloned(),
			}
		}
	};

	// Resolve namespace first
	let namespace_ident = NamespaceIdentifier {
		name: ring_buffer_ident.namespace.clone(),
	};
	let namespace = build_resolved_namespace(tx, namespace_ident)?;

	// Lookup ring buffer in catalog
	let ring_buffer_name = ring_buffer_ident.name.text();
	let def = tx.find_ring_buffer_by_name(namespace.def().id, ring_buffer_name)?.ok_or_else(
		|| -> reifydb_core::Error {
			// Return an error instead of panicking
			crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
				namespace: namespace.def().name.clone(),
				name: ring_buffer_name.to_string(),
				fragment: ring_buffer_ident.name.clone().into_owned(),
			})
			.into()
		},
	)?;

	Ok(ResolvedRingBuffer::new(ring_buffer_ident, namespace, def))
}

/// Build a resolved view
pub fn build_resolved_view<'a>(
	tx: &mut impl CatalogQueryTransaction,
	ident: SourceIdentifier<'a>,
) -> Result<ResolvedView<'a>> {
	let view_ident = match ident {
		SourceIdentifier::View(v) => v,
		_ => {
			unreachable!()
		}
	};

	// Resolve namespace first
	let namespace_ident = NamespaceIdentifier {
		name: view_ident.namespace.clone(),
	};
	let namespace = build_resolved_namespace(tx, namespace_ident)?;

	// Lookup view in catalog
	let view_name = view_ident.name.text();
	let view_name_fragment = view_ident.name.clone();
	let def = tx.find_view_by_name(namespace.def().id, view_name)?.ok_or_else(|| -> reifydb_core::Error {
		// Return an error instead of panicking
		crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
			namespace: namespace.def().name.clone(),
			name: view_name.to_string(),
			fragment: view_name_fragment.clone().into_owned(),
		})
		.into()
	})?;

	Ok(ResolvedView::new(view_ident, namespace, def))
}

/// Build a resolved source from an unresolved identifier
pub fn build_resolved_source_from_unresolved<'a>(
	tx: &mut impl CatalogQueryTransaction,
	ident: UnresolvedSourceIdentifier<'a>,
) -> Result<ResolvedSource<'a>> {
	// Try to determine the source type from the catalog
	let name_text = ident.name.text();

	// Create the resolved namespace fragment and get namespace text
	let (namespace_fragment, namespace_text) = match ident.namespace {
		Some(ns) => {
			let text = ns.text().to_string();
			(Fragment::Owned(ns.into_owned()), text)
		}
		None => {
			let text = DEFAULT_NAMESPACE.to_string();
			(
				Fragment::Owned(OwnedFragment::Internal {
					text: text.clone(),
				}),
				text,
			)
		}
	};

	// First check if it's a system table
	if is_system_table(name_text) {
		let mut t = TableVirtualIdentifier::new(
			namespace_fragment.clone(),
			Fragment::Owned(ident.name.into_owned()),
		);
		if let Some(alias) = ident.alias {
			t = t.with_alias(alias);
		}
		let source = SourceIdentifier::TableVirtual(t);
		return build_resolved_source(tx, source);
	}

	// Try to find it as a table
	let ns = tx.get_namespace_by_name(&namespace_text)?;
	if tx.find_table_by_name(ns.id, name_text)?.is_some() {
		let mut t = TableIdentifier::new(namespace_fragment.clone(), Fragment::Owned(ident.name.into_owned()));
		if let Some(alias) = ident.alias {
			t = t.with_alias(alias);
		}
		let source = SourceIdentifier::Table(t);
		return build_resolved_source(tx, source);
	}

	// Try to find it as a ring buffer
	if tx.find_ring_buffer_by_name(ns.id, name_text)?.is_some() {
		let mut rb =
			RingBufferIdentifier::new(namespace_fragment.clone(), Fragment::Owned(ident.name.into_owned()));
		if let Some(alias) = ident.alias {
			rb = rb.with_alias(alias);
		}
		let source = SourceIdentifier::RingBuffer(rb);
		return build_resolved_source(tx, source);
	}

	// Try to find it as a view
	if let Some(view) = tx.find_view_by_name(ns.id, name_text)? {
		use reifydb_core::interface::ViewKind;
		match view.kind {
			ViewKind::Deferred => {
				let mut v = ViewIdentifier::new(
					namespace_fragment.clone(),
					Fragment::Owned(ident.name.into_owned()),
				);
				if let Some(alias) = ident.alias {
					v = v.with_alias(alias);
				}
				let source = SourceIdentifier::View(v);
				build_resolved_source(tx, source)
			}
			ViewKind::Transactional => {
				let mut v = ViewIdentifier::new(
					namespace_fragment,
					Fragment::Owned(ident.name.into_owned()),
				);
				if let Some(alias) = ident.alias {
					v = v.with_alias(alias);
				}
				let source = SourceIdentifier::View(v);
				build_resolved_source(tx, source)
			}
		}
	} else {
		// Source not found
		Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
			namespace: namespace_text.to_string(),
			name: name_text.to_string(),
			fragment: ident.name.into_owned(),
		})
		.into())
	}
}

/// Build a resolved source (any type)
pub fn build_resolved_source<'a>(
	tx: &mut impl CatalogQueryTransaction,
	ident: SourceIdentifier<'a>,
) -> Result<ResolvedSource<'a>> {
	let namespace_name = ident.namespace().text();
	let source_name = ident.name().text();

	// Check if it's a system virtual table
	if namespace_name == "system" {
		if let Some(def) = get_system_table_def(source_name) {
			// For system tables, we need to get the system
			// namespace
			let namespace_ident = NamespaceIdentifier {
				name: ident.namespace().clone(),
			};
			// Build a resolved namespace for "system"
			// Since system namespace might not exist in the
			// catalog, we create a synthetic one
			let namespace = ResolvedNamespace::new(
				namespace_ident,
				reifydb_core::interface::NamespaceDef {
					id: reifydb_core::interface::NamespaceId(1), // System namespace ID
					name: "system".to_string(),
				},
			);

			// Extract or create TableVirtualIdentifier
			let virtual_ident = match ident {
				SourceIdentifier::TableVirtual(t) => t,
				_ => TableVirtualIdentifier {
					namespace: ident.namespace().clone(),
					name: ident.name().clone(),
					alias: ident.alias().cloned(),
				},
			};
			let virtual_table = ResolvedTableVirtual::new(virtual_ident, namespace, (*def).clone());
			let resolved = ResolvedSource::TableVirtual(virtual_table);
			return Ok(resolved);
		}
	}

	// Try to resolve as table first
	if let Ok(table) = build_resolved_table(tx, ident.clone()) {
		let resolved = ResolvedSource::Table(table);
		return Ok(resolved);
	}

	// Try to resolve as ring buffer
	if let Ok(ring_buffer) = build_resolved_ring_buffer(tx, ident.clone()) {
		let resolved = ResolvedSource::RingBuffer(ring_buffer);
		return Ok(resolved);
	}

	// Try to resolve as view
	if let Ok(view) = build_resolved_view(tx, ident.clone()) {
		// Check view kind and create appropriate resolved type
		let resolved = match view.def().kind {
			ViewKind::Deferred => {
				let deferred_ident = match ident {
					SourceIdentifier::View(d) => d,
					_ => ViewIdentifier {
						namespace: ident.namespace().clone(),
						name: ident.name().clone(),
						alias: ident.alias().cloned(),
					},
				};
				let deferred = ResolvedDeferredView::new(
					deferred_ident,
					view.namespace().clone(),
					view.def().clone(),
				);
				ResolvedSource::DeferredView(deferred)
			}
			ViewKind::Transactional => {
				let trans_ident = match ident {
					SourceIdentifier::View(t) => t,
					_ => ViewIdentifier {
						namespace: ident.namespace().clone(),
						name: ident.name().clone(),
						alias: ident.alias().cloned(),
					},
				};
				let transactional = ResolvedTransactionalView::new(
					trans_ident,
					view.namespace().clone(),
					view.def().clone(),
				);
				ResolvedSource::TransactionalView(transactional)
			}
		};

		return Ok(resolved);
	}

	// Source not found - return proper error
	Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
		namespace: namespace_name.to_string(),
		name: source_name.to_string(),
		fragment: ident.name().clone().into_owned(),
	})
	.into())
}

/// Helper to check if a name is a known system table
fn is_system_table(name: &str) -> bool {
	matches!(
		name,
		"sequences"
			| "namespaces" | "tables"
			| "views" | "columns" | "column_policies"
			| "primary_keys" | "primary_key_columns"
			| "versions"
	)
}

/// Helper to get system table definition
fn get_system_table_def(name: &str) -> Option<Arc<TableVirtualDef>> {
	match name {
		"sequences" => Some(SystemCatalog::get_system_sequences_table_def()),
		"namespaces" => Some(SystemCatalog::get_system_namespaces_table_def()),
		"tables" => Some(SystemCatalog::get_system_tables_table_def()),
		"views" => Some(SystemCatalog::get_system_views_table_def()),
		"columns" => Some(SystemCatalog::get_system_columns_table_def()),
		"column_policies" => Some(SystemCatalog::get_system_column_policies_table_def()),
		"primary_keys" => Some(SystemCatalog::get_system_primary_keys_table_def()),
		"primary_key_columns" => Some(SystemCatalog::get_system_primary_key_columns_table_def()),
		"versions" => Some(SystemCatalog::get_system_versions_table_def()),
		_ => None,
	}
}

/// Resolve a MaybeQualifiedTableIdentifier specifically
pub fn resolve_maybe_qualified_table<'a>(
	tx: &mut impl CatalogQueryTransaction,
	source: &MaybeQualifiedTableIdentifier<'a>,
	validate_existence: bool,
) -> Result<TableIdentifier<'static>> {
	// Get the table name
	let name_text = source.name.text();

	// Always validate namespace exists (can't create table in
	// non-existent namespace) Get namespace, passing the fragment
	// if available for error reporting
	let ns = if let Some(namespace_fragment) = &source.namespace {
		tx.get_namespace_by_name(namespace_fragment.clone())?
	} else {
		tx.get_namespace_by_name(DEFAULT_NAMESPACE)?
	};

	// Only validate table existence if requested
	if validate_existence {
		if tx.find_table_by_name(ns.id, name_text)?.is_none() {
			return Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
				namespace: ns.name.clone(),
				name: name_text.to_string(),
				fragment: source.name.clone().into_owned(),
			})
			.into());
		}
	}

	// Get namespace text for creating the identifier
	let namespace_text = source.namespace.as_ref().map(|ns| ns.text()).unwrap_or(DEFAULT_NAMESPACE);

	// Create the TableIdentifier preserving original fragments
	use reifydb_type::{Fragment, OwnedFragment};

	// For namespace, use the original fragment if available,
	// otherwise create Internal
	let namespace_fragment = if let Some(ns_frag) = &source.namespace {
		Fragment::Owned(ns_frag.clone().into_owned())
	} else {
		Fragment::Owned(OwnedFragment::Internal {
			text: namespace_text.to_string(),
		})
	};

	// For name, always preserve the original fragment for error
	// reporting
	let name_fragment = Fragment::Owned(source.name.clone().into_owned());

	let mut table = TableIdentifier::new(namespace_fragment, name_fragment);
	if let Some(alias) = &source.alias {
		table.alias = Some(Fragment::Owned(alias.clone().into_owned()));
	}

	Ok(table)
}

/// Resolve a MaybeQualifiedRingBufferIdentifier specifically
pub fn resolve_maybe_qualified_ring_buffer<'a>(
	tx: &mut impl CatalogQueryTransaction,
	source: &MaybeQualifiedRingBufferIdentifier<'a>,
	validate_existence: bool,
) -> Result<RingBufferIdentifier<'static>> {
	// Get the ring buffer name
	let name_text = source.name.text();

	// Always validate namespace exists (can't create ring buffer in
	// non-existent namespace) Get namespace, passing the fragment
	// if available for error reporting
	let ns = if let Some(namespace_fragment) = &source.namespace {
		tx.get_namespace_by_name(namespace_fragment.clone())?
	} else {
		tx.get_namespace_by_name(DEFAULT_NAMESPACE)?
	};

	// Only validate ring buffer existence if requested
	if validate_existence {
		if tx.find_ring_buffer_by_name(ns.id, name_text)?.is_none() {
			return Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
				namespace: ns.name.clone(),
				name: name_text.to_string(),
				fragment: source.name.clone().into_owned(),
			})
			.into());
		}
	}

	// Create namespace text for the identifier
	let namespace_text = ns.name.as_str();

	use reifydb_type::{Fragment, OwnedFragment};

	// For namespace, use the original fragment if available,
	// otherwise create Internal
	let namespace_fragment = if let Some(ns_frag) = &source.namespace {
		Fragment::Owned(ns_frag.clone().into_owned())
	} else {
		Fragment::Owned(OwnedFragment::Internal {
			text: namespace_text.to_string(),
		})
	};

	// For name, always preserve the original fragment for error
	// reporting
	let name_fragment = Fragment::Owned(source.name.clone().into_owned());

	let mut ring_buffer = RingBufferIdentifier::new(namespace_fragment, name_fragment);

	// Handle alias if present
	if let Some(alias) = &source.alias {
		ring_buffer = ring_buffer.with_alias(Fragment::Owned(alias.clone().into_owned()));
	}

	Ok(ring_buffer)
}

/// Helper method to resolve an unresolved source as a table
pub fn resolve_source_as_table<'a>(
	tx: &mut impl CatalogQueryTransaction,
	namespace: Option<&Fragment<'a>>,
	name: &Fragment<'a>,
	validate_existence: bool,
) -> Result<TableIdentifier<'static>> {
	use crate::ast::identifier::MaybeQualifiedTableIdentifier;

	let table_id = if let Some(ns) = namespace {
		MaybeQualifiedTableIdentifier::new(name.clone()).with_namespace(ns.clone())
	} else {
		MaybeQualifiedTableIdentifier::new(name.clone())
	};

	resolve_maybe_qualified_table(tx, &table_id, validate_existence)
}

/// Helper method to resolve an unresolved source as a ring buffer
pub fn resolve_source_as_ring_buffer<'a>(
	tx: &mut impl CatalogQueryTransaction,
	namespace: Option<&Fragment<'a>>,
	name: &Fragment<'a>,
	validate_existence: bool,
) -> Result<RingBufferIdentifier<'static>> {
	use crate::ast::identifier::MaybeQualifiedRingBufferIdentifier;

	let ring_buffer_id = if let Some(ns) = namespace {
		MaybeQualifiedRingBufferIdentifier::new(name.clone()).with_namespace(ns.clone())
	} else {
		MaybeQualifiedRingBufferIdentifier::new(name.clone())
	};

	resolve_maybe_qualified_ring_buffer(tx, &ring_buffer_id, validate_existence)
}

/// Resolve a MaybeQualifiedDeferredViewIdentifier specifically
pub fn resolve_maybe_qualified_deferred_view<'a>(
	tx: &mut impl CatalogQueryTransaction,
	source: &MaybeQualifiedDeferredViewIdentifier<'a>,
	validate_existence: bool,
) -> Result<ViewIdentifier<'static>> {
	// Get the view name
	let name_text = source.name.text();

	// Always validate namespace exists (can't create view in
	// non-existent namespace) Get namespace, passing the fragment
	// if available for error reporting
	let ns = if let Some(namespace_fragment) = &source.namespace {
		tx.get_namespace_by_name(namespace_fragment.clone())?
	} else {
		tx.get_namespace_by_name(DEFAULT_NAMESPACE)?
	};

	// Only validate view existence if requested
	if validate_existence {
		if tx.find_view_by_name(ns.id, name_text)?.is_none() {
			return Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
				namespace: ns.name.clone(),
				name: name_text.to_string(),
				fragment: source.name.clone().into_owned(),
			})
			.into());
		}
	}

	// Get namespace text for creating the identifier
	let namespace_text = source.namespace.as_ref().map(|ns| ns.text()).unwrap_or(DEFAULT_NAMESPACE);

	// Create the DeferredViewIdentifier preserving original
	// fragments
	use reifydb_type::{Fragment, OwnedFragment};

	// For namespace, use the original fragment if available,
	// otherwise create Internal
	let namespace_fragment = if let Some(ns_frag) = &source.namespace {
		Fragment::Owned(ns_frag.clone().into_owned())
	} else {
		Fragment::Owned(OwnedFragment::Internal {
			text: namespace_text.to_string(),
		})
	};

	// For name, always preserve the original fragment for error
	// reporting
	let name_fragment = Fragment::Owned(source.name.clone().into_owned());

	let mut view = ViewIdentifier::new(namespace_fragment, name_fragment);
	if let Some(alias) = &source.alias {
		view.alias = Some(Fragment::Owned(alias.clone().into_owned()));
	}

	Ok(view)
}

/// Resolve a MaybeQualifiedTransactionalViewIdentifier specifically
pub fn resolve_maybe_qualified_transactional_view<'a>(
	tx: &mut impl CatalogQueryTransaction,
	source: &MaybeQualifiedTransactionalViewIdentifier<'a>,
	validate_existence: bool,
) -> Result<ViewIdentifier<'static>> {
	// Get the view name
	let name_text = source.name.text();

	// Always validate namespace exists (can't create view in
	// non-existent namespace) Get namespace, passing the fragment
	// if available for error reporting
	let ns = if let Some(namespace_fragment) = &source.namespace {
		tx.get_namespace_by_name(namespace_fragment.clone())?
	} else {
		tx.get_namespace_by_name(DEFAULT_NAMESPACE)?
	};

	// Only validate view existence if requested
	if validate_existence {
		if tx.find_view_by_name(ns.id, name_text)?.is_none() {
			return Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
				namespace: ns.name.clone(),
				name: name_text.to_string(),
				fragment: source.name.clone().into_owned(),
			})
			.into());
		}
	}

	// Get namespace text for creating the identifier
	let namespace_text = source.namespace.as_ref().map(|ns| ns.text()).unwrap_or(DEFAULT_NAMESPACE);

	// Create the TransactionalViewIdentifier preserving original
	// fragments
	use reifydb_type::{Fragment, OwnedFragment};

	// For namespace, use the original fragment if available,
	// otherwise create Internal
	let namespace_fragment = if let Some(ns_frag) = &source.namespace {
		Fragment::Owned(ns_frag.clone().into_owned())
	} else {
		Fragment::Owned(OwnedFragment::Internal {
			text: namespace_text.to_string(),
		})
	};

	// For name, always preserve the original fragment for error
	// reporting
	let name_fragment = Fragment::Owned(source.name.clone().into_owned());

	let mut view = ViewIdentifier::new(namespace_fragment, name_fragment);
	if let Some(alias) = &source.alias {
		view.alias = Some(Fragment::Owned(alias.clone().into_owned()));
	}

	Ok(view)
}

/// Resolve a MaybeQualifiedViewIdentifier (generic view)
pub fn resolve_maybe_qualified_view<'a>(
	tx: &mut impl CatalogQueryTransaction,
	source: &MaybeQualifiedViewIdentifier<'a>,
	validate_existence: bool,
) -> Result<SourceIdentifier<'static>> {
	// Get the view name
	let name_text = source.name.text();

	// Validate if requested
	if validate_existence {
		// Get namespace, passing the fragment if available for
		// error reporting
		let ns = if let Some(namespace_fragment) = &source.namespace {
			tx.get_namespace_by_name(namespace_fragment.clone())?
		} else {
			tx.get_namespace_by_name(DEFAULT_NAMESPACE)?
		};

		// Check if it exists as a view and determine its type
		if tx.find_view_by_name(ns.id, name_text)?.is_none() {
			return Err(crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
				namespace: ns.name.clone(),
				name: name_text.to_string(),
				fragment: source.name.clone().into_owned(),
			})
			.into());
		}

		// Get the view to determine its type
		let view = tx.find_view_by_name(ns.id, name_text)?.ok_or_else(|| {
			crate::error::IdentifierError::SourceNotFound(crate::error::SourceNotFoundError {
				namespace: ns.name.clone(),
				name: name_text.to_string(),
				fragment: source.name.clone().into_owned(),
			})
		})?;

		// Get namespace text for creating the identifier
		let namespace_text = source.namespace.as_ref().map(|ns| ns.text()).unwrap_or(DEFAULT_NAMESPACE);

		// Create owned fragments
		use reifydb_type::{Fragment, OwnedFragment};
		let namespace_fragment = Fragment::Owned(OwnedFragment::Internal {
			text: namespace_text.to_string(),
		});
		let name_fragment = Fragment::Owned(OwnedFragment::Internal {
			text: name_text.to_string(),
		});

		// Create the appropriate view identifier based on type
		match view.kind {
			ViewKind::Deferred => {
				let mut v = ViewIdentifier::new(namespace_fragment, name_fragment);
				if let Some(alias) = &source.alias {
					v.alias = Some(Fragment::Owned(alias.clone().into_owned()));
				}
				Ok(SourceIdentifier::View(v))
			}
			ViewKind::Transactional => {
				let mut v = ViewIdentifier::new(namespace_fragment, name_fragment);
				if let Some(alias) = &source.alias {
					v.alias = Some(Fragment::Owned(alias.clone().into_owned()));
				}
				Ok(SourceIdentifier::View(v))
			}
		}
	} else {
		// Get namespace text for creating the identifier
		let namespace_text = source.namespace.as_ref().map(|ns| ns.text()).unwrap_or(DEFAULT_NAMESPACE);

		// Create owned fragments
		use reifydb_type::{Fragment, OwnedFragment};
		let namespace_fragment = Fragment::Owned(OwnedFragment::Internal {
			text: namespace_text.to_string(),
		});
		let name_fragment = Fragment::Owned(OwnedFragment::Internal {
			text: name_text.to_string(),
		});

		// For ALTER VIEW without validation, we can't determine
		// the type Default to DeferredView for now
		let mut v = ViewIdentifier::new(namespace_fragment, name_fragment);
		if let Some(alias) = &source.alias {
			v.alias = Some(Fragment::Owned(alias.clone().into_owned()));
		}
		Ok(SourceIdentifier::View(v))
	}
}

/// Resolve an unresolved source identifier (used in FROM clauses where
/// type is unknown)
pub fn resolve_unresolved_source<'a>(
	tx: &mut impl CatalogQueryTransaction,
	source: &UnresolvedSourceIdentifier<'a>,
) -> Result<SourceIdentifier<'static>> {
	// Resolve namespace - use default if not provided
	let namespace_fragment = match &source.namespace {
		Some(ns) => Fragment::Owned(ns.clone().into_owned()),
		None => Fragment::Owned(OwnedFragment::Internal {
			text: DEFAULT_NAMESPACE.to_string(),
		}),
	};
	let name_fragment = Fragment::Owned(source.name.clone().into_owned());
	let alias_fragment = source.alias.as_ref().map(|a| Fragment::Owned(a.clone().into_owned()));

	// Use helper to determine type from catalog
	create_source_identifier_from_catalog(tx, namespace_fragment, name_fragment, alias_fragment)
}
