// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::{
	catalog::{
		id::NamespaceId,
		namespace::Namespace,
		view::ViewKind,
		vtable::{VTable, VTableId},
	},
	resolved::{
		ResolvedDeferredView, ResolvedDictionary, ResolvedNamespace, ResolvedRingBuffer, ResolvedSegmentTree,
		ResolvedSeries, ResolvedShape, ResolvedTable, ResolvedTableVirtual, ResolvedTransactionalView,
	},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{Result, fragment::Fragment};

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

pub const DEFAULT_NAMESPACE: &str = "default";

pub fn resolve_unresolved_source(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	unresolved: &UnresolvedShapeIdentifier,
) -> Result<ResolvedSource> {
	let (namespace_str, ns_def) = resolve_namespace_context(catalog, tx, unresolved)?;
	let name_str = unresolved.name.text();

	if let Some(remote) = remote_source(&ns_def, &namespace_str, name_str) {
		return Ok(remote);
	}

	probe_shape_in_namespace(catalog, tx, unresolved, &ns_def, &namespace_str, name_str)
}

#[inline]
fn resolve_namespace_context(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	unresolved: &UnresolvedShapeIdentifier,
) -> Result<(String, Namespace)> {
	let namespace_str = if !unresolved.namespace.is_empty() {
		unresolved.namespace.iter().map(|s| s.text()).collect::<Vec<_>>().join("::")
	} else {
		DEFAULT_NAMESPACE.to_string()
	};

	let ns_def = if !unresolved.namespace.is_empty() {
		let ns_fragment = unresolved.namespace[0].to_owned().with_text(&namespace_str);
		catalog.get_namespace_by_name(tx, ns_fragment)?
	} else {
		catalog.get_namespace_by_name(tx, DEFAULT_NAMESPACE)?
	};

	Ok((namespace_str, ns_def))
}

#[inline]
fn remote_source(ns_def: &Namespace, namespace_str: &str, name_str: &str) -> Option<ResolvedSource> {
	ns_def.address().map(|address| ResolvedSource::Remote {
		address: address.to_string(),
		token: ns_def.token().map(|s| s.to_string()),
		local_namespace: namespace_str.to_string(),
		remote_name: name_str.to_string(),
	})
}

#[inline]
fn probe_shape_in_namespace(
	catalog: &Catalog,
	tx: &mut Transaction<'_>,
	unresolved: &UnresolvedShapeIdentifier,
	ns_def: &Namespace,
	namespace_str: &str,
	name_str: &str,
) -> Result<ResolvedSource> {
	let namespace_fragment = Fragment::internal(ns_def.name());
	let namespace = ResolvedNamespace::new(namespace_fragment, ns_def.clone());
	let name_fragment = Fragment::internal(name_str);
	let _alias_fragment = unresolved.alias.as_ref().map(|a| Fragment::internal(a.text()));

	if let Some(virtual_def) = catalog.find_vtable_user_by_name(tx, ns_def.id(), name_str) {
		return Ok(ResolvedSource::Shape(ResolvedShape::TableVirtual(ResolvedTableVirtual::new(
			name_fragment,
			namespace,
			(*virtual_def).clone(),
		))));
	}

	if matches!(
		ns_def.id(),
		NamespaceId::SYSTEM
			| NamespaceId::SYSTEM_METRICS_STORAGE_TABLE
			| NamespaceId::SYSTEM_METRICS_STORAGE_VIEW
			| NamespaceId::SYSTEM_METRICS_STORAGE_TABLE_VIRTUAL
			| NamespaceId::SYSTEM_METRICS_STORAGE_RINGBUFFER
			| NamespaceId::SYSTEM_METRICS_STORAGE_DICTIONARY
			| NamespaceId::SYSTEM_METRICS_STORAGE_SERIES
			| NamespaceId::SYSTEM_METRICS_STORAGE_FLOW
			| NamespaceId::SYSTEM_METRICS_STORAGE_FLOW_NODE
			| NamespaceId::SYSTEM_METRICS_STORAGE_SYSTEM
			| NamespaceId::SYSTEM_METRICS_CDC_TABLE
			| NamespaceId::SYSTEM_METRICS_CDC_VIEW
			| NamespaceId::SYSTEM_METRICS_CDC_TABLE_VIRTUAL
			| NamespaceId::SYSTEM_METRICS_CDC_RINGBUFFER
			| NamespaceId::SYSTEM_METRICS_CDC_DICTIONARY
			| NamespaceId::SYSTEM_METRICS_CDC_SERIES
			| NamespaceId::SYSTEM_METRICS_CDC_FLOW
			| NamespaceId::SYSTEM_METRICS_CDC_FLOW_NODE
			| NamespaceId::SYSTEM_METRICS_CDC_SYSTEM
			| NamespaceId::SYSTEM_PROCEDURES
			| NamespaceId::SYSTEM_BINDINGS
	) {
		let def = VTable {
			id: VTableId(0),
			namespace: ns_def.id(),
			name: name_str.to_string(),
			columns: vec![],
		};

		return Ok(ResolvedSource::Shape(ResolvedShape::TableVirtual(ResolvedTableVirtual::new(
			name_fragment,
			namespace,
			def,
		))));
	}

	if let Some(table) = catalog.find_table_by_name(tx, ns_def.id(), name_str)? {
		return Ok(ResolvedSource::Shape(ResolvedShape::Table(ResolvedTable::new(
			name_fragment,
			namespace,
			table,
		))));
	}

	if let Some(ringbuffer) = catalog.find_ringbuffer_by_name(tx, ns_def.id(), name_str)? {
		return Ok(ResolvedSource::Shape(ResolvedShape::RingBuffer(ResolvedRingBuffer::new(
			name_fragment,
			namespace,
			ringbuffer,
		))));
	}

	if let Some(view) = catalog.find_view_by_name(tx, ns_def.id(), name_str)? {
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

	if let Some(dictionary) = catalog.find_dictionary_by_name(tx, ns_def.id(), name_str)? {
		return Ok(ResolvedSource::Shape(ResolvedShape::Dictionary(ResolvedDictionary::new(
			name_fragment,
			namespace,
			dictionary,
		))));
	}

	if let Some(series) = catalog.find_series_by_name(tx, ns_def.id(), name_str)? {
		return Ok(ResolvedSource::Shape(ResolvedShape::Series(ResolvedSeries::new(
			name_fragment,
			namespace,
			series,
		))));
	}

	if let Some(segment_tree) = catalog.find_segment_tree_by_name(tx, ns_def.id(), name_str)? {
		return Ok(ResolvedSource::Shape(ResolvedShape::SegmentTree(ResolvedSegmentTree::new(
			name_fragment,
			namespace,
			segment_tree,
		))));
	}

	Err(IdentifierError::SourceNotFound(ShapeNotFoundError {
		namespace: namespace_str.to_string(),
		name: name_str.to_string(),
		fragment: unresolved.name.to_owned(),
	})
	.into())
}
