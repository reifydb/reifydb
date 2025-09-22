// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	RingBufferId,
	identifier::{NamespaceIdentifier, RingBufferIdentifier},
	resolved::{ResolvedNamespace, ResolvedRingBuffer},
};
use reifydb_type::Fragment;

use crate::transaction::{CatalogNamespaceQueryOperations, CatalogRingBufferQueryOperations};

/// Resolve a ring buffer ID to a fully resolved ring buffer with namespace and identifiers
pub fn resolve_ring_buffer<'a, T>(txn: &mut T, ring_buffer_id: RingBufferId) -> crate::Result<ResolvedRingBuffer<'a>>
where
	T: CatalogRingBufferQueryOperations + CatalogNamespaceQueryOperations,
{
	let ring_buffer_def = txn.get_ring_buffer(ring_buffer_id)?;
	let namespace_def = txn.get_namespace(ring_buffer_def.namespace)?;
	let namespace_ident = NamespaceIdentifier::new(Fragment::owned_internal(namespace_def.name.clone()));
	let resolved_namespace = ResolvedNamespace::new(namespace_ident, namespace_def);

	let ring_buffer_ident = RingBufferIdentifier::new(
		Fragment::owned_internal(resolved_namespace.name().to_string()),
		Fragment::owned_internal(ring_buffer_def.name.clone()),
	);

	Ok(ResolvedRingBuffer::new(ring_buffer_ident, resolved_namespace, ring_buffer_def))
}
