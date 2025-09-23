// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{RingBufferId, resolved::ResolvedRingBuffer};
use reifydb_type::Fragment;

use crate::{
	resolve::resolve_namespace,
	transaction::{CatalogNamespaceQueryOperations, CatalogRingBufferQueryOperations},
};

/// Resolve a ring buffer ID to a fully resolved ring buffer with namespace and identifiers
pub fn resolve_ring_buffer<'a, T>(txn: &mut T, ring_buffer_id: RingBufferId) -> crate::Result<ResolvedRingBuffer<'a>>
where
	T: CatalogRingBufferQueryOperations + CatalogNamespaceQueryOperations,
{
	let ring_buffer_def = txn.get_ring_buffer(ring_buffer_id)?;
	let resolved_namespace = resolve_namespace(txn, ring_buffer_def.namespace)?;
	let ring_buffer_ident = Fragment::owned_internal(ring_buffer_def.name.clone());

	Ok(ResolvedRingBuffer::new(ring_buffer_ident, resolved_namespace, ring_buffer_def))
}
