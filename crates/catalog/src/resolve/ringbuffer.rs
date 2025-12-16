// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{RingBufferId, resolved::ResolvedRingBuffer};
use reifydb_type::Fragment;

use crate::{
	resolve::resolve_namespace,
	transaction::{CatalogNamespaceQueryOperations, CatalogRingBufferQueryOperations},
};

/// Resolve a ring buffer ID to a fully resolved ring buffer with namespace and identifiers
pub fn resolve_ringbuffer<'a, T>(txn: &mut T, ringbuffer_id: RingBufferId) -> crate::Result<ResolvedRingBuffer<'a>>
where
	T: CatalogRingBufferQueryOperations + CatalogNamespaceQueryOperations,
{
	let ringbuffer_def = txn.get_ringbuffer(ringbuffer_id)?;
	let resolved_namespace = resolve_namespace(txn, ringbuffer_def.namespace)?;
	let ringbuffer_ident = Fragment::owned_internal(ringbuffer_def.name.clone());

	Ok(ResolvedRingBuffer::new(ringbuffer_ident, resolved_namespace, ringbuffer_def))
}
