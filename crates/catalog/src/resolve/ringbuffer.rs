// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{RingBufferId, resolved::ResolvedRingBuffer};
use reifydb_type::Fragment;

use crate::{
	resolve::resolve_namespace,
	transaction::{CatalogNamespaceQueryOperations, CatalogRingBufferQueryOperations},
};

/// Resolve a ring buffer ID to a fully resolved ring buffer with namespace and identifiers
pub async fn resolve_ringbuffer<T>(txn: &mut T, ringbuffer_id: RingBufferId) -> crate::Result<ResolvedRingBuffer>
where
	T: CatalogRingBufferQueryOperations + CatalogNamespaceQueryOperations,
{
	let ringbuffer_def = txn.get_ringbuffer(ringbuffer_id).await?;
	let resolved_namespace = resolve_namespace(txn, ringbuffer_def.namespace).await?;
	let ringbuffer_ident = Fragment::internal(ringbuffer_def.name.clone());

	Ok(ResolvedRingBuffer::new(ringbuffer_ident, resolved_namespace, ringbuffer_def))
}
