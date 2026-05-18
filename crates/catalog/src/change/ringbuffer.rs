// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::catalog::{
		id::{NamespaceId, PrimaryKeyId, RingBufferId},
		ringbuffer::RingBuffer,
	},
	key::{EncodableKey, kind::KeyKind, ringbuffer::RingBufferKey},
};
use reifydb_transaction::transaction::Transaction;

use super::CatalogChangeApplier;
use crate::{
	CatalogStore, Result,
	catalog::Catalog,
	error::CatalogChangeError,
	store::ringbuffer::shape::ringbuffer::{self, CAPACITY, ID, NAME, NAMESPACE, PRIMARY_KEY},
};

pub(super) struct RingBufferApplier;

impl CatalogChangeApplier for RingBufferApplier {
	fn set(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey, row: &EncodedRow) -> Result<()> {
		txn.set(key, row.clone())?;
		let mut rb = decode_ringbuffer(row, &catalog.cache, txn.version());
		rb.columns = CatalogStore::list_columns(txn, rb.id)?;
		catalog.cache.set_ringbuffer(rb.id, txn.version(), Some(rb));
		Ok(())
	}

	fn remove(catalog: &Catalog, txn: &mut Transaction<'_>, key: &EncodedKey) -> Result<()> {
		txn.remove(key)?;
		let id = RingBufferKey::decode(key).map(|k| k.ringbuffer).ok_or(
			CatalogChangeError::KeyDecodeFailed {
				kind: KeyKind::RingBuffer,
			},
		)?;
		catalog.cache.set_ringbuffer(id, txn.version(), None);
		Ok(())
	}
}

use reifydb_core::common::CommitVersion;

use crate::cache::CatalogCache;

fn decode_ringbuffer(row: &EncodedRow, materialized: &CatalogCache, version: CommitVersion) -> RingBuffer {
	let id = RingBufferId(ringbuffer::SHAPE.get_u64(row, ID));
	let namespace = NamespaceId(ringbuffer::SHAPE.get_u64(row, NAMESPACE));
	let name = ringbuffer::SHAPE.get_utf8(row, NAME).to_string();
	let capacity = ringbuffer::SHAPE.get_u64(row, CAPACITY);
	let pk_raw = ringbuffer::SHAPE.get_u64(row, PRIMARY_KEY);
	let primary_key = if pk_raw > 0 {
		materialized.find_primary_key_at(PrimaryKeyId(pk_raw), version)
	} else {
		None
	};

	let partition_by_str = ringbuffer::SHAPE.get_utf8(row, ringbuffer::PARTITION_BY);
	let partition_by = if partition_by_str.is_empty() {
		vec![]
	} else {
		partition_by_str.split(',').map(|s| s.to_string()).collect()
	};

	let underlying = ringbuffer::SHAPE.get_u8(row, ringbuffer::UNDERLYING) != 0;
	RingBuffer {
		id,
		name,
		namespace,
		columns: vec![],
		capacity,
		primary_key,
		partition_by,
		underlying,
	}
}
