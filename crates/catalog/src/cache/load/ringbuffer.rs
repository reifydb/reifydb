// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::{
		catalog::{
			id::{NamespaceId, PrimaryKeyId, RingBufferId},
			key::PrimaryKey,
			object::ObjectId,
			ringbuffer::RingBuffer,
		},
		store::MultiVersionRow,
	},
	key::ringbuffer::RingBufferKey,
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use super::CatalogCache;
use crate::{
	CatalogStore, Result,
	store::ringbuffer::{decode_ringbuffer_time, shape::ringbuffer},
};

pub(crate) fn load_ringbuffers(rx: &mut Transaction<'_>, catalog: &CatalogCache) -> Result<()> {
	let range = RingBufferKey::full_scan();
	let mut stream = rx.range(range, RangeScope::All, 1024)?;

	let mut ringbuffers = Vec::new();
	for entry in stream.by_ref() {
		let multi = entry?;
		let version = multi.version;

		let pk_id = get_ringbuffer_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let ringbuffer = convert_ringbuffer(multi, primary_key)?;

		if let Some(id) = pk_id {
			catalog.set_primary_key_object(ObjectId::RingBuffer(ringbuffer.id), id);
		}
		ringbuffers.push((ringbuffer, version));
	}
	drop(stream);

	for (mut ringbuffer, version) in ringbuffers {
		ringbuffer.columns = CatalogStore::list_columns(rx, ringbuffer.id)?;
		catalog.set_ringbuffer(ringbuffer.id, version, Some(ringbuffer));
	}

	Ok(())
}

fn convert_ringbuffer(multi: MultiVersionRow, primary_key: Option<PrimaryKey>) -> Result<RingBuffer> {
	let bytes = EncodedCatalogRow::try_from(multi.bytes)?;
	let id = RingBufferId(ringbuffer::get_id(&bytes));
	let namespace = NamespaceId(ringbuffer::get_namespace(&bytes));
	let name = ringbuffer::get_name(&bytes).to_string();
	let capacity = ringbuffer::get_capacity(&bytes);

	let partition_by_str = ringbuffer::get_partition_by(&bytes);
	let partition_by = if partition_by_str.is_empty() {
		vec![]
	} else {
		partition_by_str.split(',').map(|s| s.to_string()).collect()
	};

	Ok(RingBuffer {
		id,
		name,
		namespace,
		columns: vec![],
		capacity,
		primary_key,
		partition_by,
		time: decode_ringbuffer_time(&bytes),
	})
}

fn get_ringbuffer_primary_key_id(multi: &MultiVersionRow) -> Option<PrimaryKeyId> {
	let pk_id_raw = ringbuffer::get_primary_key(EncodedCatalogRow::view(&multi.bytes));
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
