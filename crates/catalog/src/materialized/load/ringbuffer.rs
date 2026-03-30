// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			id::{NamespaceId, PrimaryKeyId, RingBufferId},
			key::PrimaryKey,
			ringbuffer::RingBuffer,
		},
		store::MultiVersionRow,
	},
	key::ringbuffer::RingBufferKey,
};
use reifydb_transaction::transaction::Transaction;

use super::MaterializedCatalog;
use crate::{
	Result,
	store::ringbuffer::shape::{
		ringbuffer,
		ringbuffer::{CAPACITY, ID, NAME, NAMESPACE, PRIMARY_KEY},
	},
};

pub(crate) fn load_ringbuffers(rx: &mut Transaction<'_>, catalog: &MaterializedCatalog) -> Result<()> {
	let range = RingBufferKey::full_scan();
	let stream = rx.range(range, 1024)?;

	for entry in stream {
		let multi = entry?;
		let version = multi.version;

		let pk_id = get_ringbuffer_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let ringbuffer = convert_ringbuffer(multi, primary_key);

		catalog.set_ringbuffer(ringbuffer.id, version, Some(ringbuffer));
	}

	Ok(())
}

fn convert_ringbuffer(multi: MultiVersionRow, primary_key: Option<PrimaryKey>) -> RingBuffer {
	let row = multi.row;
	let id = RingBufferId(ringbuffer::SHAPE.get_u64(&row, ID));
	let namespace = NamespaceId(ringbuffer::SHAPE.get_u64(&row, NAMESPACE));
	let name = ringbuffer::SHAPE.get_utf8(&row, NAME).to_string();
	let capacity = ringbuffer::SHAPE.get_u64(&row, CAPACITY);

	let partition_by_str = ringbuffer::SHAPE.get_utf8(&row, ringbuffer::PARTITION_BY);
	let partition_by = if partition_by_str.is_empty() {
		vec![]
	} else {
		partition_by_str.split(',').map(|s| s.to_string()).collect()
	};

	RingBuffer {
		id,
		name,
		namespace,
		columns: vec![],
		capacity,
		primary_key,
		partition_by,
	}
}

fn get_ringbuffer_primary_key_id(multi: &MultiVersionRow) -> Option<PrimaryKeyId> {
	let pk_id_raw = ringbuffer::SHAPE.get_u64(&multi.row, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
