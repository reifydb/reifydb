// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			id::{NamespaceId, PrimaryKeyId, RingBufferId},
			key::PrimaryKeyDef,
			ringbuffer::RingBufferDef,
		},
		store::MultiVersionValues,
	},
	key::ringbuffer::RingBufferKey,
};
use reifydb_transaction::standard::IntoStandardTransaction;

use super::MaterializedCatalog;
use crate::store::ringbuffer::schema::{
	ringbuffer,
	ringbuffer::{CAPACITY, ID, NAME, NAMESPACE, PRIMARY_KEY},
};

pub(crate) fn load_ringbuffers(
	rx: &mut impl IntoStandardTransaction,
	catalog: &MaterializedCatalog,
) -> crate::Result<()> {
	let mut txn = rx.into_standard_transaction();
	let range = RingBufferKey::full_scan();
	let mut stream = txn.range(range, 1024)?;

	while let Some(entry) = stream.next() {
		let multi = entry?;
		let version = multi.version;

		let pk_id = get_ringbuffer_primary_key_id(&multi);
		let primary_key = pk_id.and_then(|id| catalog.find_primary_key_at(id, version));
		let ringbuffer_def = convert_ringbuffer(multi, primary_key);

		catalog.set_ringbuffer(ringbuffer_def.id, version, Some(ringbuffer_def));
	}

	Ok(())
}

fn convert_ringbuffer(multi: MultiVersionValues, primary_key: Option<PrimaryKeyDef>) -> RingBufferDef {
	let row = multi.values;
	let id = RingBufferId(ringbuffer::SCHEMA.get_u64(&row, ID));
	let namespace = NamespaceId(ringbuffer::SCHEMA.get_u64(&row, NAMESPACE));
	let name = ringbuffer::SCHEMA.get_utf8(&row, NAME).to_string();
	let capacity = ringbuffer::SCHEMA.get_u64(&row, CAPACITY);

	RingBufferDef {
		id,
		name,
		namespace,
		columns: vec![],
		capacity,
		primary_key,
	}
}

fn get_ringbuffer_primary_key_id(multi: &MultiVersionValues) -> Option<PrimaryKeyId> {
	let pk_id_raw = ringbuffer::SCHEMA.get_u64(&multi.values, PRIMARY_KEY);
	if pk_id_raw == 0 {
		None
	} else {
		Some(PrimaryKeyId(pk_id_raw))
	}
}
