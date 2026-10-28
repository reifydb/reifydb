// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	interface::catalog::{
		id::{RingBufferId, ViewId},
		ringbuffer::EvictionVisibility,
		storage::StorageId,
	},
	key::{
		any::TaggedKey,
		ringbuffer::{RingBufferEvictionKey, SeriesEvictionKey, TableEvictionKey, ViewEvictionKey},
	},
	return_internal_error,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::util::cowvec::CowVec;

use crate::{CatalogStore, Result};

const CAPACITY_SILENT: u8 = 0b01;
const TTL_VISIBLE: u8 = 0b10;
const KNOWN_FLAGS: u8 = CAPACITY_SILENT | TTL_VISIBLE;

impl CatalogStore {
	pub(crate) fn store_ringbuffer_eviction(
		txn: &mut AdminTransaction,
		ringbuffer: RingBufferId,
		eviction: EvictionVisibility,
	) -> Result<()> {
		set_flag(
			txn,
			RingBufferEvictionKey::new(ringbuffer).into(),
			CAPACITY_SILENT,
			eviction == EvictionVisibility::Silent,
		)
	}

	pub(crate) fn find_ringbuffer_eviction(
		rx: &mut Transaction<'_>,
		ringbuffer: RingBufferId,
	) -> Result<EvictionVisibility> {
		Ok(capacity_eviction(read_flags(rx, RingBufferEvictionKey::new(ringbuffer).into())?))
	}

	pub(crate) fn store_view_eviction(
		txn: &mut AdminTransaction,
		view: ViewId,
		eviction: EvictionVisibility,
	) -> Result<()> {
		set_flag(
			txn,
			ViewEvictionKey::new(view).into(),
			CAPACITY_SILENT,
			eviction == EvictionVisibility::Silent,
		)
	}

	pub(crate) fn find_view_eviction(rx: &mut Transaction<'_>, view: ViewId) -> Result<EvictionVisibility> {
		Ok(capacity_eviction(read_flags(rx, ViewEvictionKey::new(view).into())?))
	}

	pub(crate) fn store_ttl_eviction(
		txn: &mut AdminTransaction,
		storage: StorageId,
		eviction: EvictionVisibility,
	) -> Result<()> {
		let Some(key) = eviction_key(storage) else {
			return Ok(());
		};
		set_flag(txn, key, TTL_VISIBLE, eviction == EvictionVisibility::Visible)
	}

	pub(crate) fn find_ttl_eviction(rx: &mut Transaction<'_>, storage: StorageId) -> Result<EvictionVisibility> {
		let Some(key) = eviction_key(storage) else {
			return Ok(EvictionVisibility::Silent);
		};
		Ok(match read_flags(rx, key)? & TTL_VISIBLE {
			0 => EvictionVisibility::Silent,
			_ => EvictionVisibility::Visible,
		})
	}

	pub(crate) fn drop_eviction(txn: &mut AdminTransaction, storage: StorageId) -> Result<()> {
		let Some(key) = eviction_key(storage) else {
			return Ok(());
		};
		if txn.get(&[key.clone()])?.pop().flatten().is_some() {
			txn.remove(vec![(key, None)])?;
		}
		Ok(())
	}
}

fn eviction_key(storage: StorageId) -> Option<TaggedKey> {
	match storage {
		StorageId::Table(id) => Some(TableEvictionKey::new(id).into()),
		StorageId::View(id) => Some(ViewEvictionKey::new(id).into()),
		StorageId::RingBuffer(id) => Some(RingBufferEvictionKey::new(id).into()),
		StorageId::Series(id) => Some(SeriesEvictionKey::new(id).into()),
		StorageId::Queue(_) => None,
	}
}

fn capacity_eviction(flags: u8) -> EvictionVisibility {
	match flags & CAPACITY_SILENT {
		0 => EvictionVisibility::Visible,
		_ => EvictionVisibility::Silent,
	}
}

fn set_flag(txn: &mut AdminTransaction, key: TaggedKey, flag: u8, on: bool) -> Result<()> {
	if !on {
		return Ok(());
	}
	let flags = read_flags(&mut Transaction::Admin(&mut *txn), key.clone())? | flag;
	txn.set(vec![(key, EncodedBytes(CowVec::new(vec![flags])))])?;
	Ok(())
}

fn read_flags(rx: &mut Transaction<'_>, key: TaggedKey) -> Result<u8> {
	let Some(multi) = rx.get(&[key])?.pop().flatten() else {
		return Ok(0);
	};
	match multi.bytes.as_slice() {
		[flags] if flags & !KNOWN_FLAGS == 0 => Ok(*flags),
		other => return_internal_error!(
			"eviction record holds {:?}, expected exactly one byte of flags within {:#04b}",
			other,
			KNOWN_FLAGS
		),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::{CommitVersion, TimeSource},
		interface::catalog::{
			id::{QueueId, SeriesId, TableId},
			view::View,
		},
	};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_value::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, value_type::ValueType},
	};

	use super::*;
	use crate::{
		cache::{CatalogCache, load::CatalogCacheLoader},
		catalog::{
			Catalog,
			ringbuffer::{RingBufferColumnToCreate, RingBufferToCreate},
		},
		store::view::create::{ViewColumnToCreate, ViewStorage, ViewToCreate},
		test_utils::{ensure_test_namespace, ensure_test_ringbuffer, ensure_test_table},
	};

	fn record_present(txn: &mut AdminTransaction, key: TaggedKey) -> bool {
		txn.get(&[key]).unwrap().pop().flatten().is_some()
	}

	fn create_ringbuffer(txn: &mut AdminTransaction, eviction: EvictionVisibility) -> RingBufferId {
		let namespace = ensure_test_namespace(txn);
		Catalog::testing()
			.create_ringbuffer(
				txn,
				RingBufferToCreate {
					name: Fragment::internal("rb"),
					namespace: namespace.id(),
					columns: vec![RingBufferColumnToCreate {
						name: Fragment::internal("v"),
						fragment: Fragment::internal("v"),
						constraint: TypeConstraint::unconstrained(ValueType::Int4),
						properties: vec![],
						auto_increment: false,
						dictionary_id: None,
					}],
					capacity: 1,
					partition_by: vec![],
					time: TimeSource::Processing,
					eviction,
				},
			)
			.unwrap()
			.id
	}

	fn create_ringbuffer_view(txn: &mut AdminTransaction, eviction: EvictionVisibility) -> ViewId {
		let namespace = ensure_test_namespace(txn);
		CatalogStore::create_deferred_view(
			txn,
			ViewToCreate {
				name: Fragment::internal("v"),
				namespace: namespace.id(),
				columns: vec![ViewColumnToCreate {
					name: Fragment::internal("a"),
					fragment: Fragment::internal("a"),
					constraint: TypeConstraint::unconstrained(ValueType::Int4),
					dictionary_id: None,
				}],
				storage: ViewStorage::RingBuffer {
					capacity: 1,
					partition_by: vec![],
					eviction,
				},
				sort: vec![],
			},
		)
		.unwrap()
		.id()
	}

	fn view_eviction(view: View) -> EvictionVisibility {
		match view {
			View::RingBuffer(ringbuffer) => ringbuffer.eviction,
			other => panic!("expected a ring buffer view, got {other:?}"),
		}
	}

	#[test]
	fn a_ringbuffer_without_a_record_reads_visible() {
		// Every ring buffer stored before this record existed must keep today's visible push-out.
		let mut txn = create_test_admin_transaction();
		let created = ensure_test_ringbuffer(&mut txn);
		assert!(!record_present(&mut txn, RingBufferEvictionKey::new(created.id).into()));
		let found =
			CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut txn), created.id).unwrap().unwrap();
		assert_eq!(found.eviction, EvictionVisibility::Visible);
	}

	#[test]
	fn a_visible_ringbuffer_writes_no_record() {
		// A record for the default would add a key and a cdc change to every existing create.
		let mut txn = create_test_admin_transaction();
		let id = create_ringbuffer(&mut txn, EvictionVisibility::Visible);
		assert!(!record_present(&mut txn, RingBufferEvictionKey::new(id).into()));
	}

	#[test]
	fn a_silent_ringbuffer_round_trips_through_find_list_and_the_cache() {
		// Any read path that drops the record would turn push-outs visible again after a restart.
		let mut txn = create_test_admin_transaction();
		let id = create_ringbuffer(&mut txn, EvictionVisibility::Silent);

		let found = CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut txn), id).unwrap().unwrap();
		assert_eq!(found.eviction, EvictionVisibility::Silent);

		let listed = CatalogStore::list_ringbuffers(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(listed.iter().find(|r| r.id == id).unwrap().eviction, EvictionVisibility::Silent);

		let cache = CatalogCache::new();
		CatalogCacheLoader::load_all(&mut Transaction::Admin(&mut txn), &cache).unwrap();
		let cached = cache.find_ringbuffer_at(id, CommitVersion(u64::MAX)).expect("ring buffer in the cache");
		assert_eq!(cached.eviction, EvictionVisibility::Silent);
	}

	#[test]
	fn dropping_a_silent_ringbuffer_removes_its_record() {
		// A leftover record would make a later object reusing the id silently drop removes.
		let mut txn = create_test_admin_transaction();
		let id = create_ringbuffer(&mut txn, EvictionVisibility::Silent);
		assert!(record_present(&mut txn, RingBufferEvictionKey::new(id).into()));
		CatalogStore::drop_ringbuffer(&mut txn, id).unwrap();
		assert!(!record_present(&mut txn, RingBufferEvictionKey::new(id).into()));
	}

	#[test]
	fn a_ringbuffer_view_without_a_record_reads_visible() {
		// Every ring buffer view stored before this record existed must keep today's visible push-out.
		let mut txn = create_test_admin_transaction();
		let id = create_ringbuffer_view(&mut txn, EvictionVisibility::Visible);
		assert!(!record_present(&mut txn, ViewEvictionKey::new(id).into()));
		let found = CatalogStore::find_view(&mut Transaction::Admin(&mut txn), id).unwrap().unwrap();
		assert_eq!(view_eviction(found), EvictionVisibility::Visible);
	}

	#[test]
	fn a_silent_ringbuffer_view_round_trips_through_find_and_the_cache() {
		// Any read path that drops the record would turn the sink's push-outs visible again.
		let mut txn = create_test_admin_transaction();
		let id = create_ringbuffer_view(&mut txn, EvictionVisibility::Silent);

		let found = CatalogStore::find_view(&mut Transaction::Admin(&mut txn), id).unwrap().unwrap();
		assert_eq!(view_eviction(found), EvictionVisibility::Silent);

		let cache = CatalogCache::new();
		CatalogCacheLoader::load_all(&mut Transaction::Admin(&mut txn), &cache).unwrap();
		let cached = cache.find_view_at(id, CommitVersion(u64::MAX)).expect("view in the cache");
		assert_eq!(view_eviction(cached), EvictionVisibility::Silent);
	}

	#[test]
	fn dropping_a_silent_ringbuffer_view_removes_its_record() {
		// A leftover record would make a later view reusing the id silently drop removes.
		let mut txn = create_test_admin_transaction();
		let id = create_ringbuffer_view(&mut txn, EvictionVisibility::Silent);
		assert!(record_present(&mut txn, ViewEvictionKey::new(id).into()));
		CatalogStore::drop_view(&mut txn, id).unwrap();
		assert!(!record_present(&mut txn, ViewEvictionKey::new(id).into()));
	}

	#[test]
	fn a_record_that_is_not_one_known_byte_is_an_error() {
		// Reading garbage as visible would hide storage corruption behind a plausible default.
		let mut txn = create_test_admin_transaction();
		let created = ensure_test_ringbuffer(&mut txn);
		txn.set(vec![(RingBufferEvictionKey::new(created.id).into(), EncodedBytes(CowVec::new(vec![7])))])
			.unwrap();
		assert!(CatalogStore::find_ringbuffer_eviction(&mut Transaction::Admin(&mut txn), created.id).is_err());
	}

	fn record_byte(txn: &mut AdminTransaction, key: TaggedKey) -> Option<Vec<u8>> {
		txn.get(&[key]).unwrap().pop().flatten().map(|row| row.bytes.as_slice().to_vec())
	}

	fn ttl_eviction(txn: &mut AdminTransaction, storage: StorageId) -> EvictionVisibility {
		CatalogStore::find_ttl_eviction(&mut Transaction::Admin(txn), storage).unwrap()
	}

	#[test]
	fn a_ttl_visible_record_round_trips_for_every_storage_kind() {
		// Any kind whose record is lost would fall back to silent expiry and never retract downstream.
		let mut txn = create_test_admin_transaction();
		for (storage, key) in [
			(StorageId::Table(TableId(42)), TaggedKey::from(TableEvictionKey::new(TableId(42)))),
			(StorageId::View(ViewId(43)), ViewEvictionKey::new(ViewId(43)).into()),
			(StorageId::RingBuffer(RingBufferId(44)), RingBufferEvictionKey::new(RingBufferId(44)).into()),
			(StorageId::Series(SeriesId(45)), SeriesEvictionKey::new(SeriesId(45)).into()),
		] {
			assert_eq!(
				ttl_eviction(&mut txn, storage),
				EvictionVisibility::Silent,
				"absent record reads silent"
			);
			CatalogStore::store_ttl_eviction(&mut txn, storage, EvictionVisibility::Visible).unwrap();
			assert_eq!(record_byte(&mut txn, key), Some(vec![0b10]), "ttl visible is bit 1 only");
			assert_eq!(ttl_eviction(&mut txn, storage), EvictionVisibility::Visible);
		}
	}

	#[test]
	fn a_silent_ttl_writes_no_record() {
		// A record for the default would add a key and a cdc change to every existing create.
		let mut txn = create_test_admin_transaction();
		let storage = StorageId::Table(TableId(42));
		CatalogStore::store_ttl_eviction(&mut txn, storage, EvictionVisibility::Silent).unwrap();
		assert!(!record_present(&mut txn, TableEvictionKey::new(TableId(42)).into()));
	}

	#[test]
	fn a_queue_never_holds_a_ttl_record() {
		// Queues have no ttl expiry, so a stored policy could only ever be misleading.
		let mut txn = create_test_admin_transaction();
		let storage = StorageId::Queue(QueueId(42));
		CatalogStore::store_ttl_eviction(&mut txn, storage, EvictionVisibility::Visible).unwrap();
		assert_eq!(ttl_eviction(&mut txn, storage), EvictionVisibility::Silent);
	}

	#[test]
	fn capacity_and_ttl_flags_share_one_record_and_stay_independent() {
		// Overwriting instead of merging would drop the capacity policy when the ttl policy is stored.
		let mut txn = create_test_admin_transaction();
		let id = create_ringbuffer(&mut txn, EvictionVisibility::Silent);
		CatalogStore::store_ttl_eviction(&mut txn, StorageId::RingBuffer(id), EvictionVisibility::Visible)
			.unwrap();
		assert_eq!(record_byte(&mut txn, RingBufferEvictionKey::new(id).into()), Some(vec![0b11]));
		let found = CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut txn), id).unwrap().unwrap();
		assert_eq!(found.eviction, EvictionVisibility::Silent);
		assert_eq!(ttl_eviction(&mut txn, StorageId::RingBuffer(id)), EvictionVisibility::Visible);
	}

	#[test]
	fn a_stored_capacity_byte_keeps_meaning_capacity_silent_only() {
		// Records written before the flags existed hold [1] and must not turn ttl expiry visible.
		let mut txn = create_test_admin_transaction();
		let created = ensure_test_ringbuffer(&mut txn);
		txn.set(vec![(RingBufferEvictionKey::new(created.id).into(), EncodedBytes(CowVec::new(vec![1])))])
			.unwrap();
		let found =
			CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut txn), created.id).unwrap().unwrap();
		assert_eq!(found.eviction, EvictionVisibility::Silent);
		assert_eq!(ttl_eviction(&mut txn, StorageId::RingBuffer(created.id)), EvictionVisibility::Silent);
	}

	#[test]
	fn a_ttl_only_record_keeps_capacity_visible() {
		// Reading any record as capacity silent would stop push-outs retracting downstream.
		let mut txn = create_test_admin_transaction();
		let created = ensure_test_ringbuffer(&mut txn);
		CatalogStore::store_ttl_eviction(
			&mut txn,
			StorageId::RingBuffer(created.id),
			EvictionVisibility::Visible,
		)
		.unwrap();
		let found =
			CatalogStore::find_ringbuffer(&mut Transaction::Admin(&mut txn), created.id).unwrap().unwrap();
		assert_eq!(found.eviction, EvictionVisibility::Visible);
	}

	#[test]
	fn an_unknown_flag_bit_is_an_error_for_the_ttl_read_too() {
		// Reading an unknown bit as a default would hide storage corruption behind a plausible policy.
		let mut txn = create_test_admin_transaction();
		txn.set(vec![(TableEvictionKey::new(TableId(42)).into(), EncodedBytes(CowVec::new(vec![0b100])))])
			.unwrap();
		assert!(CatalogStore::find_ttl_eviction(
			&mut Transaction::Admin(&mut txn),
			StorageId::Table(TableId(42))
		)
		.is_err());
	}

	#[test]
	fn dropping_a_table_removes_its_ttl_record() {
		// A leftover record would make a later table reusing the id announce its expiry.
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);
		CatalogStore::store_ttl_eviction(&mut txn, StorageId::Table(table.id), EvictionVisibility::Visible)
			.unwrap();
		assert!(record_present(&mut txn, TableEvictionKey::new(table.id).into()));
		CatalogStore::drop_table(&mut txn, table.id).unwrap();
		assert!(!record_present(&mut txn, TableEvictionKey::new(table.id).into()));
	}

	#[test]
	fn dropping_a_ttl_visible_ringbuffer_with_visible_capacity_removes_its_record() {
		// The record now exists without a silent capacity, so drop must not key off the capacity policy.
		let mut txn = create_test_admin_transaction();
		let id = create_ringbuffer(&mut txn, EvictionVisibility::Visible);
		CatalogStore::store_ttl_eviction(&mut txn, StorageId::RingBuffer(id), EvictionVisibility::Visible)
			.unwrap();
		assert!(record_present(&mut txn, RingBufferEvictionKey::new(id).into()));
		CatalogStore::drop_ringbuffer(&mut txn, id).unwrap();
		assert!(!record_present(&mut txn, RingBufferEvictionKey::new(id).into()));
	}
}
