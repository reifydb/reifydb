// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	interface::catalog::{
		id::{RingBufferId, ViewId},
		ringbuffer::EvictionVisibility,
	},
	key::{
		any::TaggedKey,
		ringbuffer::{RingBufferEvictionKey, ViewEvictionKey},
	},
	return_internal_error,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::util::cowvec::CowVec;

use crate::{CatalogStore, Result};

const VISIBLE: u8 = 0;
const SILENT: u8 = 1;

impl CatalogStore {
	pub(crate) fn store_ringbuffer_eviction(
		txn: &mut AdminTransaction,
		ringbuffer: RingBufferId,
		eviction: EvictionVisibility,
	) -> Result<()> {
		store_eviction(txn, RingBufferEvictionKey::new(ringbuffer).into(), eviction)
	}

	pub(crate) fn find_ringbuffer_eviction(
		rx: &mut Transaction<'_>,
		ringbuffer: RingBufferId,
	) -> Result<EvictionVisibility> {
		find_eviction(rx, RingBufferEvictionKey::new(ringbuffer).into())
	}

	pub(crate) fn drop_ringbuffer_eviction(txn: &mut AdminTransaction, ringbuffer: RingBufferId) -> Result<()> {
		txn.remove(vec![(RingBufferEvictionKey::new(ringbuffer).into(), None)])?;
		Ok(())
	}

	pub(crate) fn store_view_eviction(
		txn: &mut AdminTransaction,
		view: ViewId,
		eviction: EvictionVisibility,
	) -> Result<()> {
		store_eviction(txn, ViewEvictionKey::new(view).into(), eviction)
	}

	pub(crate) fn find_view_eviction(rx: &mut Transaction<'_>, view: ViewId) -> Result<EvictionVisibility> {
		find_eviction(rx, ViewEvictionKey::new(view).into())
	}

	pub(crate) fn drop_view_eviction(txn: &mut AdminTransaction, view: ViewId) -> Result<()> {
		txn.remove(vec![(ViewEvictionKey::new(view).into(), None)])?;
		Ok(())
	}
}

fn store_eviction(txn: &mut AdminTransaction, key: TaggedKey, eviction: EvictionVisibility) -> Result<()> {
	let byte = match eviction {
		EvictionVisibility::Visible => VISIBLE,
		EvictionVisibility::Silent => SILENT,
	};
	txn.set(vec![(key, EncodedBytes(CowVec::new(vec![byte])))])?;
	Ok(())
}

fn find_eviction(rx: &mut Transaction<'_>, key: TaggedKey) -> Result<EvictionVisibility> {
	let Some(multi) = rx.get(&[key])?.pop().flatten() else {
		return Ok(EvictionVisibility::Visible);
	};
	match multi.bytes.as_slice() {
		[VISIBLE] => Ok(EvictionVisibility::Visible),
		[SILENT] => Ok(EvictionVisibility::Silent),
		other => return_internal_error!(
			"eviction record holds {:?}, expected exactly one byte of {} or {}",
			other,
			VISIBLE,
			SILENT
		),
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		common::{CommitVersion, TimeSource},
		interface::catalog::view::View,
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
		test_utils::{ensure_test_namespace, ensure_test_ringbuffer},
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
}
