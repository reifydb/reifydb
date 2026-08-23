// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::Bound, sync::Arc, thread};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{Cdc, CdcChange},
};
use reifydb_runtime::sync::{mutex::Mutex, waiter::WaiterHandle};
use reifydb_store_cdc::{storage::CdcStorage, store::CdcStore};
use reifydb_value::{
	util::cowvec::CowVec,
	value::{datetime::DateTime, duration::Duration},
};

mod common;

use common::Fixture;

const HANG_TIMEOUT: Duration = Duration::from_seconds_const(30);

const SUMMARY_LIMIT: usize = 1024;

fn cdc_minimal(version: u64) -> Cdc {
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(1_700_000_000_000_000_000),
		vec![CdcChange::Insert {
			key: EncodedKey::new(vec![1, 2, 3]),
			post: EncodedBytes(CowVec::new(vec![10, 20, 30])),
		}],
	)
}

fn write_all(store: &CdcStore, versions: impl IntoIterator<Item = u64>) {
	for v in versions {
		store.write(&cdc_minimal(v)).unwrap();
	}
}

struct NotifyOnDrop(Arc<WaiterHandle>);

impl Drop for NotifyOnDrop {
	fn drop(&mut self) {
		// unwinding out of the body must still wake the caller, otherwise a panic is reported as a hang
		self.0.notify();
	}
}

fn within_deadline<T, F>(what: &str, body: F) -> T
where
	F: FnOnce() -> T + Send + 'static,
	T: Send + 'static,
{
	// a call that never returns would hang the whole suite forever, so a deadline turns a hang into a failure
	let waiter = Arc::new(WaiterHandle::new());
	let signal = Arc::clone(&waiter);
	let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
	let out = Arc::clone(&slot);
	let handle = thread::spawn(move || {
		let _signal = NotifyOnDrop(signal);
		*out.lock() = Some(body());
	});
	let returned = waiter.wait_timeout(HANG_TIMEOUT);
	assert!(returned, "{what} never returned; it hangs");
	if let Err(payload) = handle.join() {
		std::panic::resume_unwind(payload);
	}
	slot.lock().take().expect("the body must have produced a value once it returned")
}

mod cases {
	use super::*;

	pub fn shutdown_seals_partial_block(fixture: Fixture) {
		// three records are nowhere near the four megabyte cut, so nothing but shutdown can ever seal them
		write_all(&fixture.store, 1..=3);
		assert_eq!(
			fixture.persistent.metrics().appends,
			0,
			"a partial block must not be sealed before shutdown"
		);

		fixture.store.shutdown();

		assert_eq!(
			fixture.persistent.metrics().appends,
			1,
			"shutdown must seal the partial commit buffer into exactly one block"
		);
		let summaries = fixture.persistent.summaries_from(CommitVersion(0), SUMMARY_LIMIT).unwrap();
		assert_eq!(summaries.len(), 1, "the block shutdown sealed must be visible in the persistent tier");
		assert_eq!(
			summaries[0].min_version,
			CommitVersion(1),
			"the sealed block must start at the first record"
		);
		assert_eq!(summaries[0].max_version, CommitVersion(3), "the sealed block must end at the last record");
		assert_eq!(summaries[0].count.as_u64(), 3, "no record accepted by write may be missing from the block");
	}

	pub fn shutdown_seals_no_empty_block(fixture: Fixture) {
		// an empty block has no version range and append_block rejects it, so an idle shutdown must not attempt
		// one
		let store = fixture.store.clone();
		within_deadline("shutdown with nothing pending", move || store.shutdown());

		let metrics = fixture.persistent.metrics();
		assert_eq!(metrics.appends, 0, "shutdown with nothing pending must not append a block");
		assert_eq!(metrics.blocks, 0, "shutdown with nothing pending must leave the persistent tier empty");
		assert_eq!(fixture.store.commit_metrics().blocks_cut, 0, "nothing was pending, so nothing may be cut");
		let summaries = fixture.persistent.summaries_from(CommitVersion(0), SUMMARY_LIMIT).unwrap();
		assert!(summaries.is_empty(), "an empty block would break prefix truncation");

		let store = fixture.store.clone();
		let alive = within_deadline("flush_pending after an idle shutdown", move || store.flush_pending());
		assert!(alive, "a rejected empty append would have killed the flusher and left it unable to answer");
	}

	pub fn shutdown_is_idempotent(fixture: Fixture) {
		// a second shutdown must be a no-op; re-sealing would duplicate a block that is written once and never
		// rewritten
		write_all(&fixture.store, 1..=3);
		fixture.store.shutdown();
		let after_first = fixture.persistent.metrics();
		assert_eq!(after_first.appends, 1, "the first shutdown must seal the pending records");

		let store = fixture.store.clone();
		within_deadline("a second shutdown", move || store.shutdown());

		let after_second = fixture.persistent.metrics();
		assert_eq!(
			after_second.appends, after_first.appends,
			"a second shutdown must not write the block again"
		);
		assert_eq!(after_second.blocks, after_first.blocks, "a second shutdown must not add a block");
	}

	pub fn flush_pending_after_shutdown(fixture: Fixture) {
		// shutdown never stops the flush actor, so a later flush_pending still has a mailbox that can answer it
		write_all(&fixture.store, 1..=3);
		fixture.store.shutdown();

		let store = fixture.store.clone();
		let drained = within_deadline("flush_pending after shutdown", move || store.flush_pending());

		assert!(drained, "flush_pending after shutdown must answer rather than time out");
		assert_eq!(
			fixture.persistent.metrics().appends,
			1,
			"a flush with nothing pending must not seal a second block"
		);
	}

	pub fn reads_after_shutdown_never_report_absence(fixture: Fixture) {
		// a closed tier must fail loudly; silently answering "no such record" loses what shutdown just
		// persisted
		write_all(&fixture.store, 1..=3);
		fixture.store.shutdown();

		match fixture.store.read(CommitVersion(2)) {
			Ok(Some(cdc)) => {
				assert_eq!(cdc.version, CommitVersion(2), "read must answer with the version asked for")
			}
			Ok(None) => panic!("read after shutdown reported version 2 absent although shutdown sealed it"),
			Err(_) => {}
		}

		match fixture.store.read_range(Bound::Unbounded, Bound::Unbounded, 100) {
			Ok(batch) => {
				let versions: Vec<u64> = batch.items.iter().map(|cdc| cdc.version.0).collect();
				assert_eq!(versions, vec![1, 2, 3], "read_range after shutdown dropped sealed records");
			}
			Err(_) => {}
		}

		match fixture.store.min_version() {
			Ok(Some(version)) => {
				assert_eq!(version, CommitVersion(1), "min_version must be the lowest sealed version")
			}
			Ok(None) => {
				panic!("min_version after shutdown reported an empty log although a block was sealed")
			}
			Err(_) => {}
		}
	}

	pub fn write_after_shutdown_is_not_lost(fixture: Fixture) {
		// the commit buffer never learns the tier below closed, so whatever it still accepts must stay
		// drainable
		write_all(&fixture.store, 1..=3);
		fixture.store.shutdown();

		assert!(
			fixture.store.write(&cdc_minimal(4)).is_ok(),
			"write after shutdown was accepted or refused, pin which"
		);
		assert_eq!(
			fixture.store.commit_metrics().entries.as_u64(),
			1,
			"a record accepted after shutdown sits in the commit buffer"
		);

		let store = fixture.store.clone();
		let drained =
			within_deadline("flush_pending after a write past shutdown", move || store.flush_pending());

		assert!(drained, "the flusher must still answer after a write past shutdown");
		assert_eq!(
			fixture.persistent.metrics().appends,
			2,
			"a record write accepted must reach the persistent tier, never be stranded in the buffer"
		);
		assert_eq!(
			fixture.store.commit_metrics().entries.as_u64(),
			0,
			"the commit buffer must not keep growing with no way to drain"
		);
	}

	pub fn shutdown_racing_inflight_flush(fixture: Fixture) {
		// two flushes contend for the same pending run, so the batch must be written exactly once, never twice
		write_all(&fixture.store, 1..=64);

		let racer = fixture.store.clone();
		let handle = thread::spawn(move || racer.flush_pending());
		fixture.store.shutdown();
		let raced = handle.join().expect("the racing flush must not panic");

		assert!(raced, "the racing flush must answer rather than time out");
		let persistent = fixture.persistent.metrics();
		let commit = fixture.store.commit_metrics();
		assert_eq!(
			commit.entries.as_u64(),
			0,
			"the in-flight batch must not be left behind in the commit buffer"
		);
		assert_eq!(
			persistent.appends, commit.blocks_cut,
			"every batch the commit tier cut must become exactly one block"
		);
		assert_eq!(
			persistent.appends, 1,
			"64 tiny records fit one cut, so a second block means the batch was written twice"
		);
	}

	pub fn drop_without_shutdown_loses_pending(fixture: Fixture) {
		// a drop runs no flush so unsealed records are lost, which only a central write ahead log makes
		// acceptable
		let Fixture {
			store,
			persistent,
			guard,
		} = fixture;
		write_all(&store, 1..=3);
		drop(store);

		assert_eq!(persistent.metrics().appends, 0, "a dropped store seals nothing on the way out");
		let summaries = persistent.summaries_from(CommitVersion(0), SUMMARY_LIMIT).unwrap();
		assert!(
			summaries.is_empty(),
			"unsealed records are lost on a drop, and only the write ahead log has them"
		);
		drop(guard);
	}
}

crate::tier_tests!(
	[
		memory = common::memory,
		memory_cached = common::memory_cached,
		sqlite = common::sqlite,
		sqlite_cached = common::sqlite_cached,
		sqlite_starved_cache = common::sqlite_starved_cache,
	],
	[
		shutdown_seals_partial_block,
		shutdown_seals_no_empty_block,
		shutdown_is_idempotent,
		flush_pending_after_shutdown,
		reads_after_shutdown_never_report_absence,
		write_after_shutdown_is_not_lost,
		shutdown_racing_inflight_flush,
		drop_without_shutdown_loses_pending,
	]
);
