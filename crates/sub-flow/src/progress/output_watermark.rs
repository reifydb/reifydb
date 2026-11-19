// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::object::ObjectId, store::SingleVersionRange},
	key::output_watermark::OutputWatermarkKey,
};
use reifydb_flow_async::transaction::output_watermark::{
	OutputWatermarkEntries, OutputWatermarkEntry, OutputWatermarks,
};
use reifydb_runtime::context::transaction::TransactionContext;
use reifydb_store_single::SingleStore;
use reifydb_transaction::single::SingleTransaction;
use reifydb_value::{error::ValueError, value::datetime::DateTime};
use tracing::warn;

const HYDRATE_BATCH: u64 = 1024;

fn encode(entry: &OutputWatermarkEntry) -> EncodedPodRow {
	let mut bytes = Vec::with_capacity(16);
	bytes.extend_from_slice(&entry.output_watermark.to_millis().to_be_bytes());
	bytes.extend_from_slice(&entry.at.0.to_be_bytes());
	EncodedPodRow::new(&bytes)
}

fn decode(object: ObjectId, row: &EncodedPodRow) -> Option<OutputWatermarkEntry> {
	let bytes = row.body();
	if bytes.len() != 16 {
		return None;
	}
	Some(OutputWatermarkEntry {
		output: object,
		output_watermark: DateTime::from_millis(i64::from_be_bytes(bytes[..8].try_into().ok()?)),
		at: CommitVersion(u64::from_be_bytes(bytes[8..].try_into().ok()?)),
	})
}

pub fn persist(
	ctx: &Arc<TransactionContext>,
	single: &SingleTransaction,
	entries: &OutputWatermarkEntries,
) -> Result<(), ValueError> {
	if entries.is_empty() {
		return Ok(());
	}

	let anchor = OutputWatermarkKey::encoded(entries[0].output);
	let mut txn = single.begin_command_ranged(ctx, [&anchor], vec![OutputWatermarkKey::full_scan().encode()])?;
	for entry in entries {
		txn.set(vec![(OutputWatermarkKey::new(entry.output), encode(entry).into_bytes())])?;
	}
	txn.commit()?;
	Ok(())
}

pub fn sweep(ctx: &Arc<TransactionContext>, single: &SingleTransaction, output_watermarks: &OutputWatermarks) {
	if let Err(e) = try_sweep(ctx, single, output_watermarks) {
		warn!(error = %e, "failed to persist output output_watermarks; the next sweep writes them again");
	}
}

pub fn try_sweep(
	ctx: &Arc<TransactionContext>,
	single: &SingleTransaction,
	output_watermarks: &OutputWatermarks,
) -> Result<(), ValueError> {
	let Some((generation, entries)) = output_watermarks.unpersisted() else {
		return Ok(());
	};
	persist(ctx, single, &entries)?;
	output_watermarks.mark_persisted(generation);
	Ok(())
}

pub fn hydrate(ctx: &Arc<TransactionContext>, store: &SingleStore) -> Result<OutputWatermarkEntries, ValueError> {
	let mut out = Vec::new();
	let batch =
		SingleVersionRange::range_batch(store, ctx, OutputWatermarkKey::full_scan().encode(), HYDRATE_BATCH)?;
	for row in batch.items {
		let Some(key) = OutputWatermarkKey::decode(&row.key) else {
			continue;
		};
		if let Some(entry) = decode(key.object, EncodedPodRow::view(&row.bytes)) {
			out.push(entry);
		}
	}
	Ok(out)
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::interface::catalog::{id::ViewId, object::ObjectId};
	use reifydb_runtime::context::{RuntimeContext, transaction::TransactionContext};
	use reifydb_value::factory::time::at_millis;

	use super::*;

	fn system_ctx() -> Arc<TransactionContext> {
		Arc::new(TransactionContext::system(RuntimeContext::testing(0, 0)))
	}

	const OUTPUT: ObjectId = ObjectId::View(ViewId(42));

	#[test]
	fn a_output_watermark_round_trips_through_the_value_encoding() {
		// A truncated stamp silently rewinds an output watermark, which re-opens a horizon that already sealed.
		let entry = OutputWatermarkEntry {
			output: OUTPUT,
			output_watermark: at_millis(1_700_000_000_123),
			at: CommitVersion(u64::MAX),
		};

		assert_eq!(decode(OUTPUT, &encode(&entry)).unwrap(), entry);
	}

	#[test]
	fn a_value_of_the_wrong_width_is_rejected_rather_than_misread() {
		// 0x1D once held FlowNodeInternalState, so a stale row must never decode as a plausible output watermark.
		assert!(decode(OUTPUT, &EncodedPodRow::new(&[0u8; 8])).is_none());
		assert!(decode(OUTPUT, &EncodedPodRow::new(&[0u8; 24])).is_none());
	}

	#[test]
	fn a_persisted_output_watermark_scans_back_out_of_a_real_store() {
		// Encoding and decoding agreeing in memory proves nothing if the key never lands inside the scan range.
		let single = SingleTransaction::testing();
		let written = vec![
			OutputWatermarkEntry {
				output: OUTPUT,
				output_watermark: at_millis(9_000),
				at: CommitVersion(10),
			},
			OutputWatermarkEntry {
				output: ObjectId::table(7),
				output_watermark: at_millis(4_000),
				at: CommitVersion(20),
			},
		];

		persist(&system_ctx(), &single, &written).unwrap();
		let mut read = hydrate(&system_ctx(), &single.read_store()).unwrap();
		read.sort_by_key(|entry| entry.at);

		assert_eq!(read, written);
	}

	#[test]
	fn a_rewritten_output_watermark_replaces_its_predecessor_rather_than_accumulating() {
		// One key per object is the whole point; a second row would make hydration order-dependent.
		let single = SingleTransaction::testing();

		persist(
			&system_ctx(),
			&single,
			&vec![OutputWatermarkEntry {
				output: OUTPUT,
				output_watermark: at_millis(4_000),
				at: CommitVersion(10),
			}],
		)
		.unwrap();
		persist(
			&system_ctx(),
			&single,
			&vec![OutputWatermarkEntry {
				output: OUTPUT,
				output_watermark: at_millis(9_000),
				at: CommitVersion(20),
			}],
		)
		.unwrap();

		let read = hydrate(&system_ctx(), &single.read_store()).unwrap();

		assert_eq!(read.len(), 1, "the object must hold exactly one output watermark row");
		assert_eq!(read[0].output_watermark, at_millis(9_000));
		assert_eq!(read[0].at, CommitVersion(20));
	}

	#[test]
	fn hydrating_an_empty_store_yields_nothing_rather_than_a_spurious_epoch_entry() {
		// A phantom entry would resolve as visible and advance every consumer to the epoch on a fresh database.
		let single = SingleTransaction::testing();

		assert!(hydrate(&system_ctx(), &single.read_store()).unwrap().is_empty());
	}

	#[test]
	fn persisting_nothing_touches_no_transaction_at_all() {
		// A quiet interval must not open a commit, otherwise an idle server writes forever.
		let single = SingleTransaction::testing();

		persist(&system_ctx(), &single, &Vec::new()).unwrap();

		assert!(hydrate(&system_ctx(), &single.read_store()).unwrap().is_empty());
	}

	#[test]
	fn a_sweep_persists_every_published_output_watermark() {
		// A sweep that skips a live entry loses it on restart, since nothing else writes the row.
		let single = SingleTransaction::testing();
		let output_watermarks = OutputWatermarks::default();
		output_watermarks.publish(OUTPUT, at_millis(9_000), CommitVersion(10));

		sweep(&system_ctx(), &single, &output_watermarks);

		let read = hydrate(&system_ctx(), &single.read_store()).unwrap();
		assert_eq!(read.len(), 1);
		assert_eq!(read[0].output_watermark, at_millis(9_000));
	}

	#[test]
	fn a_quiet_interval_opens_no_commit_at_all() {
		// Sweeping every 5s forever keeps the store from ever going idle, which starves the WAL reset.
		let single = SingleTransaction::testing();
		let output_watermarks = OutputWatermarks::default();
		output_watermarks.publish(OUTPUT, at_millis(9_000), CommitVersion(10));
		sweep(&system_ctx(), &single, &output_watermarks);

		persist(
			&system_ctx(),
			&single,
			&vec![OutputWatermarkEntry {
				output: OUTPUT,
				output_watermark: at_millis(1),
				at: CommitVersion(1),
			}],
		)
		.unwrap();
		sweep(&system_ctx(), &single, &output_watermarks);

		assert_eq!(
			hydrate(&system_ctx(), &single.read_store()).unwrap()[0].output_watermark,
			at_millis(1),
			"the second sweep rewrote a row nothing had republished"
		);
	}

	#[test]
	fn a_sweep_that_failed_to_persist_is_retried_rather_than_marked_done() {
		// Marking before the commit lands would strand the output watermark until its producer happens to publish
		// again.
		let output_watermarks = OutputWatermarks::default();
		output_watermarks.publish(OUTPUT, at_millis(9_000), CommitVersion(10));

		let (generation, entries) = output_watermarks.unpersisted().expect("a fresh publish must be unpersisted");
		assert_eq!(entries.len(), 1);

		assert!(output_watermarks.unpersisted().is_some(), "reading the entries must not count as persisting them");

		output_watermarks.mark_persisted(generation);
		assert!(output_watermarks.unpersisted().is_none());
	}

	#[test]
	fn a_publish_racing_the_persist_is_swept_on_the_next_pass() {
		// A publish between the snapshot and the mark must not be swallowed; a quiet producer may never
		// republish.
		let single = SingleTransaction::testing();
		let output_watermarks = OutputWatermarks::default();
		output_watermarks.publish(OUTPUT, at_millis(4_000), CommitVersion(10));

		let (generation, entries) = output_watermarks.unpersisted().unwrap();
		output_watermarks.publish(OUTPUT, at_millis(9_000), CommitVersion(20));
		persist(&system_ctx(), &single, &entries).unwrap();
		output_watermarks.mark_persisted(generation);

		sweep(&system_ctx(), &single, &output_watermarks);

		assert_eq!(hydrate(&system_ctx(), &single.read_store()).unwrap()[0].output_watermark, at_millis(9_000));
	}

	#[test]
	fn a_hydrated_output_watermark_is_never_swept_back_to_the_store_it_came_from() {
		// A restart must not rewrite what it just read, or every restart pays a commit for nothing.
		let single = SingleTransaction::testing();
		let output_watermarks = OutputWatermarks::default();

		output_watermarks.hydrate(vec![OutputWatermarkEntry {
			output: OUTPUT,
			output_watermark: at_millis(9_000),
			at: CommitVersion(10),
		}]);
		sweep(&system_ctx(), &single, &output_watermarks);

		assert!(hydrate(&system_ctx(), &single.read_store()).unwrap().is_empty());
	}

	#[test]
	fn a_sweep_persists_a_republish_that_lands_after_the_previous_one() {
		// A later publish must overwrite the row, otherwise a restart resumes at a stale output watermark.
		let single = SingleTransaction::testing();
		let output_watermarks = OutputWatermarks::default();
		output_watermarks.publish(OUTPUT, at_millis(4_000), CommitVersion(10));
		sweep(&system_ctx(), &single, &output_watermarks);

		output_watermarks.publish(OUTPUT, at_millis(9_000), CommitVersion(20));
		sweep(&system_ctx(), &single, &output_watermarks);

		let read = hydrate(&system_ctx(), &single.read_store()).unwrap();
		assert_eq!(read[0].output_watermark, at_millis(9_000));
		assert_eq!(read[0].at, CommitVersion(20));
	}

	#[test]
	fn the_stamp_orders_lexicographically_the_same_as_numerically() {
		// A little-endian stamp would make version 256 sort below version 2 on any raw byte compare.
		let low = encode(&OutputWatermarkEntry {
			output: OUTPUT,
			output_watermark: at_millis(0),
			at: CommitVersion(2),
		});
		let high = encode(&OutputWatermarkEntry {
			output: OUTPUT,
			output_watermark: at_millis(0),
			at: CommitVersion(256),
		});

		assert!(low.as_slice() < high.as_slice());
	}
}
