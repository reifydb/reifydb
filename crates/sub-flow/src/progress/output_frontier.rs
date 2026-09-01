// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::object::ObjectId, store::SingleVersionRange},
	key::{Key, output_frontier::OutputFrontierKey},
};
use reifydb_flow::transaction::frontier::{FrontierEntries, FrontierEntry, OutputFrontiers};
use reifydb_store_single::SingleStore;
use reifydb_transaction::single::SingleTransaction;
use reifydb_value::{Result, value::datetime::DateTime};
use tracing::warn;

const HYDRATE_BATCH: u64 = 1024;

fn encode(entry: &FrontierEntry) -> EncodedPodRow {
	let mut bytes = Vec::with_capacity(16);
	bytes.extend_from_slice(&entry.frontier.to_millis().to_be_bytes());
	bytes.extend_from_slice(&entry.at.0.to_be_bytes());
	EncodedPodRow::new(&bytes)
}

fn decode(object: ObjectId, row: &EncodedPodRow) -> Option<FrontierEntry> {
	let bytes = row.body();
	if bytes.len() != 16 {
		return None;
	}
	Some(FrontierEntry {
		output: object,
		frontier: DateTime::from_millis(u64::from_be_bytes(bytes[..8].try_into().ok()?)),
		at: CommitVersion(u64::from_be_bytes(bytes[8..].try_into().ok()?)),
	})
}

pub fn persist(single: &SingleTransaction, entries: &FrontierEntries) -> Result<()> {
	if entries.is_empty() {
		return Ok(());
	}

	let anchor = OutputFrontierKey::encoded(entries[0].output);
	let mut txn = single.begin_command_ranged([&anchor], vec![OutputFrontierKey::full_scan()])?;
	for entry in entries {
		txn.set(&OutputFrontierKey::encoded(entry.output), encode(entry).into_bytes())?;
	}
	txn.commit()?;
	Ok(())
}

pub fn sweep(single: &SingleTransaction, frontiers: &OutputFrontiers) {
	let Some((generation, entries)) = frontiers.unpersisted() else {
		return;
	};
	match persist(single, &entries) {
		Ok(()) => frontiers.mark_persisted(generation),
		Err(e) => warn!(error = %e, "failed to persist output frontiers; the next sweep writes them again"),
	}
}

pub fn hydrate(store: &SingleStore) -> Result<FrontierEntries> {
	let mut out = Vec::new();
	let batch = SingleVersionRange::range_batch(store, OutputFrontierKey::full_scan(), HYDRATE_BATCH)?;
	for row in batch.items {
		let Some(key) = OutputFrontierKey::decode(&row.key) else {
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
	use reifydb_core::interface::catalog::{id::ViewId, object::ObjectId};
	use reifydb_value::factory::time::at_millis;

	use super::*;

	const OUTPUT: ObjectId = ObjectId::View(ViewId(42));

	#[test]
	fn a_frontier_round_trips_through_the_value_encoding() {
		// A truncated stamp silently rewinds a frontier, which re-opens a horizon that already sealed.
		let entry = FrontierEntry {
			output: OUTPUT,
			frontier: at_millis(1_700_000_000_123),
			at: CommitVersion(u64::MAX),
		};

		assert_eq!(decode(OUTPUT, &encode(&entry)).unwrap(), entry);
	}

	#[test]
	fn a_value_of_the_wrong_width_is_rejected_rather_than_misread() {
		// 0x1D once held FlowNodeInternalState, so a stale row must never decode as a plausible frontier.
		assert!(decode(OUTPUT, &EncodedPodRow::new(&[0u8; 8])).is_none());
		assert!(decode(OUTPUT, &EncodedPodRow::new(&[0u8; 24])).is_none());
	}

	#[test]
	fn a_persisted_frontier_scans_back_out_of_a_real_store() {
		// Encoding and decoding agreeing in memory proves nothing if the key never lands inside the scan range.
		let single = SingleTransaction::testing();
		let written = vec![
			FrontierEntry {
				output: OUTPUT,
				frontier: at_millis(9_000),
				at: CommitVersion(10),
			},
			FrontierEntry {
				output: ObjectId::table(7),
				frontier: at_millis(4_000),
				at: CommitVersion(20),
			},
		];

		persist(&single, &written).unwrap();
		let mut read = hydrate(&single.read_store()).unwrap();
		read.sort_by_key(|entry| entry.at);

		assert_eq!(read, written);
	}

	#[test]
	fn a_rewritten_frontier_replaces_its_predecessor_rather_than_accumulating() {
		// One key per object is the whole point; a second row would make hydration order-dependent.
		let single = SingleTransaction::testing();

		persist(
			&single,
			&vec![FrontierEntry {
				output: OUTPUT,
				frontier: at_millis(4_000),
				at: CommitVersion(10),
			}],
		)
		.unwrap();
		persist(
			&single,
			&vec![FrontierEntry {
				output: OUTPUT,
				frontier: at_millis(9_000),
				at: CommitVersion(20),
			}],
		)
		.unwrap();

		let read = hydrate(&single.read_store()).unwrap();

		assert_eq!(read.len(), 1, "the object must hold exactly one frontier row");
		assert_eq!(read[0].frontier, at_millis(9_000));
		assert_eq!(read[0].at, CommitVersion(20));
	}

	#[test]
	fn hydrating_an_empty_store_yields_nothing_rather_than_a_spurious_epoch_entry() {
		// A phantom entry would resolve as visible and advance every consumer to the epoch on a fresh database.
		let single = SingleTransaction::testing();

		assert!(hydrate(&single.read_store()).unwrap().is_empty());
	}

	#[test]
	fn persisting_nothing_touches_no_transaction_at_all() {
		// A quiet interval must not open a commit, otherwise an idle server writes forever.
		let single = SingleTransaction::testing();

		persist(&single, &Vec::new()).unwrap();

		assert!(hydrate(&single.read_store()).unwrap().is_empty());
	}

	#[test]
	fn a_sweep_persists_every_published_frontier() {
		// A sweep that skips a live entry loses it on restart, since nothing else writes the row.
		let single = SingleTransaction::testing();
		let frontiers = OutputFrontiers::default();
		frontiers.publish(OUTPUT, at_millis(9_000), CommitVersion(10));

		sweep(&single, &frontiers);

		let read = hydrate(&single.read_store()).unwrap();
		assert_eq!(read.len(), 1);
		assert_eq!(read[0].frontier, at_millis(9_000));
	}

	#[test]
	fn a_quiet_interval_opens_no_commit_at_all() {
		// Sweeping every 5s forever keeps the store from ever going idle, which starves the WAL reset.
		let single = SingleTransaction::testing();
		let frontiers = OutputFrontiers::default();
		frontiers.publish(OUTPUT, at_millis(9_000), CommitVersion(10));
		sweep(&single, &frontiers);

		persist(
			&single,
			&vec![FrontierEntry {
				output: OUTPUT,
				frontier: at_millis(1),
				at: CommitVersion(1),
			}],
		)
		.unwrap();
		sweep(&single, &frontiers);

		assert_eq!(
			hydrate(&single.read_store()).unwrap()[0].frontier,
			at_millis(1),
			"the second sweep rewrote a row nothing had republished"
		);
	}

	#[test]
	fn a_sweep_that_failed_to_persist_is_retried_rather_than_marked_done() {
		// Marking before the commit lands would strand the frontier until its producer happens to publish
		// again.
		let frontiers = OutputFrontiers::default();
		frontiers.publish(OUTPUT, at_millis(9_000), CommitVersion(10));

		let (generation, entries) = frontiers.unpersisted().expect("a fresh publish must be unpersisted");
		assert_eq!(entries.len(), 1);

		assert!(frontiers.unpersisted().is_some(), "reading the entries must not count as persisting them");

		frontiers.mark_persisted(generation);
		assert!(frontiers.unpersisted().is_none());
	}

	#[test]
	fn a_publish_racing_the_persist_is_swept_on_the_next_pass() {
		// A publish between the snapshot and the mark must not be swallowed; a quiet producer may never
		// republish.
		let single = SingleTransaction::testing();
		let frontiers = OutputFrontiers::default();
		frontiers.publish(OUTPUT, at_millis(4_000), CommitVersion(10));

		let (generation, entries) = frontiers.unpersisted().unwrap();
		frontiers.publish(OUTPUT, at_millis(9_000), CommitVersion(20));
		persist(&single, &entries).unwrap();
		frontiers.mark_persisted(generation);

		sweep(&single, &frontiers);

		assert_eq!(hydrate(&single.read_store()).unwrap()[0].frontier, at_millis(9_000));
	}

	#[test]
	fn a_hydrated_frontier_is_never_swept_back_to_the_store_it_came_from() {
		// A restart must not rewrite what it just read, or every restart pays a commit for nothing.
		let single = SingleTransaction::testing();
		let frontiers = OutputFrontiers::default();

		frontiers.hydrate(vec![FrontierEntry {
			output: OUTPUT,
			frontier: at_millis(9_000),
			at: CommitVersion(10),
		}]);
		sweep(&single, &frontiers);

		assert!(hydrate(&single.read_store()).unwrap().is_empty());
	}

	#[test]
	fn a_sweep_persists_a_republish_that_lands_after_the_previous_one() {
		// A later publish must overwrite the row, otherwise a restart resumes at a stale frontier.
		let single = SingleTransaction::testing();
		let frontiers = OutputFrontiers::default();
		frontiers.publish(OUTPUT, at_millis(4_000), CommitVersion(10));
		sweep(&single, &frontiers);

		frontiers.publish(OUTPUT, at_millis(9_000), CommitVersion(20));
		sweep(&single, &frontiers);

		let read = hydrate(&single.read_store()).unwrap();
		assert_eq!(read[0].frontier, at_millis(9_000));
		assert_eq!(read[0].at, CommitVersion(20));
	}

	#[test]
	fn the_stamp_orders_lexicographically_the_same_as_numerically() {
		// A little-endian stamp would make version 256 sort below version 2 on any raw byte compare.
		let low = encode(&FrontierEntry {
			output: OUTPUT,
			frontier: at_millis(0),
			at: CommitVersion(2),
		});
		let high = encode(&FrontierEntry {
			output: OUTPUT,
			frontier: at_millis(0),
			at: CommitVersion(256),
		});

		assert!(low.as_slice() < high.as_slice());
	}
}
