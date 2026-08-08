// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use reifydb_codec::{encoded::bytes::EncodedBytes, key::encoded::EncodedKey};
use reifydb_core::{common::CommitVersion, interface::catalog::flow::OperatorId, internal_error};
use reifydb_sqlite::SqliteConfig;
use reifydb_store_operator::snapshot::{SnapshotStore, SnapshotWrite};
use reifydb_testing::tempdir::temp_dir;
use reifydb_value::{Result, util::cowvec::CowVec};
use rusqlite::Connection;

const OP: OperatorId = OperatorId(7);

fn store_at(dir: &Path) -> SnapshotStore {
	SnapshotStore::sqlite(SqliteConfig::new(dir.join("operator.db")))
}

fn raw_connection(dir: &Path) -> Connection {
	Connection::open(dir.join("operator.db")).expect("open raw connection")
}

fn entry(index: u8) -> (EncodedKey, EncodedBytes) {
	(EncodedKey::new(vec![0x10, index]), EncodedBytes(CowVec::new(vec![index; 24])))
}

fn entries(count: u8) -> Vec<(EncodedKey, EncodedBytes)> {
	(0..count).map(entry).collect()
}

fn write(
	store: &SnapshotStore,
	operator: OperatorId,
	upper: u64,
	dictionary_max: &[(u64, u128)],
	chunk_bytes: usize,
	entries: &[(EncodedKey, EncodedBytes)],
) -> Result<u64> {
	write_at(store, operator, upper, upper.saturating_sub(1), dictionary_max, chunk_bytes, entries)
}

#[allow(clippy::too_many_arguments)]
fn write_at(
	store: &SnapshotStore,
	operator: OperatorId,
	upper: u64,
	flow_cursor: u64,
	dictionary_max: &[(u64, u128)],
	chunk_bytes: usize,
	entries: &[(EncodedKey, EncodedBytes)],
) -> Result<u64> {
	store.write(
		SnapshotWrite {
			operator,
			upper: CommitVersion(upper),
			flow_cursor: CommitVersion(flow_cursor),
			dictionary_max,
			chunk_bytes,
		},
		&mut entries.to_vec().into_iter().map(Ok),
	)
}

#[test]
fn round_trip_restores_content_upper_and_dictionary_record() {
	// A snapshot must come back byte-identical, in key order, with the exact upper and
	// dictionary record it was written with; the payload is sized to span several chunks so
	// the read path must reassemble them in seq order. Falsified by scrambling the chunk read
	// order (content comes back permuted), by dropping the upper or dictionary columns, or by
	// truncating the entry stream at a chunk boundary.
	temp_dir(|dir| {
		let store = store_at(dir);
		let written = entries(16);
		let dictionary = vec![(3u64, 42u128), (9u64, 7u128)];
		let generation = write(&store, OP, 55, &dictionary, 64, &written).expect("write snapshot");
		assert_eq!(generation, 1);

		let loaded = store.load(OP, generation).expect("load snapshot");
		assert!(
			loaded.manifest.chunk_count > 1,
			"the fixture must span several chunks or the reassembly path is untested, got {}",
			loaded.manifest.chunk_count
		);
		assert_eq!(loaded.entries, written, "content must round trip byte-identical and ordered");
		assert_eq!(loaded.manifest.upper, CommitVersion(55));
		assert_eq!(loaded.manifest.dictionary_max, dictionary);
		assert_eq!(store.generations(OP).expect("generations"), vec![1]);
		Ok(())
	})
	.expect("test failed");
}

#[test]
fn tampered_or_reordered_chunks_refuse_to_load() {
	// The content hash is the only defense against a snapshot whose bytes rotted or whose
	// chunks were stitched together in the wrong order; both corruptions must fail the load
	// rather than seed the arena with garbage. Falsified by skipping the hash verification in
	// load(): the reordered stream still decodes into well-formed entries, so only the hash
	// can reject it.
	temp_dir(|dir| {
		let store = store_at(dir);
		write(&store, OP, 5, &[], 64, &entries(16)).expect("write snapshot");

		let raw = raw_connection(dir);
		raw.execute("UPDATE \"snapshot_chunk\" SET seq = 99 WHERE seq = 0", []).expect("stash chunk 0");
		raw.execute("UPDATE \"snapshot_chunk\" SET seq = 0 WHERE seq = 1", []).expect("move chunk 1");
		raw.execute("UPDATE \"snapshot_chunk\" SET seq = 1 WHERE seq = 99", []).expect("restore chunk 0");
		assert!(store.load(OP, 1).is_err(), "reordered chunks must fail the load");

		raw.execute("UPDATE \"snapshot_chunk\" SET seq = 99 WHERE seq = 1", []).expect("stash again");
		raw.execute("UPDATE \"snapshot_chunk\" SET seq = 1 WHERE seq = 0", []).expect("move back");
		raw.execute("UPDATE \"snapshot_chunk\" SET seq = 0 WHERE seq = 99", []).expect("restore order");
		assert!(store.load(OP, 1).is_ok(), "restoring the original order must load again");

		raw.execute("UPDATE \"snapshot_chunk\" SET bytes = zeroblob(length(bytes)) WHERE seq = 0", [])
			.expect("tamper chunk bytes");
		assert!(store.load(OP, 1).is_err(), "tampered chunk bytes must fail the load");
		Ok(())
	})
	.expect("test failed");
}

#[test]
fn aborted_generation_leaves_the_previous_one_untouched() {
	// Crash-atomicity contract: chunks and manifest commit in one sqlite transaction, so a
	// write that dies mid-stream must leave the previous complete generation as the newest
	// visible one and zero partial rows behind. Falsified by committing the manifest (or the
	// chunks) eagerly before the entry stream finishes: the aborted generation would then be
	// visible below.
	temp_dir(|dir| {
		let store = store_at(dir);
		let original = entries(4);
		write(&store, OP, 10, &[], 64, &original).expect("write generation 1");

		let mut failing = entries(16).into_iter().map(Ok).enumerate().map(|(index, entry)| {
			if index == 8 {
				Err(internal_error!("injected failure between chunks and manifest"))
			} else {
				entry
			}
		});
		assert!(store
			.write(
				SnapshotWrite {
					operator: OP,
					upper: CommitVersion(20),
					flow_cursor: CommitVersion(19),
					dictionary_max: &[],
					chunk_bytes: 64,
				},
				&mut failing,
			)
			.is_err());

		assert_eq!(
			store.generations(OP).expect("generations"),
			vec![1],
			"no partial generation may be visible"
		);
		let loaded = store.load(OP, 1).expect("previous generation must still load");
		assert_eq!(loaded.entries, original);
		assert_eq!(loaded.manifest.upper, CommitVersion(10));

		let raw = raw_connection(dir);
		let orphans: i64 = raw
			.query_row("SELECT COUNT(*) FROM \"snapshot_chunk\" WHERE generation = 2", [], |row| row.get(0))
			.expect("count orphan chunks");
		assert_eq!(orphans, 0, "an aborted generation must leave no chunk rows behind");
		Ok(())
	})
	.expect("test failed");
}

#[test]
fn a_third_write_prunes_the_oldest_generation_of_that_operator_only() {
	// Retention keeps exactly two generations per operator (current + previous) so load always
	// has a fallback while the file stays bounded; pruning must be scoped to the operator that
	// wrote. Falsified by pruning everything (previous generation gone), pruning nothing
	// (three generations linger), or forgetting the operator predicate (the sibling operator's
	// snapshot disappears).
	temp_dir(|dir| {
		let store = store_at(dir);
		let sibling = OperatorId(8);
		write(&store, sibling, 3, &[], 64, &entries(2)).expect("sibling snapshot");

		write(&store, OP, 10, &[], 64, &entries(4)).expect("generation 1");
		write(&store, OP, 20, &[], 64, &entries(8)).expect("generation 2");
		write(&store, OP, 30, &[], 64, &entries(12)).expect("generation 3");

		assert_eq!(store.generations(OP).expect("generations"), vec![3, 2]);
		assert_eq!(store.load(OP, 3).expect("newest loads").manifest.upper, CommitVersion(30));
		assert_eq!(store.load(OP, 2).expect("previous loads").manifest.upper, CommitVersion(20));
		assert_eq!(store.generations(sibling).expect("sibling generations"), vec![1]);

		let raw = raw_connection(dir);
		let stale: i64 = raw
			.query_row(
				"SELECT COUNT(*) FROM \"snapshot_chunk\" WHERE operator = 7 AND generation = 1",
				[],
				|row| row.get(0),
			)
			.expect("count stale chunks");
		assert_eq!(stale, 0, "the pruned generation must also drop its chunk rows");
		Ok(())
	})
	.expect("test failed");
}

#[test]
fn an_empty_snapshot_round_trips_its_upper() {
	// An operator whose state emptied out still needs a fresh snapshot: the upper is what
	// advances the flow's pin, and loading it must yield an empty arena at that version rather
	// than an error. Falsified by refusing to write or load a zero-chunk generation.
	temp_dir(|dir| {
		let store = store_at(dir);
		write(&store, OP, 44, &[], 64, &[]).expect("write empty snapshot");
		let loaded = store.load(OP, 1).expect("load empty snapshot");
		assert_eq!(loaded.manifest.chunk_count, 0);
		assert!(loaded.entries.is_empty());
		assert_eq!(loaded.manifest.upper, CommitVersion(44));
		Ok(())
	})
	.expect("test failed");
}

#[test]
fn the_flow_cursor_round_trips_independently_of_the_upper() {
	// The two versions live in different spaces: `upper` is the flow's OWN commit that last wrote
	// this operator, `flow_cursor` is how far the flow had CONSUMED the CDC log when the snapshot
	// was taken, and the latter is always strictly lower. Resuming replay from `upper` would skip
	// the window (cursor, upper], so a manifest that loses the distinction is a silent data loss.
	// Falsified by binding flow_cursor to the upper column in the INSERT or by decoding the same
	// column twice on load.
	temp_dir(|dir| {
		let store = store_at(dir);
		write_at(&store, OP, 90, 42, &[], 64, &entries(4)).expect("write snapshot");

		let loaded = store.load(OP, 1).expect("load snapshot");
		assert_eq!(loaded.manifest.upper, CommitVersion(90));
		assert_eq!(
			loaded.manifest.flow_cursor,
			CommitVersion(42),
			"the flow cursor must survive the round trip as its own value, not as a copy of the upper"
		);
		Ok(())
	})
	.expect("test failed");
}

#[test]
fn operators_lists_every_operator_that_holds_a_generation_once() {
	// The orphan sweep enumerates what operator.db still holds so it can drop generations of
	// operators the catalog no longer knows; an operator missing from this list keeps its
	// generations forever, and a duplicated one makes the sweep do the work twice. Falsified by
	// dropping the DISTINCT (op 7 appears twice) or by filtering on a single operator.
	temp_dir(|dir| {
		let store = store_at(dir);
		assert!(store.operators().expect("operators").is_empty(), "an empty database lists nothing");

		write(&store, OP, 10, &[], 64, &entries(2)).expect("generation 1");
		write(&store, OP, 20, &[], 64, &entries(2)).expect("generation 2");
		write(&store, OperatorId(3), 5, &[], 64, &entries(1)).expect("sibling generation");

		assert_eq!(store.operators().expect("operators"), vec![OperatorId(3), OP]);
		Ok(())
	})
	.expect("test failed");
}

#[test]
fn discard_removes_exactly_one_generation() {
	// The load path discards an invalid newest generation and falls back to the previous one,
	// so discard must delete only the generation it names. Falsified by discarding the whole
	// operator (the fallback generation vanishes) or by deleting only the manifest (chunk rows
	// leak forever).
	temp_dir(|dir| {
		let store = store_at(dir);
		write(&store, OP, 10, &[], 64, &entries(4)).expect("generation 1");
		write(&store, OP, 20, &[], 64, &entries(8)).expect("generation 2");

		store.discard(OP, 2).expect("discard newest");
		assert_eq!(store.generations(OP).expect("generations"), vec![1]);
		assert_eq!(store.load(OP, 1).expect("fallback loads").manifest.upper, CommitVersion(10));

		let raw = raw_connection(dir);
		let leaked: i64 = raw
			.query_row("SELECT COUNT(*) FROM \"snapshot_chunk\" WHERE generation = 2", [], |row| row.get(0))
			.expect("count leaked chunks");
		assert_eq!(leaked, 0, "discard must drop the generation's chunk rows too");
		Ok(())
	})
	.expect("test failed");
}
