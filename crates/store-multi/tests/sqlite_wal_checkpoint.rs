// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::{
		catalog::{id::TableId, shape::ShapeId},
		store::EntryKind,
	},
};
use reifydb_sqlite::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
};
use reifydb_store_multi::{persistent::MultiPersistentTier, tier::TierStorage};
use reifydb_type::util::cowvec::CowVec;

fn kind() -> EntryKind {
	EntryKind::Source(ShapeId::Table(TableId(7000)))
}

fn write(tier: &MultiPersistentTier, version: u64, key: &str) {
	let batch = HashMap::from([(
		kind(),
		vec![(EncodedKey::new(key.as_bytes().to_vec()), Some(CowVec::new(key.as_bytes().to_vec())))],
	)]);
	tier.set(CommitVersion(version), batch).unwrap();
}

// A reader pinning an old WAL snapshot is exactly what the production read pool does, and it is the
// only thing that lets the WAL grow past its auto-checkpoint threshold (PASSIVE cannot reset while a
// reader holds a mark). This test reproduces that: grow the WAL under a held reader, prove
// maybe_checkpoint refuses to reset while the reader is held, then prove it resets the WAL once the
// reader is released. RESTART arms the WAL to wrap but the logical frame count only collapses on the
// next write that wraps it, so the final write-then-measure is what proves the reset actually took
// effect. If RESTART ever stopped resetting the WAL the commit-latency regression would silently
// return, so that bounded post-reset frame count is the assertion that matters.
#[test]
fn maybe_checkpoint_resets_wal_only_after_reader_released() {
	const THRESHOLD: u32 = 16;
	const WRITES: u64 = 200;

	let (config, _guard) = SqliteConfig::test();
	let config = config.read_pool_size(1).wal_autocheckpoint(THRESHOLD);
	let tier = MultiPersistentTier::sqlite(config.clone());

	write(&tier, 1, "seed");

	let db_path = resolve_db_path(config.path.clone(), "persistent.db");
	let probe = connect(&db_path, convert_flags(&config.flags)).expect("probe connection");
	probe.execute_batch("BEGIN; SELECT count(*) FROM sqlite_master;").expect("pin read snapshot");

	for i in 0..WRITES {
		write(&tier, i + 2, &format!("key-{i}"));
	}

	let held = tier.maybe_checkpoint().unwrap();
	assert!(
		held.log_frames > THRESHOLD,
		"WAL must grow past the threshold while a reader pins an old snapshot; got {} frames",
		held.log_frames
	);
	assert!(!held.restarted, "RESTART must not reset the WAL while a reader still holds an old snapshot");

	probe.execute_batch("COMMIT").expect("release read snapshot");
	drop(probe);

	let released = tier.maybe_checkpoint().unwrap();
	assert!(released.restarted, "RESTART must succeed once the pinning reader is gone");
	assert!(released.log_frames > THRESHOLD, "the pre-restart frame count should still reflect the grown WAL");

	write(&tier, WRITES + 2, "post-restart");

	let after = tier.maybe_checkpoint().unwrap();
	assert!(
		after.log_frames <= THRESHOLD,
		"a successful RESTART must wrap the WAL so later writes reuse it from the start; got {} frames",
		after.log_frames
	);
	assert!(!after.restarted, "no RESTART is needed once the WAL is back under the threshold");
}
