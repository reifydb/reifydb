// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod ring_buffer;
mod series;
mod table;

use std::{thread::sleep, time::Duration};

use reifydb::{ConfigKey, SqliteConfig, Value, embedded, testing::db::TestDb};

pub const TTL_SECS: u64 = 1;

pub const STRADDLE_TTL_SECS: u64 = 10;

pub const EVICT_TIMEOUT: Duration = Duration::from_secs(30);

pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(90);

pub fn ttl_db(path: impl AsRef<std::path::Path>, extra: impl IntoIterator<Item = (ConfigKey, Value)>) -> TestDb {
	// These drains are timed against a wall clock, so the evict budget must be the production one; the testing
	// profile's 4x2 rows per tick is 1024x too slow, and extra wins because bootstrap rejects a duplicate key.
	let overrides: Vec<(ConfigKey, Value)> = extra.into_iter().collect();
	let mut configs: Vec<(ConfigKey, Value)> = vec![
		(ConfigKey::RetentionStartupGrace, Value::duration_seconds(1)),
		(ConfigKey::RetentionEvictInterval, Value::duration_seconds(1)),
		(ConfigKey::EpochBucketInterval, Value::duration_seconds(1)),
		(ConfigKey::MultiFlushInterval, Value::duration_seconds(1)),
		(ConfigKey::RetentionEvictBatchSize, ConfigKey::RetentionEvictBatchSize.production_value()),
		(
			ConfigKey::RetentionEvictMaxBatchesPerTick,
			ConfigKey::RetentionEvictMaxBatchesPerTick.production_value(),
		),
	]
	.into_iter()
	.filter(|(key, _)| !overrides.iter().any(|(over, _)| over == key))
	.collect();
	configs.extend(overrides);
	TestDb::from(embedded::sqlite(SqliteConfig::new(path)).with_configs(configs).build().unwrap())
}

pub fn one_row_per_tick() -> [(ConfigKey, Value); 2] {
	[
		(ConfigKey::RetentionEvictBatchSize, Value::Uint8(1)),
		(ConfigKey::RetentionEvictMaxBatchesPerTick, Value::Uint8(1)),
	]
}

pub const PARTITIONS: usize = 64;

pub const BACKLOG_ROWS: usize = 8;

pub fn backlog(target: &str, prefix: &str) -> String {
	let values: Vec<String> = (0..BACKLOG_ROWS).map(|n| format!("{{ {prefix}n: {n} }}")).collect();
	format!("insert {target} [{}]", values.join(", "))
}

pub fn spread(target: &str, prefix: &str, tag: &str, partitions: usize, base: usize) -> String {
	let values: Vec<String> = (0..partitions)
		.flat_map(|p| (0..2).map(move |r| format!("{{ {prefix}p: \"{tag}_{p}\", n: {} }}", base + p * 2 + r)))
		.collect();
	format!("insert {target} [{}]", values.join(", "))
}

pub fn await_drained(db: &TestDb, rql: &str, want: usize) {
	let got = db.await_exact_row_count(rql, want, DRAIN_TIMEOUT);
	assert_eq!(got, want, "`{rql}` must reach {want} rows even when the tick budget forces many passes");
}

pub fn age_past_ttl() {
	age_past(TTL_SECS);
}

pub fn age_past(secs: u64) {
	sleep(Duration::from_millis(secs * 1000 + 500));
}

pub fn await_evicted(db: &TestDb, rql: &str, want: usize) {
	let got = db.await_exact_row_count(rql, want, EVICT_TIMEOUT);
	assert_eq!(got, want, "`{rql}` must settle at {want} rows once the evictor has run");
}

pub fn seed_canary(db: &TestDb) {
	db.admin("create table test::canary { n: int4 } with { time: processing, row: { ttl: 1s } }");
	db.command("insert test::canary [{ n: 1 }]");
}

pub fn assert_evictor_ran(db: &TestDb) {
	let got = db.await_exact_row_count("from test::canary", 0, EVICT_TIMEOUT);
	assert_eq!(got, 0, "the canary must drain, or a no-eviction assertion proves nothing");
}

pub fn await_survivor(db: &TestDb, rql: &str, want: usize) {
	let got = db.await_exact_row_count(rql, want, Duration::from_secs(STRADDLE_TTL_SECS / 2));
	assert_eq!(got, want, "`{rql}` must settle at {want} rows while the younger write is still inside its ttl");
}
