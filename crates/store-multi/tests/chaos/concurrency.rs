// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Multi-threaded concurrency stress for StandardMultiStore. Non-deterministic by construction (real
//! threads plus the background actors), so it steps outside the seed-replay rule and is `#[ignore]`d.
//! Writer `t` owns exactly the rows where `row % writers == t`, which is what keeps each key's final
//! value deterministic - its owner's last op - under any interleaving, so the run stays checkable.

use std::{
	collections::BTreeMap,
	sync::atomic::{AtomicU64, Ordering},
	thread,
	time::Instant,
};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::{
		catalog::{flow::OperatorId, id::TableId, storage::StorageId},
		store::{MultiVersionCommit, MultiVersionGet},
	},
	key::{
		EncodableKey,
		operator_state::{GroupId, KeyspaceId, OperatorStateKey},
		row::RowKey,
	},
};
use reifydb_store_multi::{MultiVersionScope, store::StandardMultiStore};
use reifydb_value::{util::cowvec::CowVec, value::duration::Duration};

const STORAGE: StorageId = StorageId::Table(TableId(1));

const OP_NODE: OperatorId = OperatorId(9);

fn conc_op_key(row: u64) -> reifydb_codec::key::encoded::EncodedKey {
	OperatorStateKey::encoded(OP_NODE, GroupId::ROOT, KeyspaceId::CUSTOM_NOT_CACHED, row.to_be_bytes().to_vec())
}

fn scan_op_rows(store: &StandardMultiStore, read: u64, batch: usize, reverse: bool) -> Vec<(u64, Vec<u8>)> {
	let range = OperatorStateKey::node_range(OP_NODE);
	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(read),
	};
	let rows = if reverse {
		store.range_rev(range, scope, batch).collect::<Result<Vec<_>, _>>().unwrap()
	} else {
		store.range(range, scope, batch).collect::<Result<Vec<_>, _>>().unwrap()
	};
	rows.into_iter()
		.map(|r| {
			let decoded = OperatorStateKey::decode(&r.key).unwrap();
			(u64::from_be_bytes(decoded.suffix.as_slice().try_into().unwrap()), r.bytes.to_vec())
		})
		.collect()
}

fn parse_value(bytes: &[u8]) -> Option<(u64, u64, u64)> {
	let s = std::str::from_utf8(bytes).ok()?;
	let mut parts = s.split(':');
	let t = parts.next()?.strip_prefix('t')?.parse().ok()?;
	let r = parts.next()?.strip_prefix('r')?.parse().ok()?;
	let seq = parts.next()?.strip_prefix('s')?.parse().ok()?;
	if parts.next().is_some() {
		return None;
	}
	Some((t, r, seq))
}

fn check_structural(rows: &[(u64, Vec<u8>)], writers: u64, ctx: &str) {
	let mut seen: Vec<u64> = Vec::with_capacity(rows.len());
	for (row, value) in rows {
		match parse_value(value) {
			Some((t, r, _)) => {
				assert_eq!(
					r, *row,
					"{ctx}: value row {r} != key row {row} (cross-key bleed) value={value:?}"
				);
				assert_eq!(
					t,
					row % writers,
					"{ctx}: row {row} owned by t{} but value tagged t{t} value={value:?}",
					row % writers
				);
			}
			None => panic!("{ctx}: row {row} returned undecodable/torn value {value:?}"),
		}
		seen.push(*row);
	}
	let mut sorted = seen.clone();
	sorted.sort_unstable();
	sorted.dedup();
	assert_eq!(sorted.len(), seen.len(), "{ctx}: duplicate key in range result: {seen:?}");
	let mut ordered = seen.clone();
	ordered.sort_unstable();
	// Rows encode descending, so a forward (ascending-encoded) scan yields DESCENDING row numbers.
	let mut desc = ordered.clone();
	desc.reverse();
	assert!(seen == ordered || seen == desc, "{ctx}: range result not monotonically ordered: {seen:?}");
}

fn scan_rows(store: &StandardMultiStore, read: u64, batch: usize, reverse: bool) -> Vec<(u64, Vec<u8>)> {
	let range = RowKey::full_scan(STORAGE);
	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(read),
	};
	let rows = if reverse {
		store.range_rev(range, scope, batch).collect::<Result<Vec<_>, _>>().unwrap()
	} else {
		store.range(range, scope, batch).collect::<Result<Vec<_>, _>>().unwrap()
	};
	rows.into_iter().map(|r| (RowKey::decode(&r.key).unwrap().row.0, r.bytes.to_vec())).collect()
}

pub struct Config {
	pub writers: u64,
	pub readers: u64,
	pub rows_per_writer: u64,
	pub ops_per_writer: u64,
	pub timeout: Duration,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			writers: 4,
			readers: 2,
			rows_per_writer: 64,
			ops_per_writer: 3000,
			timeout: Duration::from_seconds(60).unwrap(),
		}
	}
}

/// Returns each writer's final per-row state (None = tombstoned/absent). Panics on any structural
/// violation, panic propagation from a worker, or if the run exceeds the timeout.
pub fn run(seed: u64, cfg: Config) -> BTreeMap<u64, Option<Vec<u8>>> {
	let (store, _guard) = StandardMultiStore::testing_memory_with_persistent_sqlite();
	let version = AtomicU64::new(1);
	let stop = AtomicU64::new(0);
	let start = Instant::now();

	type WriterMaps = (BTreeMap<u64, Option<Vec<u8>>>, BTreeMap<u64, Option<Vec<u8>>>);
	let final_maps: Vec<WriterMaps> = thread::scope(|s| {
		let mut writer_handles = Vec::new();
		for t in 0..cfg.writers {
			let store = store.clone();
			let version = &version;
			let stop = &stop;
			let cfg = &cfg;
			writer_handles.push(s.spawn(move || {
				let mut rng = StdRng::seed_from_u64(seed ^ (0x9E3779B97F4A7C15u64.wrapping_mul(t + 1)));
				let mut last: BTreeMap<u64, Option<Vec<u8>>> = BTreeMap::new();
				let owned: Vec<u64> = (1..=cfg.writers * cfg.rows_per_writer)
					.filter(|r| r % cfg.writers == t)
					.collect();
				let mut op_last: BTreeMap<u64, Option<Vec<u8>>> = BTreeMap::new();
				for seq in 0..cfg.ops_per_writer {
					let row = owned[rng.random_range(0..owned.len() as u32) as usize];
					let v = version.fetch_add(1, Ordering::SeqCst);
					match rng.random_range(0u32..13) {
						10 => {
							MultiVersionCommit::commit(
								&store,
								CowVec::new(vec![Delta::remove_silent(conc_op_key(
									row,
								))]),
								CommitVersion(v),
							)
							.unwrap();
							op_last.insert(row, None);
						}
						11 | 12 => {
							let value = format!("t{t}:r{row}:s{seq}").into_bytes();
							MultiVersionCommit::commit(
								&store,
								CowVec::new(vec![Delta::Set {
									key: conc_op_key(row),
									bytes: EncodedBytes(CowVec::new(value.clone())),
								}]),
								CommitVersion(v),
							)
							.unwrap();
							op_last.insert(row, Some(value));
						}
						0 => {
							MultiVersionCommit::commit(
								&store,
								CowVec::new(vec![Delta::remove_silent(
									RowKey::encoded(STORAGE, row),
								)]),
								CommitVersion(v),
							)
							.unwrap();
							last.insert(row, None);
						}
						_ => {
							let value = format!("t{t}:r{row}:s{seq}").into_bytes();
							MultiVersionCommit::commit(
								&store,
								CowVec::new(vec![Delta::Set {
									key: RowKey::encoded(STORAGE, row),
									bytes: EncodedBytes(CowVec::new(value.clone())),
								}]),
								CommitVersion(v),
							)
							.unwrap();
							last.insert(row, Some(value));
						}
					}
					if rng.random_range(0u32..50) == 0 {
						store.flush_pending_blocking();
					}
				}
				stop.fetch_add(1, Ordering::SeqCst);
				(last, op_last)
			}));
		}

		let mut reader_handles = Vec::new();
		for _ in 0..cfg.readers {
			let store = store.clone();
			let version = &version;
			let stop = &stop;
			let cfg = &cfg;
			reader_handles.push(s.spawn(move || {
				let mut rng = StdRng::seed_from_u64(seed ^ 0xD1B54A32D192ED03u64);
				while stop.load(Ordering::SeqCst) < cfg.writers {
					let read = version.load(Ordering::SeqCst);
					match rng.random_range(0u32..5) {
						0 => {
							let row =
								rng.random_range(1..=cfg.writers * cfg.rows_per_writer);
							if let Some(r) = store
								.get(
									&RowKey::encoded(STORAGE, row),
									CommitVersion(read),
								)
								.unwrap()
							{
								check_structural(
									&[(row, r.bytes.to_vec())],
									cfg.writers,
									"reader-get",
								);
							}
						}
						3 => {
							let row =
								rng.random_range(1..=cfg.writers * cfg.rows_per_writer);
							if let Some(r) = store
								.get(&conc_op_key(row), CommitVersion(read))
								.unwrap()
							{
								check_structural(
									&[(row, r.bytes.to_vec())],
									cfg.writers,
									"reader-op-get",
								);
							}
						}
						4 => {
							let reverse = rng.random_range(0u32..2) == 0;
							let batch = rng.random_range(1..=32) as usize;
							let rows = scan_op_rows(&store, read, batch, reverse);
							check_structural(&rows, cfg.writers, "reader-op-range");
						}
						_ => {
							let reverse = rng.random_range(0u32..2) == 0;
							let batch = rng.random_range(1..=32) as usize;
							let rows = scan_rows(&store, read, batch, reverse);
							check_structural(&rows, cfg.writers, "reader-range");
						}
					}
				}
			}));
		}

		// A worker hang surfaces as the stop counter never reaching `writers` within the timeout.
		while stop.load(Ordering::SeqCst) < cfg.writers {
			assert!(
				start.elapsed() < cfg.timeout.to_std(),
				"concurrency stress TIMED OUT after {:?} (seed={seed}) - possible deadlock; only {}/{} writers finished",
				cfg.timeout,
				stop.load(Ordering::SeqCst),
				cfg.writers
			);
			thread::sleep(Duration::from_milliseconds(5).unwrap().to_std());
		}

		for h in reader_handles {
			h.join().expect("reader thread panicked");
		}
		writer_handles.into_iter().map(|h| h.join().expect("writer thread panicked")).collect()
	});

	let mut expected: BTreeMap<u64, Option<Vec<u8>>> = BTreeMap::new();
	let mut op_expected: BTreeMap<u64, Option<Vec<u8>>> = BTreeMap::new();
	for (m, op_m) in final_maps {
		expected.extend(m);
		op_expected.extend(op_m);
	}

	store.flush_pending_blocking();
	store.clear_read();
	let final_version = version.load(Ordering::SeqCst);
	let live: BTreeMap<u64, Vec<u8>> = scan_rows(&store, final_version, 16, false).into_iter().collect();

	for (row, want) in &expected {
		match want {
			Some(value) => assert_eq!(
				live.get(row),
				Some(value),
				"FINAL state mismatch: row {row} owner-wrote {value:?} but store has {:?} (seed={seed})",
				live.get(row)
			),
			None => assert!(
				!live.contains_key(row),
				"FINAL state mismatch: row {row} was tombstoned by its owner but store still has {:?} (seed={seed})",
				live.get(row)
			),
		}
	}
	let live_unexpected: Vec<u64> =
		live.keys().filter(|r| !matches!(expected.get(r), Some(Some(_)))).copied().collect();
	assert!(
		live_unexpected.is_empty(),
		"FINAL state has rows no writer left live: {live_unexpected:?} (seed={seed})"
	);

	store.clear_read();
	let op_live: BTreeMap<u64, Vec<u8>> = scan_op_rows(&store, final_version, 16, false).into_iter().collect();
	for (row, want) in &op_expected {
		match want {
			Some(value) => assert_eq!(
				op_live.get(row),
				Some(value),
				"FINAL operator state mismatch: row {row} owner-wrote {value:?} but store has {:?} (seed={seed})",
				op_live.get(row)
			),
			None => assert!(
				!op_live.contains_key(row),
				"FINAL operator state mismatch: row {row} was dropped by its owner but store still has {:?} (seed={seed})",
				op_live.get(row)
			),
		}
	}
	let op_unexpected: Vec<u64> =
		op_live.keys().filter(|r| !matches!(op_expected.get(r), Some(Some(_)))).copied().collect();
	assert!(
		op_unexpected.is_empty(),
		"FINAL operator state has rows no writer left live: {op_unexpected:?} (seed={seed})"
	);

	expected
}
