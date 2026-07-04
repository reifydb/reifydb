// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Restart/recovery chaos for operator state.
//!
//! Runs the operator lifecycle ops (Set / Drop / flush / TTL / purge pump / cache wipe / reads)
//! against a single persistent store built over a kept SQLite directory, and at seed-chosen points
//! simulates a crash: the store is shut down, dropped, and rebuilt over the surviving file. The
//! commit buffer, read cache, and PendingDrops overlay do not survive; at most the flushed base of
//! each key does. The oracle's `restart()` encodes the recovery contract explicitly:
//! - commits and drops after the last covering flush are gone (dropped-but-unpurged rows legitimately resurface until
//!   re-collected);
//! - keys never covered by a flush must be exactly absent;
//! - covered keys stay merely plausible until their next Set, after which exactness resumes.
//! The reads keep checking differentially across restarts, so recovery-specific defects (a
//! resurrected row that never dies, a key lost despite being flushed, completeness rebuilt wrong)
//! surface as oracle violations in later steps.

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::{common::CommitVersion, delta::Delta, interface::store::MultiVersionCommit};
use reifydb_runtime::shutdown::Shutdown;
use reifydb_store_multi::store::StandardMultiStore;
use reifydb_value::util::cowvec::CowVec;

use crate::{
	fixtures::{build_row, flush, persistent_store_at, pump_pending_drops, restart_dir},
	operator::{OpOracle, check_get_many_op, check_get_op, check_range_op, op_key, ttl_sweep_op},
	workload::distinct_rows,
};

pub struct Params {
	pub keyspace: u64,
	pub min_steps: u32,
	pub max_steps: u32,
	pub commit_pct: u32,
	pub flush_pct: u32,
	pub ttl_pct: u32,
	pub drop_pct: u32,
	pub purge_pct: u32,
	pub wipe_pct: u32,
	pub restart_pct: u32,
	pub max_deltas: u64,
	pub max_batch: u64,
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let dir = restart_dir(seed);

	let mut configs: Vec<(&str, StandardMultiStore, OpOracle)> =
		vec![("restart-persistent", persistent_store_at(&dir), OpOracle::new(true))];

	let mut version: u64 = 0;
	let mut watermark: u64 = 0;

	let steps = rng.random_range(p.min_steps..=p.max_steps);
	for step in 0..steps {
		let roll = rng.random_range(0u32..100);
		let flush_hi = p.commit_pct + p.flush_pct;
		let ttl_hi = flush_hi + p.ttl_pct;
		let drop_hi = ttl_hi + p.drop_pct;
		let purge_hi = drop_hi + p.purge_pct;
		let wipe_hi = purge_hi + p.wipe_pct;
		let restart_hi = wipe_hi + p.restart_pct;

		if version == 0 || roll < p.commit_pct {
			version += 1;
			let count = rng.random_range(1..=p.max_deltas);
			let ids = distinct_rows(&mut rng, count, p.keyspace);
			for (_, store, oracle) in &mut configs {
				let deltas: Vec<Delta> = ids
					.iter()
					.map(|id| {
						let payload = format!("op{id}@v{version}").into_bytes();
						Delta::Set {
							key: op_key(*id),
							row: EncodedRow(CowVec::new(
								build_row(&payload).0.to_vec().into(),
							)),
						}
					})
					.collect();
				MultiVersionCommit::commit(store, CowVec::new(deltas), CommitVersion(version)).unwrap();
				for id in &ids {
					let payload = format!("op{id}@v{version}").into_bytes();
					oracle.set(*id, build_row(&payload).0.to_vec(), version);
				}
			}
		} else if roll < flush_hi {
			let cutoff = rng.random_range(1..=version);
			for (_, store, oracle) in &mut configs {
				flush(store, CommitVersion(cutoff));
				oracle.flush(cutoff);
			}
			watermark = watermark.max(cutoff);
		} else if roll < ttl_hi {
			let cutoff = rng.random_range(1..=version);
			for (_, store, oracle) in &mut configs {
				ttl_sweep_op(store, CommitVersion(cutoff));
				oracle.ttl(cutoff);
			}
		} else if roll < drop_hi {
			version += 1;
			let count = rng.random_range(1u64..=4);
			let ids = distinct_rows(&mut rng, count, p.keyspace);
			for (_, store, oracle) in &mut configs {
				let deltas: Vec<Delta> = ids
					.iter()
					.map(|id| Delta::Drop {
						key: op_key(*id),
					})
					.collect();
				MultiVersionCommit::commit(store, CowVec::new(deltas), CommitVersion(version)).unwrap();
				for id in &ids {
					oracle.drop_key(*id, version);
				}
			}
		} else if roll < purge_hi {
			for (_, store, oracle) in &mut configs {
				pump_pending_drops(store);
				oracle.pump();
			}
		} else if roll < wipe_hi {
			for (_, store, _) in &configs {
				store.clear_read();
			}
		} else if roll < restart_hi {
			for (_, store, oracle) in &mut configs {
				store.shutdown();
				oracle.restart();
			}
			let (_, store, _) = &mut configs[0];
			*store = persistent_store_at(&dir);
		} else {
			let read = if rng.random_range(0u32..2) == 0 {
				version
			} else {
				rng.random_range(watermark.max(1)..=version)
			};
			match rng.random_range(0u32..3) {
				0 => {
					let id = rng.random_range(1..=p.keyspace);
					check_get_op(&configs, id, read, step);
				}
				1 => {
					let batch = rng.random_range(1..=p.max_batch) as usize;
					check_range_op(&configs, read, batch, step);
				}
				_ => {
					let count = rng.random_range(1..=8);
					let ids = distinct_rows(&mut rng, count, p.keyspace);
					check_get_many_op(&configs, &ids, read, step);
				}
			}
		}
	}
}
