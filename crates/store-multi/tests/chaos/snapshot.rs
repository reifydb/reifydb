// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Mid-scan snapshot stability chaos: a paginated `AsOf{V}` scan must hold its creation-time snapshot
//! while commits and bounded flushes land between batch pulls. Only snapshot-preserving ops are
//! interleaved (commits get versions > V, flushes use cutoff <= V), so any divergence is a real bug.

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{common::CommitVersion, delta::Delta, interface::store::MultiVersionCommit, key::row::RowKey};
use reifydb_store_multi::{MultiVersionScope, store::StandardMultiStore};
use reifydb_testing_chaos::fuzz::pick;
use reifydb_value::util::cowvec::CowVec;

use crate::{
	STORAGE,
	fixtures::{flush, sync_persistent_store},
	oracle::{Oracle, Scope},
	workload::distinct_rows,
};

pub struct Params {
	pub keyspace: u64,
	pub seed_commits: u32,
	pub max_deltas: u64,
	pub remove_pct: u32,
	pub interleave_pct: u32,
	pub commit_vs_flush_pct: u32,
}

fn commit_rows(
	configs: &[(&str, StandardMultiStore)],
	oracle: &mut Oracle,
	version: u64,
	deltas: &[(u64, Option<Vec<u8>>)],
) {
	oracle.apply(version, deltas);
	for (_, store) in configs {
		let store_deltas: Vec<Delta> = deltas
			.iter()
			.map(|(row, value)| match value {
				Some(bytes) => Delta::Set {
					key: RowKey::encoded(STORAGE, *row),
					bytes: EncodedBytes(CowVec::new(bytes.clone())),
				},
				None => Delta::remove_silent(RowKey::encoded(STORAGE, *row)),
			})
			.collect();
		MultiVersionCommit::commit(store, CowVec::new(store_deltas), CommitVersion(version)).unwrap();
	}
}

fn gen_deltas(
	rng: &mut StdRng,
	version: u64,
	count: u64,
	keyspace: u64,
	remove_pct: u32,
) -> Vec<(u64, Option<Vec<u8>>)> {
	distinct_rows(rng, count, keyspace)
		.into_iter()
		.map(|row| {
			if rng.random_range(0u32..100) < remove_pct {
				(row, None)
			} else {
				(row, Some(format!("r{row}@v{version}").into_bytes()))
			}
		})
		.collect()
}

fn drain_with_interleave(
	rng: &mut StdRng,
	configs: &[(&str, StandardMultiStore)],
	oracle: &mut Oracle,
	version: &mut u64,
	pinned: u64,
	drain_idx: usize,
	reverse: bool,
	batch: usize,
	p: &Params,
) {
	let name = configs[drain_idx].0;
	let frozen = oracle.scan(
		Scope::AsOf {
			read: pinned,
		},
		reverse,
	);

	let scope = MultiVersionScope::AsOf {
		read: CommitVersion(pinned),
	};
	let mut iter: Box<dyn Iterator<Item = _>> = if reverse {
		Box::new(configs[drain_idx].1.range_rev(RowKey::full_scan(STORAGE), scope, batch))
	} else {
		Box::new(configs[drain_idx].1.range(RowKey::full_scan(STORAGE), scope, batch))
	};

	let mut drained: Vec<(Vec<u8>, Vec<u8>, u64)> = Vec::new();
	loop {
		match iter.next() {
			Some(item) => {
				let r = item.unwrap();
				drained.push((r.key.to_vec(), r.bytes.to_vec(), r.version.0));
			}
			None => break,
		}
		if rng.random_range(0u32..100) < p.interleave_pct {
			if rng.random_range(0u32..100) < p.commit_vs_flush_pct {
				*version += 1;
				let count = rng.random_range(1..=p.max_deltas);
				let deltas = gen_deltas(rng, *version, count, p.keyspace, p.remove_pct);
				commit_rows(configs, oracle, *version, &deltas);
			} else {
				let cutoff = rng.random_range(1..=pinned);
				for (_, store) in configs {
					if store.persistent().is_some() {
						flush(store, CommitVersion(cutoff));
					}
				}
			}
		}
	}

	assert_eq!(
		drained.len(),
		frozen.len(),
		"SNAPSHOT len mismatch: config={name} reverse={reverse} batch={batch} pinned={pinned} drained={} frozen={}",
		drained.len(),
		frozen.len()
	);
	assert_eq!(
		drained, frozen,
		"SNAPSHOT content mismatch: config={name} reverse={reverse} batch={batch} pinned={pinned} - a mid-scan commit/flush corrupted the as-of-{pinned} view"
	);
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let mut oracle = Oracle::default();

	let memory = StandardMultiStore::testing_memory();
	let (persistent, _g1) = sync_persistent_store();
	let (tiny, _g2) = sync_persistent_store();
	let page_rows = pick(&mut rng, &[256u64, 512]);
	tiny.configure_read_buffer(2, page_rows);
	let configs: Vec<(&str, StandardMultiStore)> =
		vec![("memory", memory), ("persistent", persistent), ("tiny_cache", tiny)];

	let mut version: u64 = 0;
	for _ in 0..p.seed_commits {
		version += 1;
		let count = rng.random_range(1..=p.max_deltas);
		let deltas = gen_deltas(&mut rng, version, count, p.keyspace, p.remove_pct);
		commit_rows(&configs, &mut oracle, version, &deltas);
	}
	// A partial flush makes the snapshot span both tiers, which is the merge case the iterator has to
	// hold stable mid-scan.
	let seed_cutoff = (version / 2).max(1);
	for (_, store) in &configs {
		if store.persistent().is_some() {
			flush(store, CommitVersion(seed_cutoff));
		}
	}
	if version == 0 {
		return;
	}
	let pinned = version;

	for drain_idx in 0..configs.len() {
		for reverse in [false, true] {
			let batch = rng.random_range(1..=8) as usize;
			drain_with_interleave(
				&mut rng,
				&configs,
				&mut oracle,
				&mut version,
				pinned,
				drain_idx,
				reverse,
				batch,
				&p,
			);
		}
	}

	// A fresh scan at the now-current version proves the mid-scan churn left the store uncorrupted.
	let current = version;
	let expected_fwd = oracle.scan(
		Scope::AsOf {
			read: current,
		},
		false,
	);
	for (name, store) in &configs {
		let got: Vec<(Vec<u8>, Vec<u8>, u64)> = store
			.range(
				RowKey::full_scan(STORAGE),
				MultiVersionScope::AsOf {
					read: CommitVersion(current),
				},
				16,
			)
			.collect::<Result<Vec<_>, _>>()
			.unwrap()
			.into_iter()
			.map(|r| (r.key.to_vec(), r.bytes.to_vec(), r.version.0))
			.collect();
		assert_eq!(
			got, expected_fwd,
			"POST-DRAIN corruption: config={name} current={current} store and oracle disagree after mid-scan churn"
		);
	}
}
