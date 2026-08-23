// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Everything written during the drain sits strictly above the drained range, so the expected result is a fixed
//! snapshot no matter how pages fall or which tier answers.

use std::collections::Bound;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_core::common::CommitVersion;
use reifydb_store_cdc::storage::CdcStorage;

use crate::{
	fixtures::{ChangeKind, Harness, record},
	workload::TIMESTAMP_BASE,
};

#[derive(Clone, Debug)]
pub struct Params {
	pub frozen: u64,
	pub mutable: u64,
	pub min_batch: u64,
	pub max_batch: u64,
	pub interleave_pct: u32,
	pub flush_pct: u32,
	pub max_interleaved: u32,
}

pub fn drive(seed: u64, p: Params) {
	let mut rng = StdRng::seed_from_u64(seed);
	let mut harness = Harness::new();

	// Sealing only the first half splits the drained range across a block and the commit tier, the seam pagination
	// must hand off across.
	let sealed = p.frozen / 2;
	for version in 1..=sealed {
		write(&mut harness, version);
	}
	harness.flush_all();
	for version in sealed + 1..=p.frozen {
		write(&mut harness, version);
	}

	let expected: Vec<u64> = (1..=p.frozen).collect();
	let batch = rng.random_range(p.min_batch..=p.max_batch);
	let mut next = p.frozen;

	for index in 0..harness.configs.len() {
		let mut cursor = Bound::Included(CommitVersion(1));
		let mut drained: Vec<u64> = Vec::new();
		let mut pulls = 0u64;
		loop {
			let page = harness.configs[index]
				.store
				.read_range(cursor, Bound::Included(CommitVersion(p.frozen)), batch)
				.unwrap();
			for cdc in &page.items {
				drained.push(cdc.version.0);
			}
			match page.items.last() {
				Some(cdc) => cursor = Bound::Excluded(cdc.version),
				None => break,
			}
			if !page.has_more {
				break;
			}
			pulls += 1;
			assert!(
				pulls <= p.frozen + 2,
				"DRAIN did not terminate: config={} batch={batch} after {pulls} pulls",
				harness.configs[index].name
			);
			next = interleave(&mut rng, &mut harness, &p, next);
		}
		assert_eq!(
			drained,
			expected,
			"SNAPSHOT mismatch: config={} batch={batch} (store {} rows vs snapshot {}) - a record was lost or repeated across a page boundary while writes and flushes landed above the range",
			harness.configs[index].name,
			drained.len(),
			expected.len()
		);
	}
}

/// Writes stay above the drained range, so the drained snapshot stays fixed while both tiers keep moving underneath it.
fn interleave(rng: &mut StdRng, harness: &mut Harness, p: &Params, mut next: u64) -> u64 {
	if rng.random_range(0u32..100) >= p.interleave_pct {
		return next;
	}
	let count = rng.random_range(1..=p.max_interleaved);
	for _ in 0..count {
		if next >= p.frozen + p.mutable {
			break;
		}
		next += 1;
		write(harness, next);
	}
	if rng.random_range(0u32..100) < p.flush_pct {
		harness.flush_all();
	}
	next
}

fn write(harness: &mut Harness, version: u64) {
	let (cdc, row) = record(version, TIMESTAMP_BASE + version, &[(1, version % 16 + 1, 8, ChangeKind::Insert)]);
	for config in &mut harness.configs {
		if !config.oracle.write(version, row.clone()) {
			continue;
		}
		config.store.write(&cdc).unwrap_or_else(|err| {
			panic!("SNAPSHOT write rejected: config={} version={version} err={err:?}", config.name)
		});
		if config.oracle.should_cut() {
			crate::fixtures::flush(config);
		}
	}
}
