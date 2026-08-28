// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Paginated drain under interleaved writes and flushes. `range_batch` resumes from an excluded lower bound
//! rather than holding a cursor, so a flush that moves rows from the commit buffer into sqlite between two pulls
//! is exactly where a row can be dropped or served twice. Everything mutated during the drain is kept strictly
//! outside the drained range, which makes the expected result a fixed snapshot no matter how the pages fall.

use std::ops::Bound;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::GroupId};
use reifydb_store_operator::types::{DurablePre, OperatorWrite};
use reifydb_value::{
	byte_size::ByteSize,
	value::{datetime::DateTime, row_number::RowNumber},
};

use crate::{
	fixtures::{Harness, key, row},
	oracle::Oracle,
};

const GROUP: u64 = 1;

const KEYSPACE: u8 = 0x10;

const FROZEN: OperatorId = OperatorId(1);

const NOISE: OperatorId = OperatorId(2);

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
	let harness = Harness::new();
	let mut oracle = Oracle::default();

	for suffix in 1..=(p.frozen + p.mutable) {
		let key_bytes = key(GROUP, KEYSPACE, suffix);
		let value = row(FROZEN.0, suffix, 0);
		let pre = oracle.value_bytes(FROZEN.0, key_bytes.as_slice());
		oracle.set(FROZEN.0, key_bytes.as_slice(), value.clone());
		let write = state_write(FROZEN, key_bytes, value, pre);
		for config in &harness.configs {
			config.store.apply_batch(&[write.clone()]);
		}
	}
	// Rewriting only the odd half after a flush is what leaves the drained range split across both layers.
	harness.flush_all();
	for suffix in 1..=p.frozen {
		if suffix % 2 == 0 {
			continue;
		}
		let key_bytes = key(GROUP, KEYSPACE, suffix);
		let value = row(FROZEN.0, suffix, 1);
		let pre = oracle.value_bytes(FROZEN.0, key_bytes.as_slice());
		oracle.set(FROZEN.0, key_bytes.as_slice(), value.clone());
		let write = state_write(FROZEN, key_bytes, value, pre);
		for config in &harness.configs {
			config.store.apply_batch(&[write.clone()]);
		}
	}

	let low = key(GROUP, KEYSPACE, 1);
	let high = key(GROUP, KEYSPACE, p.frozen);
	let expected = oracle.range(FROZEN.0, &Bound::Included(low.to_vec()), &Bound::Included(high.to_vec()));
	assert_eq!(
		expected.len() as u64,
		p.frozen,
		"the drained range must hold every frozen key, otherwise the scenario proves nothing"
	);

	let batch = rng.random_range(p.min_batch..=p.max_batch);
	for config in &harness.configs {
		let mut start: Bound<EncodedKey> = Bound::Included(low.clone());
		let mut drained: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
		let mut pulls = 0u32;
		loop {
			let page = config.store.range_batch(
				FROZEN,
				EncodedKeyRange::new(start.clone(), Bound::Included(high.clone())),
				batch,
			);
			for (item_key, item_row) in &page.items {
				drained.push((item_key.to_vec(), item_row.body().to_vec()));
			}
			match page.items.last() {
				Some((item_key, _)) => start = Bound::Excluded(item_key.clone()),
				None => break,
			}
			if !page.has_more {
				break;
			}
			pulls += 1;
			assert!(
				pulls <= p.frozen as u32 + 2,
				"DRAIN did not terminate: config={} batch={batch} after {pulls} pulls",
				config.name
			);
			interleave(&mut rng, &harness, &mut oracle, &p);
		}
		assert_eq!(
			drained,
			expected,
			"SNAPSHOT mismatch: config={} batch={batch} (store {} rows vs snapshot {}) - a row was lost or repeated across a page boundary while writes and flushes landed outside the range",
			config.name,
			drained.len(),
			expected.len()
		);
	}
}

/// Mutations confined to suffixes above the drained range and to a second operator, plus flushes, so the drained
/// snapshot stays fixed while both tiers keep moving underneath it.
fn interleave(rng: &mut StdRng, harness: &Harness, oracle: &mut Oracle, p: &Params) {
	if rng.random_range(0u32..100) >= p.interleave_pct {
		return;
	}
	let count = rng.random_range(1..=p.max_interleaved);
	for index in 0..count {
		match rng.random_range(0u32..4) {
			0 => {
				let suffix = p.frozen + rng.random_range(1..=p.mutable);
				let key_bytes = key(GROUP, KEYSPACE, suffix);
				let value = row(FROZEN.0, suffix, index);
				let pre = oracle.value_bytes(FROZEN.0, key_bytes.as_slice());
				oracle.set(FROZEN.0, key_bytes.as_slice(), value.clone());
				let write = state_write(FROZEN, key_bytes, value, pre);
				for config in &harness.configs {
					config.store.apply_batch(&[write.clone()]);
				}
			}
			1 => {
				let suffix = p.frozen + rng.random_range(1..=p.mutable);
				let key_bytes = key(GROUP, KEYSPACE, suffix);
				let pre = match oracle.value_bytes(FROZEN.0, key_bytes.as_slice()) {
					Some(pre_value_bytes) => DurablePre::Present(pre_value_bytes),
					None => DurablePre::Absent,
				};
				oracle.remove(FROZEN.0, key_bytes.as_slice());
				let write = OperatorWrite::Remove {
					operator: FROZEN,
					key: key_bytes,
					pre,
				};
				for config in &harness.configs {
					config.store.apply_batch(&[write.clone()]);
				}
			}
			2 => {
				let suffix = rng.random_range(1..=p.frozen + p.mutable);
				let key_bytes = key(GROUP, KEYSPACE, suffix);
				let value = row(NOISE.0, suffix, index);
				let pre = oracle.value_bytes(NOISE.0, key_bytes.as_slice());
				oracle.set(NOISE.0, key_bytes.as_slice(), value.clone());
				let write = state_write(NOISE, key_bytes, value, pre);
				for config in &harness.configs {
					config.store.apply_batch(&[write.clone()]);
				}
			}
			_ => {
				let expiry = rng.random_range(1..=64u64);
				let row_number = index as u64 + 1;
				oracle.anchor_set(FROZEN.0, GROUP, 0, row_number, expiry);
				for config in &harness.configs {
					config.store.anchor_set(
						FROZEN,
						GroupId(GROUP.into()),
						0,
						RowNumber(row_number),
						DateTime::from_millis(expiry),
					);
				}
			}
		}
	}
	if rng.random_range(0u32..100) < p.flush_pct {
		harness.flush_all();
	}
}

fn state_write(operator: OperatorId, key: EncodedKey, post: EncodedPodRow, pre: Option<ByteSize>) -> OperatorWrite {
	match pre {
		Some(pre_value_bytes) => OperatorWrite::Replace {
			operator,
			key,
			pre_value_bytes,
			post,
		},
		None => OperatorWrite::Insert {
			operator,
			key,
			post,
		},
	}
}
