// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Bound, time::Instant};

use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
use reifydb_store_multi::{
	buffer::memory::storage::MemoryPrimitiveStorage,
	tier::{RangeCursor, TierStorage},
};
use reifydb_type::util::cowvec::CowVec;

const ITERS: u32 = 200_000;
const BATCH: usize = 32;

fn make_bank_state() -> MemoryPrimitiveStorage {
	let storage = MemoryPrimitiveStorage::new();
	for v in 1..=5u64 {
		let mut batch = Vec::new();
		for i in 0..4u8 {
			batch.push((
				CowVec::new(format!("account_{}", i).into_bytes()),
				Some(CowVec::new(vec![v as u8])),
			));
		}
		storage.set(CommitVersion(v), HashMap::from([(EntryKind::Multi, batch)])).unwrap();
	}
	storage
}

fn make_counter_state() -> MemoryPrimitiveStorage {
	let storage = MemoryPrimitiveStorage::new();
	for v in 1..=5u64 {
		storage.set(
			CommitVersion(v),
			HashMap::from([(
				EntryKind::Multi,
				vec![(CowVec::new(b"counter".to_vec()), Some(CowVec::new(vec![v as u8])))],
			)]),
		)
		.unwrap();
	}
	storage
}

fn time<F: FnMut()>(name: &str, mut f: F) {
	for _ in 0..1_000 {
		f();
	}
	let start = Instant::now();
	for _ in 0..ITERS {
		f();
	}
	let elapsed = start.elapsed();
	println!("{:<52} {:>10.1} ns/iter", name, elapsed.as_nanos() as f64 / ITERS as f64);
}

fn main() {
	let bank = make_bank_state();

	time("range_next     bank_4keys_5versions   v=5", || {
		let mut cursor = RangeCursor::new();
		let _ = bank
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				CommitVersion(5),
				BATCH,
			)
			.unwrap();
	});

	time("range_rev_next bank_4keys_5versions   v=5", || {
		let mut cursor = RangeCursor::new();
		let _ = bank
			.range_rev_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				CommitVersion(5),
				BATCH,
			)
			.unwrap();
	});

	time("range_next     bank_4keys_5versions   v=3 (historical)", || {
		let mut cursor = RangeCursor::new();
		let _ = bank
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				CommitVersion(3),
				BATCH,
			)
			.unwrap();
	});

	let counter = make_counter_state();

	time("range_next     counter_1key_5versions v=5", || {
		let mut cursor = RangeCursor::new();
		let _ = counter
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				CommitVersion(5),
				BATCH,
			)
			.unwrap();
	});

	time("range_rev_next counter_1key_5versions v=5", || {
		let mut cursor = RangeCursor::new();
		let _ = counter
			.range_rev_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				CommitVersion(5),
				BATCH,
			)
			.unwrap();
	});
}
