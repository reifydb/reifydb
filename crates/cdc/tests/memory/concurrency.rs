// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::thread;

use reifydb_cdc::storage::{CdcStorage, memory::MemoryCdcStorage};
use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::cdc::{Cdc, SystemChange},
};
use reifydb_type::{util::cowvec::CowVec, value::datetime::DateTime};

fn cdc_at(version: u64) -> Cdc {
	Cdc::new(
		CommitVersion(version),
		DateTime::from_nanos(1_700_000_000_000_000_000),
		Vec::new(),
		vec![SystemChange::Insert {
			key: EncodedKey::new(vec![1, 2, 3]),
			post: EncodedRow(CowVec::new(vec![10, 20, 30])),
		}],
	)
}

#[test]
fn concurrent_access() {
	let storage = MemoryCdcStorage::new();
	let mut handles = vec![];
	for i in 0..10u64 {
		let s = storage.clone();
		handles.push(thread::spawn(move || {
			s.write(&cdc_at(i + 1)).unwrap();
		}));
	}
	for h in handles {
		h.join().unwrap();
	}
	assert_eq!(storage.len(), 10);
}
