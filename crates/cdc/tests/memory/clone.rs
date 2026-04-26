// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! `MemoryCdcStorage::clone()` shares the same backing map across handles.

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
fn clone_shares_storage() {
	let s1 = MemoryCdcStorage::new();
	let s2 = s1.clone();
	s1.write(&cdc_at(1)).unwrap();
	assert!(s1.read(CommitVersion(1)).unwrap().is_some());
	assert!(s2.read(CommitVersion(1)).unwrap().is_some());
}
