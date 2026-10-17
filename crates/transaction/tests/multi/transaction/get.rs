// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::durability::Durability;

use super::test_multi;
use crate::{as_key, as_values};

#[test]
fn test_read_after_write() {
	const N: u64 = 100;

	let engine = test_multi();

	let handles = (0..N)
		.map(|i| {
			let db = engine.clone();
			std::thread::spawn(move || {
				let k = as_key!(i);
				let v = as_values!(i);

				let mut txn = db.begin_command().unwrap();
				txn.set(vec![(k.clone(), v.clone())]).unwrap();
				txn.commit(Durability::Async).unwrap();

				let txn = db.begin_query().unwrap();
				let sv = txn.get(&[k.clone()]).unwrap().pop().flatten().unwrap();
				assert_eq!(*sv.bytes(), v);
			})
		})
		.collect::<Vec<_>>();

	for handle in handles {
		handle.join().unwrap();
	}
}
