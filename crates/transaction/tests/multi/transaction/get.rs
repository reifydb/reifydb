// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::multi::TransactionMulti;

use crate::{as_key, as_values};

#[test]
fn test_read_after_write() {
	const N: u64 = 100;

	let engine = TransactionMulti::testing();

	let handles = (0..N)
		.map(|i| {
			let db = engine.clone();
			std::thread::spawn(move || {
				let k = as_key!(i);
				let v = as_values!(i);

				let mut txn = db.begin_command().unwrap();
				txn.set(&k, v.clone()).unwrap();
				txn.commit().unwrap();

				let txn = db.begin_query().unwrap();
				let sv = txn.get(&k).unwrap().unwrap();
				assert_eq!(*sv.values(), v);
			})
		})
		.collect::<Vec<_>>();

	for handle in handles {
		handle.join().unwrap();
	}
}
