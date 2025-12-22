// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_transaction::multi::TransactionMulti;

use crate::{as_key, as_values};

#[tokio::test]
async fn test_read_after_write() {
	const N: u64 = 100;

	let engine = TransactionMulti::testing().await;

	let handles = (0..N)
		.map(|i| {
			let db = engine.clone();
			tokio::spawn(async move {
				let k = as_key!(i);
				let v = as_values!(i);

				let mut txn = db.begin_command().await.unwrap();
				txn.set(&k, v.clone()).unwrap();
				txn.commit().await.unwrap();

				let txn = db.begin_query().await.unwrap();
				let sv = txn.get(&k).await.unwrap().unwrap();
				assert_eq!(*sv.values(), v);
			})
		})
		.collect::<Vec<_>>();

	for handle in handles {
		handle.await.unwrap();
	}
}
