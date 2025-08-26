// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https:
// //github.com/al8n/skipdb), originally licensed under the Apache License,
// Version 2.0. Original copyright:
// Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
// http: //www.apache.org/licenses/LICENSE-2.0

use reifydb_core::{EncodedKey, EncodedKeyRange};
use reifydb_storage::memory::Memory;
use reifydb_transaction::{
	mvcc::transaction::{
		optimistic::{CommandTransaction, Optimistic},
		serializable::Serializable,
	},
	svl::SingleVersionLock,
};

use crate::{
	as_key, as_row, from_key, from_row,
	mvcc::transaction::{FromKey, FromRow},
};

#[test]
fn test_write_skew() {
	// accounts
	let a999: EncodedKey = as_key!(999);
	let a888: EncodedKey = as_key!(888);

	let engine = Optimistic::testing();

	// Set balance to $100 in each account.
	let mut txn = engine.begin_command().unwrap();
	txn.set(&a999, as_row!(100u64)).unwrap();
	txn.set(&a888, as_row!(100u64)).unwrap();
	txn.commit().unwrap();
	assert_eq!(2, engine.version().unwrap());

	let get_bal = |txn: &mut CommandTransaction<
		Memory,
		SingleVersionLock<Memory>,
	>,
	               k: &EncodedKey|
	 -> u64 {
		let sv = txn.get(k).unwrap().unwrap();
		let val = sv.row();
		from_row!(u64, val)
	};

	// Start two transactions, each would read both accounts and deduct from
	// one account.
	let mut txn1 = engine.begin_command().unwrap();

	let mut sum = get_bal(&mut txn1, &a999);
	sum += get_bal(&mut txn1, &a888);
	assert_eq!(200, sum);
	txn1.set(&a999, as_row!(0u64)).unwrap(); // Deduct 100 from a999

	// Let's read this back.
	let mut sum = get_bal(&mut txn1, &a999);
	assert_eq!(0, sum);
	sum += get_bal(&mut txn1, &a888);
	assert_eq!(100, sum);
	// Don't commit yet.

	let mut txn2 = engine.begin_command().unwrap();

	let mut sum = get_bal(&mut txn2, &a999);
	sum += get_bal(&mut txn2, &a888);
	assert_eq!(200, sum);
	txn2.set(&a888, as_row!(0u64)).unwrap(); // Deduct 100 from a888

	// Let's read this back.
	let mut sum = get_bal(&mut txn2, &a999);
	assert_eq!(100, sum);
	sum += get_bal(&mut txn2, &a888);
	assert_eq!(100, sum);

	// Commit both now.
	txn1.commit().unwrap();
	let err = txn2.commit().unwrap_err();
	assert!(err.to_string().contains("conflict"));

	assert_eq!(3, engine.version().unwrap());
}

// https://wiki.postgresql.org/wiki/SSI#Black_and_White
#[test]
fn test_black_white() {
	let engine = Optimistic::testing();

	// Setup
	let mut txn = engine.begin_command().unwrap();
	for i in 1..=10 {
		if i % 2 == 1 {
			txn.set(&as_key!(i), as_row!("black".to_string()))
				.unwrap();
		} else {
			txn.set(&as_key!(i), as_row!("white".to_string()))
				.unwrap();
		}
	}
	txn.commit().unwrap();

	let mut white = engine.begin_command().unwrap();
	let indices = white
		.scan()
		.unwrap()
		.filter_map(|sv| {
			if *sv.row() == as_row!("black".to_string()) {
				Some(sv.key().clone())
			} else {
				None
			}
		})
		.collect::<Vec<_>>();

	for i in indices {
		white.set(&i, as_row!("white".to_string())).unwrap();
	}

	let mut black = engine.begin_command().unwrap();
	let indices = black
		.scan()
		.unwrap()
		.filter_map(|sv| {
			if *sv.row() == as_row!("white".to_string()) {
				Some(sv.key().clone())
			} else {
				None
			}
		})
		.collect::<Vec<_>>();

	for i in indices {
		black.set(&i, as_row!("black".to_string())).unwrap();
	}

	black.commit().unwrap();
	let err = white.commit().unwrap_err();
	assert!(err.to_string().contains("conflict"));

	let rx = engine.begin_query().unwrap();
	let result: Vec<_> = rx.scan().unwrap().collect();
	assert_eq!(result.len(), 10);

	result.iter().for_each(|sv| {
		assert_eq!(sv.row, as_row!("black".to_string()));
	})
}

// https://wiki.postgresql.org/wiki/SSI#Overdraft_Protection
#[test]
fn test_overdraft_protection() {
	let engine = Optimistic::testing();

	let key = as_key!("karen");

	// Setup
	let mut txn = engine.begin_command().unwrap();
	txn.set(&key, as_row!(1000)).unwrap();
	txn.commit().unwrap();

	// txn1
	let mut txn1 = engine.begin_command().unwrap();
	let money = from_row!(i32, *txn1.get(&key).unwrap().unwrap().row());
	txn1.set(&key, as_row!(money - 500)).unwrap();

	// txn2
	let mut txn2 = engine.begin_command().unwrap();
	let money = from_row!(i32, *txn2.get(&key).unwrap().unwrap().row());
	txn2.set(&key, as_row!(money - 500)).unwrap();

	txn1.commit().unwrap();
	let err = txn2.commit().unwrap_err();
	assert!(err.to_string().contains("conflict"));

	let rx = engine.begin_query().unwrap();
	let money = from_row!(i32, *rx.get(&key).unwrap().unwrap().row());
	assert_eq!(money, 500);
}

// https://wiki.postgresql.org/wiki/SSI#Primary_Colors
#[test]
fn test_primary_colors() {
	let engine = Optimistic::testing();

	// Setup
	let mut txn = engine.begin_command().unwrap();
	for i in 1..=9000 {
		if i % 3 == 1 {
			txn.set(&as_key!(i), as_row!("red".to_string()))
				.unwrap();
		} else if i % 3 == 2 {
			txn.set(&as_key!(i), as_row!("yellow".to_string()))
				.unwrap();
		} else {
			txn.set(&as_key!(i), as_row!("blue".to_string()))
				.unwrap();
		}
	}
	txn.commit().unwrap();

	let mut red = engine.begin_command().unwrap();
	let indices = red
		.scan()
		.unwrap()
		.filter_map(|sv| {
			if *sv.row() == as_row!("yellow".to_string()) {
				Some(sv.key().clone())
			} else {
				None
			}
		})
		.collect::<Vec<_>>();
	for i in indices {
		red.set(&i, as_row!("red".to_string())).unwrap();
	}

	let mut yellow = engine.begin_command().unwrap();
	let indices = yellow
		.scan()
		.unwrap()
		.filter_map(|sv| {
			if *sv.row() == as_row!("blue".to_string()) {
				Some(sv.key().clone())
			} else {
				None
			}
		})
		.collect::<Vec<_>>();
	for i in indices {
		yellow.set(&i, as_row!("yellow".to_string())).unwrap();
	}

	let mut red_two = engine.begin_command().unwrap();
	let indices = red_two
		.scan()
		.unwrap()
		.filter_map(|sv| {
			if *sv.row() == as_row!("blue".to_string()) {
				Some(sv.key().clone())
			} else {
				None
			}
		})
		.collect::<Vec<_>>();
	for i in indices {
		red_two.set(&i, as_row!("red".to_string())).unwrap();
	}

	red.commit().unwrap();
	let err = red_two.commit().unwrap_err();
	assert!(err.to_string().contains("conflict"));

	let err = yellow.commit().unwrap_err();
	assert!(err.to_string().contains("conflict"));

	let rx = engine.begin_query().unwrap();
	let result: Vec<_> = rx.scan().unwrap().collect();
	assert_eq!(result.len(), 9000);

	let mut red_count = 0;
	let mut yellow_count = 0;
	let mut blue_count = 0;

	result.iter().for_each(|sv| {
		let value = from_row!(String, sv.row);
		match value.as_str() {
			"red" => red_count += 1,
			"yellow" => yellow_count += 1,
			"blue" => blue_count += 1,
			_ => unreachable!(),
		}
	});

	assert_eq!(red_count, 6000);
	assert_eq!(blue_count, 3000);
	assert_eq!(yellow_count, 0);
}

// https://wiki.postgresql.org/wiki/SSI#Intersecting_Data
#[test]
fn test_intersecting_data() {
	let engine = Serializable::testing();

	// Setup
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!("a1"), as_row!(10u64)).unwrap();
	txn.set(&as_key!("a2"), as_row!(20u64)).unwrap();
	txn.set(&as_key!("b1"), as_row!(100u64)).unwrap();
	txn.set(&as_key!("b2"), as_row!(200u64)).unwrap();
	txn.commit().unwrap();
	assert_eq!(2, engine.version().unwrap());

	let mut txn1 = engine.begin_command().unwrap();
	let val = txn1
		.scan()
		.unwrap()
		.filter_map(|tv| {
			let key = from_key!(String, tv.key());
			let value = from_row!(u64, *tv.row());
			if key.starts_with('a') {
				Some(value)
			} else {
				None
			}
		})
		.sum::<u64>();

	txn1.set(&as_key!("b3"), as_row!(30)).unwrap();
	assert_eq!(30, val);

	let mut txn2 = engine.begin_command().unwrap();
	let val = txn2
		.scan()
		.unwrap()
		.filter_map(|tv| {
			let key = from_key!(String, tv.key());
			let value = from_row!(u64, *tv.row());
			if key.starts_with('b') {
				Some(value)
			} else {
				None
			}
		})
		.sum::<u64>();

	txn2.set(&as_key!("a3"), as_row!(300u64)).unwrap();
	assert_eq!(300, val);

	txn2.commit().unwrap();
	let err = txn1.commit().unwrap_err();
	assert!(err.to_string().contains("conflict"));

	let mut txn3 = engine.begin_command().unwrap();
	let val = txn3
		.scan()
		.unwrap()
		.filter_map(|tv| {
			let key = from_key!(String, tv.key());
			let value = from_row!(u64, *tv.row());
			if key.starts_with('a') {
				Some(value)
			} else {
				None
			}
		})
		.sum::<u64>();

	assert_eq!(330, val);
}

// https://wiki.postgresql.org/wiki/SSI#Intersecting_Data
#[test]
fn test_intersecting_data2() {
	let engine = Serializable::testing();

	// Setup
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!("a1"), as_row!(10u64)).unwrap();
	txn.set(&as_key!("b1"), as_row!(100u64)).unwrap();
	txn.set(&as_key!("b2"), as_row!(200u64)).unwrap();
	txn.commit().unwrap();
	assert_eq!(2, engine.version().unwrap());

	let mut txn1 = engine.begin_command().unwrap();
	let val = txn1
		.range(EncodedKeyRange::parse("a..b"))
		.unwrap()
		.map(|tv| from_row!(u64, *tv.row()))
		.sum::<u64>();

	txn1.set(&as_key!("b3"), as_row!(10)).unwrap();
	assert_eq!(10, val);

	let mut txn2 = engine.begin_command().unwrap();
	let val = txn2
		.range(EncodedKeyRange::parse("b..c"))
		.unwrap()
		.map(|tv| from_row!(u64, *tv.row()))
		.sum::<u64>();

	assert_eq!(300, val);
	txn2.set(&as_key!("a3"), as_row!(300u64)).unwrap();
	txn2.commit().unwrap();

	let err = txn1.commit().unwrap_err();
	assert!(err.to_string().contains("conflict"));

	let mut txn3 = engine.begin_command().unwrap();
	let val = txn3
		.scan()
		.unwrap()
		.filter_map(|tv| {
			let key = from_key!(String, tv.key());
			let value = from_row!(u64, *tv.row());
			if key.starts_with('a') {
				Some(value)
			} else {
				None
			}
		})
		.sum::<u64>();
	assert_eq!(310, val);
}

// https://wiki.postgresql.org/wiki/SSI#Intersecting_Data
#[test]
fn test_intersecting_data3() {
	let engine = Serializable::testing();

	// // Setup
	let mut txn = engine.begin_command().unwrap();
	txn.set(&as_key!("b1"), as_row!(100u64)).unwrap();
	txn.set(&as_key!("b2"), as_row!(200u64)).unwrap();
	txn.commit().unwrap();
	assert_eq!(2, engine.version().unwrap());

	let mut txn1 = engine.begin_command().unwrap();
	let val = txn1
		.range(EncodedKeyRange::parse("a..b"))
		.unwrap()
		.map(|tv| from_row!(u64, *tv.row()))
		.sum::<u64>();
	txn1.set(&as_key!("b3"), as_row!(0u64)).unwrap();
	assert_eq!(0, val);

	let mut txn2 = engine.begin_command().unwrap();
	let val = txn2
		.range(EncodedKeyRange::parse("b..c"))
		.unwrap()
		.map(|tv| from_row!(u64, *tv.row()))
		.sum::<u64>();

	txn2.set(&as_key!("a3"), as_row!(300u64)).unwrap();
	assert_eq!(300, val);
	txn2.commit().unwrap();
	let err = txn1.commit().unwrap_err();
	assert!(err.to_string().contains("conflict"));

	let mut txn3 = engine.begin_command().unwrap();
	let val = txn3
		.scan()
		.unwrap()
		.filter_map(|tv| {
			let key = from_key!(String, tv.key());
			let value = from_row!(u64, *tv.row());
			if key.starts_with('a') {
				Some(value)
			} else {
				None
			}
		})
		.sum::<u64>();

	assert_eq!(300, val);
}
