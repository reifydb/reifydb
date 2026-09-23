// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashSet,
	hash::{DefaultHasher, Hash, Hasher},
	str::FromStr,
};

use reifydb_value::value::decimal::Decimal;

fn hash_of(decimal: &Decimal) -> u64 {
	let mut hasher = DefaultHasher::new();
	decimal.hash(&mut hasher);
	hasher.finish()
}

#[test]
fn numerically_equal_decimals_hash_equal() {
	// Hash must agree with Eq, otherwise hashed collections split equal decimals by their scale.
	let short = Decimal::from_str("1.5").unwrap();
	let long = Decimal::from_str("1.50").unwrap();

	assert_eq!(short, long, "precondition: 1.5 and 1.50 compare equal");
	assert_eq!(
		hash_of(&short),
		hash_of(&long),
		"1.5 and 1.50 are equal so their hashes must match; displayed as {short} and {long}"
	);

	let set: HashSet<Decimal> = ["1.5", "1.50", "1.500"].iter().map(|s| Decimal::from_str(s).unwrap()).collect();
	assert_eq!(set.len(), 1, "a hash set must hold one entry for three equal decimals, got {set:?}");
}
