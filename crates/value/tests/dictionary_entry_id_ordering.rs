// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	collections::{BTreeSet, HashSet},
};

use reifydb_value::value::dictionary::DictionaryEntryId;

#[test]
fn ord_and_eq_agree_across_id_widths() {
	// Ord must return Equal exactly when Eq holds, otherwise sorted and hashed collections disagree on identity.
	let narrow = DictionaryEntryId::U1(0);
	let wide = DictionaryEntryId::U4(0);

	assert_eq!(
		narrow.cmp(&wide) == Ordering::Equal,
		narrow == wide,
		"cmp says {:?} but eq says {}",
		narrow.cmp(&wide),
		narrow == wide
	);

	let sorted: BTreeSet<DictionaryEntryId> = [narrow, wide].into_iter().collect();
	let hashed: HashSet<DictionaryEntryId> = [narrow, wide].into_iter().collect();
	assert_eq!(
		sorted.len(),
		hashed.len(),
		"a sorted set and a hash set must agree on how many distinct ids they hold: {sorted:?} vs {hashed:?}"
	);
}
