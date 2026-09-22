// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::hash::{DefaultHasher, Hash, Hasher};

use reifydb_codec::key::{serializer::KeySerializer, sort::SortOrder};
use reifydb_value::value::{Value, duration::Duration};

fn key(duration: Duration) -> Vec<u8> {
	let mut serializer = KeySerializer::new();
	serializer.extend_value_with_direction(&Value::Duration(duration), SortOrder::Asc).unwrap();
	serializer.to_encoded_key().to_vec()
}

fn hash_of(duration: Duration) -> u64 {
	let mut hasher = DefaultHasher::new();
	duration.hash(&mut hasher);
	hasher.finish()
}

fn durations() -> Vec<Duration> {
	vec![
		Duration::zero(),
		Duration::from_nanoseconds(1).unwrap(),
		Duration::from_hours(1).unwrap(),
		Duration::from_hours(-1).unwrap(),
		Duration::from_days(1).unwrap(),
		Duration::new(0, 0, 86_400_000_000_000).unwrap(),
		Duration::from_days(29).unwrap(),
		Duration::from_days(30).unwrap(),
		Duration::from_days(31).unwrap(),
		Duration::from_days(-30).unwrap(),
		Duration::from_months(1).unwrap(),
		Duration::from_months(-1).unwrap(),
		Duration::new(1, -40, 0).unwrap(),
		Duration::new(-1, 40, 0).unwrap(),
		Duration::new(1, 0, 3_600_000_000_000).unwrap(),
		Duration::from_years(1).unwrap(),
		Duration::from_days(365).unwrap(),
	]
}

#[test]
fn duration_order_equality_and_hash_agree_with_the_key_bytes() {
	// Ord, Eq and Hash must agree with the key bytes, otherwise sort and join disagree with group-by and distinct.
	let all = durations();
	for &a in &all {
		for &b in &all {
			assert_eq!(
				a.cmp(&b),
				key(a).cmp(&key(b)),
				"{a:?} vs {b:?}: Ord must follow the key byte order"
			);
			assert_eq!(a == b, key(a) == key(b), "{a:?} vs {b:?}: equality must match equal key bytes");
			if a == b {
				assert_eq!(hash_of(a), hash_of(b), "{a:?} vs {b:?}: equal values must hash alike");
			}
		}
	}
}
