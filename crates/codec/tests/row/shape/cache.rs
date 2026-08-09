// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::shape::{RowFamily, RowShape, cache::RowShapeCacheCell};
use reifydb_value::value::value_type::ValueType;

fn shape(types: &[ValueType]) -> RowShape {
	RowShape::testing(RowFamily::Pod, types)
}

#[test]
fn insert_then_get_returns_same_shape() {
	let cache = RowShapeCacheCell::new(2);
	let s = shape(&[ValueType::Int4]);
	let fp = s.fingerprint();

	cache.insert(s.clone());

	assert_eq!(cache.get(&fp), Some(s));
}

#[test]
fn get_absent_fingerprint_returns_none() {
	let cache = RowShapeCacheCell::new(2);
	let absent = shape(&[ValueType::Int4]).fingerprint();

	assert_eq!(cache.get(&absent), None);
}

#[test]
fn evicts_least_recently_used_when_at_capacity() {
	let cache = RowShapeCacheCell::new(2);
	let a = shape(&[ValueType::Int4]);
	let b = shape(&[ValueType::Int8]);
	let c = shape(&[ValueType::Utf8]);

	cache.insert(a.clone());
	cache.insert(b.clone());
	// a is the least-recently-used, so inserting c must evict a, not b/c.
	cache.insert(c.clone());

	assert_eq!(cache.get(&a.fingerprint()), None);
	assert_eq!(cache.get(&b.fingerprint()), Some(b));
	assert_eq!(cache.get(&c.fingerprint()), Some(c));
}

#[test]
fn get_promotes_recency_and_protects_from_eviction() {
	let cache = RowShapeCacheCell::new(2);
	let a = shape(&[ValueType::Int4]);
	let b = shape(&[ValueType::Int8]);
	let c = shape(&[ValueType::Utf8]);

	cache.insert(a.clone());
	cache.insert(b.clone());
	// Touch a so it becomes more recent than b; the next insert must evict b.
	cache.get(&a.fingerprint());
	cache.insert(c.clone());

	assert_eq!(cache.get(&a.fingerprint()), Some(a));
	assert_eq!(cache.get(&b.fingerprint()), None);
	assert_eq!(cache.get(&c.fingerprint()), Some(c));
}

#[test]
fn insert_existing_fingerprint_updates_in_place_without_growing() {
	let cache = RowShapeCacheCell::new(2);
	let s = shape(&[ValueType::Int4]);

	cache.insert(s.clone());
	cache.insert(s.clone());

	assert_eq!(cache.len(), 1);
	assert_eq!(cache.get(&s.fingerprint()), Some(s));
}

#[test]
fn reports_contains_len_is_empty_and_capacity() {
	let cache = RowShapeCacheCell::new(3);
	assert!(cache.is_empty());
	assert_eq!(cache.capacity(), 3);

	let s = shape(&[ValueType::Int4]);
	cache.insert(s.clone());

	assert!(cache.contains_key(&s.fingerprint()));
	assert!(!cache.contains_key(&shape(&[ValueType::Int8]).fingerprint()));
	assert_eq!(cache.len(), 1);
	assert!(!cache.is_empty());
}

#[test]
fn clear_removes_all_entries() {
	let cache = RowShapeCacheCell::new(2);
	cache.insert(shape(&[ValueType::Int4]));
	cache.insert(shape(&[ValueType::Int8]));

	cache.clear();

	assert!(cache.is_empty());
	assert_eq!(cache.len(), 0);
}

#[test]
#[should_panic]
fn new_with_zero_capacity_panics() {
	RowShapeCacheCell::new(0);
}
