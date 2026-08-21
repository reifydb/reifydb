// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher},
};

#[inline]
pub fn hash_item<T: Hash>(item: &T) -> u64 {
	let mut hasher = DefaultHasher::new();
	item.hash(&mut hasher);
	hasher.finish()
}
