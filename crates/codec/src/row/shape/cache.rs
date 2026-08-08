// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, collections::HashMap};

use crate::row::shape::{RowShape, fingerprint::RowShapeFingerprint};

#[derive(Debug)]
pub struct RowShapeCacheCell {
	inner: RefCell<Inner>,
}

#[derive(Debug)]
struct Inner {
	map: HashMap<RowShapeFingerprint, Entry>,
	capacity: usize,
	counter: u64,
}

#[derive(Debug)]
struct Entry {
	shape: RowShape,
	last_access: u64,
}

impl Inner {
	fn evict_lru(&mut self) {
		let mut oldest_key: Option<RowShapeFingerprint> = None;
		let mut oldest_access = u64::MAX;
		for (key, entry) in self.map.iter() {
			if entry.last_access < oldest_access {
				oldest_access = entry.last_access;
				oldest_key = Some(*key);
			}
		}
		if let Some(key) = oldest_key {
			self.map.remove(&key);
		}
	}
}

impl RowShapeCacheCell {
	pub fn new(capacity: usize) -> Self {
		assert!(capacity > 0, "RowShapeCacheCell capacity must be greater than 0");
		Self {
			inner: RefCell::new(Inner {
				map: HashMap::with_capacity(capacity),
				capacity,
				counter: 0,
			}),
		}
	}

	pub fn get(&self, fingerprint: &RowShapeFingerprint) -> Option<RowShape> {
		let mut inner = self.inner.borrow_mut();
		let access = inner.counter;
		let shape = match inner.map.get_mut(fingerprint) {
			Some(entry) => {
				entry.last_access = access;
				entry.shape.clone()
			}
			None => return None,
		};
		inner.counter += 1;
		Some(shape)
	}

	pub fn insert(&self, shape: RowShape) {
		let fingerprint = shape.fingerprint();
		let mut inner = self.inner.borrow_mut();
		let access = inner.counter;
		inner.counter += 1;

		if let Some(entry) = inner.map.get_mut(&fingerprint) {
			entry.shape = shape;
			entry.last_access = access;
			return;
		}

		if inner.map.len() >= inner.capacity {
			inner.evict_lru();
		}

		inner.map.insert(
			fingerprint,
			Entry {
				shape,
				last_access: access,
			},
		);
	}

	pub fn contains_key(&self, fingerprint: &RowShapeFingerprint) -> bool {
		self.inner.borrow().map.contains_key(fingerprint)
	}

	pub fn clear(&self) {
		self.inner.borrow_mut().map.clear();
	}

	pub fn len(&self) -> usize {
		self.inner.borrow().map.len()
	}

	pub fn is_empty(&self) -> bool {
		self.inner.borrow().map.is_empty()
	}

	pub fn capacity(&self) -> usize {
		self.inner.borrow().capacity
	}
}
