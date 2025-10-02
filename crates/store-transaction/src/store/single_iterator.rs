// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::cmp::Ordering::{Equal, Greater, Less};

use reifydb_core::{EncodedKey, interface::SingleVersionValues};

use crate::SingleVersionIter;

/// Single-version merging iterator that combines multiple iterators
/// Returns each unique key once (from the highest priority tier)
pub struct SingleVersionMergingIterator<'a> {
	iters: Vec<Box<dyn SingleVersionIter + 'a>>,
	buffers: Vec<Option<SingleVersionValues>>,
}

impl<'a> SingleVersionMergingIterator<'a> {
	pub fn new(mut iters: Vec<Box<dyn SingleVersionIter + 'a>>) -> Self {
		let mut buffers = Vec::with_capacity(iters.len());
		for iter in iters.iter_mut() {
			buffers.push(iter.next());
		}

		Self {
			iters,
			buffers,
		}
	}
}

impl<'a> Iterator for SingleVersionMergingIterator<'a> {
	type Item = SingleVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		// Find the minimum key across all buffers
		let mut min_key: Option<&EncodedKey> = None;
		let mut indices_with_min_key = Vec::new();

		for (idx, buffer) in self.buffers.iter().enumerate() {
			if let Some(item) = buffer {
				match min_key {
					None => {
						min_key = Some(&item.key);
						indices_with_min_key.clear();
						indices_with_min_key.push(idx);
					}
					Some(current_min) => match item.key.cmp(current_min) {
						Less => {
							min_key = Some(&item.key);
							indices_with_min_key.clear();
							indices_with_min_key.push(idx);
						}
						Equal => {
							indices_with_min_key.push(idx);
						}
						Greater => {}
					},
				}
			}
		}

		if indices_with_min_key.is_empty() {
			return None;
		}

		// Take the item from the first tier (highest priority)
		let result = self.buffers[indices_with_min_key[0]].take();

		// Refill buffers for all iterators that had the minimum key
		for &idx in &indices_with_min_key {
			if self.buffers[idx].is_none() {
				self.buffers[idx] = self.iters[idx].next();
			} else {
				// Discard the item if we didn't use it
				self.buffers[idx] = None;
				self.buffers[idx] = self.iters[idx].next();
			}
		}

		result
	}
}
