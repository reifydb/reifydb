// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct Ends<K, V> {
	first: Option<(K, V)>,
	prev: Option<(K, V)>,
	newest: Option<(K, V)>,
	count: u64,
}

impl<K, V> Default for Ends<K, V> {
	fn default() -> Self {
		Self {
			first: None,
			prev: None,
			newest: None,
			count: 0,
		}
	}
}

impl<K: Clone, V: Clone> Ends<K, V> {
	pub fn push(&mut self, key: &K, value: &V) {
		let entry = (key.clone(), value.clone());
		if self.first.is_none() {
			self.first = Some(entry.clone());
		}
		self.prev = self.newest.replace(entry);
		self.count += 1;
	}

	pub fn append(&mut self, newer: &Ends<K, V>) {
		if newer.count == 0 {
			return;
		}
		if self.count == 0 {
			*self = newer.clone();
			return;
		}
		self.prev = if newer.count >= 2 {
			newer.prev.clone()
		} else {
			self.newest.take()
		};
		self.newest = newer.newest.clone();
		self.count += newer.count;
	}
}

impl<K, V> Ends<K, V> {
	pub fn first(&self) -> Option<&(K, V)> {
		self.first.as_ref()
	}

	pub fn prev(&self) -> Option<&(K, V)> {
		self.prev.as_ref()
	}

	pub fn newest(&self) -> Option<&(K, V)> {
		self.newest.as_ref()
	}

	pub fn count(&self) -> u64 {
		self.count
	}

	pub fn is_empty(&self) -> bool {
		self.count == 0
	}
}

impl<K: HeapSize, V: HeapSize> HeapSize for Ends<K, V> {
	fn heap_size(&self) -> usize {
		self.first.heap_size() + self.prev.heap_size() + self.newest.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn pushed(entries: &[(u64, i64)]) -> Ends<u64, i64> {
		let mut ends = Ends::default();
		for (k, v) in entries {
			ends.push(k, v);
		}
		ends
	}

	#[test]
	fn pushes_keep_the_first_the_previous_and_the_newest() {
		let ends = pushed(&[(1, 10), (2, 20), (3, 30)]);
		assert_eq!(ends.first(), Some(&(1, 10)));
		assert_eq!(ends.prev(), Some(&(2, 20)));
		assert_eq!(ends.newest(), Some(&(3, 30)));
		assert_eq!(ends.count(), 3);
	}

	#[test]
	fn a_single_entry_is_first_and_newest_with_no_previous() {
		let ends = pushed(&[(7, 70)]);
		assert_eq!(ends.first(), Some(&(7, 70)));
		assert_eq!(ends.prev(), None);
		assert_eq!(ends.newest(), Some(&(7, 70)));
	}

	#[test]
	fn every_three_way_split_appends_to_the_sequential_push() {
		// A one-entry newer side must take its previous from the older side's newest, never lose it.
		let entries: Vec<(u64, i64)> = (0..6).map(|k| (k, k as i64 * 11)).collect();
		let whole = pushed(&entries);
		for a in 0..=entries.len() {
			for b in a..=entries.len() {
				let mut older = pushed(&entries[..a]);
				older.append(&pushed(&entries[a..b]));
				older.append(&pushed(&entries[b..]));
				assert_eq!(older, whole, "split {a} {b}");
			}
		}
	}
}
