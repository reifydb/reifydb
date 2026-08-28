// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, btree_map},
	mem,
	ops::Bound,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_value::byte_size::ByteSize;

use crate::tier::commit::batch::{StateEntry, StateKey, state_entry_bytes};

pub type OperatorKeys = BTreeMap<EncodedKey, StateEntry>;

#[derive(Debug, Default)]
pub struct StateMap {
	operators: BTreeMap<OperatorId, OperatorKeys>,
	len: usize,
}

pub(super) struct Swept {
	pub taken: StateMap,
	pub bytes: ByteSize,
	pub cursor: Option<StateKey>,
}

impl StateMap {
	pub fn len(&self) -> usize {
		self.len
	}

	pub fn is_empty(&self) -> bool {
		self.len == 0
	}

	pub fn get(&self, key: &StateKey) -> Option<&StateEntry> {
		self.lookup(key.0, &key.1)
	}

	pub fn contains_key(&self, key: &StateKey) -> bool {
		self.get(key).is_some()
	}

	pub fn lookup(&self, operator: OperatorId, key: &EncodedKey) -> Option<&StateEntry> {
		self.operators.get(&operator)?.get(key)
	}

	pub fn iter(&self) -> Iter<'_> {
		Iter {
			outer: self.operators.iter(),
			current: None,
		}
	}

	pub fn range(&self, operator: OperatorId, lower: Bound<EncodedKey>, upper: Bound<EncodedKey>) -> Range<'_> {
		Range {
			inner: self.operators.get(&operator).map(|keys| keys.range((lower, upper))),
		}
	}

	pub(super) fn slot(&mut self, key: StateKey) -> btree_map::Entry<'_, EncodedKey, StateEntry> {
		self.operators.entry(key.0).or_default().entry(key.1)
	}

	pub(super) fn admit(&mut self) {
		self.len += 1;
	}

	pub(super) fn insert(&mut self, operator: OperatorId, key: EncodedKey, entry: StateEntry) {
		if self.operators.entry(operator).or_default().insert(key, entry).is_none() {
			self.len += 1;
		}
	}

	pub(super) fn remove_operator(&mut self, operator: OperatorId) -> Option<OperatorKeys> {
		let keys = self.operators.remove(&operator)?;
		self.len -= keys.len();
		Some(keys)
	}

	pub(super) fn take_within(&mut self, budget: ByteSize, force_first: bool) -> (StateMap, ByteSize) {
		let mut taken = ByteSize::ZERO;
		let mut count = 0usize;
		let mut boundary: Option<StateKey> = None;
		'search: for (operator, keys) in self.operators.iter() {
			for (key, entry) in keys.iter() {
				let cost = state_entry_bytes(key, entry);
				if !(force_first && count == 0) && taken.saturating_add(cost) > budget {
					boundary = Some((*operator, key.clone()));
					break 'search;
				}
				taken = taken.saturating_add(cost);
				count += 1;
			}
		}

		let Some((boundary_operator, boundary_key)) = boundary else {
			return (mem::take(self), taken);
		};

		let mut remainder = self.operators.split_off(&boundary_operator);
		let mut head = mem::take(&mut self.operators);
		let mut drained = false;
		if let Some(keys) = remainder.get_mut(&boundary_operator) {
			let tail = keys.split_off(&boundary_key);
			let front = mem::replace(keys, tail);
			drained = keys.is_empty();
			if !front.is_empty() {
				head.insert(boundary_operator, front);
			}
		}
		if drained {
			remainder.remove(&boundary_operator);
		}
		self.operators = remainder;
		self.len -= count;

		(
			StateMap {
				operators: head,
				len: count,
			},
			taken,
		)
	}

	pub(super) fn sweep(&mut self, budget: ByteSize, cursor: Option<StateKey>) -> Swept {
		let total = self.len;
		let operators: Vec<OperatorId> = self.operators.keys().copied().collect();
		let mut victims: Vec<StateKey> = Vec::new();
		let mut fallback: Vec<StateKey> = Vec::new();
		let mut fallback_bytes = ByteSize::ZERO;
		let mut coldest: Option<u8> = None;
		let mut examined = 0usize;
		let mut taken = ByteSize::ZERO;
		let mut last: Option<StateKey> = cursor.clone();
		let mut filled = false;

		'sweep: for (operator, lower, upper) in revolution(&operators, cursor) {
			let Some(keys) = self.operators.get_mut(&operator) else {
				continue;
			};
			for (key, entry) in keys.range_mut((lower, upper)) {
				examined += 1;
				last = Some((operator, key.clone()));
				if entry.count == 0 {
					taken = taken.saturating_add(state_entry_bytes(key, entry));
					victims.push((operator, key.clone()));
					if taken >= budget {
						filled = true;
						break 'sweep;
					}
				} else {
					entry.count /= 2;
					let rank = entry.count;
					let cost = state_entry_bytes(key, entry);
					match coldest {
						Some(current) if rank > current => {}
						Some(current) if rank == current => {
							if fallback_bytes < budget {
								fallback_bytes = fallback_bytes.saturating_add(cost);
								fallback.push((operator, key.clone()));
							}
						}
						_ => {
							coldest = Some(rank);
							fallback.clear();
							fallback_bytes = cost;
							fallback.push((operator, key.clone()));
						}
					}
				}
				if examined >= total {
					break 'sweep;
				}
			}
		}

		if victims.is_empty() && !filled {
			victims = fallback;
		}

		let mut map = StateMap::default();
		let mut bytes = ByteSize::ZERO;
		for (operator, key) in victims {
			let Some(keys) = self.operators.get_mut(&operator) else {
				continue;
			};
			let Some(entry) = keys.remove(&key) else {
				continue;
			};
			let drained = keys.is_empty();
			if drained {
				self.operators.remove(&operator);
			}
			self.len -= 1;
			bytes = bytes.saturating_add(state_entry_bytes(&key, &entry));
			map.insert(operator, key, entry);
		}

		Swept {
			taken: map,
			bytes,
			cursor: last,
		}
	}
}

type Segment = (OperatorId, Bound<EncodedKey>, Bound<EncodedKey>);

fn revolution(operators: &[OperatorId], cursor: Option<StateKey>) -> Vec<Segment> {
	let Some((operator, key)) = cursor else {
		return operators.iter().map(|operator| (*operator, Bound::Unbounded, Bound::Unbounded)).collect();
	};
	let start = operators.partition_point(|candidate| *candidate < operator);
	let resumed = operators.get(start).is_some_and(|candidate| *candidate == operator);
	let mut segments = Vec::with_capacity(operators.len() + 1);
	if resumed {
		segments.push((operator, Bound::Excluded(key.clone()), Bound::Unbounded));
	}
	let head = if resumed {
		start + 1
	} else {
		start
	};
	for candidate in &operators[head..] {
		segments.push((*candidate, Bound::Unbounded, Bound::Unbounded));
	}
	for candidate in &operators[..start] {
		segments.push((*candidate, Bound::Unbounded, Bound::Unbounded));
	}
	if resumed {
		segments.push((operator, Bound::Unbounded, Bound::Included(key)));
	}
	segments
}

pub struct Range<'a> {
	inner: Option<btree_map::Range<'a, EncodedKey, StateEntry>>,
}

impl Default for Range<'_> {
	fn default() -> Self {
		Self {
			inner: None,
		}
	}
}

impl<'a> Iterator for Range<'a> {
	type Item = (&'a EncodedKey, &'a StateEntry);

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.as_mut()?.next()
	}
}

impl<'a> DoubleEndedIterator for Range<'a> {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.inner.as_mut()?.next_back()
	}
}

pub struct Iter<'a> {
	outer: btree_map::Iter<'a, OperatorId, OperatorKeys>,
	current: Option<(OperatorId, btree_map::Iter<'a, EncodedKey, StateEntry>)>,
}

impl<'a> Iterator for Iter<'a> {
	type Item = ((OperatorId, &'a EncodedKey), &'a StateEntry);

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			if let Some((operator, keys)) = self.current.as_mut()
				&& let Some((key, entry)) = keys.next()
			{
				return Some(((*operator, key), entry));
			}
			let (operator, keys) = self.outer.next()?;
			self.current = Some((*operator, keys.iter()));
		}
	}
}

impl<'a> IntoIterator for &'a StateMap {
	type Item = ((OperatorId, &'a EncodedKey), &'a StateEntry);
	type IntoIter = Iter<'a>;

	fn into_iter(self) -> Iter<'a> {
		self.iter()
	}
}
