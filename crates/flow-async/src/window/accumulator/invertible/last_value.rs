// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::row::operator::state::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::accumulator::{MergeAccumulator, WindowAccumulator};

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct LastValue<V> {
	value: Option<V>,
}

impl<V> Default for LastValue<V> {
	fn default() -> Self {
		Self {
			value: None,
		}
	}
}

impl<V: Clone> LastValue<V> {
	pub fn set(&mut self, value: V) {
		self.value = Some(value);
	}

	pub fn clear(&mut self) {
		self.value = None;
	}

	pub fn get(&self) -> Option<&V> {
		self.value.as_ref()
	}

	pub fn is_empty(&self) -> bool {
		self.value.is_none()
	}
}

impl<V: Clone + Debug> WindowAccumulator for LastValue<V>
where
	V: PartialEq,
	LastValue<V>: OperatorState + StateCodec + HeapSize,
{
	type Contribution = V;
	type Output = V;

	fn add(&mut self, contribution: &V) {
		self.value = Some(contribution.clone());
	}

	fn remove(&mut self, contribution: &V) {
		if self.value.as_ref() == Some(contribution) {
			self.value = None;
		}
	}

	fn finalize(&self) -> Option<V> {
		self.value.clone()
	}

	fn is_empty(&self) -> bool {
		self.value.is_none()
	}
}

impl<V: Clone + Debug> MergeAccumulator for LastValue<V>
where
	V: PartialEq,
	LastValue<V>: OperatorState + StateCodec + HeapSize,
{
	fn merge(&mut self, other: &Self) {
		if let Some(value) = &other.value {
			self.value = Some(value.clone());
		}
	}
}

impl<V: HeapSize> HeapSize for LastValue<V> {
	fn heap_size(&self) -> usize {
		self.value.heap_size()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn last_value_is_last_write_wins() {
		let mut lv: LastValue<i64> = LastValue::default();
		assert!(lv.is_empty());
		lv.add(&10);
		lv.add(&20);
		assert_eq!(lv.finalize(), Some(20));
		lv.remove(&20);
		assert!(lv.is_empty());
		assert_eq!(lv.finalize(), None);
	}

	#[test]
	fn last_value_remove_of_a_superseded_value_keeps_the_current_one() {
		// An update fans out as remove(pre) then add(post); a reordered pair must not clear the live value.
		let mut lv: LastValue<i64> = LastValue::default();
		lv.add(&10);
		lv.add(&20);
		lv.remove(&10);
		assert_eq!(lv.finalize(), Some(20), "removing a superseded value must not clear the current one");
	}

	#[test]
	fn merge_takes_the_other_value_when_it_has_one() {
		// Panes merge oldest first, so the later pane's value must replace the earlier one.
		let mut earlier: LastValue<i64> = LastValue::default();
		earlier.add(&1);
		let mut later: LastValue<i64> = LastValue::default();
		later.add(&2);
		earlier.merge(&later);
		assert_eq!(earlier.finalize(), Some(2));

		let mut empty: LastValue<i64> = LastValue::default();
		empty.merge(&later);
		assert_eq!(empty.finalize(), Some(2));
	}

	#[test]
	fn merge_of_an_empty_other_keeps_the_current_value() {
		// An empty later pane must not erase the value an earlier pane already holds.
		let mut current: LastValue<i64> = LastValue::default();
		current.add(&7);
		current.merge(&LastValue::default());
		assert_eq!(current.finalize(), Some(7));

		let mut both_empty: LastValue<i64> = LastValue::default();
		both_empty.merge(&LastValue::default());
		assert!(both_empty.is_empty());
	}
}
