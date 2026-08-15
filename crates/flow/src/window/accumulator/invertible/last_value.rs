// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::row::operator::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::window::accumulator::WindowAccumulator;

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

	fn remove(&mut self, _contribution: &V) {
		self.value = None;
	}

	fn finalize(&self) -> Option<V> {
		self.value.clone()
	}

	fn is_empty(&self) -> bool {
		self.value.is_none()
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
}
