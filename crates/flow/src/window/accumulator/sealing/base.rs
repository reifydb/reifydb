// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

use crate::{
	operator::state::seal::coord::Coord,
	window::span::{Slot, SlotSpan},
};

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct SealingBase<C: Slot, V> {
	amendable: Option<SlotSpan<C>>,
	high_water: Option<C>,
	tail: BTreeMap<C, V>,
}

impl<C: Slot, V> Default for SealingBase<C, V> {
	fn default() -> Self {
		Self {
			amendable: None,
			high_water: None,
			tail: BTreeMap::new(),
		}
	}
}

impl<C: Slot, V> SealingBase<C, V> {
	pub fn amendable(amendable: SlotSpan<C>) -> Self {
		Self {
			amendable: Some(amendable),
			high_water: None,
			tail: BTreeMap::new(),
		}
	}

	pub fn push(&mut self, coord: C, value: V) -> Vec<(C, V)> {
		self.high_water = Some(match self.high_water {
			Some(hw) if hw >= coord => hw,
			_ => coord,
		});
		self.tail.insert(coord, value);
		let mut aged = Vec::new();
		let (Some(hw), Some(l)) = (self.high_water, self.amendable) else {
			return aged;
		};
		while let Some((&c, _)) = self.tail.iter().next() {
			if hw.order_key().span_since(c.order_key()) > l {
				aged.push(self.tail.pop_first().expect("non-empty"));
			} else {
				break;
			}
		}
		aged
	}

	pub fn remove(&mut self, coord: &C) {
		self.tail.remove(coord);
	}

	pub fn tail(&self) -> &BTreeMap<C, V> {
		&self.tail
	}

	pub fn is_tail_empty(&self) -> bool {
		self.tail.is_empty()
	}
}

impl<C: Slot + HeapSize, V: HeapSize> HeapSize for SealingBase<C, V> {
	fn heap_size(&self) -> usize {
		self.high_water.heap_size() + self.tail.heap_size()
	}
}
