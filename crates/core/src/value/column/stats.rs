// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_type::value::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Stat {
	Min,
	Max,
	NoneCount,
	TrueCount,
	FalseCount,
	IsSorted,
	IsStrictSorted,
	IsConstant,
	DistinctCount,
	RunCount,
	UncompressedSize,
}

#[derive(Clone, Debug, Default)]
pub struct StatsSet {
	facts: HashMap<Stat, Value>,
}

impl StatsSet {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn get(&self, stat: Stat) -> Option<&Value> {
		self.facts.get(&stat)
	}

	pub fn set(&mut self, stat: Stat, value: Value) {
		self.facts.insert(stat, value);
	}

	pub fn known(&self) -> impl Iterator<Item = Stat> + '_ {
		self.facts.keys().copied()
	}

	pub fn len(&self) -> usize {
		self.facts.len()
	}

	pub fn is_empty(&self) -> bool {
		self.facts.is_empty()
	}
}
