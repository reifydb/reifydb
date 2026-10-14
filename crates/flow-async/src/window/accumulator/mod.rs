// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::row::operator::state::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;

use crate::window::engine::AccumulatorEvent;

pub mod invertible;

#[cfg(test)]
pub(crate) mod mock;

#[cfg(test)]
pub(crate) mod testkit;

pub trait WindowAccumulator: Clone + Debug + Default + OperatorState + StateCodec + HeapSize {
	type Contribution: Clone + Debug;
	type Output: Clone + Debug + PartialEq;

	fn add(&mut self, contribution: &Self::Contribution);

	fn remove(&mut self, contribution: &Self::Contribution);

	fn finalize(&self) -> Option<Self::Output>;

	fn is_empty(&self) -> bool;

	fn apply(&mut self, events: &[AccumulatorEvent<Self::Contribution>]) -> Applied {
		let mut applied = Applied::default();
		for event in events {
			match event {
				AccumulatorEvent::Add(contribution) => self.add(contribution),
				AccumulatorEvent::Remove(contribution) => {
					if self.is_empty() {
						applied.dropped += 1;
						continue;
					}
					self.remove(contribution);
				}
			}
			applied.touched = true;
		}
		applied
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Applied {
	pub touched: bool,
	pub dropped: u64,
}

pub trait MergeAccumulator: WindowAccumulator {
	fn merge(&mut self, other: &Self);
}

pub trait UnmergeAccumulator: MergeAccumulator {
	fn unmerge(&mut self, other: &Self);
}
