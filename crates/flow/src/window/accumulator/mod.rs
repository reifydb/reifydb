// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::state::{ArchiveState, OperatorState};
use reifydb_core::metrics::heap::HeapSize;

pub mod invertible;
pub mod sealing;

pub trait WindowAccumulator: Clone + Debug + Default + OperatorState + ArchiveState + HeapSize {
	type Contribution: Clone + Debug;
	type Output: Clone + Debug + PartialEq;

	fn add(&mut self, contribution: &Self::Contribution);

	fn remove(&mut self, contribution: &Self::Contribution);

	/// Opt-in hook for a Remove whose matching Add was dropped as late. The default is the strict
	/// `remove`; an accumulator that must tolerate the absence overrides this.
	fn remove_if_present(&mut self, contribution: &Self::Contribution) {
		self.remove(contribution);
	}

	fn finalize(&self) -> Option<Self::Output>;

	fn is_empty(&self) -> bool;

	fn merge(&mut self, _other: &Self) {
		unimplemented!("this accumulator does not support merge")
	}

	fn unmerge(&mut self, _other: &Self) {
		unimplemented!("this accumulator does not support unmerge")
	}
}

#[cfg(test)]
mod tests;
