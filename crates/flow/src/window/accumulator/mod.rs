// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::row::operator::{OperatorState, StateCodec};
use reifydb_core::metrics::heap::HeapSize;

pub mod invertible;
pub mod sealing;

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

	fn merge(&mut self, _other: &Self) {
		unimplemented!("this accumulator does not support merge")
	}

	fn unmerge(&mut self, _other: &Self) {
		unimplemented!("this accumulator does not support unmerge")
	}
}
