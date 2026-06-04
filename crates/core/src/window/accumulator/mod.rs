// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use serde::{Serialize, de::DeserializeOwned};

pub mod invertible;
pub mod sealing;

pub trait WindowAccumulator: Clone + Debug + Default + Serialize + DeserializeOwned {
	type Contribution: Clone + Debug;
	type Output: Clone + Debug + PartialEq;

	fn add(&mut self, contribution: &Self::Contribution);

	fn remove(&mut self, contribution: &Self::Contribution);

	fn finalize(&self) -> Option<Self::Output>;

	fn is_empty(&self) -> bool;

	fn stamp(&self) -> Option<u64> {
		None
	}
}

#[cfg(test)]
mod tests;
