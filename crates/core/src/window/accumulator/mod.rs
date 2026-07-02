// SPDX-License-Identifier: Apache-2.0
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

	/// Remove a contribution that may or may not be present, treating an absent
	/// contribution as a no-op. Used only on the late-retraction path, where a
	/// Remove can legitimately target a window whose matching Add was dropped as
	/// late; the strict `remove` still guards the in-order path.
	fn remove_if_present(&mut self, contribution: &Self::Contribution) {
		self.remove(contribution);
	}

	fn finalize(&self) -> Option<Self::Output>;

	fn is_empty(&self) -> bool;

	fn stamp(&self) -> Option<u64> {
		None
	}
}

#[cfg(test)]
mod tests;
