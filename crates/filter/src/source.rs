// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

#[derive(Debug)]
pub struct FilterSlice {
	pub hashes: Vec<u64>,
	pub exhausted: bool,
}

pub trait KeyFilterSource: Send + Debug {
	fn name(&self) -> &'static str;

	fn estimated_len(&self) -> u64;

	fn restart(&mut self);

	fn next_slice(&mut self, budget: usize) -> FilterSlice;
}
