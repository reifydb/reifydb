// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use crate::sync::mutex::MutexGuard;

#[derive(Debug)]
pub struct CondvarInner;

impl CondvarInner {
	pub fn new() -> Self {
		Self
	}

	pub fn wait<'a, T>(&self, _guard: &mut MutexGuard<'a, T>) {}

	pub fn wait_for<'a, T>(&self, _guard: &mut MutexGuard<'a, T>, _timeout: Duration) -> bool {
		true
	}

	pub fn notify_one(&self) {}

	pub fn notify_all(&self) {}
}

impl Default for CondvarInner {
	fn default() -> Self {
		Self::new()
	}
}
