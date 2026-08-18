// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem;

use reifydb_core::interface::{catalog::object::ObjectId, change::Diff};

#[derive(Debug, Default)]
pub struct InlineFlowState {
	published: Vec<(ObjectId, Diff)>,
	running: bool,
}

impl InlineFlowState {
	pub fn new() -> Self {
		Self {
			published: Vec::new(),
			running: false,
		}
	}

	pub fn is_running(&self) -> bool {
		self.running
	}

	pub fn set_running(&mut self, running: bool) {
		self.running = running;
	}

	pub fn publish(&mut self, entries: impl IntoIterator<Item = (ObjectId, Diff)>) {
		self.published.extend(entries);
	}

	pub fn published_from(&self, offset: usize) -> &[(ObjectId, Diff)] {
		if offset >= self.published.len() {
			&[]
		} else {
			&self.published[offset..]
		}
	}

	pub fn published_len(&self) -> usize {
		self.published.len()
	}

	pub fn truncate_published(&mut self, len: usize) {
		self.published.truncate(len);
	}

	pub fn take_published(&mut self) -> Vec<(ObjectId, Diff)> {
		mem::take(&mut self.published)
	}
}
