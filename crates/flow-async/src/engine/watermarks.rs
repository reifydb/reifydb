// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem::take;

use reifydb_value::Result;

use crate::{
	engine::FlowEngineInner,
	transaction::{deferred::DeferredTransaction, watermark::SourceWatermarks},
};

impl FlowEngineInner {
	pub fn begin_watermarks(&mut self, txn: &mut DeferredTransaction) -> Result<()> {
		self.staged_watermarks.clear();
		for source in &self.dirty_watermarks {
			if let Some(coordinate) = self.watermarks.get(source) {
				SourceWatermarks::persist(*source, txn, *coordinate)?;
			}
		}
		Ok(())
	}

	pub fn stage_watermarks(&mut self, txn: &DeferredTransaction) {
		for (source, coordinate) in &txn.source_watermark_cache {
			if self.watermarks.get(source).is_none_or(|known| coordinate > known) {
				self.staged_watermarks.insert(*source, *coordinate);
			}
		}
	}

	pub fn accept_watermarks(&mut self, persisted: bool) {
		for (source, coordinate) in take(&mut self.staged_watermarks) {
			let known = self.watermarks.entry(source).or_insert(coordinate);
			if coordinate > *known {
				*known = coordinate;
			}
			if !persisted {
				self.dirty_watermarks.insert(source);
			}
		}
		if persisted {
			self.dirty_watermarks.clear();
		}
	}

	pub fn discard_staged_watermarks(&mut self) {
		self.staged_watermarks.clear();
	}
}
