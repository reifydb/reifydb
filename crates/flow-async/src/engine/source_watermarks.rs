// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::mem::take;

use reifydb_value::error::ValueError;

use crate::{
	engine::FlowEngineInner,
	transaction::{deferred::DeferredTransaction, source_watermark::SourceWatermarks},
};

impl FlowEngineInner {
	pub fn begin_source_watermarks(&mut self, txn: &mut DeferredTransaction) -> Result<(), ValueError> {
		self.staged_source_watermarks.clear();
		for source in &self.dirty_source_watermarks {
			if let Some(coordinate) = self.source_watermarks.get(source) {
				SourceWatermarks::persist(*source, txn, *coordinate)?;
			}
		}
		Ok(())
	}

	pub fn stage_source_watermarks(&mut self, txn: &DeferredTransaction) {
		for (source, coordinate) in &txn.source_watermark_cache {
			if self.source_watermarks.get(source).is_none_or(|known| coordinate > known) {
				self.staged_source_watermarks.insert(*source, *coordinate);
			}
		}
	}

	pub fn accept_source_watermarks(&mut self, persisted: bool) {
		for (source, coordinate) in take(&mut self.staged_source_watermarks) {
			let known = self.source_watermarks.entry(source).or_insert(coordinate);
			if coordinate > *known {
				*known = coordinate;
			}
			if !persisted {
				self.dirty_source_watermarks.insert(source);
			}
		}
		if persisted {
			self.dirty_source_watermarks.clear();
		}
	}

	pub fn discard_staged_source_watermarks(&mut self) {
		self.staged_source_watermarks.clear();
	}
}
