// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;
use reifydb_core::{common::CommitVersion, interface::catalog::object::ObjectId};
use reifydb_value::{reifydb_assertions, value::datetime::DateTime};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Published {
	watermark_ms: i64,
	at: CommitVersion,
	clamped: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishedWatermark {
	Unpublished,
	Withheld,
	Visible(DateTime),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputWatermarkEntry {
	pub output: ObjectId,
	pub watermark: DateTime,
	pub at: CommitVersion,
}

pub type OutputWatermarkEntries = Vec<OutputWatermarkEntry>;

#[derive(Clone, Default)]
pub struct OutputWatermarks {
	inner: Arc<DashMap<ObjectId, Published>>,
	generation: Arc<AtomicU64>,
	persisted: Arc<AtomicU64>,
}

impl OutputWatermarks {
	pub fn publish(&self, output: ObjectId, watermark: DateTime, at: CommitVersion) {
		reifydb_assertions! {
			assert!(
				at > CommitVersion(0),
				"an output frontier was published at version zero; every consumer past version \
				 zero would resolve it as visible, including transactions that ran before the \
				 frontier was established"
			);
		}
		let watermark_ms = watermark.to_millis();
		self.inner
			.entry(output)
			.and_modify(|current| {
				if !current.clamped {
					current.watermark_ms = watermark_ms;
					current.at = at;
					current.clamped = true;
				} else if watermark_ms > current.watermark_ms {
					current.watermark_ms = watermark_ms;
					current.at = at;
				}
			})
			.or_insert(Published {
				watermark_ms,
				at,
				clamped: true,
			});
		self.generation.fetch_add(1, Ordering::AcqRel);
	}

	pub fn resolve(&self, output: ObjectId, version: CommitVersion) -> PublishedWatermark {
		match self.inner.get(&output) {
			None => PublishedWatermark::Unpublished,
			Some(published) if !published.clamped => PublishedWatermark::Withheld,
			Some(published) if published.at < version => {
				PublishedWatermark::Visible(DateTime::from_millis(published.watermark_ms))
			}
			Some(_) => PublishedWatermark::Withheld,
		}
	}

	pub fn entries(&self) -> OutputWatermarkEntries {
		self.inner
			.iter()
			.map(|entry| OutputWatermarkEntry {
				output: *entry.key(),
				watermark: DateTime::from_millis(entry.watermark_ms),
				at: entry.at,
			})
			.collect()
	}

	pub fn unpersisted(&self) -> Option<(u64, OutputWatermarkEntries)> {
		let generation = self.generation.load(Ordering::Acquire);
		if generation == self.persisted.load(Ordering::Acquire) {
			return None;
		}
		Some((generation, self.entries()))
	}

	pub fn mark_persisted(&self, generation: u64) {
		self.persisted.fetch_max(generation, Ordering::AcqRel);
	}

	pub fn hydrate(&self, entries: OutputWatermarkEntries) {
		for entry in entries {
			let watermark_ms = entry.watermark.to_millis();
			self.inner
				.entry(entry.output)
				.and_modify(|current| {
					if watermark_ms > current.watermark_ms {
						*current = Published {
							watermark_ms,
							at: entry.at,
							clamped: false,
						};
					}
				})
				.or_insert(Published {
					watermark_ms,
					at: entry.at,
					clamped: false,
				});
		}
	}
}
