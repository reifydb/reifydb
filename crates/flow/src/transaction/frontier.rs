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
	frontier_ms: u64,
	at: CommitVersion,
	clamped: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Frontier {
	Unpublished,
	Withheld,
	Visible(DateTime),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrontierEntry {
	pub output: ObjectId,
	pub frontier: DateTime,
	pub at: CommitVersion,
}

pub type FrontierEntries = Vec<FrontierEntry>;

#[derive(Clone, Default)]
pub struct OutputFrontiers {
	inner: Arc<DashMap<ObjectId, Published>>,
	generation: Arc<AtomicU64>,
	persisted: Arc<AtomicU64>,
}

impl OutputFrontiers {
	pub fn publish(&self, output: ObjectId, frontier: DateTime, at: CommitVersion) {
		reifydb_assertions! {
			assert!(
				at > CommitVersion(0),
				"an output frontier was published at version zero; every consumer past version \
				 zero would resolve it as visible while it never becomes dirty, so it never \
				 reaches disk and the claim vanishes on the next restart"
			);
		}
		let frontier_ms = frontier.to_millis();
		self.inner
			.entry(output)
			.and_modify(|current| {
				if !current.clamped {
					current.frontier_ms = frontier_ms;
					current.at = at;
					current.clamped = true;
				} else if frontier_ms > current.frontier_ms {
					current.frontier_ms = frontier_ms;
					current.at = at;
				}
			})
			.or_insert(Published {
				frontier_ms,
				at,
				clamped: true,
			});
		self.generation.fetch_add(1, Ordering::AcqRel);
	}

	pub fn resolve(&self, output: ObjectId, version: CommitVersion) -> Frontier {
		match self.inner.get(&output) {
			None => Frontier::Unpublished,
			Some(published) if !published.clamped => Frontier::Withheld,
			Some(published) if published.at < version => {
				Frontier::Visible(DateTime::from_millis(published.frontier_ms))
			}
			Some(_) => Frontier::Withheld,
		}
	}

	pub fn entries(&self) -> FrontierEntries {
		self.inner
			.iter()
			.map(|entry| FrontierEntry {
				output: *entry.key(),
				frontier: DateTime::from_millis(entry.frontier_ms),
				at: entry.at,
			})
			.collect()
	}

	pub fn unpersisted(&self) -> Option<(u64, FrontierEntries)> {
		let generation = self.generation.load(Ordering::Acquire);
		if generation == self.persisted.load(Ordering::Acquire) {
			return None;
		}
		Some((generation, self.entries()))
	}

	pub fn mark_persisted(&self, generation: u64) {
		self.persisted.fetch_max(generation, Ordering::AcqRel);
	}

	pub fn hydrate(&self, entries: FrontierEntries) {
		for entry in entries {
			let frontier_ms = entry.frontier.to_millis();
			self.inner
				.entry(entry.output)
				.and_modify(|current| {
					if frontier_ms > current.frontier_ms {
						*current = Published {
							frontier_ms,
							at: entry.at,
							clamped: false,
						};
					}
				})
				.or_insert(Published {
					frontier_ms,
					at: entry.at,
					clamped: false,
				});
		}
	}
}
