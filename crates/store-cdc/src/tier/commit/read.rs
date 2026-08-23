// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{common::CommitVersion, interface::cdc::Cdc};
use tracing::instrument;

use crate::tier::commit::CdcCommitBufferTier;

impl CdcCommitBufferTier {
	#[instrument(name = "store::cdc::commit::floor", level = "trace", skip(self))]
	pub fn floor(&self) -> Option<CommitVersion> {
		let inner = self.shared.inner.lock();
		let in_flight = inner.in_flight.as_ref().and_then(|batch| batch.min_version());
		let live = inner.live.keys().next().copied();
		match (in_flight, live) {
			(Some(in_flight), Some(live)) => Some(in_flight.min(live)),
			(in_flight, live) => in_flight.or(live),
		}
	}

	#[instrument(name = "store::cdc::commit::head", level = "trace", skip(self))]
	pub fn head(&self) -> Option<CommitVersion> {
		let inner = self.shared.inner.lock();
		let in_flight = inner.in_flight.as_ref().and_then(|batch| batch.max_version());
		let live = inner.live.keys().next_back().copied();
		match (in_flight, live) {
			(Some(in_flight), Some(live)) => Some(in_flight.max(live)),
			(in_flight, live) => in_flight.or(live),
		}
	}

	#[instrument(name = "store::cdc::commit::get", level = "trace", skip(self), fields(version = version.0))]
	pub fn get(&self, version: CommitVersion) -> Option<Arc<Cdc>> {
		let inner = self.shared.inner.lock();
		inner.in_flight
			.as_ref()
			.and_then(|batch| batch.get(version))
			.or_else(|| inner.live.get(&version).cloned())
	}

	#[instrument(name = "store::cdc::commit::range", level = "trace", skip(self), fields(lo = lo.0, hi = hi.0, want = want))]
	pub fn range(&self, lo: CommitVersion, hi: CommitVersion, want: usize) -> Vec<Arc<Cdc>> {
		if want == 0 || lo > hi {
			return Vec::new();
		}
		let mut out = Vec::new();
		let inner = self.shared.inner.lock();
		let mut in_flight = Vec::new();
		if let Some(batch) = inner.in_flight.as_ref() {
			batch.collect_range(lo, hi, want, &mut in_flight);
		}
		let mut cut = in_flight.into_iter().peekable();
		let mut live = inner.live.range(lo..=hi).map(|(_, cdc)| Arc::clone(cdc)).peekable();
		while out.len() < want {
			let take_cut = match (cut.peek(), live.peek()) {
				(Some(cut), Some(live)) => cut.version <= live.version,
				(Some(_), None) => true,
				(None, Some(_)) => false,
				(None, None) => break,
			};
			let next = if take_cut {
				cut.next()
			} else {
				live.next()
			};
			let Some(next) = next else {
				break;
			};
			if out.last().is_some_and(|last: &Arc<Cdc>| last.version == next.version) {
				continue;
			}
			out.push(next);
		}
		out
	}
}
