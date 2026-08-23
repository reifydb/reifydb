// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::common::CommitVersion;

#[derive(Default)]
pub(crate) struct SealedRanges {
	spans: BTreeMap<u64, u64>,
}

impl SealedRanges {
	pub(crate) fn contains(&self, version: CommitVersion) -> bool {
		self.spans.range(..=version.0).next_back().is_some_and(|(_, end)| version.0 <= *end)
	}

	pub(crate) fn insert(&mut self, lo: CommitVersion, hi: CommitVersion) {
		let mut start = lo.0;
		let mut end = hi.0;
		if let Some((&prior_start, &prior_end)) = self.spans.range(..=start).next_back()
			&& prior_end.saturating_add(1) >= start
		{
			start = prior_start;
			end = end.max(prior_end);
		}
		let absorbed: Vec<u64> =
			self.spans.range(start..=end.saturating_add(1)).map(|(&start, _)| start).collect();
		for key in absorbed {
			let merged = self.spans.remove(&key).unwrap_or(key);
			end = end.max(merged);
		}
		self.spans.insert(start, end);
	}

	pub(crate) fn next_start_above(&self, version: CommitVersion) -> Option<CommitVersion> {
		self.spans.range(version.0.saturating_add(1)..).next().map(|(&start, _)| CommitVersion(start))
	}
}
