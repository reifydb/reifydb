// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::typed::Edge;

use crate::{
	coverage::index::CoverageIndex,
	tier::range::{RangeDomain, RangeTier},
};

pub(super) fn in_head_band<D: RangeDomain>(dimension: D::Dimension, key: &D::Key) -> bool {
	match D::head_band(dimension) {
		Some((start, end)) => start.cmp_key(key).is_le() && end.cmp_key(key).is_ge(),
		None => false,
	}
}

pub(super) fn advance_to_head<D: RangeDomain>(
	coverage: &CoverageIndex<D::Dimension, D::Key>,
	dimension: D::Dimension,
	lo: D::Key,
	hi: &Edge<D::Key>,
) -> D::Key {
	let Some((start, _)) = D::head_band(dimension) else {
		return lo;
	};
	if start.covers(&lo) {
		return lo;
	}
	match coverage.head(dimension) {
		Some(at) if at.cmp_key(&lo).is_gt() && hi > at => match at.key() {
			Some(key) => key.clone(),
			None => lo,
		},
		_ => lo,
	}
}

impl<D: RangeDomain> RangeTier<D> {
	pub fn head_proves_empty(&self, dimension: D::Dimension, lo: &Edge<D::Key>, range_hi: &Edge<D::Key>) -> bool {
		let Some((start, end)) = D::head_band(dimension) else {
			return false;
		};
		if *lo < start {
			return false;
		}
		let coverage = self.coverage().read();
		coverage.head(dimension).is_some_and(|at| at > range_hi || (at == range_hi && *range_hi >= end))
	}

	pub fn head(&self, dimension: D::Dimension) -> Option<Edge<D::Key>> {
		self.coverage().read().head(dimension).cloned()
	}

	pub fn raise_head(
		&self,
		dimension: D::Dimension,
		lo: &Edge<D::Key>,
		through: &Edge<D::Key>,
		first: Option<&D::Key>,
		token: u64,
	) {
		let Some((start, end)) = D::head_band(dimension) else {
			return;
		};
		if *lo > start {
			return;
		}
		let proven = match first {
			Some(key) => Edge::Key(key.clone()),
			None => through.clone(),
		};
		let proven = proven.min(end);
		if proven <= start {
			return;
		}
		let mut coverage = self.coverage().write();
		if !self.retractions_unchanged(token) {
			return;
		}
		if coverage.head(dimension).is_none_or(|current| *current < proven) {
			coverage.set_head(dimension, proven);
		}
	}

	pub fn lower_head(&self, dimension: D::Dimension, key: &D::Key) {
		if !in_head_band::<D>(dimension, key) {
			return;
		}
		{
			let coverage = self.coverage().read();
			if coverage.head(dimension).is_none_or(|current| !current.covers(key)) {
				return;
			}
		}
		let mut coverage = self.coverage().write();
		if coverage.head(dimension).is_none_or(|current| !current.covers(key)) {
			return;
		}
		coverage.set_head(dimension, Edge::Key(key.clone()));
		self.record_retraction();
	}
}
