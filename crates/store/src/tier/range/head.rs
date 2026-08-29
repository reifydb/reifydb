// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::key::typed::{ExclusiveUpperEnd, Key};

use crate::{
	coverage::index::CoverageIndex,
	tier::range::{RangeDomain, RangeTier},
};

pub(super) fn in_head_band<D: RangeDomain>(dimension: D::Dimension, key: &EncodedKey) -> bool {
	match D::head_band(dimension) {
		Some((start, end)) => start.as_slice() <= key.as_slice() && key.as_slice() <= end.as_slice(),
		None => false,
	}
}

pub(super) fn advance_to_head<D: RangeDomain>(
	coverage: &CoverageIndex<D::Dimension>,
	dimension: D::Dimension,
	lo: EncodedKey,
	hi: &ExclusiveUpperEnd,
) -> EncodedKey {
	let Some((start, _)) = D::head_band(dimension) else {
		return lo;
	};
	if lo.as_slice() < start.as_slice() {
		return lo;
	}
	match coverage.head(dimension) {
		Some(at) if lo.as_slice() < at.as_slice() && hi.covers(at) => at.clone(),
		_ => lo,
	}
}

impl<D: RangeDomain> RangeTier<D> {
	pub fn head_proves_empty(&self, dimension: D::Dimension, lo: &EncodedKey, range_hi: &EncodedKey) -> bool {
		let Some((start, end)) = D::head_band(dimension) else {
			return false;
		};
		if lo.as_slice() < start.as_slice() {
			return false;
		}
		let coverage = self.coverage().read();
		coverage.head(dimension).is_some_and(|at| {
			at.as_slice() > range_hi.as_slice()
				|| (at.as_slice() == range_hi.as_slice() && range_hi.as_slice() >= end.as_slice())
		})
	}

	pub fn head(&self, dimension: D::Dimension) -> Option<EncodedKey> {
		self.coverage().read().head(dimension).cloned()
	}

	pub fn raise_head(
		&self,
		dimension: D::Dimension,
		lo: &EncodedKey,
		through: &EncodedKey,
		first: Option<&EncodedKey>,
		token: u64,
	) {
		let Some((start, end)) = D::head_band(dimension) else {
			return;
		};
		if lo.as_slice() > start.as_slice() {
			return;
		}
		let proven = match first {
			Some(key) => key.clone(),
			None => through.successor().unwrap_or_else(|| end.clone()),
		};
		let proven = if proven.as_slice() > end.as_slice() {
			end
		} else {
			proven
		};
		if proven.as_slice() <= start.as_slice() {
			return;
		}
		let mut coverage = self.coverage().write();
		if !self.retractions_unchanged(token) {
			return;
		}
		if coverage.head(dimension).is_none_or(|current| current.as_slice() < proven.as_slice()) {
			coverage.set_head(dimension, proven);
		}
	}

	pub fn lower_head(&self, dimension: D::Dimension, key: &EncodedKey) {
		if !in_head_band::<D>(dimension, key) {
			return;
		}
		{
			let coverage = self.coverage().read();
			if coverage.head(dimension).is_none_or(|current| current.as_slice() <= key.as_slice()) {
				return;
			}
		}
		let mut coverage = self.coverage().write();
		if coverage.head(dimension).is_none_or(|current| current.as_slice() <= key.as_slice()) {
			return;
		}
		coverage.set_head(dimension, key.clone());
		self.record_retraction();
	}
}
