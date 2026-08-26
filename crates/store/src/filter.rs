// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, sync::Arc};

use reifydb_filter::adaptive::{AdaptiveKeyFilter, FilterMetrics};

pub trait FilterDomain {
	type Key<'a>;

	fn hash(key: Self::Key<'_>) -> u64;
}

pub struct KeyFilter<D: FilterDomain>(Arc<AdaptiveKeyFilter>, PhantomData<fn() -> D>);

impl<D: FilterDomain> Clone for KeyFilter<D> {
	fn clone(&self) -> Self {
		Self(self.0.clone(), PhantomData)
	}
}

impl<D: FilterDomain> KeyFilter<D> {
	#[allow(clippy::new_without_default)]
	pub fn new() -> Self {
		Self(Arc::new(AdaptiveKeyFilter::new()), PhantomData)
	}

	pub fn armed(size_for_keys: u64) -> Self {
		Self(Arc::new(AdaptiveKeyFilter::armed(size_for_keys)), PhantomData)
	}

	pub fn add(&self, key: D::Key<'_>) {
		self.0.add(D::hash(key));
	}

	pub fn may_contain(&self, key: D::Key<'_>) -> bool {
		self.0.may_contain(D::hash(key))
	}

	pub fn metrics(&self) -> FilterMetrics {
		self.0.metrics()
	}

	pub fn handle(&self) -> Arc<AdaptiveKeyFilter> {
		self.0.clone()
	}
}
