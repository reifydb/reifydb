// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	fmt::{Debug, Formatter, Result as FmtResult},
	marker::PhantomData,
};

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::key::{
	operator::{state::KeyspaceId, traits::Keyspace},
	typed::{ExclusiveUpperEnd, Key},
};
use reifydb_store::tier::range::RangeDomain;

use crate::tier::typed::TypedPartition;

pub struct TypedDomain<K: Keyspace>(PhantomData<K>);

impl<K: Keyspace> Clone for TypedDomain<K> {
	fn clone(&self) -> Self {
		*self
	}
}

impl<K: Keyspace> Copy for TypedDomain<K> {}

impl<K: Keyspace> Debug for TypedDomain<K> {
	fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
		f.write_str(K::NAME)
	}
}

impl<K: Keyspace> RangeDomain for TypedDomain<K> {
	type Dimension = TypedPartition;
	type Partition = TypedPartition;
	type Key = K::Suffix;
	type MetricBucket = KeyspaceId;
	type Row = EncodedPodRow;

	const METRIC_BUCKETS: usize = 1;

	const SCOPE: &'static str = "operator_range";

	const GAP_SCOPE: &'static str = "operator_range::gaps";

	fn dimension(partition: &Self::Partition) -> Self::Dimension {
		*partition
	}

	fn span(_partition: &Self::Partition) -> (Self::Key, ExclusiveUpperEnd<Self::Key>) {
		(K::Suffix::low(), ExclusiveUpperEnd::Top)
	}

	fn head_band(_dimension: Self::Dimension) -> Option<(Self::Key, Self::Key)> {
		None
	}

	fn caches_ranges(_partition: &Self::Partition) -> bool {
		K::CACHE.caches_ranges()
	}

	fn cache_tiers_run_end(_partition: &Self::Partition) -> ExclusiveUpperEnd<Self::Key> {
		ExclusiveUpperEnd::Top
	}

	fn metric_bucket(_partition: &Self::Partition) -> usize {
		0
	}

	fn metric_bucket_at(_index: usize) -> Self::MetricBucket {
		K::ID
	}

	fn metric_bucket_name(_bucket: Self::MetricBucket) -> Cow<'static, str> {
		Cow::Borrowed(K::NAME)
	}
}
