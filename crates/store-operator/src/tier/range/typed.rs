// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	fmt::{Debug, Formatter, Result as FmtResult},
	marker::PhantomData,
};

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			state::{GroupId, KeyspaceId},
			traits::Keyspace,
		},
		typed::{ExclusiveUpperEnd, Key},
	},
};
use reifydb_store::tier::range::{RangeDomain, RowBytes};

pub trait KeyspaceRow: Keyspace {
	type Row: RowBytes + Clone + Send + Sync + 'static;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TypedPartition {
	pub operator: OperatorId,
	pub group: GroupId,
}

pub struct TypedDomain<K: KeyspaceRow>(PhantomData<K>);

impl<K: KeyspaceRow> Clone for TypedDomain<K> {
	fn clone(&self) -> Self {
		*self
	}
}

impl<K: KeyspaceRow> Copy for TypedDomain<K> {}

impl<K: KeyspaceRow> Debug for TypedDomain<K> {
	fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
		f.write_str(K::NAME)
	}
}

impl<K: KeyspaceRow> RangeDomain for TypedDomain<K> {
	type Dimension = OperatorId;
	type Partition = TypedPartition;
	type Key = K::Suffix;
	type MetricBucket = KeyspaceId;
	type Row = K::Row;

	const METRIC_BUCKETS: usize = 1;

	const SCOPE: &'static str = "operator_range_typed";

	const GAP_SCOPE: &'static str = "operator_range_typed::gaps";

	fn dimension(partition: &Self::Partition) -> Self::Dimension {
		partition.operator
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
