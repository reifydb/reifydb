// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	borrow::Cow,
	fmt::{Debug, Formatter, Result as FmtResult},
	marker::PhantomData,
};

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::key::operator::{state::KeyspaceId, traits::Keyspace};
use reifydb_store::tier::point::PointDomain;

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

impl<K: Keyspace> PointDomain for TypedDomain<K> {
	type Dimension = TypedPartition;
	type Key = K::Suffix;
	type MetricBucket = KeyspaceId;
	type Row = EncodedPodRow;

	const METRIC_BUCKETS: usize = 1;

	const SCOPE: &'static str = "operator_point";

	fn metric_bucket(_key: &Self::Key) -> Option<usize> {
		Some(0)
	}

	fn caches_points(_bucket: usize) -> bool {
		K::CACHE.caches_points()
	}

	fn metric_bucket_at(_index: usize) -> Self::MetricBucket {
		K::ID
	}

	fn metric_bucket_name(_bucket: Self::MetricBucket) -> Cow<'static, str> {
		Cow::Borrowed(K::NAME)
	}
}
