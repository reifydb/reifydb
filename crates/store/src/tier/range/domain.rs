// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{borrow::Cow, sync::LazyLock};

use reifydb_codec::{
	key::{decode_fixed, encode_u8, encoded::EncodedKey},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::state::{GroupId, KeyspaceId, OperatorStateKey},
		typed::{Edge, MultiKey},
	},
};

use crate::tier::range::RangeDomain;

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
	let last = prefix.iter().rposition(|&byte| byte != 0xff)?;
	let mut out = prefix[..=last].to_vec();
	out[last] += 1;
	Some(out)
}

#[derive(Clone, Copy, Debug)]
pub(super) struct TestDomain;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(super) struct TestPartition {
	pub dimension: OperatorId,
	pub group: GroupId,
	pub keyspace: KeyspaceId,
}

impl TestPartition {
	pub const PREFIX_LEN: usize = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize + 1;

	pub fn of(dimension: OperatorId, key: &EncodedKey) -> Self {
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		let mut bytes = key.as_slice().to_vec();
		bytes.resize(Self::PREFIX_LEN, 0);
		let group = GroupId::from_bytes(decode_fixed(
			bytes[..offset].try_into().expect("a padded key spans the group prefix"),
		));
		Self {
			dimension,
			group,
			keyspace: KeyspaceId(encode_u8(bytes[offset])),
		}
	}

	pub fn prefix(&self) -> EncodedKey {
		EncodedKey::new(OperatorStateKey::inner_encoded(self.group, self.keyspace, [0u8; 0]).as_bytes())
	}

	pub fn span(&self) -> (Edge<MultiKey>, Edge<MultiKey>) {
		let start = self.prefix();
		let end = match prefix_successor(start.as_slice()) {
			Some(successor) => Edge::of(successor),
			None => Edge::Top,
		};
		(Edge::Key(start), end)
	}

	pub fn group_end(&self) -> Edge<MultiKey> {
		let prefix = self.prefix();
		let group = &prefix.as_slice()[..OperatorStateKey::KEYSPACE_INNER_OFFSET as usize];
		match prefix_successor(group) {
			Some(successor) => Edge::of(successor),
			None => Edge::Top,
		}
	}

	pub fn caches_ranges(&self) -> bool {
		self.keyspace.cache_tiers().caches_ranges()
	}
}

static CACHE_TIERS_RUN_FLOOR: LazyLock<[u8; 256]> = LazyLock::new(|| {
	let mut floor = [0u8; 256];
	let mut lowest = 0u8;
	for keyspace in 0..=u8::MAX {
		if keyspace > 0
			&& KeyspaceId(keyspace).cache_tiers().caches_ranges()
				!= KeyspaceId(keyspace - 1).cache_tiers().caches_ranges()
		{
			lowest = keyspace;
		}
		floor[keyspace as usize] = lowest;
	}
	floor
});

impl RangeDomain for TestDomain {
	type Dimension = OperatorId;
	type Partition = TestPartition;
	type Key = MultiKey;
	type MetricBucket = KeyspaceId;
	type Row = EncodedPodRow;

	const METRIC_BUCKETS: usize = 256;

	const SCOPE: &'static str = "operator_range";

	const GAP_SCOPE: &'static str = "operator_range::gaps";

	fn partition(dimension: Self::Dimension, key: &Self::Key) -> Self::Partition {
		TestPartition::of(dimension, key)
	}

	fn dimension(partition: &Self::Partition) -> Self::Dimension {
		partition.dimension
	}

	fn span(partition: &Self::Partition) -> (Edge<Self::Key>, Edge<Self::Key>) {
		partition.span()
	}

	fn head_band(_dimension: Self::Dimension) -> Option<(Edge<Self::Key>, Edge<Self::Key>)> {
		None
	}

	fn caches_ranges(partition: &Self::Partition) -> bool {
		partition.caches_ranges()
	}

	fn cache_tiers_run_end(partition: &Self::Partition) -> Edge<Self::Key> {
		let floor = CACHE_TIERS_RUN_FLOOR[partition.keyspace.0 as usize];
		if floor == partition.keyspace.0 {
			return partition.span().1;
		}
		TestPartition {
			keyspace: KeyspaceId(floor),
			..*partition
		}
		.span()
		.1
	}

	fn partition_walk_end(partition: &Self::Partition) -> Edge<Self::Key> {
		partition.group_end()
	}

	fn metric_bucket(partition: &Self::Partition) -> usize {
		partition.keyspace.0 as usize
	}

	fn metric_bucket_at(index: usize) -> Self::MetricBucket {
		KeyspaceId(index as u8)
	}

	fn metric_bucket_name(bucket: Self::MetricBucket) -> Cow<'static, str> {
		bucket.name()
	}
}

#[derive(Clone, Copy, Debug)]
pub(super) struct AdmittingDomain;

impl RangeDomain for AdmittingDomain {
	type Dimension = OperatorId;
	type Partition = TestPartition;
	type Key = MultiKey;
	type MetricBucket = KeyspaceId;
	type Row = EncodedPodRow;

	const METRIC_BUCKETS: usize = 256;

	const SCOPE: &'static str = "admitting_range";

	const GAP_SCOPE: &'static str = "admitting_range::gaps";

	fn partition(dimension: Self::Dimension, key: &Self::Key) -> Self::Partition {
		TestDomain::partition(dimension, key)
	}

	fn dimension(partition: &Self::Partition) -> Self::Dimension {
		TestDomain::dimension(partition)
	}

	fn span(partition: &Self::Partition) -> (Edge<Self::Key>, Edge<Self::Key>) {
		TestDomain::span(partition)
	}

	fn head_band(dimension: Self::Dimension) -> Option<(Edge<Self::Key>, Edge<Self::Key>)> {
		TestDomain::head_band(dimension)
	}

	fn caches_ranges(partition: &Self::Partition) -> bool {
		TestDomain::caches_ranges(partition)
	}

	fn cache_tiers_run_end(partition: &Self::Partition) -> Edge<Self::Key> {
		TestDomain::cache_tiers_run_end(partition)
	}

	fn admits_unproven_writes() -> bool {
		true
	}

	fn metric_bucket(partition: &Self::Partition) -> usize {
		TestDomain::metric_bucket(partition)
	}

	fn metric_bucket_at(index: usize) -> Self::MetricBucket {
		TestDomain::metric_bucket_at(index)
	}

	fn metric_bucket_name(bucket: Self::MetricBucket) -> Cow<'static, str> {
		TestDomain::metric_bucket_name(bucket)
	}
}
