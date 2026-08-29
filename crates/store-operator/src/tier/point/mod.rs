// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::borrow::Cow;

use reifydb_codec::{
	key::{encode_u8, encoded::EncodedKey},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::state::{KeyspaceId, OperatorStateKey},
		typed::MultiKey,
	},
};
use reifydb_store::tier::point::{
	PointBucketMetrics, PointConfig, PointDomain, PointMetrics, PointShardMetrics, PointTier,
};

pub type OperatorPointConfig = PointConfig;
pub type OperatorPointTier = PointTier<OperatorDomain>;
pub type OperatorPointMetrics = PointMetrics;
pub type OperatorPointShardMetrics = PointShardMetrics;
pub type OperatorPointKeyspaceMetrics = PointBucketMetrics<OperatorDomain>;

#[derive(Clone, Copy, Debug)]
pub struct OperatorDomain;

impl PointDomain for OperatorDomain {
	type Dimension = OperatorId;
	type Key = MultiKey;
	type MetricBucket = KeyspaceId;
	type Row = EncodedPodRow;

	const METRIC_BUCKETS: usize = 256;

	const SCOPE: &'static str = "operator_point";

	fn metric_bucket(key: &EncodedKey) -> Option<usize> {
		let bytes = key.as_slice();
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		if bytes.len() <= offset {
			return None;
		}
		Some(encode_u8(bytes[offset]) as usize)
	}

	fn caches_points(bucket: usize) -> bool {
		KeyspaceId(bucket as u8).cache_tiers().caches_points()
	}

	fn metric_bucket_at(index: usize) -> Self::MetricBucket {
		KeyspaceId(index as u8)
	}

	fn metric_bucket_name(bucket: Self::MetricBucket) -> Cow<'static, str> {
		bucket.name()
	}
}
