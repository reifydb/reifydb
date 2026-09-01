// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{any::Any, collections::HashMap, sync::Arc};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::{
		keyspace::{KEYSPACES, KeyspaceVisitor, dispatch},
		state::{GroupId, KeyspaceId, OperatorStateKey},
		traits::Keyspace,
	},
	metrics::{collect::MetricsCollector, sample::MetricsSample},
	state::typed::SuffixBytes,
};
use reifydb_store::tier::range::{RangeConfig, RangeMetrics, RangeTier};
use reifydb_value::byte_size::ByteSize;

use crate::tier::{range::typed::TypedDomain, typed::TypedPartition};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperatorRangeKeyspaceMetrics {
	pub bucket: KeyspaceId,
	pub used: ByteSize,
	pub limit: ByteSize,
	pub partitions: usize,
	pub intervals: usize,
	pub entries: usize,
	pub counters: RangeMetrics,
}

pub trait AnyRangeTier: Send + Sync {
	fn keyspace(&self) -> KeyspaceId;

	fn lookup(&self, operator: OperatorId, group: GroupId, suffix: &[u8]) -> Option<Option<EncodedPodRow>>;

	fn overwrite(&self, operator: OperatorId, group: GroupId, suffix: &[u8], row: EncodedPodRow);

	fn insert(&self, operator: OperatorId, group: GroupId, suffix: &[u8], row: EncodedPodRow);

	fn mark_deleted(&self, operator: OperatorId, group: GroupId, suffix: &[u8]);

	fn retract(&self, operator: OperatorId, group: GroupId, suffix: &[u8]);

	fn invalidate_operator(&self, operator: OperatorId);

	fn keyspace_metrics(&self) -> Option<OperatorRangeKeyspaceMetrics>;

	fn entries(&self) -> usize;

	fn intervals(&self) -> usize;

	fn partitions(&self) -> usize;

	fn resident_bytes(&self) -> ByteSize;

	fn limit_bytes(&self) -> ByteSize;

	fn metrics(&self) -> RangeMetrics;

	fn as_any(&self) -> &dyn Any;
}

impl<K: Keyspace> AnyRangeTier for RangeTier<TypedDomain<K>> {
	fn keyspace(&self) -> KeyspaceId {
		K::ID
	}

	fn lookup(&self, operator: OperatorId, group: GroupId, suffix: &[u8]) -> Option<Option<EncodedPodRow>> {
		let key = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix)?;
		let partition = TypedPartition {
			operator,
			group,
		};
		self.lookup_in(partition, partition, &key)
	}

	fn overwrite(&self, operator: OperatorId, group: GroupId, suffix: &[u8], row: EncodedPodRow) {
		let Some(key) = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix) else {
			return;
		};
		let partition = TypedPartition {
			operator,
			group,
		};
		self.overwrite_in(partition, partition, key, row);
	}

	fn insert(&self, operator: OperatorId, group: GroupId, suffix: &[u8], row: EncodedPodRow) {
		let Some(key) = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix) else {
			return;
		};
		let partition = TypedPartition {
			operator,
			group,
		};
		self.insert_in(partition, partition, key, row);
	}

	fn mark_deleted(&self, operator: OperatorId, group: GroupId, suffix: &[u8]) {
		let Some(key) = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix) else {
			return;
		};
		let partition = TypedPartition {
			operator,
			group,
		};
		self.mark_deleted_in(partition, partition, &key);
	}

	fn retract(&self, operator: OperatorId, group: GroupId, suffix: &[u8]) {
		let Some(key) = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix) else {
			return;
		};
		let partition = TypedPartition {
			operator,
			group,
		};
		self.retract_in(partition, partition, &key);
	}

	fn invalidate_operator(&self, operator: OperatorId) {
		self.invalidate_dimensions_where(|dimension| dimension.operator == operator);
	}

	fn keyspace_metrics(&self) -> Option<OperatorRangeKeyspaceMetrics> {
		self.bucket_metrics().into_iter().next().map(|row| OperatorRangeKeyspaceMetrics {
			bucket: K::ID,
			used: row.used,
			limit: RangeTier::shard_limit_bytes(self),
			partitions: row.partitions,
			intervals: row.intervals,
			entries: row.entries,
			counters: row.counters,
		})
	}

	fn entries(&self) -> usize {
		RangeTier::entries(self)
	}

	fn intervals(&self) -> usize {
		RangeTier::intervals(self)
	}

	fn partitions(&self) -> usize {
		RangeTier::partitions(self)
	}

	fn resident_bytes(&self) -> ByteSize {
		RangeTier::resident_bytes(self)
	}

	fn limit_bytes(&self) -> ByteSize {
		RangeTier::shard_limit_bytes(self)
	}

	fn metrics(&self) -> RangeMetrics {
		RangeTier::metrics(self)
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}

struct Build(RangeConfig);

impl KeyspaceVisitor for Build {
	type Output = Option<Box<dyn AnyRangeTier>>;

	fn visit<K: Keyspace>(self) -> Self::Output {
		RangeTier::<TypedDomain<K>>::new(self.0).map(|tier| Box::new(tier) as Box<dyn AnyRangeTier>)
	}
}

#[derive(Clone)]
pub struct RangeTiers {
	tiers: Arc<HashMap<KeyspaceId, Box<dyn AnyRangeTier>>>,
}

impl RangeTiers {
	pub fn new(config: RangeConfig) -> Option<Self> {
		let mut tiers: HashMap<KeyspaceId, Box<dyn AnyRangeTier>> = HashMap::new();
		for spec in KEYSPACES.iter().filter(|spec| spec.cache.caches_ranges()) {
			let tier = dispatch(spec.id, Build(config)).expect("a catalogued keyspace must dispatch")?;
			tiers.insert(spec.id, tier);
		}
		Some(Self {
			tiers: Arc::new(tiers),
		})
	}

	pub fn of(&self, keyspace: KeyspaceId) -> Option<&dyn AnyRangeTier> {
		self.tiers.get(&keyspace).map(AsRef::as_ref)
	}

	pub fn typed<K: Keyspace>(&self) -> Option<&RangeTier<TypedDomain<K>>> {
		self.of(K::ID)?.as_any().downcast_ref()
	}

	pub fn lookup(&self, operator: OperatorId, key: &EncodedKey) -> Option<Option<EncodedPodRow>> {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())?;
		self.of(keyspace)?.lookup(operator, group, &suffix)
	}

	pub fn overwrite(&self, operator: OperatorId, key: &EncodedKey, row: EncodedPodRow) {
		let Some((group, keyspace, suffix)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			return;
		};
		if let Some(tier) = self.of(keyspace) {
			tier.overwrite(operator, group, &suffix, row);
		}
	}

	pub fn insert(&self, operator: OperatorId, key: &EncodedKey, row: EncodedPodRow) {
		let Some((group, keyspace, suffix)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			return;
		};
		if let Some(tier) = self.of(keyspace) {
			tier.insert(operator, group, &suffix, row);
		}
	}

	pub fn mark_deleted(&self, operator: OperatorId, key: &EncodedKey) {
		let Some((group, keyspace, suffix)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			return;
		};
		if let Some(tier) = self.of(keyspace) {
			tier.mark_deleted(operator, group, &suffix);
		}
	}

	pub fn retract(&self, operator: OperatorId, key: &EncodedKey) {
		let Some((group, keyspace, suffix)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			return;
		};
		if let Some(tier) = self.of(keyspace) {
			tier.retract(operator, group, &suffix);
		}
	}

	pub fn invalidate_operator(&self, operator: OperatorId) {
		for tier in self.tiers.values() {
			tier.invalidate_operator(operator);
		}
	}

	pub fn keyspace_metrics(&self) -> Vec<OperatorRangeKeyspaceMetrics> {
		let mut out: Vec<OperatorRangeKeyspaceMetrics> =
			self.tiers.values().filter_map(|tier| tier.keyspace_metrics()).collect();
		out.sort_by_key(|row| row.bucket.0);
		out
	}

	pub fn entries(&self) -> usize {
		self.tiers.values().map(|tier| tier.entries()).sum()
	}

	pub fn intervals(&self) -> usize {
		self.tiers.values().map(|tier| tier.intervals()).sum()
	}

	pub fn partitions(&self) -> usize {
		self.tiers.values().map(|tier| tier.partitions()).sum()
	}

	pub fn metrics(&self) -> RangeMetrics {
		self.tiers.values().fold(RangeMetrics::default(), |mut total, tier| {
			let counters = tier.metrics();
			total.hits += counters.hits;
			total.misses += counters.misses;
			total.exempt += counters.exempt;
			total.materializes += counters.materializes;
			total.materializes_refused += counters.materializes_refused;
			total.materializes_raced += counters.materializes_raced;
			total.evictions += counters.evictions;
			total.point_hits += counters.point_hits;
			total.point_misses += counters.point_misses;
			total
		})
	}
}

impl MetricsCollector for RangeTiers {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		const SCOPE: &str = "operator_range";
		let mut counters = RangeMetrics::default();
		let mut used = 0u64;
		let mut limit = 0u64;
		for tier in self.tiers.values() {
			let source = tier.metrics();
			counters.hits += source.hits;
			counters.misses += source.misses;
			counters.exempt += source.exempt;
			counters.materializes += source.materializes;
			counters.materializes_refused += source.materializes_refused;
			counters.materializes_raced += source.materializes_raced;
			counters.evictions += source.evictions;
			counters.point_hits += source.point_hits;
			counters.point_misses += source.point_misses;
			used += tier.resident_bytes().as_bytes();
			limit += tier.limit_bytes().as_bytes();
		}

		out.push(MetricsSample::heap(SCOPE, "resident_bytes", ByteSize::from_bytes(used)));
		out.push(MetricsSample::count(SCOPE, "resident_intervals", self.intervals() as u64));
		out.push(MetricsSample::count(
			SCOPE,
			"resident_partitions",
			self.tiers.values().map(|tier| tier.partitions()).sum::<usize>() as u64,
		));
		out.push(MetricsSample::count(SCOPE, "resident_entries", self.entries() as u64));
		out.push(MetricsSample::counter(SCOPE, "hits", counters.hits));
		out.push(MetricsSample::counter(SCOPE, "misses", counters.misses));
		out.push(MetricsSample::counter(SCOPE, "exempt", counters.exempt));
		out.push(MetricsSample::counter(SCOPE, "materializes", counters.materializes));
		out.push(MetricsSample::counter(SCOPE, "materializes_refused", counters.materializes_refused));
		out.push(MetricsSample::counter(SCOPE, "materializes_raced", counters.materializes_raced));
		out.push(MetricsSample::counter(SCOPE, "evictions", counters.evictions));
		out.push(MetricsSample::counter(SCOPE, "point_hits", counters.point_hits));
		out.push(MetricsSample::counter(SCOPE, "point_misses", counters.point_misses));
		out.push(MetricsSample::bytes(SCOPE, "tier_limit_bytes", ByteSize::from_bytes(limit)));

		for keyspace in self.keyspace_metrics() {
			let scope = format!("{SCOPE}::keyspace::{}", keyspace.bucket.name());
			out.push(MetricsSample::bytes(scope.clone(), "used_bytes", keyspace.used));
			out.push(MetricsSample::count(scope.clone(), "partitions", keyspace.partitions as u64));
			out.push(MetricsSample::count(scope.clone(), "entries", keyspace.entries as u64));
			out.push(MetricsSample::counter(scope.clone(), "hits", keyspace.counters.hits));
			out.push(MetricsSample::counter(scope, "misses", keyspace.counters.misses));
		}
	}
}
