// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	collections::HashMap,
	sync::{Arc, atomic::{AtomicU64, Ordering}},
};

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
use reifydb_store::tier::point::{PointConfig, PointMetrics, PointTier};
use reifydb_value::byte_size::ByteSize;

use crate::tier::point::typed::{TypedDomain, TypedPartition};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperatorPointKeyspaceMetrics {
	pub bucket: KeyspaceId,
	pub used: ByteSize,
	pub limit: ByteSize,
	pub entries: usize,
	pub counters: PointMetrics,
}

pub trait AnyPointTier: Send + Sync {
	fn keyspace(&self) -> KeyspaceId;

	fn get(&self, operator: OperatorId, group: GroupId, suffix: &[u8]) -> Option<Option<EncodedPodRow>>;

	fn contains(&self, operator: OperatorId, group: GroupId, suffix: &[u8]) -> Option<bool>;

	fn begin_fill(&self, operator: OperatorId, group: GroupId, suffix: &[u8]) -> bool;

	fn finish_fill(&self, operator: OperatorId, group: GroupId, suffix: &[u8], row: Option<EncodedPodRow>) -> bool;

	fn abort_fill(&self, operator: OperatorId, group: GroupId, suffix: &[u8]);

	fn overwrite(&self, operator: OperatorId, group: GroupId, suffix: &[u8], row: EncodedPodRow);

	fn invalidate(&self, operator: OperatorId, group: GroupId, suffix: &[u8]);

	fn invalidate_operator(&self, operator: OperatorId);

	fn keyspace_metrics(&self) -> Option<OperatorPointKeyspaceMetrics>;

	fn entries(&self) -> usize;

	fn resident_bytes(&self) -> ByteSize;

	fn limit_bytes(&self) -> ByteSize;

	fn metrics(&self) -> PointMetrics;

	fn as_any(&self) -> &dyn Any;
}

impl<K: Keyspace> AnyPointTier for PointTier<TypedDomain<K>> {
	fn keyspace(&self) -> KeyspaceId {
		K::ID
	}

	fn get(&self, operator: OperatorId, group: GroupId, suffix: &[u8]) -> Option<Option<EncodedPodRow>> {
		let key = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix)?;
		PointTier::get(
			self,
			TypedPartition {
				operator,
				group,
			},
			&key,
		)
	}

	fn contains(&self, operator: OperatorId, group: GroupId, suffix: &[u8]) -> Option<bool> {
		let key = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix)?;
		PointTier::contains(
			self,
			TypedPartition {
				operator,
				group,
			},
			&key,
		)
	}

	fn begin_fill(&self, operator: OperatorId, group: GroupId, suffix: &[u8]) -> bool {
		let Some(key) = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix) else {
			return false;
		};
		PointTier::begin_fill(
			self,
			TypedPartition {
				operator,
				group,
			},
			&key,
		)
	}

	fn finish_fill(&self, operator: OperatorId, group: GroupId, suffix: &[u8], row: Option<EncodedPodRow>) -> bool {
		let Some(key) = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix) else {
			return false;
		};
		PointTier::finish_fill(
			self,
			TypedPartition {
				operator,
				group,
			},
			key,
			row,
		)
	}

	fn abort_fill(&self, operator: OperatorId, group: GroupId, suffix: &[u8]) {
		let Some(key) = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix) else {
			return;
		};
		PointTier::abort_fill(
			self,
			TypedPartition {
				operator,
				group,
			},
			&key,
		);
	}

	fn overwrite(&self, operator: OperatorId, group: GroupId, suffix: &[u8], row: EncodedPodRow) {
		let Some(key) = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix) else {
			return;
		};
		PointTier::overwrite(
			self,
			TypedPartition {
				operator,
				group,
			},
			key,
			row,
		);
	}

	fn invalidate(&self, operator: OperatorId, group: GroupId, suffix: &[u8]) {
		let Some(key) = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix) else {
			return;
		};
		PointTier::invalidate(
			self,
			TypedPartition {
				operator,
				group,
			},
			&key,
		);
	}

	fn invalidate_operator(&self, operator: OperatorId) {
		self.invalidate_dimensions_where(|dimension| dimension.operator == operator);
	}

	fn keyspace_metrics(&self) -> Option<OperatorPointKeyspaceMetrics> {
		self.bucket_metrics().into_iter().next().map(|row| OperatorPointKeyspaceMetrics {
			bucket: K::ID,
			used: row.used,
			limit: PointTier::shard_limit_bytes(self),
			entries: row.entries,
			counters: row.counters,
		})
	}

	fn entries(&self) -> usize {
		PointTier::entries(self)
	}

	fn resident_bytes(&self) -> ByteSize {
		PointTier::resident_bytes(self)
	}

	fn limit_bytes(&self) -> ByteSize {
		PointTier::shard_limit_bytes(self)
	}

	fn metrics(&self) -> PointMetrics {
		PointTier::metrics(self)
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}

struct Build(PointConfig);

impl KeyspaceVisitor for Build {
	type Output = Option<Box<dyn AnyPointTier>>;

	fn visit<K: Keyspace>(self) -> Self::Output {
		PointTier::<TypedDomain<K>>::new(self.0).map(|tier| Box::new(tier) as Box<dyn AnyPointTier>)
	}
}

#[derive(Clone)]
pub struct PointTiers {
	tiers: Arc<HashMap<KeyspaceId, Box<dyn AnyPointTier>>>,
	excluded: Arc<HashMap<KeyspaceId, AtomicU64>>,
}

impl PointTiers {
	pub fn new(config: PointConfig) -> Option<Self> {
		let mut tiers: HashMap<KeyspaceId, Box<dyn AnyPointTier>> = HashMap::new();
		let mut excluded: HashMap<KeyspaceId, AtomicU64> = HashMap::new();
		for spec in KEYSPACES {
			if !spec.cache.caches_points() {
				excluded.insert(spec.id, AtomicU64::new(0));
				continue;
			}
			let tier = dispatch(spec.id, Build(config)).expect("a catalogued keyspace must dispatch")?;
			tiers.insert(spec.id, tier);
		}
		Some(Self {
			tiers: Arc::new(tiers),
			excluded: Arc::new(excluded),
		})
	}

	fn charge_excluded(&self, keyspace: KeyspaceId) {
		if let Some(counter) = self.excluded.get(&keyspace) {
			counter.fetch_add(1, Ordering::Relaxed);
		}
	}

	fn excluded_misses(&self, keyspace: KeyspaceId) -> u64 {
		self.excluded.get(&keyspace).map(|counter| counter.load(Ordering::Relaxed)).unwrap_or(0)
	}

	pub fn of(&self, keyspace: KeyspaceId) -> Option<&dyn AnyPointTier> {
		self.tiers.get(&keyspace).map(|tier| tier.as_ref())
	}

	pub fn typed<K: Keyspace>(&self) -> Option<&PointTier<TypedDomain<K>>> {
		self.of(K::ID)?.as_any().downcast_ref()
	}

	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<Option<EncodedPodRow>> {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())?;
		let Some(tier) = self.of(keyspace) else {
			self.charge_excluded(keyspace);
			return None;
		};
		tier.get(operator, group, &suffix)
	}

	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> Option<bool> {
		let (group, keyspace, suffix) = OperatorStateKey::decode_inner(key.as_slice())?;
		let Some(tier) = self.of(keyspace) else {
			self.charge_excluded(keyspace);
			return None;
		};
		tier.contains(operator, group, &suffix)
	}

	pub fn begin_fill(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		let Some((group, keyspace, suffix)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			return false;
		};
		self.of(keyspace).is_some_and(|tier| tier.begin_fill(operator, group, &suffix))
	}

	pub fn finish_fill(&self, operator: OperatorId, key: &EncodedKey, row: Option<EncodedPodRow>) -> bool {
		let Some((group, keyspace, suffix)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			return false;
		};
		self.of(keyspace).is_some_and(|tier| tier.finish_fill(operator, group, &suffix, row))
	}

	pub fn abort_fill(&self, operator: OperatorId, key: &EncodedKey) {
		let Some((group, keyspace, suffix)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			return;
		};
		if let Some(tier) = self.of(keyspace) {
			tier.abort_fill(operator, group, &suffix);
		}
	}

	pub fn overwrite(&self, operator: OperatorId, key: &EncodedKey, row: EncodedPodRow) {
		let Some((group, keyspace, suffix)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			return;
		};
		if let Some(tier) = self.of(keyspace) {
			tier.overwrite(operator, group, &suffix, row);
		}
	}

	pub fn invalidate(&self, operator: OperatorId, key: &EncodedKey) {
		let Some((group, keyspace, suffix)) = OperatorStateKey::decode_inner(key.as_slice()) else {
			return;
		};
		if let Some(tier) = self.of(keyspace) {
			tier.invalidate(operator, group, &suffix);
		}
	}

	pub fn invalidate_operator(&self, operator: OperatorId) {
		for tier in self.tiers.values() {
			tier.invalidate_operator(operator);
		}
	}

	pub fn keyspace_metrics(&self) -> Vec<OperatorPointKeyspaceMetrics> {
		let mut out: Vec<OperatorPointKeyspaceMetrics> =
			self.tiers.values().filter_map(|tier| tier.keyspace_metrics()).collect();
		for keyspace in self.excluded.keys() {
			out.push(OperatorPointKeyspaceMetrics {
				bucket: *keyspace,
				used: ByteSize::ZERO,
				limit: ByteSize::ZERO,
				entries: 0,
				counters: PointMetrics {
					misses: self.excluded_misses(*keyspace),
					..PointMetrics::default()
				},
			});
		}
		out.sort_by_key(|row| row.bucket.0);
		out
	}

	pub fn entries(&self) -> usize {
		self.tiers.values().map(|tier| tier.entries()).sum()
	}

	pub fn resident_bytes(&self) -> ByteSize {
		ByteSize::from_bytes(self.tiers.values().map(|tier| tier.resident_bytes().as_bytes()).sum())
	}

	pub fn metrics(&self) -> PointMetrics {
		let mut totals = self.tiers.values().fold(PointMetrics::default(), |mut total, tier| {
			let counters = tier.metrics();
			total.hits += counters.hits;
			total.misses += counters.misses;
			total.insertions += counters.insertions;
			total.evictions += counters.evictions;
			total.fills_started += counters.fills_started;
			total.fills_dirty_aborted += counters.fills_dirty_aborted;
			total.fills_duplicate += counters.fills_duplicate;
			total
		});
		totals.misses += self.excluded.values().map(|counter| counter.load(Ordering::Relaxed)).sum::<u64>();
		totals
	}

	pub fn hits(&self) -> u64 {
		self.metrics().hits
	}

	pub fn misses(&self) -> u64 {
		self.metrics().misses
	}

	pub fn evictions(&self) -> u64 {
		self.metrics().evictions
	}
}

impl MetricsCollector for PointTiers {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		const SCOPE: &str = "operator_point";
		let counters = self.metrics();
		let limit: u64 = self.tiers.values().map(|tier| tier.limit_bytes().as_bytes()).sum();

		out.push(MetricsSample::heap(SCOPE, "resident_bytes", self.resident_bytes()));
		out.push(MetricsSample::count(SCOPE, "resident_entries", self.entries() as u64));
		out.push(MetricsSample::counter(SCOPE, "hits", counters.hits));
		out.push(MetricsSample::counter(SCOPE, "misses", counters.misses));
		out.push(MetricsSample::counter(SCOPE, "insertions", counters.insertions));
		out.push(MetricsSample::counter(SCOPE, "evictions", counters.evictions));
		out.push(MetricsSample::counter(SCOPE, "fills_started", counters.fills_started));
		out.push(MetricsSample::counter(SCOPE, "fills_dirty_aborted", counters.fills_dirty_aborted));
		out.push(MetricsSample::counter(SCOPE, "fills_duplicate", counters.fills_duplicate));
		out.push(MetricsSample::bytes(SCOPE, "tier_limit_bytes", ByteSize::from_bytes(limit)));

		for keyspace in self.keyspace_metrics() {
			let scope = format!("{SCOPE}::keyspace::{}", keyspace.bucket.name());
			out.push(MetricsSample::bytes(scope.clone(), "used_bytes", keyspace.used));
			out.push(MetricsSample::count(scope.clone(), "entries", keyspace.entries as u64));
			out.push(MetricsSample::counter(scope.clone(), "hits", keyspace.counters.hits));
			out.push(MetricsSample::counter(scope, "misses", keyspace.counters.misses));
		}
	}
}
