// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(test)]
mod scan;
#[cfg(test)]
mod surface;

use std::{borrow::Cow, sync::LazyLock};

use reifydb_codec::{
	key::{decode_u64, encode_u8, encoded::EncodedKey},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey},
};
use reifydb_store::{
	coverage::ExclusiveUpperEnd,
	tier::range::{
		RangeConfig, RangeDomain, RangeMetrics, RangeShardMetrics, RangeSlotMetrics, RangeTier,
		prefix_successor,
	},
};

pub type OperatorRangeConfig = RangeConfig;
pub type OperatorRangeTier = RangeTier<OperatorDomain>;
pub type OperatorRangeMetrics = RangeMetrics;
pub type OperatorRangeShardMetrics = RangeShardMetrics;
pub type OperatorRangeKeyspaceMetrics = RangeSlotMetrics<OperatorDomain>;
pub type RangeScan = reifydb_store::tier::range::RangeScan<OperatorDomain>;

#[derive(Clone, Copy, Debug)]
pub struct OperatorDomain;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PartitionId {
	pub operator: OperatorId,
	pub group: GroupId,
	pub keyspace: Keyspace,
}

impl PartitionId {
	pub const PREFIX_LEN: usize = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize + 1;

	pub fn of(operator: OperatorId, key: &EncodedKey) -> Option<Self> {
		let bytes = key.as_slice();
		let offset = OperatorStateKey::KEYSPACE_INNER_OFFSET as usize;
		if bytes.len() <= offset {
			return None;
		}
		let group = GroupId(decode_u64(bytes[..offset].try_into().ok()?));
		Some(Self {
			operator,
			group,
			keyspace: Keyspace(encode_u8(bytes[offset])),
		})
	}

	pub fn prefix(&self) -> EncodedKey {
		EncodedKey::new(OperatorStateKey::inner_encoded(self.group, self.keyspace, [0u8; 0]).as_bytes())
	}

	pub fn span(&self) -> (EncodedKey, ExclusiveUpperEnd) {
		let start = self.prefix();
		let end = match prefix_successor(start.as_slice()) {
			Some(successor) => ExclusiveUpperEnd::of(successor),
			None => ExclusiveUpperEnd::Top,
		};
		(start, end)
	}

	pub fn caches_ranges(&self) -> bool {
		self.keyspace.cache_policy().caches_ranges()
	}
}

static POLICY_RUN_FLOOR: LazyLock<[u8; 256]> = LazyLock::new(|| {
	let mut floor = [0u8; 256];
	let mut lowest = 0u8;
	for keyspace in 0..=u8::MAX {
		if keyspace > 0
			&& Keyspace(keyspace).cache_policy().caches_ranges()
				!= Keyspace(keyspace - 1).cache_policy().caches_ranges()
		{
			lowest = keyspace;
		}
		floor[keyspace as usize] = lowest;
	}
	floor
});

impl RangeDomain for OperatorDomain {
	type Dimension = OperatorId;
	type Partition = PartitionId;
	type Slot = Keyspace;
	type Row = EncodedPodRow;

	const PREFIX_LEN: usize = PartitionId::PREFIX_LEN;
	const SLOTS: usize = 256;

	const SCOPE: &'static str = "operator_range";

	const GAP_SCOPE: &'static str = "operator_range::gaps";

	fn partition(dimension: Self::Dimension, key: &EncodedKey) -> Option<Self::Partition> {
		PartitionId::of(dimension, key)
	}

	fn dimension(partition: &Self::Partition) -> Self::Dimension {
		partition.operator
	}

	fn span(partition: &Self::Partition) -> (EncodedKey, ExclusiveUpperEnd) {
		partition.span()
	}

	fn head_band(_dimension: Self::Dimension) -> Option<(EncodedKey, EncodedKey)> {
		None
	}

	fn caches_ranges(partition: &Self::Partition) -> bool {
		partition.caches_ranges()
	}

	fn policy_run_end(partition: &Self::Partition) -> ExclusiveUpperEnd {
		let floor = POLICY_RUN_FLOOR[partition.keyspace.0 as usize];
		if floor == partition.keyspace.0 {
			return partition.span().1;
		}
		PartitionId {
			keyspace: Keyspace(floor),
			..*partition
		}
		.span()
		.1
	}

	fn slot(partition: &Self::Partition) -> usize {
		partition.keyspace.0 as usize
	}

	fn slot_at(index: usize) -> Self::Slot {
		Keyspace(index as u8)
	}

	fn slot_name(slot: Self::Slot) -> Cow<'static, str> {
		slot.name()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::operator_state::{GroupId, Keyspace, OperatorStateKey, keyspace_inner_range},
	};
	use reifydb_store::{
		coverage::{
			cursor::{RangeCursor, ServedChunk},
			plan::Segment,
		},
		tier::range::Materialize,
	};

	use super::{OperatorRangeConfig, OperatorRangeTier};

	const OP_A: OperatorId = OperatorId(1);
	const OP_B: OperatorId = OperatorId(2);
	const GROUP_A: GroupId = GroupId(10);

	fn tier() -> OperatorRangeTier {
		OperatorRangeTier::new(OperatorRangeConfig::testing())
			.expect("a tier with a byte budget must be constructed")
	}

	fn key(keyspace: Keyspace, suffix: &[u8]) -> EncodedKey {
		OperatorStateKey::inner_encoded(GROUP_A, keyspace, suffix).into_encoded()
	}

	fn row(body: &str) -> EncodedPodRow {
		EncodedPodRow::new(body.as_bytes())
	}

	fn claim(
		tier: &OperatorRangeTier,
		operator: OperatorId,
		keyspace: Keyspace,
		page: &[(EncodedKey, EncodedPodRow)],
	) {
		let range = keyspace_inner_range(GROUP_A, keyspace);
		let scan = tier.plan_scan(operator, &range).expect("a whole-keyspace range must be plannable");
		let gap = scan
			.segments()
			.iter()
			.find_map(|segment| match segment {
				Segment::Gap {
					interval,
					..
				} => Some(interval.clone()),
				Segment::Resident(_) => None,
			})
			.expect("an uncovered keyspace must plan as a gap the fixture can materialize over");
		assert_eq!(tier.materialize(&scan, &gap, page), Materialize::Materialized);
	}

	fn serve_ram(tier: &OperatorRangeTier, operator: OperatorId, keyspace: Keyspace) -> Option<Vec<EncodedKey>> {
		let range = keyspace_inner_range(GROUP_A, keyspace);
		let scan = tier.plan_scan(operator, &range)?;
		let mut out: Vec<EncodedKey> = Vec::new();
		let mut resident = false;
		for segment in scan.segments() {
			let Segment::Resident(interval) = segment else {
				continue;
			};
			resident = true;
			let mut cursor = RangeCursor::new();
			while !cursor.is_exhausted() {
				match tier.serve(&scan, interval, &mut cursor, 64) {
					ServedChunk::Served(rows) => out.extend(rows.into_iter().map(|(key, _)| key)),
					ServedChunk::Gap => break,
				}
			}
		}
		resident.then_some(out)
	}

	#[test]
	fn a_key_that_names_no_keyspace_is_declined_by_the_operator_mapping() {
		// A key too short to name a partition must never go resident; the tier cannot place its claim.
		let tier = tier();
		let short = EncodedKey::new(b"short");

		tier.insert(OP_A, short.clone(), row("v"));

		assert_eq!(tier.entries(), 0);
		assert_eq!(tier.lookup(OP_A, &short), None);
	}

	#[test]
	fn a_keyspace_the_operator_policy_never_caches_is_declined() {
		// The cache policy must reach the tier here, or a never-cached keyspace goes resident.
		let tier = tier();
		let at = key(Keyspace::CUSTOM_NOT_CACHED, b"a");

		tier.insert(OP_A, at.clone(), row("v"));

		assert_eq!(tier.entries(), 0);
		assert_eq!(tier.lookup(OP_A, &at), None);
		assert!(tier.plan_scan(OP_A, &keyspace_inner_range(GROUP_A, Keyspace::CUSTOM_NOT_CACHED)).is_none());
	}

	#[test]
	fn a_claim_and_a_serve_round_trip_for_the_operator_that_made_it() {
		// The operator must reach the coverage index, or one operator's claim answers another operator's read.
		let tier = tier();
		let at = key(Keyspace::ACCUMULATOR, b"a");
		claim(&tier, OP_A, Keyspace::ACCUMULATOR, &[(at.clone(), row("v"))]);

		assert_eq!(serve_ram(&tier, OP_A, Keyspace::ACCUMULATOR), Some(vec![at.clone()]));
		assert_eq!(tier.lookup(OP_A, &at), Some(Some(row("v"))));

		assert_eq!(serve_ram(&tier, OP_B, Keyspace::ACCUMULATOR), None);
		assert_eq!(tier.lookup(OP_B, &at), None);
	}

	#[test]
	fn invalidating_an_operator_withdraws_the_claim_it_made() {
		// A claim that outlives its operator answers absences for rows the persistent tier still holds.
		let tier = tier();
		let at = key(Keyspace::ACCUMULATOR, b"a");
		claim(&tier, OP_A, Keyspace::ACCUMULATOR, &[(at.clone(), row("v"))]);
		assert_eq!(tier.lookup(OP_A, &at), Some(Some(row("v"))));

		tier.invalidate_operator(OP_A);

		assert_eq!(tier.entries(), 0);
		assert_eq!(tier.intervals(), 0);
		assert_eq!(tier.lookup(OP_A, &at), None);
		assert_eq!(serve_ram(&tier, OP_A, Keyspace::ACCUMULATOR), None);
	}
}
