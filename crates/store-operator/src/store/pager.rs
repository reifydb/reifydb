// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			keyspace::KeyspaceVisitor,
			state::{GroupId, OperatorStateKey, keyspace_inner_range},
			traits::Keyspace,
		},
		typed::{ExclusiveUpperEnd, Key, range::KeyRange},
	},
	state::typed::SuffixBytes,
};
use reifydb_store::{
	coverage::{
		cursor::{Cursor, ServedChunk},
		interval::Interval,
		plan::Segment,
	},
	tier::range::{Materialize, RangeScan, RangeTier, proven_span},
};

use crate::{
	tier::{
		persistent::OperatorPersistentTier,
		range::{
			tiers::RangeTiers,
			typed::{TypedDomain, TypedPartition},
		},
	},
	types::OperatorBatch,
};

pub(crate) type Page = Vec<(EncodedKey, EncodedPodRow)>;

pub(crate) trait PageSource {
	fn next_page(&mut self, limit: u64) -> Page;

	fn is_exhausted(&self) -> bool;
}

pub(crate) struct PersistentPager<'a> {
	operator: OperatorId,
	persistent: Option<&'a OperatorPersistentTier>,
	lower: Bound<EncodedKey>,
	end: Bound<EncodedKey>,
	exhausted: bool,
}

impl<'a> PersistentPager<'a> {
	pub(crate) fn new(
		operator: OperatorId,
		persistent: Option<&'a OperatorPersistentTier>,
		range: &EncodedKeyRange,
	) -> Self {
		Self {
			operator,
			persistent,
			lower: range.start.clone(),
			end: range.end.clone(),
			exhausted: persistent.is_none(),
		}
	}
}

impl PageSource for PersistentPager<'_> {
	fn next_page(&mut self, limit: u64) -> Page {
		let Some(persistent) = self.persistent else {
			self.exhausted = true;
			return Vec::new();
		};
		let batch = persistent.range_batch(
			self.operator,
			EncodedKeyRange::new(self.lower.clone(), self.end.clone()),
			limit,
		);
		self.exhausted = !batch.has_more || batch.items.is_empty();
		if let Some((key, _)) = batch.items.last() {
			self.lower = Bound::Excluded(key.clone());
		}
		batch.items
	}

	fn is_exhausted(&self) -> bool {
		self.exhausted
	}
}

pub(crate) struct TierPager<'a, K: Keyspace> {
	operator: OperatorId,
	group: GroupId,
	tier: &'a RangeTier<TypedDomain<K>>,
	persistent: Option<&'a OperatorPersistentTier>,
	scan: RangeScan<TypedDomain<K>>,
	segment_index: usize,
	cursor: Cursor<(), K::Suffix>,
	pending: Option<(Interval<K::Suffix>, bool, usize)>,
	claim_start: Option<K::Suffix>,
	materializing: bool,
	exhausted: bool,
}

impl<'a, K: Keyspace> TierPager<'a, K> {
	pub(crate) fn new(
		operator: OperatorId,
		group: GroupId,
		tier: &'a RangeTier<TypedDomain<K>>,
		persistent: Option<&'a OperatorPersistentTier>,
		scan: RangeScan<TypedDomain<K>>,
	) -> Self {
		Self {
			operator,
			group,
			tier,
			persistent,
			scan,
			segment_index: 0,
			cursor: Cursor::new(),
			pending: None,
			claim_start: None,
			materializing: true,
			exhausted: false,
		}
	}

	fn encode(&self, suffix: &K::Suffix) -> EncodedKey {
		OperatorStateKey::inner_encoded(self.group, K::ID, suffix.to_suffix_bytes()).into_encoded()
	}

	fn read_range(&self, interval: &Interval<K::Suffix>) -> EncodedKeyRange {
		let start = match self.cursor.last_key() {
			Some(last) => Bound::Excluded(self.encode(last)),
			None => Bound::Included(self.encode(&interval.start)),
		};
		let end = match &interval.end {
			ExclusiveUpperEnd::Key(key) => Bound::Excluded(self.encode(key)),
			ExclusiveUpperEnd::Top => keyspace_inner_range(self.group, K::ID).end,
		};
		EncodedKeyRange::new(start, end)
	}

	fn decode_rows(&self, rows: &Page) -> Vec<(K::Suffix, EncodedPodRow)> {
		rows.iter()
			.map(|(key, row)| {
				let (_, _, suffix) = OperatorStateKey::decode_inner(key.as_slice())
					.expect("a row read from the keyspace must carry a decodable inner key");
				let suffix = <K::Suffix as SuffixBytes>::from_suffix_bytes(&suffix)
					.expect("a stored suffix must match the width its keyspace declares");
				(suffix, row.clone())
			})
			.collect()
	}
}

impl<K: Keyspace> PageSource for TierPager<'_, K> {
	fn next_page(&mut self, limit: u64) -> Page {
		loop {
			if let Some((interval, materializable, consumed)) = self.pending.take() {
				let Some(persistent) = self.persistent else {
					self.exhausted = true;
					return Vec::new();
				};
				let batch: OperatorBatch =
					persistent.range_batch(self.operator, self.read_range(&interval), limit);
				let complete = !batch.has_more || batch.items.is_empty();
				let typed = self.decode_rows(&batch.items);

				if materializable && self.materializing {
					let start = self.claim_start.clone().unwrap_or_else(|| interval.start.clone());
					let span = Interval::new(start, interval.end.clone());
					let last = typed.last().map(|(key, _)| key);
					if let Some(proven) = proven_span(&span, last, complete) {
						match self.tier.materialize(&self.scan, &proven, &typed) {
							Materialize::Materialized | Materialize::NothingCacheable => {
								self.claim_start = typed
									.last()
									.and_then(|(key, _)| key.successor());
							}
							Materialize::Refused => self.materializing = false,
						}
					}
				}

				if let Some((key, _)) = typed.last() {
					self.cursor.advance(key.clone());
				}
				if complete {
					self.segment_index += consumed;
					self.cursor.reset();
					self.claim_start = None;
				} else {
					self.pending = Some((interval, materializable, consumed));
				}

				if batch.items.is_empty() {
					continue;
				}
				return batch.items;
			}

			let Some(segment) = self.scan.segments().get(self.segment_index) else {
				self.exhausted = true;
				return Vec::new();
			};
			match segment {
				Segment::Resident(interval) => {
					let interval = interval.clone();
					match self.tier.serve(&self.scan, &interval, &mut self.cursor, limit as usize) {
						ServedChunk::Served(rows) => {
							let done = self.cursor.is_exhausted();
							assert!(
								done || !rows.is_empty(),
								"a served chunk that reports more work must carry a row, or the cursor never advances"
							);
							if done {
								self.segment_index += 1;
								self.cursor.reset();
							}
							if rows.is_empty() {
								continue;
							}
							return rows
								.into_iter()
								.map(|(suffix, row)| (self.encode(&suffix), row))
								.collect();
						}
						ServedChunk::Gap => {
							self.pending = Some((interval, false, 1));
						}
					}
				}
				Segment::Gap {
					interval,
					..
				} => {
					let mut span = interval.clone();
					let mut consumed = 1usize;
					while let Some(Segment::Gap {
						interval: next,
						..
					}) = self.scan.segments().get(self.segment_index + consumed)
					{
						if span.end != ExclusiveUpperEnd::Key(next.start.clone()) {
							break;
						}
						span.end = next.end.clone();
						consumed += 1;
					}
					self.pending = Some((span, true, consumed));
				}
			}
		}
	}

	fn is_exhausted(&self) -> bool {
		self.exhausted
	}
}

pub(crate) struct ExhaustedPager;

impl PageSource for ExhaustedPager {
	fn next_page(&mut self, _limit: u64) -> Page {
		Vec::new()
	}

	fn is_exhausted(&self) -> bool {
		true
	}
}

pub(crate) struct PlanScan<'a> {
	pub(crate) tiers: &'a RangeTiers,
	pub(crate) operator: OperatorId,
	pub(crate) group: GroupId,
	pub(crate) persistent: Option<&'a OperatorPersistentTier>,
	pub(crate) start: Bound<Vec<u8>>,
	pub(crate) end: Bound<Vec<u8>>,
}

impl<'a> KeyspaceVisitor for PlanScan<'a> {
	type Output = Option<Box<dyn PageSource + 'a>>;

	fn visit<K: Keyspace>(self) -> Self::Output {
		let tier = self.tiers.typed::<K>()?;
		let range = KeyRange::new(bound::<K>(self.start)?, bound::<K>(self.end)?);
		let partition = TypedPartition {
			operator: self.operator,
			group: self.group,
		};
		let scan = tier.plan_scan_in(partition, partition, &range)?;
		Some(Box::new(TierPager::new(self.operator, self.group, tier, self.persistent, scan)))
	}
}

fn bound<K: Keyspace>(source: Bound<Vec<u8>>) -> Option<Bound<K::Suffix>> {
	match source {
		Bound::Unbounded => Some(Bound::Unbounded),
		Bound::Included(bytes) if bytes.is_empty() => Some(Bound::Included(K::Suffix::low())),
		Bound::Included(bytes) => <K::Suffix as SuffixBytes>::from_suffix_bytes(&bytes).map(Bound::Included),
		Bound::Excluded(bytes) => <K::Suffix as SuffixBytes>::from_suffix_bytes(&bytes).map(Bound::Excluded),
	}
}
