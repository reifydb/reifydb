// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp::Reverse, ops::Bound};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			keyspace::{KEYSPACES, KeyspaceVisitor, dispatch},
			state::{GroupId, KeyspaceId, OperatorStateKey, group_inner_range, keyspace_inner_range},
			traits::{Keyspace, group_scoped},
		},
		typed::{BoundedKey, Edge, range::KeyRange},
	},
	state::typed::SuffixBytes,
};
use reifydb_store::{
	coverage::{
		cursor::{Cursor, ServedChunk},
		interval::Interval,
		plan::Segment,
	},
	tier::range::{Materialize, RangeDomain, RangeScan, RangeTier, proven_span},
};

use crate::{
	error::Result,
	persistent::{Page as PersistentPage, Persistent, PersistentTier},
	range::{TypedPartition, tiers::RangeTiers, typed::TypedDomain},
	store::occupancy::occupies,
	types::OperatorBatch,
};

pub(crate) type Page = Vec<(EncodedKey, EncodedPodRow)>;

pub(crate) trait PageSource {
	fn next_page(&mut self, limit: u64) -> Result<Page>;

	fn is_exhausted(&self) -> bool;

	fn ceiling(&self) -> Option<&EncodedKey> {
		None
	}
}

pub(crate) struct PersistentPager<'a> {
	operator: OperatorId,
	persistent: &'a PersistentTier,
	lower: Bound<EncodedKey>,
	end: Bound<EncodedKey>,
	exhausted: bool,
}

impl<'a> PersistentPager<'a> {
	pub(crate) fn new(operator: OperatorId, persistent: &'a PersistentTier, range: &EncodedKeyRange) -> Self {
		Self {
			operator,
			persistent,
			lower: range.start.clone(),
			end: range.end.clone(),
			exhausted: persistent.is_absent(),
		}
	}
}

fn decoded(batch: OperatorBatch) -> Vec<(EncodedKey, EncodedPodRow)> {
	batch.items.into_iter().map(|(key, row)| (key.into_encoded(), row)).collect()
}

impl PageSource for PersistentPager<'_> {
	fn next_page(&mut self, limit: u64) -> Result<Page> {
		if self.persistent.is_absent() {
			self.exhausted = true;
			return Ok(Vec::new());
		}
		let batch = self.persistent.range_batch(
			self.operator,
			EncodedKeyRange::new(self.lower.clone(), self.end.clone()),
			limit,
			u64::MAX,
		)?;
		self.exhausted = !batch.has_more || batch.items.is_empty();
		if let Some((key, _)) = batch.items.last() {
			self.lower = Bound::Excluded(key.as_encoded().clone());
		}
		Ok(decoded(batch))
	}

	fn is_exhausted(&self) -> bool {
		self.exhausted
	}
}

pub(crate) struct GroupPager<'a> {
	operator: OperatorId,
	persistent: &'a PersistentTier,
	groups: &'a [GroupId],
	mask: u64,
	exhausted: bool,
	ceiling: Option<EncodedKey>,
	served: usize,
	fetch: u64,
}

impl<'a> GroupPager<'a> {
	pub(crate) fn new(
		operator: OperatorId,
		persistent: &'a PersistentTier,
		groups: &'a [GroupId],
		mask: u64,
		dropped: bool,
	) -> Self {
		Self {
			operator,
			persistent,
			groups,
			mask,
			exhausted: dropped || persistent.is_absent() || groups.is_empty(),
			ceiling: None,
			served: 0,
			fetch: 0,
		}
	}
}

impl PageSource for GroupPager<'_> {
	fn ceiling(&self) -> Option<&EncodedKey> {
		self.ceiling.as_ref()
	}

	fn next_page(&mut self, limit: u64) -> Result<Page> {
		if self.exhausted || self.persistent.is_absent() {
			self.exhausted = true;
			return Ok(Vec::new());
		}
		self.fetch = match self.fetch {
			0 => limit.max(1),
			current => current.saturating_mul(2),
		};
		let batch = self.persistent.group_page(self.operator, self.groups, self.fetch, self.mask)?;
		match batch.has_more {
			true => self.ceiling = batch.items.last().map(|(key, _)| key.as_encoded().clone()),
			false => {
				self.exhausted = true;
				self.ceiling = None;
			}
		}
		let mut items = decoded(batch);
		let taken = self.served.min(items.len());
		let fresh = items.split_off(taken);
		if fresh.is_empty() {
			self.exhausted = true;
			self.ceiling = None;
			return Ok(Vec::new());
		}
		self.served += fresh.len();
		Ok(fresh)
	}

	fn is_exhausted(&self) -> bool {
		self.exhausted
	}
}

pub(crate) struct TierPager<'a, K: Keyspace> {
	operator: OperatorId,
	group: GroupId,
	tier: &'a RangeTier<TypedDomain<K>>,
	persistent: &'a PersistentTier,
	scan: RangeScan<TypedDomain<K>>,
	segment_index: usize,
	cursor: Cursor<(), K::Suffix>,
	pending: Option<(Interval<K::Suffix>, bool, usize)>,
	claim_start: Option<Edge<K::Suffix>>,
	materializing: bool,
	exhausted: bool,
}

impl<'a, K: Keyspace> TierPager<'a, K> {
	pub(crate) fn new(
		operator: OperatorId,
		group: GroupId,
		tier: &'a RangeTier<TypedDomain<K>>,
		persistent: &'a PersistentTier,
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
		let whole = keyspace_inner_range(self.group, K::ID);
		let start = match self.cursor.last_key() {
			Some(last) => Bound::Excluded(self.encode(last)),
			None => match interval.start.lower_bound() {
				Some(Bound::Included(key)) => Bound::Included(self.encode(&key)),
				Some(Bound::Excluded(key)) => Bound::Excluded(self.encode(&key)),
				Some(Bound::Unbounded) => whole.start.clone(),
				None => return EncodedKeyRange::new(whole.start.clone(), whole.start),
			},
		};
		let end = match interval.end.upper_bound() {
			Some(Bound::Included(key)) => Bound::Included(self.encode(&key)),
			Some(Bound::Excluded(key)) => Bound::Excluded(self.encode(&key)),
			Some(Bound::Unbounded) => whole.end,
			None => return EncodedKeyRange::new(whole.start.clone(), whole.start),
		};
		EncodedKeyRange::new(start, end)
	}

	fn decode_rows(&self, rows: &Page) -> Vec<(K::Suffix, EncodedPodRow)> {
		rows.iter()
			.map(|(key, row)| {
				let (_, _, suffix) = OperatorStateKey::decode_inner(key.as_slice())
					.expect("a row read from the keyspace must carry a decodable inner key");
				let suffix = <K::Suffix as SuffixBytes>::from_suffix_bytes(suffix)
					.expect("a stored suffix must match the width its keyspace declares");
				(suffix, row.clone())
			})
			.collect()
	}
}

impl<K: Keyspace> PageSource for TierPager<'_, K> {
	fn next_page(&mut self, limit: u64) -> Result<Page> {
		loop {
			if let Some((interval, materializable, consumed)) = self.pending.take() {
				if self.persistent.is_absent() {
					self.exhausted = true;
					return Ok(Vec::new());
				}
				let batch: OperatorBatch = self.persistent.range_batch(
					self.operator,
					self.read_range(&interval),
					limit,
					u64::MAX,
				)?;
				let complete = !batch.has_more || batch.items.is_empty();
				let items = decoded(batch);
				let typed = self.decode_rows(&items);

				if materializable && self.materializing {
					let start = self.claim_start.clone().unwrap_or_else(|| interval.start.clone());
					let span = Interval::new(start, interval.end.clone());
					let last = typed.last().map(|(key, _)| key);
					if let Some(proven) = proven_span::<TypedDomain<K>>(&span, last, complete) {
						match self.tier.materialize(&self.scan, &proven, &typed) {
							Materialize::Materialized | Materialize::NothingCacheable => {
								self.claim_start = typed.last().map(|(key, _)| {
									TypedDomain::<K>::just_past(key)
								});
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

				if items.is_empty() {
					continue;
				}
				return Ok(items);
			}

			let Some(segment) = self.scan.segments().get(self.segment_index) else {
				self.exhausted = true;
				return Ok(Vec::new());
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
							return Ok(rows
								.into_iter()
								.map(|(suffix, row)| (self.encode(&suffix), row))
								.collect());
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
						if span.end != next.start {
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

struct GroupScoped;

impl KeyspaceVisitor for GroupScoped {
	type Output = bool;

	fn visit<K: Keyspace>(self) -> Self::Output {
		const { group_scoped::<K>() }
	}
}

fn within(range: &EncodedKeyRange, key: &EncodedKey) -> bool {
	let after_start = match &range.start {
		Bound::Unbounded => true,
		Bound::Included(start) => key.as_slice() >= start.as_slice(),
		Bound::Excluded(start) => key.as_slice() > start.as_slice(),
	};
	let before_end = match &range.end {
		Bound::Unbounded => true,
		Bound::Included(end) => key.as_slice() <= end.as_slice(),
		Bound::Excluded(end) => key.as_slice() < end.as_slice(),
	};
	after_start && before_end
}

pub(crate) fn keyspaces_of(group: GroupId, range: &EncodedKeyRange, occupied: u64) -> Vec<KeyspaceId> {
	let mut ids: Vec<KeyspaceId> = KEYSPACES
		.iter()
		.map(|spec| spec.id)
		.filter(|id| occupies(occupied, *id))
		.filter(|id| group.is_root() || dispatch(*id, GroupScoped).unwrap_or(false))
		.filter(|id| match keyspace_inner_range(group, *id).start {
			Bound::Included(start) | Bound::Excluded(start) => within(range, &start),
			Bound::Unbounded => false,
		})
		.collect();
	ids.sort_by_key(|id| Reverse(id.0));
	ids
}

pub(crate) struct GroupKeyspacePager<'a> {
	tiers: &'a RangeTiers,
	operator: OperatorId,
	group: GroupId,
	persistent: &'a PersistentTier,
	keyspaces: Vec<KeyspaceId>,
	at: usize,
	current: Option<Box<dyn PageSource + 'a>>,
}

impl<'a> GroupKeyspacePager<'a> {
	pub(crate) fn new(
		tiers: &'a RangeTiers,
		operator: OperatorId,
		group: GroupId,
		persistent: &'a PersistentTier,
		keyspaces: Vec<KeyspaceId>,
	) -> Self {
		Self {
			tiers,
			operator,
			group,
			persistent,
			keyspaces,
			at: 0,
			current: None,
		}
	}

	fn source_of(&self, keyspace: KeyspaceId) -> Box<dyn PageSource + 'a> {
		dispatch(
			keyspace,
			PlanScan {
				tiers: self.tiers,
				operator: self.operator,
				group: self.group,
				persistent: self.persistent,
				start: Bound::Included(Vec::new()),
				end: Bound::Unbounded,
			},
		)
		.flatten()
		.unwrap_or_else(|| {
			Box::new(PersistentPager::new(
				self.operator,
				self.persistent,
				&keyspace_inner_range(self.group, keyspace),
			))
		})
	}
}

impl PageSource for GroupKeyspacePager<'_> {
	fn next_page(&mut self, limit: u64) -> Result<Page> {
		loop {
			let Some(source) = self.current.as_mut() else {
				let Some(keyspace) = self.keyspaces.get(self.at).copied() else {
					return Ok(Vec::new());
				};
				self.at += 1;
				self.current = Some(self.source_of(keyspace));
				continue;
			};
			if source.is_exhausted() {
				self.current = None;
				continue;
			}
			let page = source.next_page(limit)?;
			if page.is_empty() {
				continue;
			}
			return Ok(page);
		}
	}

	fn is_exhausted(&self) -> bool {
		self.at == self.keyspaces.len() && self.current.as_ref().is_none_or(|source| source.is_exhausted())
	}
}

pub(crate) struct GroupsPager<'a> {
	tiers: &'a RangeTiers,
	operator: OperatorId,
	persistent: &'a PersistentTier,
	groups: Vec<(GroupId, Vec<KeyspaceId>)>,
	at: usize,
	current: Option<GroupKeyspacePager<'a>>,
	exhausted: bool,
}

impl<'a> GroupsPager<'a> {
	pub(crate) fn new(
		tiers: &'a RangeTiers,
		operator: OperatorId,
		persistent: &'a PersistentTier,
		groups: &[GroupId],
		occupied: u64,
		dropped: bool,
	) -> Self {
		let groups: Vec<(GroupId, Vec<KeyspaceId>)> = groups
			.iter()
			.map(|group| (*group, keyspaces_of(*group, &group_inner_range(*group), occupied)))
			.collect();
		let exhausted = dropped || groups.is_empty();
		Self {
			tiers,
			operator,
			persistent,
			groups,
			at: 0,
			current: None,
			exhausted,
		}
	}
}

impl PageSource for GroupsPager<'_> {
	fn next_page(&mut self, limit: u64) -> Result<Page> {
		loop {
			if self.exhausted {
				return Ok(Vec::new());
			}
			let Some(source) = self.current.as_mut() else {
				let Some((group, keyspaces)) = self.groups.get(self.at) else {
					self.exhausted = true;
					return Ok(Vec::new());
				};
				self.at += 1;
				self.current = Some(GroupKeyspacePager::new(
					self.tiers,
					self.operator,
					*group,
					self.persistent,
					keyspaces.clone(),
				));
				continue;
			};
			let page = source.next_page(limit)?;
			if page.is_empty() {
				self.current = None;
				continue;
			}
			return Ok(page);
		}
	}

	fn is_exhausted(&self) -> bool {
		self.exhausted
	}
}

pub(crate) struct ExhaustedPager;

impl PageSource for ExhaustedPager {
	fn next_page(&mut self, _limit: u64) -> Result<Page> {
		Ok(Vec::new())
	}

	fn is_exhausted(&self) -> bool {
		true
	}
}

pub(crate) struct PlanScan<'a> {
	pub(crate) tiers: &'a RangeTiers,
	pub(crate) operator: OperatorId,
	pub(crate) group: GroupId,
	pub(crate) persistent: &'a PersistentTier,
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
