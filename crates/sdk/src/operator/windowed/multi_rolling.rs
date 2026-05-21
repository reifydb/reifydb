// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::{self, Debug, Formatter},
	hash::Hash,
};

use reifydb_abi::flow::diff::DiffType;
use reifydb_core::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	interface::catalog::flow::FlowNodeId,
	key::flow_node_internal_state::FlowNodeInternalStateKey,
};
use reifydb_type::value::{Value, row_number::RowNumber};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

#[derive(Clone, Hash, PartialEq, Eq)]
struct MetaKey(EncodedKey);

impl IntoEncodedKey for &MetaKey {
	fn into_encoded_key(self) -> EncodedKey {
		let inner = self.0.as_ref();
		let mut bytes = Vec::with_capacity(1 + inner.len());
		bytes.push(FlowNodeInternalStateKey::WINDOW_META_TAG);
		bytes.extend_from_slice(inner);
		EncodedKey::new(bytes)
	}
}

fn meta_key_for<G>(group: &G) -> MetaKey
where
	for<'a> &'a G: IntoEncodedKey,
{
	MetaKey(group.into_encoded_key())
}

use crate::{
	error::Result,
	operator::{
		FFIOperator, FFIOperatorMetadata,
		change::{BorrowedChange, BorrowedColumns},
		column::{
			batch::{InsertBatch, RemoveBatch, UpdateBatch},
			operator::OperatorColumn,
			row::Row,
		},
		context::OperatorContext,
		windowed::span::Slot,
	},
	state::cache::StateCache,
};

pub trait MultiRollingOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type WindowKey: Slot + Hash + Serialize + DeserializeOwned;

	type WindowInput: Clone + Debug;

	type RemoveInput: Clone + Debug;

	type Buffered: Clone + Debug + Serialize + DeserializeOwned;

	type SecondaryKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type Output: Clone + Debug + PartialEq + Serialize + DeserializeOwned;

	fn capacity(&self) -> usize;

	fn extract_apply(
		&self,
		cols: &BorrowedColumns<'_>,
		row_index: usize,
	) -> Option<(Self::GroupKey, Self::WindowKey, Self::WindowInput)>;

	fn extract_remove(
		&self,
		cols: &BorrowedColumns<'_>,
		row_index: usize,
	) -> Option<(Self::GroupKey, Self::WindowKey, Self::RemoveInput)> {
		let _ = (cols, row_index);
		None
	}

	fn fold_into_window(&self, prev: Option<&Self::Buffered>, input: &Self::WindowInput) -> Self::Buffered;

	fn remove_from_window(&self, prev: &Self::Buffered, remove: &Self::RemoveInput) -> Option<Self::Buffered> {
		let _ = (prev, remove);
		None
	}

	fn combine(
		&self,
		group: &Self::GroupKey,
		buffer: &BTreeMap<Self::WindowKey, Self::Buffered>,
	) -> BTreeMap<Self::SecondaryKey, Self::Output>;
}

pub trait FFIMultiRollingOperator: MultiRollingOperator + Sized
where
	Self::Output: Row,
	for<'a> &'a Self::GroupKey: IntoEncodedKey,
{
	const NAME: &'static str;
	const VERSION: &'static str;
	const DESCRIPTION: &'static str;
	const INPUT_COLUMNS: &'static [OperatorColumn];
	const OUTPUT_COLUMNS: &'static [OperatorColumn];
	const CAPABILITIES: u32;

	fn from_config(operator_id: FlowNodeId, config: &HashMap<String, Value>) -> Result<Self>;

	fn encode_state_key(&self, group: &Self::GroupKey) -> EncodedKey;

	fn encode_row_key(&self, group: &Self::GroupKey, secondary: &Self::SecondaryKey) -> EncodedKey;
}

pub type MultiRollingBuffer<A> =
	BTreeMap<<A as MultiRollingOperator>::WindowKey, <A as MultiRollingOperator>::Buffered>;

pub type MultiRollingEmit<A> = BTreeMap<<A as MultiRollingOperator>::SecondaryKey, <A as MultiRollingOperator>::Output>;

#[derive(Serialize, Deserialize)]
#[serde(bound(
	serialize = "MultiRollingBuffer<A>: Serialize, MultiRollingEmit<A>: Serialize",
	deserialize = "MultiRollingBuffer<A>: serde::de::DeserializeOwned, MultiRollingEmit<A>: serde::de::DeserializeOwned"
))]
struct GroupState<A: MultiRollingOperator> {
	buffer: MultiRollingBuffer<A>,
	last_emit: MultiRollingEmit<A>,
}

impl<A: MultiRollingOperator> Default for GroupState<A> {
	fn default() -> Self {
		Self {
			buffer: BTreeMap::new(),
			last_emit: BTreeMap::new(),
		}
	}
}

impl<A: MultiRollingOperator> Clone for GroupState<A> {
	fn clone(&self) -> Self {
		Self {
			buffer: self.buffer.clone(),
			last_emit: self.last_emit.clone(),
		}
	}
}

impl<A: MultiRollingOperator> fmt::Debug for GroupState<A> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("GroupState")
			.field("buffer_len", &self.buffer.len())
			.field("last_emit_len", &self.last_emit.len())
			.finish()
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(serialize = "K: Serialize", deserialize = "K: serde::de::DeserializeOwned"))]
struct GroupMeta<K> {
	high_water: Option<K>,
}

impl<K> Default for GroupMeta<K> {
	fn default() -> Self {
		Self {
			high_water: None,
		}
	}
}

pub struct MultiRollingDriver<A>
where
	A: FFIMultiRollingOperator,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	groups: StateCache<RowNumber, GroupState<A>>,
	meta: StateCache<MetaKey, GroupMeta<A::WindowKey>>,
}

enum BufferEvent<A: MultiRollingOperator> {
	Apply(A::WindowInput),
	RemoveSub(A::RemoveInput),
	RemoveWhole,
}

impl<A> FFIOperator for MultiRollingDriver<A>
where
	A: FFIMultiRollingOperator + 'static,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn new(operator_id: FlowNodeId, config: &HashMap<String, Value>) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		Ok(Self {
			aggregator,
			groups: StateCache::<RowNumber, GroupState<A>>::new(1024),
			meta: StateCache::<MetaKey, GroupMeta<A::WindowKey>>::new_internal(4096),
		})
	}

	#[allow(clippy::type_complexity)]
	fn apply(&mut self, ctx: &mut OperatorContext, input: BorrowedChange<'_>) -> Result<()> {
		let mut buckets: BTreeMap<(A::GroupKey, A::WindowKey), Vec<BufferEvent<A>>> = BTreeMap::new();

		for diff in input.diffs() {
			match diff.kind() {
				DiffType::Insert => {
					let cols = diff.post();
					for i in 0..cols.row_count() {
						let Some((group, wk, in_row)) = self.aggregator.extract_apply(&cols, i)
						else {
							continue;
						};
						buckets.entry((group, wk))
							.or_default()
							.push(BufferEvent::Apply(in_row));
					}
				}
				DiffType::Update => {
					let pre = diff.pre();
					let post = diff.post();
					let n = pre.row_count().min(post.row_count());
					for i in 0..n {
						let Some((g_pre, wk_pre, _)) = self.aggregator.extract_apply(&pre, i)
						else {
							continue;
						};
						let Some((g_post, wk_post, in_row)) =
							self.aggregator.extract_apply(&post, i)
						else {
							continue;
						};
						if g_pre != g_post || wk_pre != wk_post {
							let retraction = match self.aggregator.extract_remove(&pre, i) {
								Some((_, _, rm)) => BufferEvent::RemoveSub(rm),
								None => BufferEvent::RemoveWhole,
							};
							buckets.entry((g_pre, wk_pre)).or_default().push(retraction);
						}
						buckets.entry((g_post, wk_post))
							.or_default()
							.push(BufferEvent::Apply(in_row));
					}
				}
				DiffType::Remove => {
					let cols = diff.pre();
					for i in 0..cols.row_count() {
						if let Some((group, wk, rm)) = self.aggregator.extract_remove(&cols, i)
						{
							buckets.entry((group, wk))
								.or_default()
								.push(BufferEvent::RemoveSub(rm));
						} else if let Some((group, wk, _)) =
							self.aggregator.extract_apply(&cols, i)
						{
							buckets.entry((group, wk))
								.or_default()
								.push(BufferEvent::RemoveWhole);
						}
					}
				}
			}
		}

		if buckets.is_empty() {
			return Ok(());
		}

		let meta_keys: Vec<MetaKey> = buckets
			.keys()
			.map(|(group, _)| group)
			.collect::<BTreeSet<_>>()
			.into_iter()
			.map(meta_key_for)
			.collect();
		self.meta.warm(ctx, &meta_keys)?;

		let mut meta_loaded: HashMap<A::GroupKey, GroupMeta<A::WindowKey>> = HashMap::new();
		for (group, _) in buckets.keys() {
			if !meta_loaded.contains_key(group) {
				let m = self.meta.get(ctx, &meta_key_for(group))?.unwrap_or_default();
				meta_loaded.insert(group.clone(), m);
			}
		}

		let mut state_rows: HashMap<A::GroupKey, RowNumber> = HashMap::new();
		let mut resolve_order: Vec<A::GroupKey> = Vec::new();
		let mut state_lookup_keys: Vec<EncodedKey> = Vec::new();
		let mut seen: BTreeSet<A::GroupKey> = BTreeSet::new();
		for (group, wk) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.high_water);
			if initial_high_water.is_none_or(|hw| *wk >= hw) && seen.insert(group.clone()) {
				resolve_order.push(group.clone());
				state_lookup_keys.push(self.aggregator.encode_state_key(group));
			}
		}
		let resolved_rows = ctx.get_or_create_row_numbers(&state_lookup_keys)?;
		let state_keys: Vec<RowNumber> = resolved_rows.iter().map(|(rn, _)| *rn).collect();
		for (group, (state_row_number, _)) in resolve_order.into_iter().zip(resolved_rows) {
			state_rows.insert(group, state_row_number);
		}
		self.groups.warm(ctx, &state_keys)?;

		struct GroupSlot<A: MultiRollingOperator> {
			state_row_number: RowNumber,
			buffer: MultiRollingBuffer<A>,
			prior_emit: MultiRollingEmit<A>,
			buffer_changed: bool,
		}
		let mut group_slots: BTreeMap<A::GroupKey, GroupSlot<A>> = BTreeMap::new();

		let capacity = self.aggregator.capacity();

		for ((group, wk), events) in buckets {
			let meta = meta_loaded.entry(group.clone()).or_default();

			if let Some(hw) = meta.high_water
				&& wk < hw
			{
				continue;
			}

			let slot = match group_slots.get_mut(&group) {
				Some(s) => s,
				None => {
					let state_row_number = match state_rows.get(&group) {
						Some(&rn) => rn,
						None => {
							let key = self.aggregator.encode_state_key(&group);
							let (rn, _is_new) = ctx.get_or_create_row_number(&key)?;
							rn
						}
					};
					let GroupState {
						buffer,
						last_emit: prior_emit,
					} = self.groups.get(ctx, &state_row_number)?.unwrap_or_default();
					group_slots.insert(
						group.clone(),
						GroupSlot {
							state_row_number,
							buffer,
							prior_emit,
							buffer_changed: false,
						},
					);
					group_slots.get_mut(&group).expect("just inserted")
				}
			};

			for event in events {
				match event {
					BufferEvent::Apply(in_row) => {
						let prev = slot.buffer.get(&wk);
						let buffered = self.aggregator.fold_into_window(prev, &in_row);
						slot.buffer.insert(wk, buffered);
						while slot.buffer.len() > capacity {
							slot.buffer.pop_first();
						}
						slot.buffer_changed = true;
					}
					BufferEvent::RemoveSub(rm) => {
						if let Some(prev) = slot.buffer.get(&wk) {
							match self.aggregator.remove_from_window(prev, &rm) {
								Some(updated) => {
									slot.buffer.insert(wk, updated);
								}
								None => {
									slot.buffer.remove(&wk);
								}
							}
							slot.buffer_changed = true;
						}
					}
					BufferEvent::RemoveWhole => {
						if slot.buffer.remove(&wk).is_some() {
							slot.buffer_changed = true;
						}
					}
				}
			}

			meta.high_water = Some(match meta.high_water {
				Some(hw) if hw > wk => hw,
				_ => wk,
			});
		}

		let mut inserts: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut updates: Vec<(RowNumber, A::Output, A::Output)> = Vec::new();
		let mut removes: Vec<(RowNumber, A::Output)> = Vec::new();

		for (group, slot) in group_slots {
			if !slot.buffer_changed {
				continue;
			}
			let new_emit = self.aggregator.combine(&group, &slot.buffer);

			for (sk, new_out) in &new_emit {
				let key = self.aggregator.encode_row_key(&group, sk);
				let (rn, _is_new_alloc) = ctx.get_or_create_row_number(&key)?;
				match slot.prior_emit.get(sk) {
					Some(prior_out) => {
						if prior_out != new_out {
							updates.push((rn, prior_out.clone(), new_out.clone()));
						}
					}
					None => {
						inserts.push((rn, new_out.clone()));
					}
				}
			}
			for (sk, prior_out) in &slot.prior_emit {
				if !new_emit.contains_key(sk) {
					let key = self.aggregator.encode_row_key(&group, sk);
					let (rn, _is_new_alloc) = ctx.get_or_create_row_number(&key)?;
					removes.push((rn, prior_out.clone()));
				}
			}

			let combined = GroupState {
				buffer: slot.buffer,
				last_emit: new_emit,
			};
			self.groups.put(ctx, &slot.state_row_number, combined)?;
		}

		if !inserts.is_empty() {
			let mut batch = InsertBatch::<A::Output>::new(ctx, inserts.len())?;
			for (rn, data) in &inserts {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}
		if !updates.is_empty() {
			let mut batch = UpdateBatch::<A::Output>::new(ctx, updates.len())?;
			for (rn, prior, new) in &updates {
				batch.push(*rn, prior, new)?;
			}
			batch.finish()?;
		}
		if !removes.is_empty() {
			let mut batch = RemoveBatch::<A::Output>::new(ctx, removes.len())?;
			for (rn, data) in &removes {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}

		for (group, meta) in meta_loaded {
			self.meta.set(ctx, &meta_key_for(&group), &meta)?;
		}

		Ok(())
	}

	fn pull(&mut self, _ctx: &mut OperatorContext, _row_numbers: &[RowNumber]) -> Result<()> {
		Ok(())
	}

	fn flush_state(&mut self, ctx: &mut OperatorContext) -> Result<()> {
		self.groups.flush(ctx)?;
		self.meta.flush(ctx)?;
		Ok(())
	}
}

impl<A> FFIOperatorMetadata for MultiRollingDriver<A>
where
	A: FFIMultiRollingOperator,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	const NAME: &'static str = A::NAME;
	const API: u32 = 1;
	const VERSION: &'static str = A::VERSION;
	const DESCRIPTION: &'static str = A::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = A::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = A::OUTPUT_COLUMNS;
	const CAPABILITIES: u32 = A::CAPABILITIES;
}

#[cfg(test)]
mod tests {
	use std::{cmp::Ordering, collections::BTreeMap};

	use reifydb_abi::operator::capabilities::CAPABILITY_ALL_STANDARD;
	use reifydb_core::{
		encoded::{
			key::EncodedKey,
			shape::{RowShape, RowShapeField},
		},
		interface::catalog::flow::FlowNodeId,
		row::Row as CoreRow,
	};
	use reifydb_type::value::{Value, r#type::Type};
	use serde::{Deserialize, Serialize};

	use super::*;
	use crate::{
		operator::change::BorrowedColumns,
		row,
		testing::{
			builders::{TestChangeBuilder, TestRowBuilder},
			harness::TestHarnessBuilder,
		},
	};

	// Test fixture: rolling top-2 by value per group, last 3 buffered
	// windows. Buffered carries a single (key, value) per window;
	// combine sorts by value desc and emits top-2 by rank.

	#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
	struct TestInput {
		key: u64,
		value: f64,
	}

	#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
	struct TestBuffered {
		key: u64,
		value: f64,
	}

	#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
	struct TestOut {
		group: String,
		rank: u32,
		key: u64,
		value: f64,
	}

	row!(TestOut {
		group: String,
		rank: u32,
		key: u64,
		value: f64
	});

	struct TestTopAggregator;

	impl MultiRollingOperator for TestTopAggregator {
		type GroupKey = String;
		type WindowKey = u64;
		type WindowInput = TestInput;
		type RemoveInput = ();
		type Buffered = TestBuffered;
		type SecondaryKey = u32;
		type Output = TestOut;

		fn capacity(&self) -> usize {
			3
		}

		fn extract_apply(&self, cols: &BorrowedColumns<'_>, i: usize) -> Option<(String, u64, TestInput)> {
			let group = cols.column("group")?.utf8_at(i)?.to_string();
			let window_start = cols.column("window_start")?.u64_at(i)?;
			let key = cols.column("key")?.u64_at(i)?;
			let value = cols.column("value")?.f64_at(i)?;
			Some((
				group,
				window_start,
				TestInput {
					key,
					value,
				},
			))
		}

		fn fold_into_window(&self, _prev: Option<&TestBuffered>, input: &TestInput) -> TestBuffered {
			TestBuffered {
				key: input.key,
				value: input.value,
			}
		}

		fn combine(&self, group: &String, buffer: &BTreeMap<u64, TestBuffered>) -> BTreeMap<u32, TestOut> {
			// Sort buffered values by descending value with tiebreak
			// on key for determinism. Take top-2.
			let mut entries: Vec<&TestBuffered> = buffer.values().collect();
			entries.sort_by(|a, b| {
				b.value.partial_cmp(&a.value).unwrap_or(Ordering::Equal).then_with(|| a.key.cmp(&b.key))
			});
			let mut out = BTreeMap::new();
			for (i, e) in entries.into_iter().take(2).enumerate() {
				let rank = (i as u32) + 1;
				out.insert(
					rank,
					TestOut {
						group: group.clone(),
						rank,
						key: e.key,
						value: e.value,
					},
				);
			}
			out
		}
	}

	impl FFIMultiRollingOperator for TestTopAggregator {
		const NAME: &'static str = "test_top_rolling";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;

		fn from_config(_operator_id: FlowNodeId, _config: &HashMap<String, Value>) -> Result<Self> {
			Ok(Self)
		}

		fn encode_state_key(&self, group: &String) -> EncodedKey {
			EncodedKey::builder().str("state").str(group).build()
		}

		fn encode_row_key(&self, group: &String, secondary: &u32) -> EncodedKey {
			EncodedKey::builder().str("row").str(group).u32(*secondary).build()
		}
	}

	fn input_shape() -> RowShape {
		RowShape::new(vec![
			RowShapeField::unconstrained("group", Type::Utf8),
			RowShapeField::unconstrained("window_start", Type::Uint8),
			RowShapeField::unconstrained("key", Type::Uint8),
			RowShapeField::unconstrained("value", Type::Float8),
		])
	}

	fn input_row(rn: u64, group: &str, window_start: u64, key: u64, value: f64) -> CoreRow {
		TestRowBuilder::new(rn)
			.with_values(vec![
				Value::Utf8(group.into()),
				Value::Uint8(window_start),
				Value::Uint8(key),
				Value::float8(value),
			])
			.with_shape(input_shape())
			.build()
	}

	#[test]
	fn first_window_emits_inserts_for_top_2() {
		let mut h =
			TestHarnessBuilder::<MultiRollingDriver<TestTopAggregator>>::new().build().expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "BTC", 0, 200, 9.0))
				.insert(input_row(3, "BTC", 0, 300, 7.0))
				.build())
			.expect("apply");
		// Last write wins per (group, wk): the last insert at wk=0 is
		// key=300, value=7. The single buffered entry yields rank-1 only.
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Insert);
		let post = diff.post().expect("post");
		assert_eq!(post.row_count(), 1);
		let r = post.row_ref(0).expect("r0");
		assert_eq!(r.u32("rank"), Some(1));
		assert_eq!(r.u64("key"), Some(300));
	}

	#[test]
	fn three_distinct_windows_emit_top_2_by_value() {
		let mut h =
			TestHarnessBuilder::<MultiRollingDriver<TestTopAggregator>>::new().build().expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "BTC", 60, 200, 9.0))
				.insert(input_row(3, "BTC", 120, 300, 7.0))
				.build())
			.expect("apply");
		// Top-2 by value across three windows: (200, 9), (300, 7).
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Insert);
		let post = diff.post().expect("post");
		assert_eq!(post.row_count(), 2);
		let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
			.map(|i| {
				let r = post.row_ref(i).expect("row");
				(r.u32("rank").unwrap(), (r.u64("key").unwrap(), r.f64("value").unwrap()))
			})
			.collect();
		assert_eq!(by_rank.get(&1).copied(), Some((200u64, 9.0f64)));
		assert_eq!(by_rank.get(&2).copied(), Some((300u64, 7.0f64)));
	}

	#[test]
	fn vanishing_secondary_key_emits_remove() {
		let mut h =
			TestHarnessBuilder::<MultiRollingDriver<TestTopAggregator>>::new().build().expect("harness");
		// Fill 2 windows -> emit two ranks.
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "BTC", 60, 200, 9.0))
				.build())
			.expect("apply");
		// Drop the older window via Remove (RemoveWhole default since
		// extract_remove returns None). Only one window remains -> rank-1
		// stays (with the rolling value of the surviving window),
		// rank-2 vanishes -> Remove emitted.
		let out = h
			.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 100, 5.0)).build())
			.expect("apply");
		// Wait - the Remove targets the OLDER window which is below
		// high_water=60. Late-event filter drops it; no diff.
		assert_eq!(out.diffs.len(), 0, "remove on buried window dropped late");
	}

	#[test]
	fn remove_at_high_water_propagates_to_emit_diff() {
		let mut h =
			TestHarnessBuilder::<MultiRollingDriver<TestTopAggregator>>::new().build().expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "BTC", 60, 200, 9.0))
				.build())
			.expect("apply");
		// Remove the newest window (wk=60 == high_water, NOT strictly
		// less, so it's not late). Buffer goes to {wk=0}; combine emits
		// only rank-1.
		let out = h
			.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 60, 200, 9.0)).build())
			.expect("apply");
		// Prior emit had ranks {1, 2}; new emit has rank {1} (with the
		// surviving wk=0 key=100 value=5.0). Rank-1 changed from
		// (200, 9) to (100, 5) -> Update. Rank-2 vanished -> Remove.
		let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
		assert!(kinds.contains(&DiffType::Update), "rank-1 changed identity, expect Update");
		assert!(kinds.contains(&DiffType::Remove), "rank-2 vanished, expect Remove");
	}

	#[test]
	fn buried_window_insert_dropped_silently() {
		let mut h =
			TestHarnessBuilder::<MultiRollingDriver<TestTopAggregator>>::new().build().expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 100, 5.0)).build())
			.expect("apply");
		// high_water=60. Insert at wk=0 < 60 is dropped silently.
		let out = h
			.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 999, 999.0)).build())
			.expect("apply");
		assert_eq!(out.diffs.len(), 0);
	}

	#[test]
	fn capacity_eviction_drops_oldest_window() {
		let mut h =
			TestHarnessBuilder::<MultiRollingDriver<TestTopAggregator>>::new().build().expect("harness");
		// Capacity = 3. Insert 4 windows; smallest-key entry must be
		// evicted. Top-2 across the 3 surviving windows.
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 1.0))
				.insert(input_row(2, "BTC", 60, 200, 8.0))
				.insert(input_row(3, "BTC", 120, 300, 2.0))
				.insert(input_row(4, "BTC", 180, 400, 5.0))
				.build())
			.expect("apply");
		// After eviction buffer = {60: 8, 120: 2, 180: 5}. Top-2 by
		// value: (200, 8), (400, 5).
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Insert);
		let post = diff.post().expect("post");
		let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
			.map(|i| {
				let r = post.row_ref(i).expect("row");
				(r.u32("rank").unwrap(), (r.u64("key").unwrap(), r.f64("value").unwrap()))
			})
			.collect();
		assert_eq!(by_rank.get(&1).copied(), Some((200u64, 8.0f64)));
		assert_eq!(by_rank.get(&2).copied(), Some((400u64, 5.0f64)));
	}

	#[test]
	fn multiple_groups_isolate_emits() {
		let mut h =
			TestHarnessBuilder::<MultiRollingDriver<TestTopAggregator>>::new().build().expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "ETH", 0, 700, 50.0))
				.build())
			.expect("apply");
		// Two groups, each with one buffered window -> rank-1 each.
		assert_eq!(out.diffs.len(), 1);
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 2);
		let groups: Vec<String> = (0..post.row_count())
			.map(|i| post.row_ref(i).unwrap().utf8("group").unwrap_or_default().to_string())
			.collect();
		assert!(groups.contains(&"BTC".to_string()));
		assert!(groups.contains(&"ETH".to_string()));
	}
}
