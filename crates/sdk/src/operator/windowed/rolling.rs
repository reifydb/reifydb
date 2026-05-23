// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Debug,
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
			batch::{InsertBatch, UpdateBatch},
			operator::OperatorColumn,
			row::Row,
		},
		context::ffi::FFIOperatorContext,
		windowed::span::Slot,
	},
	state::cache::StateCache,
};

pub trait RollingOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type WindowKey: Slot + Hash + Serialize + DeserializeOwned;

	type WindowInput: Clone + Debug;

	type Buffered: Clone + Debug + Serialize + DeserializeOwned;

	type Output: Clone + Debug + PartialEq;

	fn capacity(&self) -> usize;

	fn extract(
		&self,
		cols: &BorrowedColumns<'_>,
		row_index: usize,
	) -> Option<(Self::GroupKey, Self::WindowKey, Self::WindowInput)>;

	fn fold_into_window(&self, prev: Option<&Self::Buffered>, input: &Self::WindowInput) -> Self::Buffered;

	fn combine(
		&self,
		group: &Self::GroupKey,
		buffer: &BTreeMap<Self::WindowKey, Self::Buffered>,
	) -> Option<Self::Output>;
}

pub trait FFIRollingOperator: RollingOperator + Sized
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

	fn encode_row_key(&self, group: &Self::GroupKey) -> EncodedKey;
}

pub type RollingBuffer<A> = BTreeMap<<A as RollingOperator>::WindowKey, <A as RollingOperator>::Buffered>;

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

pub struct RollingDriver<A>
where
	A: FFIRollingOperator,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	buffers: StateCache<RowNumber, RollingBuffer<A>>,

	meta: StateCache<MetaKey, GroupMeta<A::WindowKey>>,
}

enum BufferEvent<A: RollingOperator> {
	Apply(A::WindowInput),
	Remove,
}

impl<A> FFIOperator for RollingDriver<A>
where
	A: FFIRollingOperator + 'static,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn new(operator_id: FlowNodeId, config: &HashMap<String, Value>) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		Ok(Self {
			aggregator,
			buffers: StateCache::<RowNumber, RollingBuffer<A>>::new(1024),
			meta: StateCache::<MetaKey, GroupMeta<A::WindowKey>>::new_internal(4096),
		})
	}

	#[allow(clippy::type_complexity)]
	fn apply(&mut self, ctx: &mut FFIOperatorContext, input: BorrowedChange<'_>) -> Result<()> {
		let mut buckets: BTreeMap<(A::GroupKey, A::WindowKey), Vec<BufferEvent<A>>> = BTreeMap::new();

		for diff in input.diffs() {
			match diff.kind() {
				DiffType::Insert => {
					let cols = diff.post();
					for i in 0..cols.row_count() {
						let Some((group, wk, in_row)) = self.aggregator.extract(&cols, i)
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
						let Some((g_pre, wk_pre, _)) = self.aggregator.extract(&pre, i) else {
							continue;
						};
						let Some((g_post, wk_post, in_row)) = self.aggregator.extract(&post, i)
						else {
							continue;
						};
						if g_pre != g_post || wk_pre != wk_post {
							buckets.entry((g_pre, wk_pre))
								.or_default()
								.push(BufferEvent::Remove);
						}
						buckets.entry((g_post, wk_post))
							.or_default()
							.push(BufferEvent::Apply(in_row));
					}
				}
				DiffType::Remove => {
					let cols = diff.pre();
					for i in 0..cols.row_count() {
						let Some((group, wk, _)) = self.aggregator.extract(&cols, i) else {
							continue;
						};
						buckets.entry((group, wk)).or_default().push(BufferEvent::Remove);
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

		let mut buffer_rows: HashMap<A::GroupKey, (RowNumber, bool)> = HashMap::new();
		let mut resolve_order: Vec<A::GroupKey> = Vec::new();
		let mut group_keys: Vec<EncodedKey> = Vec::new();
		let mut seen: BTreeSet<A::GroupKey> = BTreeSet::new();
		for (group, wk) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.high_water);
			if initial_high_water.is_none_or(|hw| *wk >= hw) && seen.insert(group.clone()) {
				resolve_order.push(group.clone());
				group_keys.push(self.aggregator.encode_row_key(group));
			}
		}
		let resolved_rows = ctx.get_or_create_row_numbers(&group_keys)?;
		let buffer_keys: Vec<RowNumber> = resolved_rows.iter().map(|(rn, _)| *rn).collect();
		for (group, resolved) in resolve_order.into_iter().zip(resolved_rows) {
			buffer_rows.insert(group, resolved);
		}
		self.buffers.warm(ctx, &buffer_keys)?;

		struct GroupSlot<A: RollingOperator> {
			row_number: RowNumber,
			is_new: bool,
			buffer: RollingBuffer<A>,
			was_empty_before: bool,
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
					let (row_number, is_new) = match buffer_rows.get(&group) {
						Some(&resolved) => resolved,
						None => {
							let key = self.aggregator.encode_row_key(&group);
							ctx.get_or_create_row_number(&key)?
						}
					};
					let buffer: RollingBuffer<A> =
						self.buffers.get(ctx, &row_number)?.unwrap_or_default();
					let was_empty_before = buffer.is_empty();
					group_slots.insert(
						group.clone(),
						GroupSlot {
							row_number,
							is_new,
							buffer,
							was_empty_before,
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
					BufferEvent::Remove => {
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
		let mut updates: Vec<(RowNumber, A::Output)> = Vec::new();

		for (group, slot) in group_slots {
			if !slot.buffer_changed {
				continue;
			}
			let output = self.aggregator.combine(&group, &slot.buffer);
			self.buffers.put(ctx, &slot.row_number, slot.buffer)?;

			if let Some(out) = output {
				if slot.is_new || slot.was_empty_before {
					inserts.push((slot.row_number, out));
				} else {
					updates.push((slot.row_number, out));
				}
			}
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
			for (rn, data) in &updates {
				batch.push(*rn, data, data)?;
			}
			batch.finish()?;
		}

		for (group, meta) in meta_loaded {
			self.meta.set(ctx, &meta_key_for(&group), &meta)?;
		}

		Ok(())
	}

	fn flush_state(&mut self, ctx: &mut FFIOperatorContext) -> Result<()> {
		self.buffers.flush(ctx)?;
		self.meta.flush(ctx)?;
		Ok(())
	}
}

impl<A> FFIOperatorMetadata for RollingDriver<A>
where
	A: FFIRollingOperator,
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
	use std::collections::BTreeMap;

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

	// Test fixture: rolling sum of last 3 windows per group. Buffered
	// stores the per-window value; combine sums values across the buffer.

	#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
	struct TestInput {
		value: f64,
	}

	#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
	struct TestBuffered {
		value: f64,
	}

	#[derive(Clone, Debug, PartialEq)]
	struct TestOut {
		group: String,
		rolling_sum: f64,
		count: u32,
	}

	row!(TestOut {
		group: String,
		rolling_sum: f64,
		count: u32
	});

	struct TestRollingSumAggregator {
		capacity: usize,
	}

	impl RollingOperator for TestRollingSumAggregator {
		type GroupKey = String;
		type WindowKey = u64;
		type WindowInput = TestInput;
		type Buffered = TestBuffered;
		type Output = TestOut;

		fn capacity(&self) -> usize {
			self.capacity
		}

		fn extract(&self, cols: &BorrowedColumns<'_>, i: usize) -> Option<(String, u64, TestInput)> {
			let group = cols.column("group")?.utf8_at(i)?.to_string();
			let window_start = cols.column("window_start")?.u64_at(i)?;
			let value = cols.column("value")?.f64_at(i)?;
			Some((
				group,
				window_start,
				TestInput {
					value,
				},
			))
		}

		fn fold_into_window(&self, _prev: Option<&TestBuffered>, input: &TestInput) -> TestBuffered {
			TestBuffered {
				value: input.value,
			}
		}

		fn combine(&self, group: &String, buffer: &BTreeMap<u64, TestBuffered>) -> Option<TestOut> {
			(!buffer.is_empty()).then(|| TestOut {
				group: group.clone(),
				rolling_sum: buffer.values().map(|b| b.value).sum(),
				count: buffer.len() as u32,
			})
		}
	}

	impl FFIRollingOperator for TestRollingSumAggregator {
		const NAME: &'static str = "test_rolling_sum";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;

		fn from_config(_operator_id: FlowNodeId, _config: &HashMap<String, Value>) -> Result<Self> {
			Ok(Self {
				capacity: 3,
			})
		}

		fn encode_row_key(&self, group: &String) -> EncodedKey {
			EncodedKey::builder().str(group).build()
		}
	}

	fn input_shape() -> RowShape {
		RowShape::new(vec![
			RowShapeField::unconstrained("group", Type::Utf8),
			RowShapeField::unconstrained("window_start", Type::Uint8),
			RowShapeField::unconstrained("value", Type::Float8),
		])
	}

	fn input_row(rn: u64, group: &str, window_start: u64, value: f64) -> CoreRow {
		TestRowBuilder::new(rn)
			.with_values(vec![Value::Utf8(group.into()), Value::Uint8(window_start), Value::float8(value)])
			.with_shape(input_shape())
			.build()
	}

	#[test]
	fn single_insert_emits_insert() {
		let mut h =
			TestHarnessBuilder::<RollingDriver<TestRollingSumAggregator>>::new().build().expect("harness");
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Insert);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.utf8("group").as_deref(), Some("BTC"));
		assert_eq!(r.f64("rolling_sum"), Some(10.0));
		assert_eq!(r.u32("count"), Some(1));
	}

	#[test]
	fn buffer_fills_then_evicts_smallest() {
		let mut h =
			TestHarnessBuilder::<RollingDriver<TestRollingSumAggregator>>::new().build().expect("harness");

		// Insert 4 windows; capacity = 3. The smallest (window_start=0)
		// is evicted on the 4th insert; the resulting Update reflects
		// the buffer of [60, 120, 180] -> values [2, 3, 4] -> sum 9.
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 1.0))
				.insert(input_row(2, "BTC", 60, 2.0))
				.insert(input_row(3, "BTC", 120, 3.0))
				.insert(input_row(4, "BTC", 180, 4.0))
				.build())
			.expect("apply");

		// One emit per Change: a single Insert (the four inserts collapse
		// to one row keyed by group "BTC") with the post-eviction state.
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("rolling_sum"), Some(9.0));
		assert_eq!(r.u32("count"), Some(3));
	}

	#[test]
	fn update_on_buried_window_dropped_late() {
		// The driver's late-event filter mirrors the tumbling driver:
		// once high_water has advanced past a WindowKey, any further
		// event for that older key (Insert, Update, Remove) is dropped
		// silently. An Update for a buried (non-newest) window does NOT
		// reach `fold_into_window` and the buffer is unchanged.
		let mut h =
			TestHarnessBuilder::<RollingDriver<TestRollingSumAggregator>>::new().build().expect("harness");

		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 1.0))
				.insert(input_row(2, "BTC", 60, 2.0))
				.insert(input_row(3, "BTC", 120, 3.0))
				.build())
			.expect("apply");

		// Update the OLDEST (buried) window. high_water = 120; this
		// Update for wk=0 is dropped. No diff emitted because the
		// buffer didn't change.
		let out = h
			.apply(TestChangeBuilder::new()
				.update(input_row(1, "BTC", 0, 1.0), input_row(1, "BTC", 0, 99.0))
				.build())
			.expect("apply");

		assert_eq!(out.diffs.len(), 0, "buried-window Update is dropped late, no emit");
	}

	#[test]
	fn remove_late_window_dropped() {
		// Remove for a WindowKey strictly older than high_water is
		// dropped silently, matching the tumbling driver's behaviour.
		// The buffer keeps the would-be-removed entry.
		let mut h =
			TestHarnessBuilder::<RollingDriver<TestRollingSumAggregator>>::new().build().expect("harness");

		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "BTC", 60, 20.0))
				.build())
			.expect("apply");

		// high_water = 60; Remove for wk=0 is dropped late. No diff.
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");

		assert_eq!(out.diffs.len(), 0, "late Remove dropped, buffer unchanged");
	}

	#[test]
	fn remove_newest_window_drops_from_buffer() {
		// A Remove for the newest WindowKey (wk == high_water) is NOT
		// late and is processed normally: the entry is dropped from the
		// buffer and an Update is emitted with the remaining contents.
		let mut h =
			TestHarnessBuilder::<RollingDriver<TestRollingSumAggregator>>::new().build().expect("harness");

		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "BTC", 60, 20.0))
				.build())
			.expect("apply");

		// high_water = 60; Remove for wk=60 is at the high-water mark,
		// not strictly older, so the filter lets it through.
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 60, 20.0)).build()).expect("apply");

		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Update);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("rolling_sum"), Some(10.0));
		assert_eq!(r.u32("count"), Some(1));
	}

	#[test]
	fn late_insert_for_buried_window_dropped() {
		// Late Insert for a window older than high_water is dropped
		// silently, mirroring the tumbling driver's late-event rule.
		let mut h =
			TestHarnessBuilder::<RollingDriver<TestRollingSumAggregator>>::new().build().expect("harness");

		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 5.0)).build()).expect("apply");

		// high_water = 60; an Insert for wk=0 is strictly older. Dropped.
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 0);
	}

	#[test]
	fn remove_clears_buffer_emits_nothing() {
		let mut h =
			TestHarnessBuilder::<RollingDriver<TestRollingSumAggregator>>::new().build().expect("harness");

		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");

		// Remove the only entry; combine returns None.
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");

		assert_eq!(out.diffs.len(), 0);
	}

	#[test]
	fn update_replaces_window_does_not_double_count() {
		// Same (group, window_start) Update should replace the
		// buffered value, not add to it. The chaos-test defect class
		// "accumulate-on-Update" lands here.
		let mut h =
			TestHarnessBuilder::<RollingDriver<TestRollingSumAggregator>>::new().build().expect("harness");

		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		let out = h
			.apply(TestChangeBuilder::new()
				.update(input_row(1, "BTC", 0, 10.0), input_row(1, "BTC", 0, 25.0))
				.build())
			.expect("apply");

		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Update);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		// 25, not 10 + 25 = 35.
		assert_eq!(r.f64("rolling_sum"), Some(25.0));
		assert_eq!(r.u32("count"), Some(1));
	}

	#[test]
	fn multiple_groups_isolate_buffers() {
		let mut h =
			TestHarnessBuilder::<RollingDriver<TestRollingSumAggregator>>::new().build().expect("harness");

		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "ETH", 0, 50.0))
				.build())
			.expect("apply");

		assert_eq!(out.diffs.len(), 1);
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 2);
		let r0 = post.row_ref(0).expect("r0");
		let r1 = post.row_ref(1).expect("r1");
		assert_eq!(r0.utf8("group").as_deref(), Some("BTC"));
		assert_eq!(r0.f64("rolling_sum"), Some(10.0));
		assert_eq!(r1.utf8("group").as_deref(), Some("ETH"));
		assert_eq!(r1.f64("rolling_sum"), Some(50.0));
	}
}
