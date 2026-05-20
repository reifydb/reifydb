// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap};

use reifydb_abi::flow::diff::DiffType;
use reifydb_core::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	interface::catalog::flow::FlowNodeId,
	key::flow_node_internal_state::FlowNodeInternalStateKey,
};
use reifydb_type::value::{Value, row_number::RowNumber};
use serde::{Deserialize, Serialize};

use crate::{
	error::Result,
	operator::{
		FFIOperator, FFIOperatorMetadata,
		change::BorrowedChange,
		column::{
			batch::{InsertBatch, UpdateBatch},
			operator::OperatorColumn,
			row::Row,
		},
		context::OperatorContext,
		windowed::{FFITumblingOperator, TumblingOperator, WindowSlots, span::WindowSpan},
	},
	state::cache::StateCache,
};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
	serialize = "K: Serialize, S: Serialize",
	deserialize = "K: serde::de::DeserializeOwned, S: serde::de::DeserializeOwned"
))]
struct GroupMeta<K, S> {
	high_water: Option<K>,

	carry_for_current: Option<S>,

	current_window_carry: Option<S>,
}

impl<K, S> Default for GroupMeta<K, S> {
	fn default() -> Self {
		Self {
			high_water: None,
			carry_for_current: None,
			current_window_carry: None,
		}
	}
}

pub struct TumblingDriver<A>
where
	A: FFITumblingOperator,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	slots: StateCache<RowNumber, WindowSlots<A>>,

	meta: StateCache<MetaKey, GroupMeta<A::SlotKey, A::SlotContribution>>,
}

enum SlotEvent<A: TumblingOperator> {
	Apply(A::SlotKey, A::SlotInput),
	Remove(A::SlotKey),
}

#[derive(Clone, Copy)]
enum DiffKind {
	Apply,
	Remove,
}

impl<A> FFIOperator for TumblingDriver<A>
where
	A: FFITumblingOperator + 'static,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn new(operator_id: FlowNodeId, config: &HashMap<String, Value>) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		Ok(Self {
			aggregator,
			slots: StateCache::<RowNumber, WindowSlots<A>>::new(1024),
			meta: StateCache::<MetaKey, GroupMeta<A::SlotKey, A::SlotContribution>>::new_internal(4096),
		})
	}

	#[allow(clippy::type_complexity)]
	fn apply(&mut self, ctx: &mut OperatorContext, input: BorrowedChange<'_>) -> Result<()> {
		let mut buckets: BTreeMap<(A::GroupKey, WindowSpan<A::SlotKey>), Vec<SlotEvent<A>>> = BTreeMap::new();

		for diff in input.diffs() {
			let (cols, kind) = match diff.kind() {
				DiffType::Insert | DiffType::Update => (diff.post(), DiffKind::Apply),
				DiffType::Remove => (diff.pre(), DiffKind::Remove),
			};

			for i in 0..cols.row_count() {
				let Some((group, slot, slot_input)) = self.aggregator.extract(&cols, i) else {
					continue;
				};
				let span = self.aggregator.window_for(slot);
				let event = match kind {
					DiffKind::Apply => SlotEvent::Apply(slot, slot_input),
					DiffKind::Remove => SlotEvent::Remove(slot),
				};
				buckets.entry((group, span)).or_default().push(event);
			}
		}

		if buckets.is_empty() {
			return Ok(());
		}

		let mut meta_loaded: HashMap<A::GroupKey, GroupMeta<A::SlotKey, A::SlotContribution>> = HashMap::new();
		for (group, _) in buckets.keys() {
			if !meta_loaded.contains_key(group) {
				let m = self.meta.get(ctx, &meta_key_for(group))?.unwrap_or_default();
				meta_loaded.insert(group.clone(), m);
			}
		}

		let mut inserts: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut updates: Vec<(RowNumber, A::Output)> = Vec::new();

		for ((group, span), events) in buckets {
			let entry = meta_loaded.entry(group.clone()).or_default();
			match entry.high_water {
				Some(hw) if span.start < hw => continue,
				Some(hw) if span.start > hw => {
					entry.carry_for_current = entry.current_window_carry.take();
					entry.high_water = Some(span.start);
				}
				Some(_) => {}
				None => {
					entry.high_water = Some(span.start);
				}
			}
			let prev_close = entry.carry_for_current.clone();

			let key = self.aggregator.encode_row_key(&group, span.start);
			let (row_number, is_new) = ctx.get_or_create_row_number(&key)?;

			let mut slot_map: WindowSlots<A> = self.slots.get(ctx, &row_number)?.unwrap_or_default();
			let was_empty_before = slot_map.is_empty();

			for event in events {
				match event {
					SlotEvent::Apply(slot, in_row) => {
						let prev = slot_map.get(&slot);
						let contribution = self.aggregator.fold_into_slot(prev, &in_row);
						slot_map.insert(slot, contribution);
					}
					SlotEvent::Remove(slot) => {
						slot_map.remove(&slot);
					}
				}
			}

			let output = self.aggregator.combine(&group, span, &slot_map, prev_close.as_ref());

			if output.is_some()
				&& let Some(new_carry) = self.aggregator.carry_forward(&slot_map, prev_close.as_ref())
			{
				let entry = meta_loaded.entry(group.clone()).or_default();
				entry.current_window_carry = Some(new_carry);
			}

			self.slots.put(ctx, &row_number, slot_map)?;

			if let Some(out) = output {
				if is_new || was_empty_before {
					inserts.push((row_number, out));
				} else {
					updates.push((row_number, out));
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

	fn pull(&mut self, _ctx: &mut OperatorContext, _row_numbers: &[RowNumber]) -> Result<()> {
		Ok(())
	}

	fn flush_state(&mut self, ctx: &mut OperatorContext) -> Result<()> {
		self.slots.flush(ctx)?;
		self.meta.flush(ctx)?;
		Ok(())
	}
}

impl<A> FFIOperatorMetadata for TumblingDriver<A>
where
	A: FFITumblingOperator,
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

	// Test fixture: a per-window volume aggregator with last-write-wins
	// per-slot replacement. Keyed by group `String`, slotted by `u64`.
	// `combine` returns total volume across the window plus its
	// `(group, window_start)` identifier so downstream assertions can
	// inspect routing, not just value math.

	#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
	struct TestInput {
		size: f64,
	}

	#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
	struct TestSlot {
		size: f64,
	}

	#[derive(Clone, Debug, PartialEq)]
	struct TestOut {
		group: String,
		window_start: u64,
		volume: f64,
	}

	row!(TestOut {
		group: String,
		window_start: u64,
		volume: f64
	});

	struct TestVolumeAggregator;

	impl TumblingOperator for TestVolumeAggregator {
		type GroupKey = String;
		type SlotKey = u64;
		type SlotInput = TestInput;
		type SlotContribution = TestSlot;
		type Output = TestOut;

		fn extract(&self, cols: &BorrowedColumns<'_>, i: usize) -> Option<(String, u64, TestInput)> {
			let group = cols.column("group")?.utf8_at(i)?.to_string();
			let slot = cols.column("slot")?.u64_at(i)?;
			let size = cols.column("size")?.f64_at(i)?;
			Some((
				group,
				slot,
				TestInput {
					size,
				},
			))
		}

		fn fold_into_slot(&self, _prev: Option<&TestSlot>, input: &TestInput) -> TestSlot {
			TestSlot {
				size: input.size,
			}
		}

		fn combine(
			&self,
			group: &String,
			span: WindowSpan<u64>,
			slots: &BTreeMap<u64, TestSlot>,
			_prev_window_close: Option<&TestSlot>,
		) -> Option<TestOut> {
			(!slots.is_empty()).then(|| TestOut {
				group: group.clone(),
				window_start: span.start,
				volume: slots.values().map(|s| s.size).sum(),
			})
		}

		fn window_for(&self, slot: u64) -> WindowSpan<u64> {
			WindowSpan::for_slot(slot, 60)
		}
	}

	impl FFITumblingOperator for TestVolumeAggregator {
		const NAME: &'static str = "test_volume_tumbling";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;

		fn from_config(_operator_id: FlowNodeId, _config: &HashMap<String, Value>) -> Result<Self> {
			Ok(Self)
		}

		fn encode_row_key(&self, group: &String, window_start: u64) -> EncodedKey {
			EncodedKey::builder().str(group).u64(window_start).build()
		}
	}

	fn input_shape() -> RowShape {
		RowShape::new(vec![
			RowShapeField::unconstrained("group", Type::Utf8),
			RowShapeField::unconstrained("slot", Type::Uint8),
			RowShapeField::unconstrained("size", Type::Float8),
		])
	}

	fn input_row(rn: u64, group: &str, slot: u64, size: f64) -> CoreRow {
		TestRowBuilder::new(rn)
			.with_values(vec![Value::Utf8(group.into()), Value::Uint8(slot), Value::float8(size)])
			.with_shape(input_shape())
			.build()
	}

	#[test]
	fn single_insert_emits_insert() {
		let mut h = TestHarnessBuilder::<TumblingDriver<TestVolumeAggregator>>::new().build().expect("harness");
		let change = TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build();
		let out = h.apply(change).expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Insert);
		let post = diff.post().expect("post");
		assert_eq!(post.row_count(), 1);
		let r = post.row_ref(0).expect("r0");
		assert_eq!(r.utf8("group").as_deref(), Some("BTC"));
		assert_eq!(r.u64("window_start"), Some(0));
		assert_eq!(r.f64("volume"), Some(10.0));
	}

	#[test]
	fn update_replaces_slot_does_not_double_count() {
		let mut h = TestHarnessBuilder::<TumblingDriver<TestVolumeAggregator>>::new().build().expect("harness");

		// First batch: insert volume 10 at slot 0 of window [0, 60).
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");

		// Second batch: Update at slot 0 with volume 25 - per-slot
		// replacement means the window's volume should now be 25,
		// NOT 10 + 25 = 35 (the historical accumulate-on-Update bug).
		let out = h
			.apply(TestChangeBuilder::new()
				.update(input_row(1, "BTC", 0, 10.0), input_row(1, "BTC", 0, 25.0))
				.build())
			.expect("apply");

		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Update);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("volume"), Some(25.0));
	}

	#[test]
	fn remove_drops_slot_and_emits_update_with_remaining() {
		let mut h = TestHarnessBuilder::<TumblingDriver<TestVolumeAggregator>>::new().build().expect("harness");

		// Insert two slots in the same window.
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "BTC", 30, 5.0))
				.build())
			.expect("apply");

		// Remove slot 30. Window should now hold only slot 0
		// (volume 10), emitted as Update.
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 30, 5.0)).build()).expect("apply");

		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Update);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("volume"), Some(10.0));
	}

	#[test]
	fn remove_clears_window_emits_nothing() {
		let mut h = TestHarnessBuilder::<TumblingDriver<TestVolumeAggregator>>::new().build().expect("harness");

		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");

		// Remove the only slot. combine returns None, no diff.
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");

		assert_eq!(out.diffs.len(), 0);
	}

	#[test]
	fn boundary_slot_belongs_to_next_window() {
		let mut h = TestHarnessBuilder::<TumblingDriver<TestVolumeAggregator>>::new().build().expect("harness");

		// Slots 59 and 60 should land in DIFFERENT windows: 59 in
		// [0, 60), 60 in [60, 120). Two emitted rows.
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 59, 1.0))
				.insert(input_row(2, "BTC", 60, 1.0))
				.build())
			.expect("apply");

		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Insert);
		let post = diff.post().expect("post");
		assert_eq!(post.row_count(), 2);
		let r0 = post.row_ref(0).expect("r0");
		let r1 = post.row_ref(1).expect("r1");
		// BTreeMap keys windows by start, so the [0, 60) row comes first.
		assert_eq!(r0.u64("window_start"), Some(0));
		assert_eq!(r1.u64("window_start"), Some(60));
	}

	#[test]
	fn late_event_for_closed_window_dropped() {
		let mut h = TestHarnessBuilder::<TumblingDriver<TestVolumeAggregator>>::new().build().expect("harness");

		// Open window [60, 120): emit volume 5.
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 5.0)).build()).expect("apply");

		// Late event for window [0, 60): should be dropped silently
		// because the high-water mark is now 60. No diff.
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");

		assert_eq!(out.diffs.len(), 0);
	}

	#[test]
	fn multiple_groups_isolate_state() {
		let mut h = TestHarnessBuilder::<TumblingDriver<TestVolumeAggregator>>::new().build().expect("harness");

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
		// BTreeMap orders by group string: "BTC" < "ETH".
		assert_eq!(r0.utf8("group").as_deref(), Some("BTC"));
		assert_eq!(r0.f64("volume"), Some(10.0));
		assert_eq!(r1.utf8("group").as_deref(), Some("ETH"));
		assert_eq!(r1.f64("volume"), Some(50.0));
	}
}
