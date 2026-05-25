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
use reifydb_type::value::row_number::RowNumber;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
	config::Config,
	error::Result,
	operator::{
		OperatorLogic, OperatorMetadata,
		column::{
			batch::{InsertBatch, UpdateBatch},
			operator::OperatorColumn,
			row::Row,
		},
		context::OperatorContext,
		view::{ChangeView, ColumnsView, DiffView, RowView},
		windowed::{accumulator::WindowAccumulator, span::Slot},
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

type AccContribution<A> = <<A as RollingOperator>::WindowAcc as WindowAccumulator>::Contribution;

pub trait RollingOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type WindowCoord: Slot + Hash + Serialize + DeserializeOwned;

	type WindowAcc: WindowAccumulator;

	type Output: Clone + Debug + PartialEq;

	fn capacity(&self) -> usize;

	fn extract(&self, row: &impl RowView) -> Option<(Self::GroupKey, Self::WindowCoord, AccContribution<Self>)>;

	fn combine(
		&self,
		group: &Self::GroupKey,
		buffer: &BTreeMap<Self::WindowCoord, Self::WindowAcc>,
	) -> Option<Self::Output>;
}

pub trait RollingRegistration: RollingOperator + Sized
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

	fn from_config(operator_id: FlowNodeId, config: &Config) -> Result<Self>;

	fn encode_row_key(&self, group: &Self::GroupKey) -> EncodedKey;
}

pub type RollingBuffer<A> = BTreeMap<<A as RollingOperator>::WindowCoord, <A as RollingOperator>::WindowAcc>;

pub struct RollingDriver<A>
where
	A: RollingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	buffers: StateCache<RowNumber, RollingBuffer<A>>,
	meta: StateCache<MetaKey, GroupMeta<A::WindowCoord>>,
}

enum AccEvent<A: RollingOperator> {
	Add(AccContribution<A>),
	Remove(AccContribution<A>),
}

impl<A> OperatorMetadata for RollingDriver<A>
where
	A: RollingRegistration + 'static,
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

impl<A> OperatorLogic for RollingDriver<A>
where
	A: RollingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowCoord: Send + Sync,
	A::WindowAcc: Send + Sync,
	AccContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn create(operator_id: FlowNodeId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		Ok(Self {
			aggregator,
			buffers: StateCache::<RowNumber, RollingBuffer<A>>::new(8),
			meta: StateCache::<MetaKey, GroupMeta<A::WindowCoord>>::new_internal(64),
		})
	}

	#[allow(clippy::type_complexity)]
	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> Result<()> {
		let mut buckets: BTreeMap<(A::GroupKey, A::WindowCoord), Vec<AccEvent<A>>> = BTreeMap::new();

		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					if let Some(cols) = diff.post() {
						for i in 0..cols.row_count() {
							let Some(row) = cols.row(i) else {
								continue;
							};
							let Some((group, coord, contribution)) =
								self.aggregator.extract(&row)
							else {
								continue;
							};
							buckets.entry((group, coord))
								.or_default()
								.push(AccEvent::Add(contribution));
						}
					}
				}
				DiffType::Update => {
					if let (Some(pre), Some(post)) = (diff.pre(), diff.post()) {
						let n = pre.row_count().min(post.row_count());
						for i in 0..n {
							if let Some(pre_row) = pre.row(i)
								&& let Some((group, coord, contribution)) =
									self.aggregator.extract(&pre_row)
							{
								buckets.entry((group, coord))
									.or_default()
									.push(AccEvent::Remove(contribution));
							}
							if let Some(post_row) = post.row(i)
								&& let Some((group, coord, contribution)) =
									self.aggregator.extract(&post_row)
							{
								buckets.entry((group, coord))
									.or_default()
									.push(AccEvent::Add(contribution));
							}
						}
					}
				}
				DiffType::Remove => {
					if let Some(cols) = diff.pre() {
						for i in 0..cols.row_count() {
							let Some(row) = cols.row(i) else {
								continue;
							};
							let Some((group, coord, contribution)) =
								self.aggregator.extract(&row)
							else {
								continue;
							};
							buckets.entry((group, coord))
								.or_default()
								.push(AccEvent::Remove(contribution));
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

		let mut meta_loaded: HashMap<A::GroupKey, GroupMeta<A::WindowCoord>> = HashMap::new();
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
		for (group, coord) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.high_water);
			if initial_high_water.is_none_or(|hw| *coord >= hw) && seen.insert(group.clone()) {
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

		for ((group, coord), events) in buckets {
			let meta = meta_loaded.entry(group.clone()).or_default();

			if let Some(hw) = meta.high_water
				&& coord < hw
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

			let mut acc = slot.buffer.remove(&coord).unwrap_or_default();
			for event in events {
				match event {
					AccEvent::Add(c) => acc.add(&c),
					AccEvent::Remove(c) => acc.remove(&c),
				}
			}
			if !acc.is_empty() {
				slot.buffer.insert(coord, acc);
			}
			while slot.buffer.len() > capacity {
				slot.buffer.pop_first();
			}
			slot.buffer_changed = true;

			meta.high_water = Some(match meta.high_water {
				Some(hw) if hw > coord => hw,
				_ => coord,
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
			let mut batch = InsertBatch::<A::Output, _>::new(ctx, inserts.len())?;
			for (rn, data) in &inserts {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}
		if !updates.is_empty() {
			let mut batch = UpdateBatch::<A::Output, _>::new(ctx, updates.len())?;
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

	fn flush_state(&mut self, ctx: &mut impl OperatorContext) -> Result<()> {
		self.buffers.flush(ctx)?;
		self.meta.flush(ctx)?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
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
		operator::{FFIOperatorAdapter, view::RowView, windowed::accumulator::Moments},
		row,
		testing::{
			builders::{TestChangeBuilder, TestRowBuilder},
			harness::FFIOperatorHarnessBuilder,
		},
	};

	// Rolling sum over the last 3 windows, where EACH window is itself an
	// invertible sum accumulator. This exercises the rolling improvement:
	// multiple input rows can share a window coordinate and accumulate, and a
	// single event can be removed from inside a window (remove(pre)) without
	// dropping the whole window - which the old last-write-wins buffer could
	// not do.

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	struct WindowSum {
		moments: Moments,
	}

	impl WindowAccumulator for WindowSum {
		type Contribution = f64;
		type Output = f64;

		fn add(&mut self, contribution: &f64) {
			self.moments.add(*contribution);
		}

		fn remove(&mut self, contribution: &f64) {
			self.moments.remove(*contribution);
		}

		fn finalize(&self) -> Option<f64> {
			(!self.moments.is_empty()).then(|| self.moments.sum())
		}

		fn is_empty(&self) -> bool {
			self.moments.is_empty()
		}
	}

	#[derive(Clone, Debug, PartialEq)]
	struct TestOut {
		group: String,
		rolling_sum: f64,
		windows: u32,
	}

	row!(TestOut {
		group: String,
		rolling_sum: f64,
		windows: u32
	});

	struct TestRollingSum {
		capacity: usize,
	}

	impl RollingOperator for TestRollingSum {
		type GroupKey = String;
		type WindowCoord = u64;
		type WindowAcc = WindowSum;
		type Output = TestOut;

		fn capacity(&self) -> usize {
			self.capacity
		}

		fn extract(&self, row: &impl RowView) -> Option<(String, u64, f64)> {
			let group = row.utf8("group")?.to_string();
			let window_start = row.u64("window_start")?;
			let value = row.f64("value")?;
			Some((group, window_start, value))
		}

		fn combine(&self, group: &String, buffer: &BTreeMap<u64, WindowSum>) -> Option<TestOut> {
			if buffer.is_empty() {
				return None;
			}
			let rolling_sum = buffer.values().filter_map(|w| w.finalize()).sum();
			Some(TestOut {
				group: group.clone(),
				rolling_sum,
				windows: buffer.len() as u32,
			})
		}
	}

	impl RollingRegistration for TestRollingSum {
		const NAME: &'static str = "test_rolling_sum";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: u32 = CAPABILITY_ALL_STANDARD;

		fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
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
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Insert);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("rolling_sum"), Some(10.0));
		assert_eq!(r.u32("windows"), Some(1));
	}

	#[test]
	fn multiple_events_accumulate_within_one_window() {
		// Two rows share window coordinate 0; the window accumulates to 7.
		// This is the capability the old last-write-wins buffer lacked.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 3.0))
				.insert(input_row(2, "BTC", 0, 4.0))
				.build())
			.expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("rolling_sum"), Some(7.0));
		assert_eq!(r.u32("windows"), Some(1), "both rows landed in the same window");
	}

	#[test]
	fn partial_remove_within_window_keeps_window_alive() {
		// Window 0 holds two events (sum 7). Removing one event leaves the
		// window with sum 4 - the window is NOT dropped wholesale.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 3.0))
				.insert(input_row(2, "BTC", 0, 4.0))
				.build())
			.expect("apply");
		let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 3.0)).build()).expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("rolling_sum"), Some(4.0));
		assert_eq!(r.u32("windows"), Some(1), "window survives partial removal");
	}

	#[test]
	fn update_within_window_applies_post_minus_pre() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		let out = h
			.apply(TestChangeBuilder::new()
				.update(input_row(1, "BTC", 0, 10.0), input_row(1, "BTC", 0, 25.0))
				.build())
			.expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("rolling_sum"), Some(25.0), "25, not 10 + 25");
	}

	#[test]
	fn buffer_fills_then_evicts_oldest_window() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 1.0))
				.insert(input_row(2, "BTC", 60, 2.0))
				.insert(input_row(3, "BTC", 120, 3.0))
				.insert(input_row(4, "BTC", 180, 4.0))
				.build())
			.expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("rolling_sum"), Some(9.0), "window 0 evicted: 2+3+4");
		assert_eq!(r.u32("windows"), Some(3));
	}

	#[test]
	fn late_window_event_dropped() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 5.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 0, "event for a buried window is dropped");
	}

	#[test]
	fn remove_clears_buffer_emits_nothing() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 0);
	}

	#[test]
	fn multiple_groups_isolate_buffers() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingDriver<TestRollingSum>>>::new()
			.build()
			.expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "ETH", 0, 50.0))
				.build())
			.expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 2);
		assert_eq!(post.row_ref(0).expect("r0").utf8("group").as_deref(), Some("BTC"));
		assert_eq!(post.row_ref(0).expect("r0").f64("rolling_sum"), Some(10.0));
		assert_eq!(post.row_ref(1).expect("r1").utf8("group").as_deref(), Some("ETH"));
		assert_eq!(post.row_ref(1).expect("r1").f64("rolling_sum"), Some(50.0));
	}
}
