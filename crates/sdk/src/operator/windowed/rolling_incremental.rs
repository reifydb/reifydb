// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Debug,
	hash::Hash,
};

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_core::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	interface::catalog::flow::FlowNodeId,
	key::flow_node_internal_state::FlowNodeInternalStateKey,
};
use reifydb_value::{reifydb_assertions, value::row_number::RowNumber};
use serde::{Deserialize, Serialize};

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
		view::{ChangeView, ColumnsView, DiffView},
		windowed::{
			accumulator::WindowAccumulator,
			rolling::{RollingOperator, RollingRegistration},
		},
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

type WindowContribution<A> = <<A as RollingOperator>::WindowAcc as WindowAccumulator>::Contribution;
type WindowValue<A> = <<A as RollingOperator>::WindowAcc as WindowAccumulator>::Output;
type RunningContribution<A> = <<A as RollingIncrementalOperator>::Running as WindowAccumulator>::Contribution;

pub trait RollingIncrementalOperator: RollingOperator {
	type Running: WindowAccumulator;

	fn window_contribution(&self, window_value: &WindowValue<Self>) -> RunningContribution<Self>;

	fn combine_running(
		&self,
		group: &Self::GroupKey,
		running: &Self::Running,
		newest_value: &WindowValue<Self>,
		newest_coord: Self::WindowCoord,
	) -> Option<Self::Output>;
}

pub type RollingBuffer<A> = BTreeMap<<A as RollingOperator>::WindowCoord, <A as RollingOperator>::WindowAcc>;

pub struct RollingIncrementalDriver<A>
where
	A: RollingIncrementalOperator + RollingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	buffers: StateCache<RowNumber, RollingBuffer<A>>,
	running: StateCache<RowNumber, A::Running>,
	meta: StateCache<MetaKey, GroupMeta<A::WindowCoord>>,
}

enum AccEvent<A: RollingOperator> {
	Add(WindowContribution<A>),
	Remove(WindowContribution<A>),
}

impl<A> OperatorMetadata for RollingIncrementalDriver<A>
where
	A: RollingIncrementalOperator + RollingRegistration + 'static,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	const NAME: &'static str = A::NAME;
	const API: u32 = 1;
	const VERSION: &'static str = A::VERSION;
	const DESCRIPTION: &'static str = A::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = A::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = A::OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = A::CAPABILITIES;
}

impl<A> OperatorLogic for RollingIncrementalDriver<A>
where
	A: RollingIncrementalOperator + RollingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowCoord: Send + Sync,
	A::WindowAcc: Send + Sync,
	A::Running: Send + Sync,
	WindowContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn create(operator_id: FlowNodeId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		Ok(Self {
			aggregator,
			buffers: StateCache::<RowNumber, RollingBuffer<A>>::new(8),
			running: StateCache::<RowNumber, A::Running>::new(8),
			meta: StateCache::<MetaKey, GroupMeta<A::WindowCoord>>::new_internal(64),
		})
	}

	#[allow(clippy::type_complexity)]
	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> Result<()> {
		let buckets = self.route_diffs_to_buckets(&change);

		if buckets.is_empty() {
			return Ok(());
		}

		let mut meta_loaded = self.warm_and_load_meta(ctx, &buckets)?;
		let buffer_rows = self.resolve_buffer_rows(ctx, &buckets, &meta_loaded)?;

		struct GroupSlot<A: RollingIncrementalOperator> {
			row_number: RowNumber,
			is_new: bool,
			buffer: RollingBuffer<A>,
			running: A::Running,
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
					let running: A::Running =
						self.running.get(ctx, &row_number)?.unwrap_or_default();
					let was_empty_before = buffer.is_empty();
					group_slots.insert(
						group.clone(),
						GroupSlot {
							row_number,
							is_new,
							buffer,
							running,
							was_empty_before,
							buffer_changed: false,
						},
					);
					group_slots.get_mut(&group).expect("just inserted")
				}
			};

			let mut acc = slot.buffer.remove(&coord).unwrap_or_default();
			let old_value = acc.finalize();
			for event in events {
				match event {
					AccEvent::Add(c) => acc.add(&c),
					AccEvent::Remove(c) => acc.remove(&c),
				}
			}
			let new_value = acc.finalize();

			if let Some(old) = &old_value {
				slot.running.remove(&self.aggregator.window_contribution(old));
			}
			if let Some(new) = &new_value {
				slot.running.add(&self.aggregator.window_contribution(new));
			}

			if !acc.is_empty() {
				slot.buffer.insert(coord, acc);
			}
			while slot.buffer.len() > capacity {
				if let Some((_, evicted)) = slot.buffer.pop_first()
					&& let Some(value) = evicted.finalize()
				{
					slot.running.remove(&self.aggregator.window_contribution(&value));
				}
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
			let output = match slot.buffer.iter().next_back() {
				Some((coord, acc)) => acc.finalize().and_then(|newest| {
					self.aggregator.combine_running(&group, &slot.running, &newest, *coord)
				}),
				None => None,
			};
			self.buffers.put(ctx, &slot.row_number, slot.buffer)?;
			self.running.put(ctx, &slot.row_number, slot.running)?;

			if let Some(out) = output {
				if slot.is_new || slot.was_empty_before {
					inserts.push((slot.row_number, out));
				} else {
					updates.push((slot.row_number, out));
				}
			}
		}

		Self::emit_insert_update_batches(ctx, &inserts, &updates)?;
		self.persist_meta(ctx, meta_loaded)?;

		Ok(())
	}

	fn flush_state(&mut self, ctx: &mut impl OperatorContext) -> Result<()> {
		self.buffers.flush(ctx)?;
		self.running.flush(ctx)?;
		self.meta.flush(ctx)?;
		Ok(())
	}
}

type EventBuckets<A> =
	BTreeMap<(<A as RollingOperator>::GroupKey, <A as RollingOperator>::WindowCoord), Vec<AccEvent<A>>>;

type MetaByGroup<A> = HashMap<<A as RollingOperator>::GroupKey, GroupMeta<<A as RollingOperator>::WindowCoord>>;

type BufferRowsByGroup<A> = HashMap<<A as RollingOperator>::GroupKey, (RowNumber, bool)>;

impl<A> RollingIncrementalDriver<A>
where
	A: RollingIncrementalOperator + RollingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowCoord: Send + Sync,
	A::WindowAcc: Send + Sync,
	A::Running: Send + Sync,
	WindowContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	#[inline]
	fn route_diffs_to_buckets(&self, change: &impl ChangeView) -> EventBuckets<A> {
		let mut buckets: EventBuckets<A> = BTreeMap::new();

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

		buckets
	}

	#[inline]
	fn warm_and_load_meta(
		&mut self,
		ctx: &mut impl OperatorContext,
		buckets: &EventBuckets<A>,
	) -> Result<MetaByGroup<A>> {
		let meta_keys: Vec<MetaKey> = buckets
			.keys()
			.map(|(group, _)| group)
			.collect::<BTreeSet<_>>()
			.into_iter()
			.map(meta_key_for)
			.collect();
		self.meta.warm(ctx, &meta_keys)?;

		let mut meta_loaded: MetaByGroup<A> = HashMap::new();
		for (group, _) in buckets.keys() {
			if !meta_loaded.contains_key(group) {
				let m = self.meta.get(ctx, &meta_key_for(group))?.unwrap_or_default();
				meta_loaded.insert(group.clone(), m);
			}
		}
		Ok(meta_loaded)
	}

	#[inline]
	fn resolve_buffer_rows(
		&mut self,
		ctx: &mut impl OperatorContext,
		buckets: &EventBuckets<A>,
		meta_loaded: &MetaByGroup<A>,
	) -> Result<BufferRowsByGroup<A>> {
		let mut buffer_rows: BufferRowsByGroup<A> = HashMap::new();
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
		reifydb_assertions! {
			let resolved = resolved_rows.len();
			let requested = group_keys.len();
			assert!(
				resolved == requested,
				"get_or_create_row_numbers returned {resolved} rows for {requested} group keys; \
				 the zip below pairs resolve_order with resolved_rows by position, so a length \
				 mismatch would silently leave some groups without a buffer_rows entry and route \
				 them through the per-bucket get_or_create_row_number fallback, diverging behaviour"
			);
		}
		let state_keys: Vec<RowNumber> = resolved_rows.iter().map(|(rn, _)| *rn).collect();
		for (group, resolved) in resolve_order.into_iter().zip(resolved_rows) {
			buffer_rows.insert(group, resolved);
		}
		self.buffers.warm(ctx, &state_keys)?;
		self.running.warm(ctx, &state_keys)?;
		Ok(buffer_rows)
	}

	#[inline]
	fn emit_insert_update_batches(
		ctx: &mut impl OperatorContext,
		inserts: &[(RowNumber, A::Output)],
		updates: &[(RowNumber, A::Output)],
	) -> Result<()> {
		if !inserts.is_empty() {
			let mut batch = InsertBatch::<A::Output, _>::new(ctx, inserts.len())?;
			for (rn, data) in inserts {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}
		if !updates.is_empty() {
			let mut batch = UpdateBatch::<A::Output, _>::new(ctx, updates.len())?;
			for (rn, data) in updates {
				batch.push(*rn, data, data)?;
			}
			batch.finish()?;
		}
		Ok(())
	}

	#[inline]
	fn persist_meta(&mut self, ctx: &mut impl OperatorContext, meta_loaded: MetaByGroup<A>) -> Result<()> {
		for (group, meta) in meta_loaded {
			self.meta.set(ctx, &meta_key_for(&group), &meta)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		encoded::{
			key::EncodedKey,
			shape::{RowShape, RowShapeField},
		},
		interface::catalog::flow::FlowNodeId,
		row::Row as CoreRow,
	};
	use reifydb_value::value::{Value, value_type::ValueType};

	use super::*;
	use crate::{
		operator::{
			FFIOperatorAdapter,
			view::RowView,
			windowed::accumulator::{LastValue, Moments},
		},
		row,
		testing::{
			builders::{TestChangeBuilder, TestRowBuilder},
			harness::FFIOperatorHarnessBuilder,
		},
	};

	// Velocity-style operator: per-window value held last-write-wins; the
	// cross-window Running accumulator is Moments over ALL window values, and
	// the baseline is (Running minus the newest window) so the score
	// (newest / baseline_mean) is computed in O(1) from the running moments.

	#[derive(Clone, Debug, PartialEq)]
	struct TestOut {
		group: String,
		recent: f64,
		baseline: f64,
		windows: u32,
	}

	row!(TestOut {
		group: String,
		recent: f64,
		baseline: f64,
		windows: u32
	});

	struct TestVelocity {
		capacity: usize,
	}

	impl RollingOperator for TestVelocity {
		type GroupKey = String;
		type WindowCoord = u64;
		type WindowAcc = LastValue<f64>;
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

		// Reference combine over the buffer: baseline = mean of all-but-newest.
		fn combine(&self, group: &String, buffer: &BTreeMap<u64, LastValue<f64>>) -> Option<TestOut> {
			let (_, newest) = buffer.iter().next_back()?;
			let newest = (*newest.get()?) as f64;
			let mut sum = 0.0_f64;
			let mut count = 0u32;
			let total = buffer.len();
			for (i, acc) in buffer.values().enumerate() {
				if i + 1 == total {
					continue;
				}
				if let Some(v) = acc.get() {
					sum += *v;
					count += 1;
				}
			}
			let baseline = if count > 0 {
				sum / count as f64
			} else {
				0.0
			};
			Some(TestOut {
				group: group.clone(),
				recent: newest,
				baseline,
				windows: buffer.len() as u32,
			})
		}
	}

	impl RollingIncrementalOperator for TestVelocity {
		type Running = Moments;

		fn window_contribution(&self, window_value: &f64) -> f64 {
			*window_value
		}

		fn combine_running(
			&self,
			group: &String,
			running: &Moments,
			newest_value: &f64,
			_newest_coord: u64,
		) -> Option<TestOut> {
			// baseline = (running over ALL windows) minus the newest window.
			let total_count = running.count();
			let baseline_count = total_count.saturating_sub(1);
			let baseline = if baseline_count > 0 {
				(running.sum() - *newest_value) / baseline_count as f64
			} else {
				0.0
			};
			Some(TestOut {
				group: group.clone(),
				recent: *newest_value,
				baseline,
				windows: total_count as u32,
			})
		}
	}

	impl RollingRegistration for TestVelocity {
		const NAME: &'static str = "test_velocity_incremental";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

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
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("window_start", ValueType::Uint8),
			RowShapeField::unconstrained("value", ValueType::Float8),
		])
	}

	fn input_row(rn: u64, group: &str, window_start: u64, value: f64) -> CoreRow {
		TestRowBuilder::new(rn)
			.with_values(vec![Value::Utf8(group.into()), Value::Uint8(window_start), Value::float8(value)])
			.with_shape(input_shape())
			.build()
	}

	#[test]
	fn baseline_excludes_newest_window() {
		let mut h =
			FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingIncrementalDriver<TestVelocity>>>::new()
				.build()
				.expect("harness");
		// Windows 0=10, 60=20, 120=60. Newest=120 (recent 60); baseline =
		// mean(10, 20) = 15. Running moments maintained incrementally.
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "BTC", 60, 20.0))
				.insert(input_row(3, "BTC", 120, 60.0))
				.build())
			.expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("recent"), Some(60.0));
		assert_eq!(r.f64("baseline"), Some(15.0));
		assert_eq!(r.u32("windows"), Some(3));
	}

	#[test]
	fn update_window_value_keeps_running_consistent() {
		let mut h =
			FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingIncrementalDriver<TestVelocity>>>::new()
				.build()
				.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "BTC", 60, 20.0))
				.build())
			.expect("apply");
		// Update the NEWEST window (60) from 20 to 40. (Updating a buried
		// window like 0 would be dropped late, per the rolling contract.)
		// Running must reflect old->new: windows {10, 40}, newest=40,
		// baseline = mean(10) = 10.
		let out = h
			.apply(TestChangeBuilder::new()
				.update(input_row(2, "BTC", 60, 20.0), input_row(2, "BTC", 60, 40.0))
				.build())
			.expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("recent"), Some(40.0));
		assert_eq!(r.f64("baseline"), Some(10.0), "running updated old->new: baseline=mean(10)");
	}

	#[test]
	fn eviction_drops_oldest_from_running() {
		let mut h =
			FFIOperatorHarnessBuilder::<FFIOperatorAdapter<RollingIncrementalDriver<TestVelocity>>>::new()
				.build()
				.expect("harness");
		// Capacity 3; insert 4 windows. Window 0 (value 1) is evicted, so the
		// running moments must drop it: baseline = mean(2, 3) = 2.5, recent=4.
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 1.0))
				.insert(input_row(2, "BTC", 60, 2.0))
				.insert(input_row(3, "BTC", 120, 3.0))
				.insert(input_row(4, "BTC", 180, 4.0))
				.build())
			.expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("recent"), Some(4.0));
		assert_eq!(r.f64("baseline"), Some(2.5), "evicted window 0 removed from running");
		assert_eq!(r.u32("windows"), Some(3));
	}
}
