// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::{self, Debug, Formatter},
	hash::Hash,
};

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_core::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	interface::catalog::flow::FlowNodeId,
	key::flow_node_internal_state::FlowNodeInternalStateKey,
};
use reifydb_runtime::reifydb_assertions;
use reifydb_value::value::row_number::RowNumber;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
	config::Config,
	error::Result,
	operator::{
		OperatorLogic, OperatorMetadata,
		column::{
			batch::{InsertBatch, RemoveBatch, UpdateBatch},
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

type AccContribution<A> = <<A as MultiRollingOperator>::WindowAcc as WindowAccumulator>::Contribution;

pub trait MultiRollingOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type WindowCoord: Slot + Hash + Serialize + DeserializeOwned;

	type WindowAcc: WindowAccumulator;

	type SecondaryKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type Output: Clone + Debug + PartialEq + Serialize + DeserializeOwned;

	fn capacity(&self) -> usize;

	fn extract(&self, row: &impl RowView) -> Option<(Self::GroupKey, Self::WindowCoord, AccContribution<Self>)>;

	fn combine(
		&self,
		group: &Self::GroupKey,
		buffer: &BTreeMap<Self::WindowCoord, Self::WindowAcc>,
	) -> BTreeMap<Self::SecondaryKey, Self::Output>;
}

pub trait MultiRollingRegistration: MultiRollingOperator + Sized
where
	Self::Output: Row,
	for<'a> &'a Self::GroupKey: IntoEncodedKey,
{
	const NAME: &'static str;
	const VERSION: &'static str;
	const DESCRIPTION: &'static str;
	const INPUT_COLUMNS: &'static [OperatorColumn];
	const OUTPUT_COLUMNS: &'static [OperatorColumn];
	const CAPABILITIES: &'static [OperatorCapability];

	fn from_config(operator_id: FlowNodeId, config: &Config) -> Result<Self>;

	fn encode_state_key(&self, group: &Self::GroupKey) -> EncodedKey;

	fn encode_row_key(&self, group: &Self::GroupKey, secondary: &Self::SecondaryKey) -> EncodedKey;
}

pub type MultiRollingBuffer<A> =
	BTreeMap<<A as MultiRollingOperator>::WindowCoord, <A as MultiRollingOperator>::WindowAcc>;

pub type MultiRollingEmit<A> = BTreeMap<<A as MultiRollingOperator>::SecondaryKey, <A as MultiRollingOperator>::Output>;

type EmitDiffs<A> = (
	Vec<(RowNumber, <A as MultiRollingOperator>::Output)>,
	Vec<(RowNumber, <A as MultiRollingOperator>::Output, <A as MultiRollingOperator>::Output)>,
	Vec<(RowNumber, <A as MultiRollingOperator>::Output)>,
);

struct GroupSlot<A: MultiRollingOperator> {
	state_row_number: RowNumber,
	buffer: MultiRollingBuffer<A>,
	prior_emit: MultiRollingEmit<A>,
	buffer_changed: bool,
}

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
	A: MultiRollingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	groups: StateCache<RowNumber, GroupState<A>>,
	meta: StateCache<MetaKey, GroupMeta<A::WindowCoord>>,
}

enum AccEvent<A: MultiRollingOperator> {
	Add(AccContribution<A>),
	Remove(AccContribution<A>),
}

impl<A> OperatorMetadata for MultiRollingDriver<A>
where
	A: MultiRollingRegistration + 'static,
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

impl<A> OperatorLogic for MultiRollingDriver<A>
where
	A: MultiRollingRegistration + Send + Sync + 'static,
	A::Output: Row + Send + Sync,
	A::GroupKey: Send + Sync,
	A::WindowCoord: Send + Sync,
	A::WindowAcc: Send + Sync,
	A::SecondaryKey: Send + Sync,
	AccContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn create(operator_id: FlowNodeId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		Ok(Self {
			aggregator,
			groups: StateCache::<RowNumber, GroupState<A>>::new(8),
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
		let state_rows = self.resolve_state_rows(ctx, &buckets, &meta_loaded)?;
		let group_slots = self.apply_events_into_buffers(ctx, buckets, &mut meta_loaded, &state_rows)?;

		let (inserts, updates, removes) = self.diff_emits(ctx, group_slots)?;
		Self::emit_three_batches(ctx, &inserts, &updates, &removes)?;
		self.persist_meta(ctx, meta_loaded)
	}

	fn flush_state(&mut self, ctx: &mut impl OperatorContext) -> Result<()> {
		self.groups.flush(ctx)?;
		self.meta.flush(ctx)?;
		Ok(())
	}
}

impl<A> MultiRollingDriver<A>
where
	A: MultiRollingRegistration + Send + Sync + 'static,
	A::Output: Row + Send + Sync,
	A::GroupKey: Send + Sync,
	A::WindowCoord: Send + Sync,
	A::WindowAcc: Send + Sync,
	A::SecondaryKey: Send + Sync,
	AccContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	#[inline]
	#[allow(clippy::type_complexity)]
	fn route_diffs_to_buckets(
		&self,
		change: &impl ChangeView,
	) -> BTreeMap<(A::GroupKey, A::WindowCoord), Vec<AccEvent<A>>> {
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
							if let Some((group, coord, contribution)) =
								self.aggregator.extract(&row)
							{
								buckets.entry((group, coord))
									.or_default()
									.push(AccEvent::Add(contribution));
							}
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
							if let Some((group, coord, contribution)) =
								self.aggregator.extract(&row)
							{
								buckets.entry((group, coord))
									.or_default()
									.push(AccEvent::Remove(contribution));
							}
						}
					}
				}
			}
		}

		buckets
	}

	#[inline]
	#[allow(clippy::type_complexity)]
	fn warm_and_load_meta(
		&mut self,
		ctx: &mut impl OperatorContext,
		buckets: &BTreeMap<(A::GroupKey, A::WindowCoord), Vec<AccEvent<A>>>,
	) -> Result<HashMap<A::GroupKey, GroupMeta<A::WindowCoord>>> {
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
		Ok(meta_loaded)
	}

	#[inline]
	#[allow(clippy::type_complexity)]
	fn resolve_state_rows(
		&mut self,
		ctx: &mut impl OperatorContext,
		buckets: &BTreeMap<(A::GroupKey, A::WindowCoord), Vec<AccEvent<A>>>,
		meta_loaded: &HashMap<A::GroupKey, GroupMeta<A::WindowCoord>>,
	) -> Result<HashMap<A::GroupKey, RowNumber>> {
		let mut state_rows: HashMap<A::GroupKey, RowNumber> = HashMap::new();
		let mut resolve_order: Vec<A::GroupKey> = Vec::new();
		let mut state_lookup_keys: Vec<EncodedKey> = Vec::new();
		let mut seen: BTreeSet<A::GroupKey> = BTreeSet::new();
		for (group, coord) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.high_water);
			if initial_high_water.is_none_or(|hw| *coord >= hw) && seen.insert(group.clone()) {
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
		Ok(state_rows)
	}

	#[inline]
	#[allow(clippy::type_complexity)]
	fn apply_events_into_buffers(
		&mut self,
		ctx: &mut impl OperatorContext,
		buckets: BTreeMap<(A::GroupKey, A::WindowCoord), Vec<AccEvent<A>>>,
		meta_loaded: &mut HashMap<A::GroupKey, GroupMeta<A::WindowCoord>>,
		state_rows: &HashMap<A::GroupKey, RowNumber>,
	) -> Result<BTreeMap<A::GroupKey, GroupSlot<A>>> {
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

			let next_high_water = match meta.high_water {
				Some(hw) if hw > coord => hw,
				_ => coord,
			};
			reifydb_assertions! {
				assert!(
					next_high_water >= coord,
					"high_water regressed below the window coord it just admitted, so the next batch would \
					 treat an already-processed window as late and silently drop its events (coord={coord:?}, \
					 prev_high_water={prev:?}, next_high_water={next_high_water:?})",
					prev = meta.high_water
				);
				if let Some(prev) = meta.high_water {
					assert!(
						next_high_water >= prev,
						"high_water moved backwards across an admit, breaking the monotonic late-event \
						 cutoff that buried-window dropping relies on (coord={coord:?}, prev_high_water={prev:?}, \
						 next_high_water={next_high_water:?})"
					);
				}
			}
			meta.high_water = Some(next_high_water);
		}

		Ok(group_slots)
	}

	#[inline]
	fn diff_emits(
		&mut self,
		ctx: &mut impl OperatorContext,
		group_slots: BTreeMap<A::GroupKey, GroupSlot<A>>,
	) -> Result<EmitDiffs<A>> {
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

		Ok((inserts, updates, removes))
	}

	#[inline]
	fn emit_three_batches(
		ctx: &mut impl OperatorContext,
		inserts: &[(RowNumber, A::Output)],
		updates: &[(RowNumber, A::Output, A::Output)],
		removes: &[(RowNumber, A::Output)],
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
			for (rn, prior, new) in updates {
				batch.push(*rn, prior, new)?;
			}
			batch.finish()?;
		}
		if !removes.is_empty() {
			let mut batch = RemoveBatch::<A::Output, _>::new(ctx, removes.len())?;
			for (rn, data) in removes {
				batch.push(*rn, data)?;
			}
			batch.finish()?;
		}
		Ok(())
	}

	#[inline]
	fn persist_meta(
		&mut self,
		ctx: &mut impl OperatorContext,
		meta_loaded: HashMap<A::GroupKey, GroupMeta<A::WindowCoord>>,
	) -> Result<()> {
		for (group, meta) in meta_loaded {
			self.meta.set(ctx, &meta_key_for(&group), &meta)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::{cmp::Ordering, collections::BTreeMap};

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
			windowed::accumulator::{KeyedInvertibleAcc, Moments},
		},
		row,
		testing::{
			builders::{TestChangeBuilder, TestRowBuilder},
			harness::FFIOperatorHarnessBuilder,
		},
	};

	// Rolling top-2 traders by summed volume over the last 3 windows. Each
	// window cell is a KeyedInvertibleAcc<trader, Moments> so a trade's
	// volume accumulates per trader and an Update/Remove subtracts it
	// (invertible). combine merges all buffered windows' per-trader sums,
	// ranks by total volume, and emits the top 2 keyed by rank.

	#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
	struct TopOut {
		group: String,
		rank: u32,
		trader: u64,
		volume: f64,
	}

	row!(TopOut {
		group: String,
		rank: u32,
		trader: u64,
		volume: f64
	});

	struct TestTopVolume;

	impl MultiRollingOperator for TestTopVolume {
		type GroupKey = String;
		type WindowCoord = u64;
		type WindowAcc = KeyedInvertibleAcc<u64, Moments>;
		type SecondaryKey = u32;
		type Output = TopOut;

		fn capacity(&self) -> usize {
			3
		}

		fn extract(&self, row: &impl RowView) -> Option<(String, u64, (u64, f64))> {
			let group = row.utf8("group")?.to_string();
			let window_start = row.u64("window_start")?;
			let trader = row.u64("trader")?;
			let volume = row.f64("volume")?;
			Some((group, window_start, (trader, volume)))
		}

		fn combine(
			&self,
			group: &String,
			buffer: &BTreeMap<u64, KeyedInvertibleAcc<u64, Moments>>,
		) -> BTreeMap<u32, TopOut> {
			let mut totals: BTreeMap<u64, f64> = BTreeMap::new();
			for window in buffer.values() {
				if let Some(per_trader) = window.finalize() {
					for (trader, moments) in per_trader {
						*totals.entry(trader).or_insert(0.0) += moments.sum();
					}
				}
			}
			let mut ranked: Vec<(u64, f64)> = totals.into_iter().collect();
			ranked.sort_by(|a, b| {
				b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal).then_with(|| a.0.cmp(&b.0))
			});
			let mut out = BTreeMap::new();
			for (i, (trader, volume)) in ranked.into_iter().take(2).enumerate() {
				let rank = (i as u32) + 1;
				out.insert(
					rank,
					TopOut {
						group: group.clone(),
						rank,
						trader,
						volume,
					},
				);
			}
			out
		}
	}

	impl MultiRollingRegistration for TestTopVolume {
		const NAME: &'static str = "test_top_volume";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

		fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
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
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("window_start", ValueType::Uint8),
			RowShapeField::unconstrained("trader", ValueType::Uint8),
			RowShapeField::unconstrained("volume", ValueType::Float8),
		])
	}

	fn input_row(rn: u64, group: &str, window_start: u64, trader: u64, volume: f64) -> CoreRow {
		TestRowBuilder::new(rn)
			.with_values(vec![
				Value::Utf8(group.into()),
				Value::Uint8(window_start),
				Value::Uint8(trader),
				Value::float8(volume),
			])
			.with_shape(input_shape())
			.build()
	}

	#[test]
	fn same_window_volume_accumulates_per_trader() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
			.build()
			.expect("harness");
		// Two trades for trader 100 in one window must SUM (5+3=8), unlike
		// the old last-write-wins fold. Trader 200 has 9.
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "BTC", 0, 200, 9.0))
				.insert(input_row(3, "BTC", 0, 100, 3.0))
				.build())
			.expect("apply");
		let post = out.diffs[0].post().expect("post");
		let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
			.map(|i| {
				let r = post.row_ref(i).expect("row");
				(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
			})
			.collect();
		assert_eq!(by_rank.get(&1).copied(), Some((200u64, 9.0)), "trader 200 leads at 9.0");
		assert_eq!(by_rank.get(&2).copied(), Some((100u64, 8.0)), "trader 100 volume summed 5+3 = 8.0");
	}

	#[test]
	fn update_subtracts_old_volume_no_double_count() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "BTC", 0, 200, 9.0))
				.build())
			.expect("apply");
		// Update trader 100's trade 5 -> 20. The driver routes remove(5)+add(20)
		// so trader 100's total is 20, NOT 5+20=25. It overtakes trader 200.
		let out = h
			.apply(TestChangeBuilder::new()
				.update(input_row(1, "BTC", 0, 100, 5.0), input_row(1, "BTC", 0, 100, 20.0))
				.build())
			.expect("apply");
		let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
		assert!(kinds.contains(&DiffType::Update), "ranks changed, expect Update");
		let post = out.diffs.iter().find(|d| d.kind() == DiffType::Update).unwrap().post().expect("post");
		let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
			.map(|i| {
				let r = post.row_ref(i).expect("row");
				(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
			})
			.collect();
		assert_eq!(by_rank.get(&1).copied(), Some((100u64, 20.0)), "trader 100 now leads at 20, not 25");
	}

	#[test]
	fn top_2_across_three_windows() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
			.build()
			.expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "BTC", 60, 200, 9.0))
				.insert(input_row(3, "BTC", 120, 300, 7.0))
				.build())
			.expect("apply");
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 2);
		let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
			.map(|i| {
				let r = post.row_ref(i).expect("row");
				(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
			})
			.collect();
		assert_eq!(by_rank.get(&1).copied(), Some((200u64, 9.0)));
		assert_eq!(by_rank.get(&2).copied(), Some((300u64, 7.0)));
	}

	#[test]
	fn vanishing_rank_emits_remove_at_high_water() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 5.0))
				.insert(input_row(2, "BTC", 60, 200, 9.0))
				.build())
			.expect("apply");
		// Remove the newest window (wk=60 == high_water, not late). Trader
		// 200's only trade leaves -> its window empties and is dropped from
		// the buffer; rank-1 changes to trader 100, rank-2 vanishes -> Remove.
		let out = h
			.apply(TestChangeBuilder::new().remove(input_row(2, "BTC", 60, 200, 9.0)).build())
			.expect("apply");
		let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
		assert!(kinds.contains(&DiffType::Update), "rank-1 changed identity, expect Update");
		assert!(kinds.contains(&DiffType::Remove), "rank-2 vanished, expect Remove");
	}

	#[test]
	fn capacity_eviction_drops_oldest_window() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
			.build()
			.expect("harness");
		// Capacity 3; 4 windows. Window 0 (trader 100, vol 1) is evicted.
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 100, 1.0))
				.insert(input_row(2, "BTC", 60, 200, 8.0))
				.insert(input_row(3, "BTC", 120, 300, 2.0))
				.insert(input_row(4, "BTC", 180, 400, 5.0))
				.build())
			.expect("apply");
		let post = out.diffs[0].post().expect("post");
		let by_rank: BTreeMap<u32, (u64, f64)> = (0..post.row_count())
			.map(|i| {
				let r = post.row_ref(i).expect("row");
				(r.u32("rank").unwrap(), (r.u64("trader").unwrap(), r.f64("volume").unwrap()))
			})
			.collect();
		assert_eq!(by_rank.get(&1).copied(), Some((200u64, 8.0)));
		assert_eq!(by_rank.get(&2).copied(), Some((400u64, 5.0)), "window 0 evicted; trader 100 gone");
	}

	#[test]
	fn buried_window_insert_dropped_silently() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<MultiRollingDriver<TestTopVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 100, 5.0)).build())
			.expect("apply");
		let out = h
			.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 999, 999.0)).build())
			.expect("apply");
		assert_eq!(out.diffs.len(), 0, "insert below high-water dropped");
	}
}
