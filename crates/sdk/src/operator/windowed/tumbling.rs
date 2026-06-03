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
		windowed::{
			accumulator::WindowAccumulator,
			span::{Slot, WindowSpan},
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

type AccContribution<A> = <<A as TumblingOperator>::Acc as WindowAccumulator>::Contribution;
type AccValue<A> = <<A as TumblingOperator>::Acc as WindowAccumulator>::Output;
type Buckets<A> = BTreeMap<
	(<A as TumblingOperator>::GroupKey, WindowSpan<<A as TumblingOperator>::WindowCoord>),
	Vec<AccEvent<A>>,
>;
type MetaLoaded<A> = HashMap<<A as TumblingOperator>::GroupKey, GroupMeta<<A as TumblingOperator>::WindowCoord>>;
type SlotResolved = Vec<Option<(RowNumber, bool)>>;
type WindowOutputs<A> =
	(Vec<(RowNumber, <A as TumblingOperator>::Output)>, Vec<(RowNumber, <A as TumblingOperator>::Output)>);

pub trait TumblingOperator {
	type GroupKey: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned;

	type WindowCoord: Slot + Hash + Serialize + DeserializeOwned;

	type Acc: WindowAccumulator;

	type Output: Clone + Debug + PartialEq;

	fn extract(&self, row: &impl RowView) -> Option<(Self::GroupKey, Self::WindowCoord, AccContribution<Self>)>;

	fn window_for(&self, coord: Self::WindowCoord) -> WindowSpan<Self::WindowCoord>;

	fn build_output(
		&self,
		group: &Self::GroupKey,
		span: WindowSpan<Self::WindowCoord>,
		value: AccValue<Self>,
	) -> Option<Self::Output>;

	fn new_accumulator(&self) -> Self::Acc {
		Self::Acc::default()
	}
}

pub trait TumblingRegistration: TumblingOperator + Sized
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

	fn encode_row_key(&self, group: &Self::GroupKey, window_start: Self::WindowCoord) -> EncodedKey;
}

pub struct TumblingDriver<A>
where
	A: TumblingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	aggregator: A,
	accs: StateCache<RowNumber, A::Acc>,
	meta: StateCache<MetaKey, GroupMeta<A::WindowCoord>>,
}

enum AccEvent<A: TumblingOperator> {
	Add(AccContribution<A>),
	Remove(AccContribution<A>),
}

impl<A> TumblingDriver<A>
where
	A: TumblingRegistration,
	A::Output: Row,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn route(&self, change: &impl ChangeView) -> Buckets<A> {
		let mut buckets: Buckets<A> = BTreeMap::new();

		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			match diff.kind() {
				DiffType::Insert => {
					if let Some(cols) = diff.post() {
						self.push_all(&cols, &mut buckets, true);
					}
				}
				DiffType::Update => {
					if let (Some(pre), Some(post)) = (diff.pre(), diff.post()) {
						self.push_all(&pre, &mut buckets, false);
						self.push_all(&post, &mut buckets, true);
					}
				}
				DiffType::Remove => {
					if let Some(cols) = diff.pre() {
						self.push_all(&cols, &mut buckets, false);
					}
				}
			}
		}
		buckets
	}

	fn push_all<C: ColumnsView>(&self, cols: &C, buckets: &mut Buckets<A>, is_add: bool) {
		for i in 0..cols.row_count() {
			let Some(row) = cols.row(i) else {
				continue;
			};
			let Some((group, coord, contribution)) = self.aggregator.extract(&row) else {
				continue;
			};
			let span = self.aggregator.window_for(coord);
			let event = if is_add {
				AccEvent::Add(contribution)
			} else {
				AccEvent::Remove(contribution)
			};
			buckets.entry((group, span)).or_default().push(event);
		}
	}
}

impl<A> TumblingDriver<A>
where
	A: TumblingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowCoord: Send + Sync,
	A::Acc: Send + Sync,
	AccContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	#[inline]
	fn warm_and_load_meta(
		&mut self,
		ctx: &mut impl OperatorContext,
		buckets: &Buckets<A>,
	) -> Result<MetaLoaded<A>> {
		let meta_keys: Vec<MetaKey> = buckets
			.keys()
			.map(|(group, _)| group)
			.collect::<BTreeSet<_>>()
			.into_iter()
			.map(meta_key_for)
			.collect();
		self.meta.warm(ctx, &meta_keys)?;

		let mut meta_loaded: MetaLoaded<A> = HashMap::new();
		for (group, _) in buckets.keys() {
			if !meta_loaded.contains_key(group) {
				let m = self.meta.get(ctx, &meta_key_for(group))?.unwrap_or_default();
				meta_loaded.insert(group.clone(), m);
			}
		}
		Ok(meta_loaded)
	}

	#[inline]
	fn resolve_survivor_rows(
		&mut self,
		ctx: &mut impl OperatorContext,
		buckets: &Buckets<A>,
		meta_loaded: &MetaLoaded<A>,
	) -> Result<SlotResolved> {
		let mut survivor_keys: Vec<EncodedKey> = Vec::new();
		let mut slot_survives: Vec<bool> = Vec::with_capacity(buckets.len());
		for (group, span) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.high_water);
			let survives = initial_high_water.is_none_or(|hw| span.start >= hw);
			slot_survives.push(survives);
			if survives {
				survivor_keys.push(self.aggregator.encode_row_key(group, span.start));
			}
		}
		let resolved_rows = ctx.get_or_create_row_numbers(&survivor_keys)?;
		reifydb_assertions! {
			let survivors = survivor_keys.len();
			let resolved = resolved_rows.len();
			assert!(
				resolved == survivors,
				"get_or_create_row_numbers must return exactly one row per survivor key; a short batch would \
				 leave a surviving slot with no resolved row, so the slot_resolved zip below pairs it with None \
				 and apply_events_build_outputs silently re-creates a fresh row instead of reusing the existing \
				 window state, double-counting it (survivor_keys={survivors}, resolved_rows={resolved})"
			);
		}
		let acc_keys: Vec<RowNumber> = resolved_rows.iter().map(|(rn, _)| *rn).collect();
		self.accs.warm(ctx, &acc_keys)?;
		let mut resolved_rows = resolved_rows.into_iter();
		let slot_resolved: SlotResolved = slot_survives
			.into_iter()
			.map(|survives| {
				if survives {
					resolved_rows.next()
				} else {
					None
				}
			})
			.collect();
		Ok(slot_resolved)
	}

	#[inline]
	fn apply_events_build_outputs(
		&mut self,
		ctx: &mut impl OperatorContext,
		buckets: Buckets<A>,
		slot_resolved: SlotResolved,
		meta_loaded: &mut MetaLoaded<A>,
	) -> Result<WindowOutputs<A>> {
		let mut inserts: Vec<(RowNumber, A::Output)> = Vec::new();
		let mut updates: Vec<(RowNumber, A::Output)> = Vec::new();

		for (((group, span), events), slot_pre) in buckets.into_iter().zip(slot_resolved) {
			let entry = meta_loaded.entry(group.clone()).or_default();
			match entry.high_water {
				Some(hw) if span.start < hw => continue,
				Some(hw) if span.start > hw => entry.high_water = Some(span.start),
				Some(_) => {}
				None => entry.high_water = Some(span.start),
			}

			let (row_number, is_new) = match slot_pre {
				Some(resolved) => resolved,
				None => {
					let key = self.aggregator.encode_row_key(&group, span.start);
					ctx.get_or_create_row_number(&key)?
				}
			};

			let mut acc: A::Acc =
				self.accs.get(ctx, &row_number)?.unwrap_or_else(|| self.aggregator.new_accumulator());
			let was_empty_before = acc.is_empty();

			for event in events {
				match event {
					AccEvent::Add(c) => acc.add(&c),
					AccEvent::Remove(c) => acc.remove(&c),
				}
			}

			let output = acc.finalize().and_then(|value| self.aggregator.build_output(&group, span, value));
			self.accs.put(ctx, &row_number, acc)?;

			if let Some(out) = output {
				if is_new || was_empty_before {
					inserts.push((row_number, out));
				} else {
					updates.push((row_number, out));
				}
			}
		}
		Ok((inserts, updates))
	}

	#[inline]
	fn emit_insert_update_batches(
		&self,
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
	fn persist_meta(&mut self, ctx: &mut impl OperatorContext, meta_loaded: MetaLoaded<A>) -> Result<()> {
		for (group, meta) in meta_loaded {
			self.meta.set(ctx, &meta_key_for(&group), &meta)?;
		}
		Ok(())
	}
}

impl<A> OperatorMetadata for TumblingDriver<A>
where
	A: TumblingRegistration + 'static,
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

impl<A> OperatorLogic for TumblingDriver<A>
where
	A: TumblingRegistration + Send + Sync + 'static,
	A::Output: Row,
	A::GroupKey: Send + Sync,
	A::WindowCoord: Send + Sync,
	A::Acc: Send + Sync,
	AccContribution<A>: Send + Sync,
	for<'a> &'a A::GroupKey: IntoEncodedKey,
{
	fn create(operator_id: FlowNodeId, config: &Config) -> Result<Self> {
		let aggregator = A::from_config(operator_id, config)?;
		Ok(Self {
			aggregator,
			accs: StateCache::<RowNumber, A::Acc>::new(8),
			meta: StateCache::<MetaKey, GroupMeta<A::WindowCoord>>::new_internal(64),
		})
	}

	#[allow(clippy::type_complexity)]
	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> Result<()> {
		let buckets = self.route(&change);
		if buckets.is_empty() {
			return Ok(());
		}

		let mut meta_loaded = self.warm_and_load_meta(ctx, &buckets)?;
		let slot_resolved = self.resolve_survivor_rows(ctx, &buckets, &meta_loaded)?;
		let (inserts, updates) =
			self.apply_events_build_outputs(ctx, buckets, slot_resolved, &mut meta_loaded)?;
		self.emit_insert_update_batches(ctx, &inserts, &updates)?;
		self.persist_meta(ctx, meta_loaded)?;

		Ok(())
	}

	fn flush_state(&mut self, ctx: &mut impl OperatorContext) -> Result<()> {
		self.accs.flush(ctx)?;
		self.meta.flush(ctx)?;
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
	use serde::{Deserialize, Serialize};

	use super::*;
	use crate::{
		operator::{
			FFIOperatorAdapter,
			view::RowView,
			windowed::accumulator::{Moments, Multiset, OrdF64},
		},
		row,
		testing::{
			builders::{TestChangeBuilder, TestRowBuilder},
			harness::FFIOperatorHarnessBuilder,
		},
	};

	// An invertible volume aggregator. Its accumulator keeps only running
	// Moments (no per-slot map): Insert adds, Update is routed by the driver
	// as remove(pre)+add(post), Remove subtracts. This is the case the old
	// per-slot map existed to handle and that the pre/post diff now subsumes.

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	struct VolumeAcc {
		moments: Moments,
	}

	impl WindowAccumulator for VolumeAcc {
		type Contribution = f64;
		type Output = OrdF64;

		fn add(&mut self, contribution: &f64) {
			self.moments.add(*contribution);
		}

		fn remove(&mut self, contribution: &f64) {
			self.moments.remove(*contribution);
		}

		fn finalize(&self) -> Option<OrdF64> {
			(!self.moments.is_empty()).then(|| OrdF64::new(self.moments.sum()).expect("finite"))
		}

		fn is_empty(&self) -> bool {
			self.moments.is_empty()
		}
	}

	#[derive(Clone, Debug, PartialEq)]
	struct VolumeOut {
		group: String,
		window_start: u64,
		volume: f64,
	}

	row!(VolumeOut {
		group: String,
		window_start: u64,
		volume: f64
	});

	struct TestVolume;

	impl TumblingOperator for TestVolume {
		type GroupKey = String;
		type WindowCoord = u64;
		type Acc = VolumeAcc;
		type Output = VolumeOut;

		fn extract(&self, row: &impl RowView) -> Option<(String, u64, f64)> {
			let group = row.utf8("group")?.to_string();
			let slot = row.u64("slot")?;
			let size = row.f64("size")?;
			Some((group, slot, size))
		}

		fn window_for(&self, coord: u64) -> WindowSpan<u64> {
			WindowSpan::for_slot(coord, 60)
		}

		fn build_output(&self, group: &String, span: WindowSpan<u64>, value: OrdF64) -> Option<VolumeOut> {
			Some(VolumeOut {
				group: group.clone(),
				window_start: span.start,
				volume: value.get(),
			})
		}
	}

	impl TumblingRegistration for TestVolume {
		const NAME: &'static str = "test_volume";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

		fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
			Ok(Self)
		}

		fn encode_row_key(&self, group: &String, window_start: u64) -> EncodedKey {
			EncodedKey::builder().str(group).u64(window_start).build()
		}
	}

	// A removal-safe minimum aggregator over an ordered multiset. Demonstrates
	// the non-invertible family: an Update that replaces the current minimum
	// with a larger value must raise the window minimum, which a scalar
	// running-min could not do.

	#[derive(Clone, Debug, Default, Serialize, Deserialize)]
	struct MinAcc {
		values: Multiset<OrdF64>,
	}

	impl WindowAccumulator for MinAcc {
		type Contribution = OrdF64;
		type Output = OrdF64;

		fn add(&mut self, contribution: &OrdF64) {
			self.values.add(*contribution);
		}

		fn remove(&mut self, contribution: &OrdF64) {
			self.values.remove(contribution);
		}

		fn finalize(&self) -> Option<OrdF64> {
			self.values.min().copied()
		}

		fn is_empty(&self) -> bool {
			self.values.is_empty()
		}
	}

	#[derive(Clone, Debug, PartialEq)]
	struct MinOut {
		group: String,
		window_start: u64,
		min: f64,
	}

	row!(MinOut {
		group: String,
		window_start: u64,
		min: f64
	});

	struct TestMin;

	impl TumblingOperator for TestMin {
		type GroupKey = String;
		type WindowCoord = u64;
		type Acc = MinAcc;
		type Output = MinOut;

		fn extract(&self, row: &impl RowView) -> Option<(String, u64, OrdF64)> {
			let group = row.utf8("group")?.to_string();
			let slot = row.u64("slot")?;
			let size = row.f64("size")?;
			Some((group, slot, OrdF64::new(size)?))
		}

		fn window_for(&self, coord: u64) -> WindowSpan<u64> {
			WindowSpan::for_slot(coord, 60)
		}

		fn build_output(&self, group: &String, span: WindowSpan<u64>, value: OrdF64) -> Option<MinOut> {
			Some(MinOut {
				group: group.clone(),
				window_start: span.start,
				min: value.get(),
			})
		}
	}

	impl TumblingRegistration for TestMin {
		const NAME: &'static str = "test_min";
		const VERSION: &'static str = "0.0.1";
		const DESCRIPTION: &'static str = "test fixture";
		const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
		const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;

		fn from_config(_operator_id: FlowNodeId, _config: &Config) -> Result<Self> {
			Ok(Self)
		}

		fn encode_row_key(&self, group: &String, window_start: u64) -> EncodedKey {
			EncodedKey::builder().str(group).u64(window_start).build()
		}
	}

	fn input_shape() -> RowShape {
		RowShape::new(vec![
			RowShapeField::unconstrained("group", ValueType::Utf8),
			RowShapeField::unconstrained("slot", ValueType::Uint8),
			RowShapeField::unconstrained("size", ValueType::Float8),
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
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Insert);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.utf8("group").as_deref(), Some("BTC"));
		assert_eq!(r.u64("window_start"), Some(0));
		assert_eq!(r.f64("volume"), Some(10.0));
	}

	#[test]
	fn update_applies_post_minus_pre_no_double_count() {
		// The crux of the redesign: an Update carries pre=10, post=25.
		// The driver routes it as remove(10)+add(25) on a running sum,
		// yielding 25 - not 10 + 25 = 35 - with NO per-slot map.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
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
	fn two_contributions_then_remove_subtracts_pre() {
		// Two distinct slots in one window sum to 15; a Remove carrying
		// pre=5 subtracts that contribution, leaving 10. No slot key is
		// needed - the diff's pre value is what gets subtracted.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 10.0))
				.insert(input_row(2, "BTC", 30, 5.0))
				.build())
			.expect("apply");
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
		// Locked decision: an emptied window emits nothing (no downstream
		// Remove). The accumulator is empty; finalize returns None.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 10.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 0);
	}

	#[test]
	fn boundary_slot_belongs_to_next_window() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let out = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 59, 1.0))
				.insert(input_row(2, "BTC", 60, 1.0))
				.build())
			.expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let post = out.diffs[0].post().expect("post");
		assert_eq!(post.row_count(), 2);
		assert_eq!(post.row_ref(0).expect("r0").u64("window_start"), Some(0));
		assert_eq!(post.row_ref(1).expect("r1").u64("window_start"), Some(60));
	}

	#[test]
	fn late_event_for_closed_window_dropped() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
			.build()
			.expect("harness");
		let _ = h.apply(TestChangeBuilder::new().insert(input_row(1, "BTC", 60, 5.0)).build()).expect("apply");
		let out =
			h.apply(TestChangeBuilder::new().insert(input_row(2, "BTC", 0, 99.0)).build()).expect("apply");
		assert_eq!(out.diffs.len(), 0);
	}

	#[test]
	fn multiple_groups_isolate_state() {
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestVolume>>>::new()
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
		assert_eq!(post.row_ref(0).expect("r0").f64("volume"), Some(10.0));
		assert_eq!(post.row_ref(1).expect("r1").utf8("group").as_deref(), Some("ETH"));
		assert_eq!(post.row_ref(1).expect("r1").f64("volume"), Some(50.0));
	}

	#[test]
	fn min_update_replacing_minimum_raises_window_min() {
		// The removal-safe multiset case: window holds {5, 8, 6}, min = 5.
		// An Update replacing the 5 with 10 must raise the min to 6. A
		// running scalar min cannot do this; the multiset remove(5)+add(10)
		// leaves {6, 8, 10}.
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestMin>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 5.0))
				.insert(input_row(2, "BTC", 10, 8.0))
				.insert(input_row(3, "BTC", 20, 6.0))
				.build())
			.expect("apply");
		let out = h
			.apply(TestChangeBuilder::new()
				.update(input_row(1, "BTC", 0, 5.0), input_row(1, "BTC", 0, 10.0))
				.build())
			.expect("apply");
		assert_eq!(out.diffs.len(), 1);
		let diff = &out.diffs[0];
		assert_eq!(diff.kind(), DiffType::Update);
		let r = diff.post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("min"), Some(6.0));
	}

	#[test]
	fn min_remove_duplicate_keeps_value_until_last_removed() {
		// Two events share value 5. Removing one occurrence must keep the
		// min at 5 (the multiset still holds one 5).
		let mut h = FFIOperatorHarnessBuilder::<FFIOperatorAdapter<TumblingDriver<TestMin>>>::new()
			.build()
			.expect("harness");
		let _ = h
			.apply(TestChangeBuilder::new()
				.insert(input_row(1, "BTC", 0, 5.0))
				.insert(input_row(2, "BTC", 10, 5.0))
				.insert(input_row(3, "BTC", 20, 9.0))
				.build())
			.expect("apply");
		let out = h.apply(TestChangeBuilder::new().remove(input_row(1, "BTC", 0, 5.0)).build()).expect("apply");
		let r = out.diffs[0].post().expect("post").row_ref(0).expect("r0");
		assert_eq!(r.f64("min"), Some(5.0), "one occurrence of 5 remains, min stays 5");
	}
}
