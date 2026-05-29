// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, BTreeSet, HashMap};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	row::Row as CoreRow,
	value::column::columns::Columns,
};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use super::{
	context::ChaosContext,
	event::{ChaosBatch, ChaosEvent},
	materialize::materialize_history,
	oracle::MaterializedTable,
};
use crate::operator::{
	column::{row::Row, sink::native::NativeRowSink},
	view::{ColumnsView, native::NativeColumnsView},
	windowed::{
		accumulator::WindowAccumulator, multi_rolling::MultiRollingOperator, rolling::RollingOperator,
		rolling_incremental::RollingIncrementalOperator, span::WindowSpan, tumbling::TumblingOperator,
		tumbling_carry::TumblingCarryOperator,
	},
};

type Coord<A> = <A as TumblingOperator>::WindowCoord;
type Group<A> = <A as TumblingOperator>::GroupKey;
type WindowKey<A> = (Group<A>, Coord<A>);

pub fn tumbling_accumulator_oracle<A>(
	agg: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedTable
where
	A: TumblingOperator,
	A::Output: Row,
{
	let mut accs: HashMap<WindowKey<A>, A::Acc> = HashMap::new();
	let mut spans: HashMap<WindowKey<A>, WindowSpan<Coord<A>>> = HashMap::new();
	let mut high_water: HashMap<Group<A>, Coord<A>> = HashMap::new();
	let mut last_visible: HashMap<WindowKey<A>, A::Output> = HashMap::new();

	for batch in batches {
		let snapshot = high_water.clone();
		let mut touched: BTreeSet<WindowKey<A>> = BTreeSet::new();

		for event in &batch.events {
			match event {
				ChaosEvent::Insert {
					row,
					..
				} => apply_leg(agg, row, true, &snapshot, &mut accs, &mut spans, &mut touched),
				ChaosEvent::Update {
					pre,
					post,
					..
				} => {
					apply_leg(agg, pre, false, &snapshot, &mut accs, &mut spans, &mut touched);
					apply_leg(agg, post, true, &snapshot, &mut accs, &mut spans, &mut touched);
				}
				ChaosEvent::Remove {
					row,
					..
				} => apply_leg(agg, row, false, &snapshot, &mut accs, &mut spans, &mut touched),
			}
		}

		for key in touched {
			let hw = high_water.entry(key.0.clone()).or_insert(key.1);
			if key.1 > *hw {
				*hw = key.1;
			}
			if let Some(acc) = accs.get(&key)
				&& let Some(value) = acc.finalize()
				&& let Some(span) = spans.get(&key).copied()
				&& let Some(out) = agg.build_output(&key.0, span, value)
			{
				last_visible.insert(key.clone(), out);
			}
		}
	}

	materialize_outputs(last_visible.into_values(), ctx.now_nanos(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn apply_leg<A>(
	agg: &A,
	row: &CoreRow,
	is_add: bool,
	snapshot: &HashMap<Group<A>, Coord<A>>,
	accs: &mut HashMap<WindowKey<A>, A::Acc>,
	spans: &mut HashMap<WindowKey<A>, WindowSpan<Coord<A>>>,
	touched: &mut BTreeSet<WindowKey<A>>,
) where
	A: TumblingOperator,
{
	let Some((group, coord, contribution)) = extract_one(agg, row) else {
		return;
	};
	let span = agg.window_for(coord);
	let survives = snapshot.get(&group).is_none_or(|hw| span.start >= *hw);
	if !survives {
		return;
	}
	let key = (group, span.start);
	spans.insert(key.clone(), span);
	let acc = accs.entry(key.clone()).or_insert_with(|| agg.new_accumulator());
	if is_add {
		acc.add(&contribution);
	} else {
		acc.remove(&contribution);
	}
	touched.insert(key);
}

#[allow(clippy::type_complexity)]
fn extract_one<A>(agg: &A, row: &CoreRow) -> Option<(Group<A>, Coord<A>, <A::Acc as WindowAccumulator>::Contribution)>
where
	A: TumblingOperator,
{
	let columns = Columns::from_row(row);
	let view = NativeColumnsView::new(&columns);
	let row_view = view.row(0)?;
	agg.extract(&row_view)
}

fn materialize_outputs<O: Row>(
	outputs: impl Iterator<Item = O>,
	now_nanos: u64,
	output_key_columns: &[String],
) -> MaterializedTable {
	let mut sink = NativeRowSink::new(<O as Row>::COLUMNS).expect("output sink");
	let mut row_numbers: Vec<RowNumber> = Vec::new();
	let mut count = 0u64;
	for output in outputs {
		output.encode_into(&mut sink).expect("encode output");
		count += 1;
		row_numbers.push(RowNumber(count));
	}
	if count == 0 {
		return MaterializedTable::empty();
	}
	let columns = sink.finish(row_numbers, now_nanos).expect("finish sink");
	let change = Change::from_flow(
		FlowNodeId(0),
		CommitVersion(0),
		vec![Diff::insert(columns)],
		DateTime::from_nanos(now_nanos),
	);
	materialize_history(&[change], output_key_columns)
}

type RollingCoord<A> = <A as RollingOperator>::WindowCoord;
type RollingGroup<A> = <A as RollingOperator>::GroupKey;

type RollingContribution<A> = <<A as RollingOperator>::WindowAcc as WindowAccumulator>::Contribution;
type RollingBuckets<A> = BTreeMap<(RollingGroup<A>, RollingCoord<A>), Vec<Leg<RollingContribution<A>>>>;

enum Leg<C> {
	Add(C),
	Remove(C),
}

fn bucket_rolling<A>(agg: &A, batch: &ChaosBatch) -> RollingBuckets<A>
where
	A: RollingOperator,
{
	let mut buckets: RollingBuckets<A> = BTreeMap::new();
	for event in &batch.events {
		match event {
			ChaosEvent::Insert {
				row,
				..
			} => push_rolling(agg, row, true, &mut buckets),
			ChaosEvent::Update {
				pre,
				post,
				..
			} => {
				push_rolling(agg, pre, false, &mut buckets);
				push_rolling(agg, post, true, &mut buckets);
			}
			ChaosEvent::Remove {
				row,
				..
			} => push_rolling(agg, row, false, &mut buckets),
		}
	}
	buckets
}

fn push_rolling<A>(agg: &A, row: &CoreRow, is_add: bool, buckets: &mut RollingBuckets<A>)
where
	A: RollingOperator,
{
	if let Some((group, coord, contribution)) = extract_rolling(agg, row) {
		let leg = if is_add {
			Leg::Add(contribution)
		} else {
			Leg::Remove(contribution)
		};
		buckets.entry((group, coord)).or_default().push(leg);
	}
}

#[allow(clippy::type_complexity)]
fn apply_rolling_buckets<A>(
	capacity: usize,
	snapshot: &HashMap<RollingGroup<A>, RollingCoord<A>>,
	buckets: RollingBuckets<A>,
	buffers: &mut HashMap<RollingGroup<A>, BTreeMap<RollingCoord<A>, A::WindowAcc>>,
	high_water: &mut HashMap<RollingGroup<A>, RollingCoord<A>>,
) -> BTreeSet<RollingGroup<A>>
where
	A: RollingOperator,
{
	let mut touched: BTreeSet<RollingGroup<A>> = BTreeSet::new();
	for ((group, coord), legs) in buckets {
		if snapshot.get(&group).is_some_and(|hw| coord < *hw) {
			continue;
		}
		let buffer = buffers.entry(group.clone()).or_default();
		let mut acc = buffer.remove(&coord).unwrap_or_default();
		for leg in legs {
			match leg {
				Leg::Add(c) => acc.add(&c),
				Leg::Remove(c) => acc.remove(&c),
			}
		}
		if !acc.is_empty() {
			buffer.insert(coord, acc);
		}
		while buffer.len() > capacity {
			buffer.pop_first();
		}
		high_water
			.entry(group.clone())
			.and_modify(|hw| {
				if coord > *hw {
					*hw = coord;
				}
			})
			.or_insert(coord);
		touched.insert(group);
	}
	touched
}

pub fn rolling_accumulator_oracle<A>(
	agg: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedTable
where
	A: RollingOperator,
	A::Output: Row,
{
	let capacity = agg.capacity();
	let mut buffers: HashMap<RollingGroup<A>, BTreeMap<RollingCoord<A>, A::WindowAcc>> = HashMap::new();
	let mut high_water: HashMap<RollingGroup<A>, RollingCoord<A>> = HashMap::new();
	let mut last_visible: HashMap<RollingGroup<A>, A::Output> = HashMap::new();

	for batch in batches {
		let snapshot = high_water.clone();
		let buckets = bucket_rolling(agg, batch);
		let touched = apply_rolling_buckets::<A>(capacity, &snapshot, buckets, &mut buffers, &mut high_water);
		for group in touched {
			if let Some(buffer) = buffers.get(&group)
				&& let Some(out) = agg.combine(&group, buffer)
			{
				last_visible.insert(group, out);
			}
		}
	}

	materialize_outputs(last_visible.into_values(), ctx.now_nanos(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn extract_rolling<A>(
	agg: &A,
	row: &CoreRow,
) -> Option<(RollingGroup<A>, RollingCoord<A>, <A::WindowAcc as WindowAccumulator>::Contribution)>
where
	A: RollingOperator,
{
	let columns = Columns::from_row(row);
	let view = NativeColumnsView::new(&columns);
	let row_view = view.row(0)?;
	agg.extract(&row_view)
}

pub fn rolling_incremental_accumulator_oracle<A>(
	agg: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedTable
where
	A: RollingIncrementalOperator,
	A::Output: Row,
{
	let capacity = agg.capacity();
	let mut buffers: HashMap<RollingGroup<A>, BTreeMap<RollingCoord<A>, A::WindowAcc>> = HashMap::new();
	let mut high_water: HashMap<RollingGroup<A>, RollingCoord<A>> = HashMap::new();
	let mut last_visible: HashMap<RollingGroup<A>, A::Output> = HashMap::new();

	for batch in batches {
		let snapshot = high_water.clone();
		let buckets = bucket_rolling(agg, batch);
		let touched = apply_rolling_buckets::<A>(capacity, &snapshot, buckets, &mut buffers, &mut high_water);
		for group in touched {
			let Some(buffer) = buffers.get(&group) else {
				continue;
			};
			let mut running = A::Running::default();
			for acc in buffer.values() {
				if let Some(value) = acc.finalize() {
					running.add(&agg.window_contribution(&value));
				}
			}
			if let Some((coord, acc)) = buffer.iter().next_back()
				&& let Some(newest) = acc.finalize()
				&& let Some(out) = agg.combine_running(&group, &running, &newest, *coord)
			{
				last_visible.insert(group, out);
			}
		}
	}

	materialize_outputs(last_visible.into_values(), ctx.now_nanos(), output_key_columns)
}

type CarryCoord<A> = <A as TumblingCarryOperator>::WindowCoord;
type CarryGroup<A> = <A as TumblingCarryOperator>::GroupKey;
type CarryWindowKey<A> = (CarryGroup<A>, CarryCoord<A>);

struct GroupCarry<K, C> {
	high_water: Option<K>,
	carry_for_current: Option<C>,
	current_window_carry: Option<C>,
}

impl<K, C> Default for GroupCarry<K, C> {
	fn default() -> Self {
		Self {
			high_water: None,
			carry_for_current: None,
			current_window_carry: None,
		}
	}
}

pub fn tumbling_carry_accumulator_oracle<A>(
	agg: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedTable
where
	A: TumblingCarryOperator,
	A::Output: Row,
{
	let mut accs: HashMap<CarryWindowKey<A>, A::Acc> = HashMap::new();
	let mut spans: HashMap<CarryWindowKey<A>, WindowSpan<CarryCoord<A>>> = HashMap::new();
	let mut carry: HashMap<CarryGroup<A>, GroupCarry<CarryCoord<A>, A::Carry>> = HashMap::new();
	let mut last_visible: HashMap<CarryWindowKey<A>, A::Output> = HashMap::new();

	for batch in batches {
		let snapshot: HashMap<CarryGroup<A>, CarryCoord<A>> =
			carry.iter().filter_map(|(g, c)| c.high_water.map(|hw| (g.clone(), hw))).collect();
		let mut touched: BTreeSet<CarryWindowKey<A>> = BTreeSet::new();

		for event in &batch.events {
			match event {
				ChaosEvent::Insert {
					row,
					..
				} => apply_carry_leg(agg, row, true, &snapshot, &mut accs, &mut spans, &mut touched),
				ChaosEvent::Update {
					pre,
					post,
					..
				} => {
					apply_carry_leg(
						agg,
						pre,
						false,
						&snapshot,
						&mut accs,
						&mut spans,
						&mut touched,
					);
					apply_carry_leg(
						agg,
						post,
						true,
						&snapshot,
						&mut accs,
						&mut spans,
						&mut touched,
					);
				}
				ChaosEvent::Remove {
					row,
					..
				} => apply_carry_leg(agg, row, false, &snapshot, &mut accs, &mut spans, &mut touched),
			}
		}

		for key in touched {
			let meta = carry.entry(key.0.clone()).or_default();
			match meta.high_water {
				Some(hw) if key.1 < hw => continue,
				Some(hw) if key.1 > hw => {
					meta.carry_for_current = meta.current_window_carry.take();
					meta.high_water = Some(key.1);
				}
				Some(_) => {}
				None => meta.high_water = Some(key.1),
			}
			let prev_carry = meta.carry_for_current.clone();

			if let Some(acc) = accs.get(&key)
				&& let Some(value) = acc.finalize()
				&& let Some(span) = spans.get(&key).copied()
				&& let Some(out) = agg.build_output(&key.0, span, &value, prev_carry.as_ref())
			{
				last_visible.insert(key.clone(), out);
				if let Some(new_carry) = agg.carry_forward(&value, prev_carry.as_ref()) {
					carry.entry(key.0.clone()).or_default().current_window_carry = Some(new_carry);
				}
			}
		}
	}

	materialize_outputs(last_visible.into_values(), ctx.now_nanos(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn apply_carry_leg<A>(
	agg: &A,
	row: &CoreRow,
	is_add: bool,
	snapshot: &HashMap<CarryGroup<A>, CarryCoord<A>>,
	accs: &mut HashMap<CarryWindowKey<A>, A::Acc>,
	spans: &mut HashMap<CarryWindowKey<A>, WindowSpan<CarryCoord<A>>>,
	touched: &mut BTreeSet<CarryWindowKey<A>>,
) where
	A: TumblingCarryOperator,
{
	let Some((group, coord, contribution)) = extract_carry(agg, row) else {
		return;
	};
	let span = agg.window_for(coord);
	let survives = snapshot.get(&group).is_none_or(|hw| span.start >= *hw);
	if !survives {
		return;
	}
	let key = (group, span.start);
	spans.insert(key.clone(), span);
	let acc = accs.entry(key.clone()).or_insert_with(|| agg.new_accumulator());
	if is_add {
		acc.add(&contribution);
	} else {
		acc.remove(&contribution);
	}
	touched.insert(key);
}

type MultiCoord<A> = <A as MultiRollingOperator>::WindowCoord;
type MultiGroup<A> = <A as MultiRollingOperator>::GroupKey;
type MultiContribution<A> = <<A as MultiRollingOperator>::WindowAcc as WindowAccumulator>::Contribution;
type MultiBuckets<A> = BTreeMap<(MultiGroup<A>, MultiCoord<A>), Vec<Leg<MultiContribution<A>>>>;

fn bucket_multi<A>(agg: &A, batch: &ChaosBatch) -> MultiBuckets<A>
where
	A: MultiRollingOperator,
{
	let mut buckets: MultiBuckets<A> = BTreeMap::new();
	for event in &batch.events {
		match event {
			ChaosEvent::Insert {
				row,
				..
			} => push_multi(agg, row, true, &mut buckets),
			ChaosEvent::Update {
				pre,
				post,
				..
			} => {
				push_multi(agg, pre, false, &mut buckets);
				push_multi(agg, post, true, &mut buckets);
			}
			ChaosEvent::Remove {
				row,
				..
			} => push_multi(agg, row, false, &mut buckets),
		}
	}
	buckets
}

fn push_multi<A>(agg: &A, row: &CoreRow, is_add: bool, buckets: &mut MultiBuckets<A>)
where
	A: MultiRollingOperator,
{
	if let Some((group, coord, contribution)) = extract_multi(agg, row) {
		let leg = if is_add {
			Leg::Add(contribution)
		} else {
			Leg::Remove(contribution)
		};
		buckets.entry((group, coord)).or_default().push(leg);
	}
}

#[allow(clippy::type_complexity)]
fn apply_multi_buckets<A>(
	capacity: usize,
	snapshot: &HashMap<MultiGroup<A>, MultiCoord<A>>,
	buckets: MultiBuckets<A>,
	buffers: &mut HashMap<MultiGroup<A>, BTreeMap<MultiCoord<A>, A::WindowAcc>>,
	high_water: &mut HashMap<MultiGroup<A>, MultiCoord<A>>,
) -> BTreeSet<MultiGroup<A>>
where
	A: MultiRollingOperator,
{
	let mut touched: BTreeSet<MultiGroup<A>> = BTreeSet::new();
	for ((group, coord), legs) in buckets {
		if snapshot.get(&group).is_some_and(|hw| coord < *hw) {
			continue;
		}
		let buffer = buffers.entry(group.clone()).or_default();
		let mut acc = buffer.remove(&coord).unwrap_or_default();
		for leg in legs {
			match leg {
				Leg::Add(c) => acc.add(&c),
				Leg::Remove(c) => acc.remove(&c),
			}
		}
		if !acc.is_empty() {
			buffer.insert(coord, acc);
		}
		while buffer.len() > capacity {
			buffer.pop_first();
		}
		high_water
			.entry(group.clone())
			.and_modify(|hw| {
				if coord > *hw {
					*hw = coord;
				}
			})
			.or_insert(coord);
		touched.insert(group);
	}
	touched
}

pub fn multi_rolling_accumulator_oracle<A>(
	agg: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedTable
where
	A: MultiRollingOperator,
	A::Output: Row,
{
	let capacity = agg.capacity();
	let mut buffers: HashMap<MultiGroup<A>, BTreeMap<MultiCoord<A>, A::WindowAcc>> = HashMap::new();
	let mut high_water: HashMap<MultiGroup<A>, MultiCoord<A>> = HashMap::new();
	let mut last_visible: HashMap<MultiGroup<A>, Vec<A::Output>> = HashMap::new();

	for batch in batches {
		let snapshot = high_water.clone();
		let buckets = bucket_multi(agg, batch);
		let touched = apply_multi_buckets::<A>(capacity, &snapshot, buckets, &mut buffers, &mut high_water);
		for group in touched {
			if let Some(buffer) = buffers.get(&group) {
				let emit = agg.combine(&group, buffer);
				last_visible.insert(group, emit.into_values().collect());
			}
		}
	}

	let outputs: Vec<A::Output> = last_visible.into_values().flatten().collect();
	materialize_outputs(outputs.into_iter(), ctx.now_nanos(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn extract_multi<A>(
	agg: &A,
	row: &CoreRow,
) -> Option<(MultiGroup<A>, MultiCoord<A>, <A::WindowAcc as WindowAccumulator>::Contribution)>
where
	A: MultiRollingOperator,
{
	let columns = Columns::from_row(row);
	let view = NativeColumnsView::new(&columns);
	let row_view = view.row(0)?;
	agg.extract(&row_view)
}

#[allow(clippy::type_complexity)]
fn extract_carry<A>(
	agg: &A,
	row: &CoreRow,
) -> Option<(CarryGroup<A>, CarryCoord<A>, <A::Acc as WindowAccumulator>::Contribution)>
where
	A: TumblingCarryOperator,
{
	let columns = Columns::from_row(row);
	let view = NativeColumnsView::new(&columns);
	let row_view = view.row(0)?;
	agg.extract(&row_view)
}
