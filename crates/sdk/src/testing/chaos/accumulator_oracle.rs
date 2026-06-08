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
	window::{accumulator::WindowAccumulator, span::WindowSpan},
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
		multi_rolling::MultiRollingOperator, rolling::RollingOperator,
		rolling_incremental::RollingIncrementalOperator, tumbling::TumblingOperator,
		tumbling_carry::TumblingCarryOperator,
	},
};

type Coord<A> = <A as TumblingOperator>::WindowCoord;
type Group<A> = <A as TumblingOperator>::GroupKey;
type WindowKey<A> = (Group<A>, Coord<A>);

pub fn tumbling_accumulator_oracle<A>(
	aggregate: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedTable
where
	A: TumblingOperator,
	A::Output: Row,
{
	let mut accumulators: HashMap<WindowKey<A>, A::Accumulator> = HashMap::new();
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
				} => apply_leg(
					aggregate,
					row,
					true,
					&snapshot,
					&mut accumulators,
					&mut spans,
					&mut touched,
				),
				ChaosEvent::Update {
					pre,
					post,
					..
				} => {
					apply_leg(
						aggregate,
						pre,
						false,
						&snapshot,
						&mut accumulators,
						&mut spans,
						&mut touched,
					);
					apply_leg(
						aggregate,
						post,
						true,
						&snapshot,
						&mut accumulators,
						&mut spans,
						&mut touched,
					);
				}
				ChaosEvent::Remove {
					row,
					..
				} => apply_leg(
					aggregate,
					row,
					false,
					&snapshot,
					&mut accumulators,
					&mut spans,
					&mut touched,
				),
			}
		}

		for key in touched {
			let hw = high_water.entry(key.0.clone()).or_insert(key.1);
			if key.1 > *hw {
				*hw = key.1;
			}
			let finalized = accumulators.get(&key).and_then(|a| a.finalize());
			if let Some(value) = finalized
				&& let Some(span) = spans.get(&key).copied()
				&& let Some(out) = aggregate.build_output(&key.0, span, value)
			{
				last_visible.insert(key.clone(), out);
			} else {
				last_visible.remove(&key);
			}
		}
	}

	materialize_outputs(last_visible.into_values(), ctx.now_nanos(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn apply_leg<A>(
	aggregate: &A,
	row: &CoreRow,
	is_add: bool,
	snapshot: &HashMap<Group<A>, Coord<A>>,
	accumulators: &mut HashMap<WindowKey<A>, A::Accumulator>,
	spans: &mut HashMap<WindowKey<A>, WindowSpan<Coord<A>>>,
	touched: &mut BTreeSet<WindowKey<A>>,
) where
	A: TumblingOperator,
{
	let Some((group, coord, contribution)) = extract_one(aggregate, row) else {
		return;
	};
	let span = aggregate.window_for(coord);
	let survives = snapshot.get(&group).is_none_or(|hw| span.start >= *hw);
	if !survives {
		return;
	}
	let key = (group, span.start);
	spans.insert(key.clone(), span);
	let accumulator = accumulators.entry(key.clone()).or_insert_with(|| aggregate.new_accumulator());
	if is_add {
		accumulator.add(&contribution);
	} else {
		accumulator.remove(&contribution);
	}
	touched.insert(key);
}

#[allow(clippy::type_complexity)]
fn extract_one<A>(
	aggregate: &A,
	row: &CoreRow,
) -> Option<(Group<A>, Coord<A>, <A::Accumulator as WindowAccumulator>::Contribution)>
where
	A: TumblingOperator,
{
	let columns = Columns::from_row(row);
	let view = NativeColumnsView::new(&columns);
	let row_view = view.row(0)?;
	aggregate.extract(&row_view)
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

type RollingContribution<A> = <<A as RollingOperator>::Accumulator as WindowAccumulator>::Contribution;
type RollingBuckets<A> = BTreeMap<(RollingGroup<A>, RollingCoord<A>), Vec<Leg<RollingContribution<A>>>>;

enum Leg<C> {
	Add(C),
	Remove(C),
}

fn bucket_rolling<A>(aggregate: &A, batch: &ChaosBatch) -> RollingBuckets<A>
where
	A: RollingOperator,
{
	let mut buckets: RollingBuckets<A> = BTreeMap::new();
	for event in &batch.events {
		match event {
			ChaosEvent::Insert {
				row,
				..
			} => push_rolling(aggregate, row, true, &mut buckets),
			ChaosEvent::Update {
				pre,
				post,
				..
			} => {
				push_rolling(aggregate, pre, false, &mut buckets);
				push_rolling(aggregate, post, true, &mut buckets);
			}
			ChaosEvent::Remove {
				row,
				..
			} => push_rolling(aggregate, row, false, &mut buckets),
		}
	}
	buckets
}

fn push_rolling<A>(aggregate: &A, row: &CoreRow, is_add: bool, buckets: &mut RollingBuckets<A>)
where
	A: RollingOperator,
{
	if let Some((group, coord, contribution)) = extract_rolling(aggregate, row) {
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
	buffers: &mut HashMap<RollingGroup<A>, BTreeMap<RollingCoord<A>, A::Accumulator>>,
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
		let mut accumulator = buffer.remove(&coord).unwrap_or_default();
		for leg in legs {
			match leg {
				Leg::Add(c) => accumulator.add(&c),
				Leg::Remove(c) => accumulator.remove(&c),
			}
		}
		if !accumulator.is_empty() {
			buffer.insert(coord, accumulator);
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
	aggregate: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedTable
where
	A: RollingOperator,
	A::Output: Row,
{
	let capacity = aggregate.capacity();
	let mut buffers: HashMap<RollingGroup<A>, BTreeMap<RollingCoord<A>, A::Accumulator>> = HashMap::new();
	let mut high_water: HashMap<RollingGroup<A>, RollingCoord<A>> = HashMap::new();
	let mut last_visible: HashMap<RollingGroup<A>, A::Output> = HashMap::new();

	for batch in batches {
		let snapshot = high_water.clone();
		let buckets = bucket_rolling(aggregate, batch);
		let touched = apply_rolling_buckets::<A>(capacity, &snapshot, buckets, &mut buffers, &mut high_water);
		for group in touched {
			if let Some(buffer) = buffers.get(&group)
				&& let Some(out) = aggregate.combine(&group, buffer)
			{
				last_visible.insert(group, out);
			}
		}
	}

	materialize_outputs(last_visible.into_values(), ctx.now_nanos(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn extract_rolling<A>(
	aggregate: &A,
	row: &CoreRow,
) -> Option<(RollingGroup<A>, RollingCoord<A>, <A::Accumulator as WindowAccumulator>::Contribution)>
where
	A: RollingOperator,
{
	let columns = Columns::from_row(row);
	let view = NativeColumnsView::new(&columns);
	let row_view = view.row(0)?;
	aggregate.extract(&row_view)
}

pub fn rolling_incremental_accumulator_oracle<A>(
	aggregate: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedTable
where
	A: RollingIncrementalOperator,
	A::Output: Row,
{
	let capacity = aggregate.capacity();
	let mut buffers: HashMap<RollingGroup<A>, BTreeMap<RollingCoord<A>, A::Accumulator>> = HashMap::new();
	let mut high_water: HashMap<RollingGroup<A>, RollingCoord<A>> = HashMap::new();
	let mut last_visible: HashMap<RollingGroup<A>, A::Output> = HashMap::new();

	for batch in batches {
		let snapshot = high_water.clone();
		let buckets = bucket_rolling(aggregate, batch);
		let touched = apply_rolling_buckets::<A>(capacity, &snapshot, buckets, &mut buffers, &mut high_water);
		for group in touched {
			let Some(buffer) = buffers.get(&group) else {
				continue;
			};
			let mut running = A::Running::default();
			for accumulator in buffer.values() {
				if let Some(value) = accumulator.finalize() {
					running.add(&aggregate.window_contribution(&value));
				}
			}
			if let Some((coord, accumulator)) = buffer.iter().next_back()
				&& let Some(newest) = accumulator.finalize()
				&& let Some(out) = aggregate.combine_running(&group, &running, &newest, *coord)
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
	aggregate: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedTable
where
	A: TumblingCarryOperator,
	A::Output: Row,
{
	let mut accumulators: HashMap<CarryWindowKey<A>, A::Accumulator> = HashMap::new();
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
				} => apply_carry_leg(
					aggregate,
					row,
					true,
					&snapshot,
					&mut accumulators,
					&mut spans,
					&mut touched,
				),
				ChaosEvent::Update {
					pre,
					post,
					..
				} => {
					apply_carry_leg(
						aggregate,
						pre,
						false,
						&snapshot,
						&mut accumulators,
						&mut spans,
						&mut touched,
					);
					apply_carry_leg(
						aggregate,
						post,
						true,
						&snapshot,
						&mut accumulators,
						&mut spans,
						&mut touched,
					);
				}
				ChaosEvent::Remove {
					row,
					..
				} => apply_carry_leg(
					aggregate,
					row,
					false,
					&snapshot,
					&mut accumulators,
					&mut spans,
					&mut touched,
				),
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

			if let Some(accumulator) = accumulators.get(&key)
				&& let Some(value) = accumulator.finalize()
				&& let Some(span) = spans.get(&key).copied()
				&& let Some(out) = aggregate.build_output(&key.0, span, &value, prev_carry.as_ref())
			{
				last_visible.insert(key.clone(), out);
				if let Some(new_carry) = aggregate.carry_forward(&value, prev_carry.as_ref()) {
					carry.entry(key.0.clone()).or_default().current_window_carry = Some(new_carry);
				}
			}
		}
	}

	materialize_outputs(last_visible.into_values(), ctx.now_nanos(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn apply_carry_leg<A>(
	aggregate: &A,
	row: &CoreRow,
	is_add: bool,
	snapshot: &HashMap<CarryGroup<A>, CarryCoord<A>>,
	accumulators: &mut HashMap<CarryWindowKey<A>, A::Accumulator>,
	spans: &mut HashMap<CarryWindowKey<A>, WindowSpan<CarryCoord<A>>>,
	touched: &mut BTreeSet<CarryWindowKey<A>>,
) where
	A: TumblingCarryOperator,
{
	let Some((group, coord, contribution)) = extract_carry(aggregate, row) else {
		return;
	};
	let span = aggregate.window_for(coord);
	let survives = snapshot.get(&group).is_none_or(|hw| span.start >= *hw);
	if !survives {
		return;
	}
	let key = (group, span.start);
	spans.insert(key.clone(), span);
	let accumulator = accumulators.entry(key.clone()).or_insert_with(|| aggregate.new_accumulator());
	if is_add {
		accumulator.add(&contribution);
	} else {
		accumulator.remove(&contribution);
	}
	touched.insert(key);
}

type MultiCoord<A> = <A as MultiRollingOperator>::WindowCoord;
type MultiGroup<A> = <A as MultiRollingOperator>::GroupKey;
type MultiContribution<A> = <<A as MultiRollingOperator>::Accumulator as WindowAccumulator>::Contribution;
type MultiBuckets<A> = BTreeMap<(MultiGroup<A>, MultiCoord<A>), Vec<Leg<MultiContribution<A>>>>;

fn bucket_multi<A>(aggregate: &A, batch: &ChaosBatch) -> MultiBuckets<A>
where
	A: MultiRollingOperator,
{
	let mut buckets: MultiBuckets<A> = BTreeMap::new();
	for event in &batch.events {
		match event {
			ChaosEvent::Insert {
				row,
				..
			} => push_multi(aggregate, row, true, &mut buckets),
			ChaosEvent::Update {
				pre,
				post,
				..
			} => {
				push_multi(aggregate, pre, false, &mut buckets);
				push_multi(aggregate, post, true, &mut buckets);
			}
			ChaosEvent::Remove {
				row,
				..
			} => push_multi(aggregate, row, false, &mut buckets),
		}
	}
	buckets
}

fn push_multi<A>(aggregate: &A, row: &CoreRow, is_add: bool, buckets: &mut MultiBuckets<A>)
where
	A: MultiRollingOperator,
{
	if let Some((group, coord, contribution)) = extract_multi(aggregate, row) {
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
	buffers: &mut HashMap<MultiGroup<A>, BTreeMap<MultiCoord<A>, A::Accumulator>>,
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
		let mut accumulator = buffer.remove(&coord).unwrap_or_default();
		for leg in legs {
			match leg {
				Leg::Add(c) => accumulator.add(&c),
				Leg::Remove(c) => accumulator.remove(&c),
			}
		}
		if !accumulator.is_empty() {
			buffer.insert(coord, accumulator);
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
	aggregate: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedTable
where
	A: MultiRollingOperator,
	A::Output: Row,
{
	let capacity = aggregate.capacity();
	let mut buffers: HashMap<MultiGroup<A>, BTreeMap<MultiCoord<A>, A::Accumulator>> = HashMap::new();
	let mut high_water: HashMap<MultiGroup<A>, MultiCoord<A>> = HashMap::new();
	let mut last_visible: HashMap<MultiGroup<A>, Vec<A::Output>> = HashMap::new();

	for batch in batches {
		let snapshot = high_water.clone();
		let buckets = bucket_multi(aggregate, batch);
		let touched = apply_multi_buckets::<A>(capacity, &snapshot, buckets, &mut buffers, &mut high_water);
		for group in touched {
			if let Some(buffer) = buffers.get(&group) {
				let emit = aggregate.combine(&group, buffer);
				last_visible.insert(group, emit.into_values().collect());
			}
		}
	}

	let outputs: Vec<A::Output> = last_visible.into_values().flatten().collect();
	materialize_outputs(outputs.into_iter(), ctx.now_nanos(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn extract_multi<A>(
	aggregate: &A,
	row: &CoreRow,
) -> Option<(MultiGroup<A>, MultiCoord<A>, <A::Accumulator as WindowAccumulator>::Contribution)>
where
	A: MultiRollingOperator,
{
	let columns = Columns::from_row(row);
	let view = NativeColumnsView::new(&columns);
	let row_view = view.row(0)?;
	aggregate.extract(&row_view)
}

#[allow(clippy::type_complexity)]
fn extract_carry<A>(
	aggregate: &A,
	row: &CoreRow,
) -> Option<(CarryGroup<A>, CarryCoord<A>, <A::Accumulator as WindowAccumulator>::Contribution)>
where
	A: TumblingCarryOperator,
{
	let columns = Columns::from_row(row);
	let view = NativeColumnsView::new(&columns);
	let row_view = view.row(0)?;
	aggregate.extract(&row_view)
}
