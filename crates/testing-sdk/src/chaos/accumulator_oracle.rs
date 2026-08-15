// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	ffi::c_void,
};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
	},
	row::Row as CoreRow,
	value::column::columns::Columns,
};
use reifydb_flow::{
	operator::state::seal::coord::Coord,
	window::{accumulator::WindowAccumulator, span::WindowSpan},
};
use reifydb_sdk::flow::operator::{
	column::{row::Row, sink::in_process::InProcessRowSink},
	extern_c::{binding::context::ExternCContext, wire::context::ExternCContextRaw},
	view::{ColumnsView, RowView, in_process::InProcessColumnsView},
	windowed::{
		rolling::RollingOperator, rolling_top_k::RollingTopKOperator, tumbling::TumblingOperator,
		tumbling_carry::TumblingCarryOperator,
	},
};
use reifydb_testing_chaos::operator::{
	event::{ChaosBatch, ChaosEvent},
	view::MaterializedView,
};
use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};

use super::{context::ChaosContext, materialize::materialize_history};
use crate::{callbacks::create_test_callbacks, context::TestContext};

fn with_oracle_ctx<R>(f: impl FnOnce(&mut ExternCContext) -> R) -> R {
	let test_ctx = TestContext::new(CommitVersion(1));
	let mut extern_c_context = ExternCContextRaw {
		txn_ptr: &test_ctx as *const TestContext as *mut c_void,
		written_at_nanos: 0,
		operator_id: 1,
		callbacks: create_test_callbacks(),
	};
	let mut op_ctx = ExternCContext::new(&mut extern_c_context as *mut ExternCContextRaw);
	f(&mut op_ctx)
}

type TumblingCoord = DateTime;
type Group<A> = <A as TumblingOperator>::GroupKey;
type WindowKey<A> = (Group<A>, TumblingCoord);

pub fn tumbling_accumulator_oracle<A>(
	aggregate: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedView
where
	A: TumblingOperator,
	A::Output: Row,
{
	let mut accumulators: HashMap<WindowKey<A>, A::Accumulator> = HashMap::new();
	let mut spans: HashMap<WindowKey<A>, WindowSpan<TumblingCoord>> = HashMap::new();
	let mut high_water: HashMap<Group<A>, TumblingCoord> = HashMap::new();
	let mut last_visible: HashMap<WindowKey<A>, A::Output> = HashMap::new();

	for batch in batches {
		let snapshot = HashMap::new();
		let mut touched: BTreeSet<WindowKey<A>> = BTreeSet::new();

		fan_out(batch, |row, is_add| {
			apply_leg(aggregate, row, is_add, &snapshot, &mut accumulators, &mut spans, &mut touched)
		});

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

	materialize_outputs(last_visible.into_values(), ctx.now(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn apply_leg<A>(
	aggregate: &A,
	row: &CoreRow,
	is_add: bool,
	snapshot: &HashMap<Group<A>, TumblingCoord>,
	accumulators: &mut HashMap<WindowKey<A>, A::Accumulator>,
	spans: &mut HashMap<WindowKey<A>, WindowSpan<TumblingCoord>>,
	touched: &mut BTreeSet<WindowKey<A>>,
) where
	A: TumblingOperator,
{
	let Some((group, coord, contribution)) = extract_one(aggregate, row) else {
		return;
	};
	let span = aggregate.window_for(coord);
	let key = (group, span.start);
	if is_add {
		let survives = snapshot.get(&key.0).is_none_or(|hw| span.start >= *hw);
		if !survives {
			return;
		}
		spans.insert(key.clone(), span);
		let accumulator = accumulators.entry(key.clone()).or_insert_with(|| aggregate.new_accumulator());
		accumulator.add(&contribution);
		touched.insert(key);
	} else if let Some(accumulator) = accumulators.get_mut(&key)
		&& !accumulator.is_empty()
	{
		let survives = snapshot.get(&key.0).is_none_or(|hw| span.start >= *hw);
		if !survives {
			return;
		}
		accumulator.remove(&contribution);
		spans.insert(key.clone(), span);
		touched.insert(key);
	}
}

#[allow(clippy::type_complexity)]
fn extract_one<A>(
	aggregate: &A,
	row: &CoreRow,
) -> Option<(Group<A>, TumblingCoord, <A::Accumulator as WindowAccumulator>::Contribution)>
where
	A: TumblingOperator,
{
	let columns = Columns::from_row(row);
	let view = InProcessColumnsView::new(&columns);
	let row_view = view.row(0)?;
	let coord = row_view.row_time()?;
	let (group, contribution) = with_oracle_ctx(|ctx| aggregate.extract(ctx, &row_view))?;
	Some((group, coord, contribution))
}

fn materialize_outputs<O: Row>(
	outputs: impl Iterator<Item = O>,
	now: DateTime,
	output_key_columns: &[String],
) -> MaterializedView {
	let mut sink = InProcessRowSink::new(<O as Row>::COLUMNS).expect("output sink");
	let mut row_numbers: Vec<RowNumber> = Vec::new();
	let mut count = 0u64;
	for output in outputs {
		output.encode_into(&mut sink).expect("encode output");
		count += 1;
		row_numbers.push(RowNumber(count));
	}
	if count == 0 {
		return MaterializedView::empty();
	}
	let columns = sink.finish(row_numbers, now).expect("finish sink");
	let change = Change::from_flow(OperatorId(0), CommitVersion(0), vec![Diff::insert(columns)], now);
	materialize_history(&[change], output_key_columns)
}

type RollingCoord = DateTime;
type RollingGroup<A> = <A as RollingOperator>::GroupKey;

type RollingContribution<A> = <<A as RollingOperator>::Accumulator as WindowAccumulator>::Contribution;
type RollingBuckets<A> = BTreeMap<(RollingGroup<A>, RollingCoord), Vec<Leg<RollingContribution<A>>>>;

enum Leg<C> {
	Add(C),
	Remove(C),
}

fn fan_out(batch: &ChaosBatch, mut leg: impl FnMut(&CoreRow, bool)) {
	for event in &batch.events {
		match event {
			ChaosEvent::Insert {
				row,
				..
			} => leg(row, true),
			ChaosEvent::Update {
				pre,
				post,
				..
			} => {
				leg(pre, false);
				leg(post, true);
			}
			ChaosEvent::Remove {
				row,
				..
			} => leg(row, false),
		}
	}
}

fn bucket_rolling<A>(aggregate: &A, batch: &ChaosBatch) -> RollingBuckets<A>
where
	A: RollingOperator,
{
	let mut buckets: RollingBuckets<A> = BTreeMap::new();
	fan_out(batch, |row, is_add| push_rolling(aggregate, row, is_add, &mut buckets));
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
	snapshot: &HashMap<RollingGroup<A>, RollingCoord>,
	buckets: RollingBuckets<A>,
	buffers: &mut HashMap<RollingGroup<A>, BTreeMap<RollingCoord, A::Accumulator>>,
	high_water: &mut HashMap<RollingGroup<A>, RollingCoord>,
) -> BTreeSet<RollingGroup<A>>
where
	A: RollingOperator,
{
	let mut touched: BTreeSet<RollingGroup<A>> = BTreeSet::new();
	for ((group, coord), legs) in buckets {
		let buffer = buffers.entry(group.clone()).or_default();

		let late = snapshot.get(&group).is_some_and(|hw| coord < *hw) && !buffer.contains_key(&coord);
		let mut accumulator = buffer.remove(&coord).unwrap_or_default();
		let mut changed = false;
		for leg in legs {
			match leg {
				Leg::Add(c) => {
					if late {
						continue;
					}
					accumulator.add(&c);
					changed = true;
				}
				Leg::Remove(c) => {
					if accumulator.is_empty() {
						continue;
					}
					accumulator.remove(&c);
					changed = true;
				}
			}
		}
		if !accumulator.is_empty() {
			buffer.insert(coord, accumulator);
		}

		if !changed {
			continue;
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
) -> MaterializedView
where
	A: RollingOperator,
	A::Output: Row,
{
	let capacity = aggregate.capacity();
	let mut buffers: HashMap<RollingGroup<A>, BTreeMap<RollingCoord, A::Accumulator>> = HashMap::new();
	let mut high_water: HashMap<RollingGroup<A>, RollingCoord> = HashMap::new();
	let mut last_visible: HashMap<RollingGroup<A>, A::Output> = HashMap::new();

	for batch in batches {
		let snapshot = HashMap::new();
		let buckets = bucket_rolling(aggregate, batch);
		let touched = apply_rolling_buckets::<A>(capacity, &snapshot, buckets, &mut buffers, &mut high_water);
		for group in touched {
			match buffers.get(&group).and_then(|buffer| aggregate.combine(&group, buffer)) {
				Some(out) => {
					last_visible.insert(group, out);
				}
				None => {
					last_visible.remove(&group);
				}
			}
		}
	}

	materialize_outputs(last_visible.into_values(), ctx.now(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn extract_rolling<A>(
	aggregate: &A,
	row: &CoreRow,
) -> Option<(RollingGroup<A>, RollingCoord, <A::Accumulator as WindowAccumulator>::Contribution)>
where
	A: RollingOperator,
{
	let columns = Columns::from_row(row);
	let view = InProcessColumnsView::new(&columns);
	let row_view = view.row(0)?;
	let coord = row_view.row_time()?;
	let (group, contribution) = with_oracle_ctx(|ctx| aggregate.extract(ctx, &row_view))?;
	Some((group, coord.floor_to(aggregate.bucket_size()), contribution))
}

type CarryCoord = DateTime;
type CarryGroup<A> = <A as TumblingCarryOperator>::GroupKey;
type CarryWindowKey<A> = (CarryGroup<A>, CarryCoord);

type CarryContribution<A> = <<A as TumblingCarryOperator>::Accumulator as WindowAccumulator>::Contribution;
type CarryBuckets<A> = BTreeMap<CarryWindowKey<A>, (WindowSpan<CarryCoord>, Vec<Leg<CarryContribution<A>>>)>;

struct CarryGroupState<C, Carry> {
	high_water: Option<C>,
	sealed_up_to: Option<C>,
	sealed_carry: Option<Carry>,
	windows: BTreeMap<C, Option<Carry>>,
}

impl<C, Carry> Default for CarryGroupState<C, Carry> {
	fn default() -> Self {
		Self {
			high_water: None,
			sealed_up_to: None,
			sealed_carry: None,
			windows: BTreeMap::new(),
		}
	}
}

fn bucket_carry<A>(aggregate: &A, batch: &ChaosBatch) -> CarryBuckets<A>
where
	A: TumblingCarryOperator,
{
	let mut buckets: CarryBuckets<A> = BTreeMap::new();
	fan_out(batch, |row, is_add| push_carry(aggregate, row, is_add, &mut buckets));
	buckets
}

fn push_carry<A>(aggregate: &A, row: &CoreRow, is_add: bool, buckets: &mut CarryBuckets<A>)
where
	A: TumblingCarryOperator,
{
	if let Some((group, coord, contribution)) = extract_carry(aggregate, row) {
		let span = aggregate.window_for(coord);
		let leg = if is_add {
			Leg::Add(contribution)
		} else {
			Leg::Remove(contribution)
		};
		buckets.entry((group, span.start)).or_insert_with(|| (span, Vec::new())).1.push(leg);
	}
}

pub fn tumbling_carry_accumulator_oracle<A>(
	aggregate: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
	retention: Option<Duration>,
) -> MaterializedView
where
	A: TumblingCarryOperator,
	A::Output: Row,
{
	let mut accumulators: HashMap<CarryWindowKey<A>, A::Accumulator> = HashMap::new();
	let mut spans: HashMap<CarryWindowKey<A>, WindowSpan<CarryCoord>> = HashMap::new();
	let mut metas: HashMap<CarryGroup<A>, CarryGroupState<CarryCoord, A::Carry>> = HashMap::new();
	let mut last_visible: HashMap<CarryWindowKey<A>, A::Output> = HashMap::new();

	for batch in batches {
		let snapshot: HashMap<CarryGroup<A>, CarryCoord> = HashMap::new();
		let buckets = bucket_carry(aggregate, batch);

		let mut earliest_affected: HashMap<CarryGroup<A>, CarryCoord> = HashMap::new();
		for ((group, start), (span, legs)) in buckets {
			let meta = metas.entry(group.clone()).or_default();
			if matches!(meta.sealed_up_to, Some(s) if start <= s) {
				continue;
			}
			let snap_hw = snapshot.get(&group).copied();
			let tracked = meta.windows.contains_key(&start);
			let survives = snap_hw.is_none_or(|hw| start >= hw);
			if !tracked && !survives {
				continue;
			}
			let drop_adds = snap_hw.is_some_and(|hw| start < hw);
			let key = (group.clone(), start);
			let accumulator =
				accumulators.entry(key.clone()).or_insert_with(|| aggregate.new_accumulator());
			let mut changed = false;
			for leg in legs {
				match leg {
					Leg::Add(c) => {
						if drop_adds {
							continue;
						}
						accumulator.add(&c);
						changed = true;
					}
					Leg::Remove(c) => {
						if accumulator.is_empty() {
							continue;
						}
						accumulator.remove(&c);
						changed = true;
					}
				}
			}
			if !changed {
				continue;
			}
			spans.insert(key, span);
			meta.windows.entry(start).or_insert(None);
			if meta.high_water.is_none_or(|hw| start > hw) {
				meta.high_water = Some(start);
			}
			let e = earliest_affected.entry(group).or_insert(start);
			if start < *e {
				*e = start;
			}
		}

		for (group, start) in earliest_affected {
			let meta = metas.get_mut(&group).expect("affected group has meta");
			let mut prev_carry: Option<A::Carry> = match meta.windows.range(..start).next_back() {
				Some((_, c)) => c.clone(),
				None => meta.sealed_carry.clone(),
			};
			let coords: Vec<CarryCoord> = meta.windows.range(start..).map(|(c, _)| *c).collect();
			let mut emptied: Vec<CarryCoord> = Vec::new();
			for coord in coords {
				let key = (group.clone(), coord);
				let span = *spans.get(&key).expect("span recorded for tracked window");
				let value = accumulators.get(&key).and_then(|a| a.finalize());
				match value
					.as_ref()
					.and_then(|v| aggregate.build_output(&group, span, v, prev_carry.as_ref()))
				{
					Some(out) => {
						let new_carry = value
							.as_ref()
							.and_then(|v| aggregate.carry_forward(v, prev_carry.as_ref()));
						last_visible.insert(key, out);
						*meta.windows.get_mut(&coord).expect("window entry present") =
							new_carry.clone();
						if new_carry.is_some() {
							prev_carry = new_carry;
						}
					}
					None => {
						last_visible.remove(&key);
						emptied.push(coord);
					}
				}
			}
			for coord in emptied {
				meta.windows.remove(&coord);
			}

			if let (Some(retention), Some(hw)) = (retention, meta.high_water) {
				loop {
					let Some((&first, carry_out)) = meta.windows.iter().next() else {
						break;
					};
					if hw.span_since(first) <= retention {
						break;
					}
					let carry_out = carry_out.clone();
					meta.windows.remove(&first);
					meta.sealed_up_to = Some(first);
					meta.sealed_carry = carry_out;
					accumulators.remove(&(group.clone(), first));
					spans.remove(&(group.clone(), first));
				}
			}
		}
	}

	materialize_outputs(last_visible.into_values(), ctx.now(), output_key_columns)
}

type TopKCoord = DateTime;
type TopKGroup<A> = <A as RollingTopKOperator>::GroupKey;
type TopKContribution<A> = <<A as RollingTopKOperator>::Accumulator as WindowAccumulator>::Contribution;
type TopKBuckets<A> = BTreeMap<(TopKGroup<A>, TopKCoord), Vec<Leg<TopKContribution<A>>>>;

fn bucket_top_k<A>(aggregate: &A, batch: &ChaosBatch) -> TopKBuckets<A>
where
	A: RollingTopKOperator,
{
	let mut buckets: TopKBuckets<A> = BTreeMap::new();
	fan_out(batch, |row, is_add| push_top_k(aggregate, row, is_add, &mut buckets));
	buckets
}

fn push_top_k<A>(aggregate: &A, row: &CoreRow, is_add: bool, buckets: &mut TopKBuckets<A>)
where
	A: RollingTopKOperator,
{
	if let Some((group, coord, contribution)) = extract_top_k(aggregate, row) {
		let leg = if is_add {
			Leg::Add(contribution)
		} else {
			Leg::Remove(contribution)
		};
		buckets.entry((group, coord)).or_default().push(leg);
	}
}

#[allow(clippy::type_complexity)]
fn apply_top_k_buckets<A>(
	capacity: usize,
	snapshot: &HashMap<TopKGroup<A>, TopKCoord>,
	buckets: TopKBuckets<A>,
	buffers: &mut HashMap<TopKGroup<A>, BTreeMap<TopKCoord, A::Accumulator>>,
	high_water: &mut HashMap<TopKGroup<A>, TopKCoord>,
) -> BTreeSet<TopKGroup<A>>
where
	A: RollingTopKOperator,
{
	let mut touched: BTreeSet<TopKGroup<A>> = BTreeSet::new();
	for ((group, coord), legs) in buckets {
		let buffer = buffers.entry(group.clone()).or_default();

		let late = snapshot.get(&group).is_some_and(|hw| coord < *hw) && !buffer.contains_key(&coord);
		let mut accumulator = buffer.remove(&coord).unwrap_or_default();
		let mut changed = false;
		for leg in legs {
			match leg {
				Leg::Add(c) => {
					if late {
						continue;
					}
					accumulator.add(&c);
					changed = true;
				}
				Leg::Remove(c) => {
					if accumulator.is_empty() {
						continue;
					}
					accumulator.remove(&c);
					changed = true;
				}
			}
		}
		if !accumulator.is_empty() {
			buffer.insert(coord, accumulator);
		}
		if !changed {
			continue;
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

pub fn rolling_top_k_accumulator_oracle<A>(
	aggregate: &A,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedView
where
	A: RollingTopKOperator,
	A::Output: Row,
{
	let capacity = aggregate.capacity();
	let mut buffers: HashMap<TopKGroup<A>, BTreeMap<TopKCoord, A::Accumulator>> = HashMap::new();
	let mut high_water: HashMap<TopKGroup<A>, TopKCoord> = HashMap::new();
	let mut last_visible: HashMap<TopKGroup<A>, Vec<A::Output>> = HashMap::new();

	for batch in batches {
		let snapshot = HashMap::new();
		let buckets = bucket_top_k(aggregate, batch);
		let touched = apply_top_k_buckets::<A>(capacity, &snapshot, buckets, &mut buffers, &mut high_water);
		for group in touched {
			if let Some(buffer) = buffers.get(&group) {
				let emit = aggregate.combine(&group, buffer);
				last_visible.insert(group, emit.into_values().collect());
			}
		}
	}

	let outputs: Vec<A::Output> = last_visible.into_values().flatten().collect();
	materialize_outputs(outputs.into_iter(), ctx.now(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn extract_top_k<A>(
	aggregate: &A,
	row: &CoreRow,
) -> Option<(TopKGroup<A>, TopKCoord, <A::Accumulator as WindowAccumulator>::Contribution)>
where
	A: RollingTopKOperator,
{
	let columns = Columns::from_row(row);
	let view = InProcessColumnsView::new(&columns);
	let row_view = view.row(0)?;
	let coord = row_view.row_time()?;
	let (group, contribution) = with_oracle_ctx(|ctx| aggregate.extract(ctx, &row_view))?;
	Some((group, coord.floor_to(aggregate.bucket_size()), contribution))
}

#[allow(clippy::type_complexity)]
fn extract_carry<A>(
	aggregate: &A,
	row: &CoreRow,
) -> Option<(CarryGroup<A>, CarryCoord, <A::Accumulator as WindowAccumulator>::Contribution)>
where
	A: TumblingCarryOperator,
{
	let columns = Columns::from_row(row);
	let view = InProcessColumnsView::new(&columns);
	let row_view = view.row(0)?;
	let coord = row_view.row_time()?;
	let (group, contribution) = with_oracle_ctx(|ctx| aggregate.extract(ctx, &row_view))?;
	Some((group, coord, contribution))
}
