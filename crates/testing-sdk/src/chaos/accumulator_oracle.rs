// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	ffi::c_void,
};

use reifydb_core::{
	common::{ChangeVersion, CommitVersion},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
	},
	row::Row as CoreRow,
	value::column::columns::Columns,
};
use reifydb_flow::{
	operator::state::seal::coord::Coord,
	window::{
		accumulator::{MergeAccumulator, WindowAccumulator},
		span::WindowSpan,
	},
};
use reifydb_sdk::flow::operator::{
	column::{row::Row, sink::in_process::InProcessRowSink},
	extern_c::{binding::context::ExternCContext, wire::context::ExternCContextRaw},
	view::{ColumnsView, in_process::InProcessColumnsView},
	windowed::operator::{CarryEmit, Contribution, Emit, WindowSettings, WindowedOperator},
};
use reifydb_testing_chaos::operator::{
	event::{ChaosBatch, ChaosEvent},
	view::MaterializedView,
};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

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

type TumblingCoord<A> = <A as WindowedOperator>::Coord;
type Group<A> = <A as WindowedOperator>::GroupKey;
type WindowKey<A> = (Group<A>, TumblingCoord<A>);

pub fn tumbling_accumulator_oracle<A>(
	aggregate: &A,
	settings: &WindowSettings<A::Coord>,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedView
where
	A: Emit,
	A::Output: Row,
{
	let mut accumulators: HashMap<WindowKey<A>, A::Accumulator> = HashMap::new();
	let mut spans: HashMap<WindowKey<A>, WindowSpan<TumblingCoord<A>>> = HashMap::new();
	let mut high_water: HashMap<Group<A>, TumblingCoord<A>> = HashMap::new();
	let mut last_visible: HashMap<WindowKey<A>, A::Output> = HashMap::new();

	for batch in batches {
		let snapshot = HashMap::new();
		let mut touched: BTreeSet<WindowKey<A>> = BTreeSet::new();

		fan_out(batch, |row, is_add| {
			apply_leg(
				aggregate,
				settings,
				row,
				is_add,
				&snapshot,
				&mut accumulators,
				&mut spans,
				&mut touched,
			)
		});

		for key in touched {
			let hw = high_water.entry(key.0.clone()).or_insert(key.1);
			if key.1 > *hw {
				*hw = key.1;
			}
			let finalized = accumulators.get(&key).and_then(|a| a.finalize());
			if let Some(value) = finalized
				&& let Some(span) = spans.get(&key).copied()
				&& let Some(out) = aggregate.build_output(&key.0, span, &value)
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
	settings: &WindowSettings<A::Coord>,
	row: &CoreRow,
	is_add: bool,
	snapshot: &HashMap<Group<A>, TumblingCoord<A>>,
	accumulators: &mut HashMap<WindowKey<A>, A::Accumulator>,
	spans: &mut HashMap<WindowKey<A>, WindowSpan<TumblingCoord<A>>>,
	touched: &mut BTreeSet<WindowKey<A>>,
) where
	A: WindowedOperator,
{
	let Some((group, coord, contribution)) = extract_one(aggregate, row) else {
		return;
	};
	let span = WindowSpan::for_coord(coord, settings.size);
	let key = (group, span.start);
	if is_add {
		let survives = snapshot.get(&key.0).is_none_or(|hw| span.start >= *hw);
		if !survives {
			return;
		}
		spans.insert(key.clone(), span);
		let accumulator =
			accumulators.entry(key.clone()).or_insert_with(|| aggregate.new_accumulator(settings));
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
fn extract_one<A>(aggregate: &A, row: &CoreRow) -> Option<(Group<A>, TumblingCoord<A>, Contribution<A>)>
where
	A: WindowedOperator,
{
	let columns = Columns::from_row(row);
	let view = InProcessColumnsView::new(&columns);
	let row_view = view.row(0)?;
	let coord = aggregate.coord(&row_view)?;
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
	let change = Change::from_flow(
		OperatorId(0),
		ChangeVersion::from(CommitVersion(0)),
		vec![Diff::insert(columns)],
		now,
	);
	materialize_history(&[change], output_key_columns)
}

type RollingCoord<A> = <A as WindowedOperator>::Coord;
type RollingGroup<A> = <A as WindowedOperator>::GroupKey;

type RollingContribution<A> = Contribution<A>;
type RollingBuckets<A> = BTreeMap<(RollingGroup<A>, RollingCoord<A>), Vec<Leg<RollingContribution<A>>>>;

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

fn bucket_rolling<A>(aggregate: &A, pane: <RollingCoord<A> as Coord>::Span, batch: &ChaosBatch) -> RollingBuckets<A>
where
	A: WindowedOperator,
{
	let mut buckets: RollingBuckets<A> = BTreeMap::new();
	fan_out(batch, |row, is_add| push_rolling(aggregate, pane, row, is_add, &mut buckets));
	buckets
}

fn push_rolling<A>(
	aggregate: &A,
	pane: <RollingCoord<A> as Coord>::Span,
	row: &CoreRow,
	is_add: bool,
	buckets: &mut RollingBuckets<A>,
) where
	A: WindowedOperator,
{
	if let Some((group, coord, contribution)) = extract_rolling(aggregate, pane, row) {
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
	size: <RollingCoord<A> as Coord>::Span,
	snapshot: &HashMap<RollingGroup<A>, RollingCoord<A>>,
	buckets: RollingBuckets<A>,
	buffers: &mut HashMap<RollingGroup<A>, BTreeMap<RollingCoord<A>, A::Accumulator>>,
	high_water: &mut HashMap<RollingGroup<A>, RollingCoord<A>>,
) -> BTreeSet<RollingGroup<A>>
where
	A: WindowedOperator,
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
		let cutoff = buffer.last_key_value().and_then(|(newest, _)| newest.checked_sub_span(size));
		if let Some(cutoff) = cutoff {
			buffer.retain(|coord, _| *coord > cutoff);
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

fn fold_panes<A>(buffer: &BTreeMap<RollingCoord<A>, A::Accumulator>) -> A::Accumulator
where
	A: WindowedOperator,
	A::Accumulator: MergeAccumulator,
{
	let mut folded = A::Accumulator::default();
	for pane in buffer.values() {
		folded.merge(pane);
	}
	folded
}

fn combine_rolling<A>(
	aggregate: &A,
	settings: &WindowSettings<A::Coord>,
	pane: <RollingCoord<A> as Coord>::Span,
	group: &RollingGroup<A>,
	buffer: &BTreeMap<RollingCoord<A>, A::Accumulator>,
) -> Option<A::Output>
where
	A: Emit,
	A::Accumulator: MergeAccumulator,
{
	let value = fold_panes::<A>(buffer).finalize()?;
	let (newest, _) = buffer.last_key_value()?;
	let end = newest.add_span(pane);
	let span = WindowSpan::new(end.saturating_sub_span(settings.size), end);
	aggregate.build_output(group, span, &value)
}

fn rolling_pane<A>(settings: &WindowSettings<A::Coord>) -> <RollingCoord<A> as Coord>::Span
where
	A: WindowedOperator,
{
	settings.pane.expect("a rolling window needs a pane")
}

pub fn rolling_accumulator_oracle<A>(
	aggregate: &A,
	settings: &WindowSettings<A::Coord>,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedView
where
	A: Emit,
	A::Accumulator: MergeAccumulator,
	A::Output: Row,
{
	let pane = rolling_pane::<A>(settings);
	let mut buffers: HashMap<RollingGroup<A>, BTreeMap<RollingCoord<A>, A::Accumulator>> = HashMap::new();
	let mut high_water: HashMap<RollingGroup<A>, RollingCoord<A>> = HashMap::new();
	let mut last_visible: HashMap<RollingGroup<A>, A::Output> = HashMap::new();

	for batch in batches {
		let snapshot = HashMap::new();
		let buckets = bucket_rolling(aggregate, pane, batch);
		let touched = apply_rolling_buckets::<A>(settings.size, &snapshot, buckets, &mut buffers, &mut high_water);
		for group in touched {
			match buffers
				.get(&group)
				.and_then(|buffer| combine_rolling(aggregate, settings, pane, &group, buffer))
			{
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
	pane: <RollingCoord<A> as Coord>::Span,
	row: &CoreRow,
) -> Option<(RollingGroup<A>, RollingCoord<A>, RollingContribution<A>)>
where
	A: WindowedOperator,
{
	let columns = Columns::from_row(row);
	let view = InProcessColumnsView::new(&columns);
	let row_view = view.row(0)?;
	let coord = aggregate.coord(&row_view)?;
	let (group, contribution) = with_oracle_ctx(|ctx| aggregate.extract(ctx, &row_view))?;
	Some((group, coord.floor_to(pane), contribution))
}

type CarryCoord<A> = <A as WindowedOperator>::Coord;
type CarryGroup<A> = <A as WindowedOperator>::GroupKey;
type CarryWindowKey<A> = (CarryGroup<A>, CarryCoord<A>);

type CarryContribution<A> = Contribution<A>;
type CarryBuckets<A> = BTreeMap<CarryWindowKey<A>, (WindowSpan<CarryCoord<A>>, Vec<Leg<CarryContribution<A>>>)>;

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

fn bucket_carry<A>(aggregate: &A, settings: &WindowSettings<A::Coord>, batch: &ChaosBatch) -> CarryBuckets<A>
where
	A: WindowedOperator,
{
	let mut buckets: CarryBuckets<A> = BTreeMap::new();
	fan_out(batch, |row, is_add| push_carry(aggregate, settings, row, is_add, &mut buckets));
	buckets
}

fn push_carry<A>(
	aggregate: &A,
	settings: &WindowSettings<A::Coord>,
	row: &CoreRow,
	is_add: bool,
	buckets: &mut CarryBuckets<A>,
) where
	A: WindowedOperator,
{
	if let Some((group, coord, contribution)) = extract_carry(aggregate, row) {
		let span = WindowSpan::for_coord(coord, settings.size);
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
	settings: &WindowSettings<A::Coord>,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedView
where
	A: CarryEmit,
	A::Output: Row,
{
	let mut accumulators: HashMap<CarryWindowKey<A>, A::Accumulator> = HashMap::new();
	let mut spans: HashMap<CarryWindowKey<A>, WindowSpan<CarryCoord<A>>> = HashMap::new();
	let mut metas: HashMap<CarryGroup<A>, CarryGroupState<CarryCoord<A>, A::Carry>> = HashMap::new();
	let mut last_visible: HashMap<CarryWindowKey<A>, A::Output> = HashMap::new();

	for batch in batches {
		let snapshot: HashMap<CarryGroup<A>, CarryCoord<A>> = HashMap::new();
		let buckets = bucket_carry(aggregate, settings, batch);

		let mut earliest_affected: HashMap<CarryGroup<A>, CarryCoord<A>> = HashMap::new();
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
				accumulators.entry(key.clone()).or_insert_with(|| aggregate.new_accumulator(settings));
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
			let coords: Vec<CarryCoord<A>> = meta.windows.range(start..).map(|(c, _)| *c).collect();
			let mut emptied: Vec<CarryCoord<A>> = Vec::new();
			for coord in coords {
				let key = (group.clone(), coord);
				let span = *spans.get(&key).expect("span recorded for tracked window");
				let value = accumulators.get(&key).and_then(|a| a.finalize());
				match value.as_ref().and_then(|v| {
					CarryEmit::build_output(aggregate, &group, span, v, prev_carry.as_ref())
				}) {
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

			if let (Some(retention), Some(hw)) = (settings.immutable, meta.high_water) {
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

pub fn rolling_top_k_accumulator_oracle<A, SK, R>(
	aggregate: &A,
	settings: &WindowSettings<A::Coord>,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedView
where
	A: Emit<Output = BTreeMap<SK, R>>,
	A::Accumulator: MergeAccumulator,
	SK: Ord,
	R: Row,
{
	let pane = rolling_pane::<A>(settings);
	let mut buffers: HashMap<RollingGroup<A>, BTreeMap<RollingCoord<A>, A::Accumulator>> = HashMap::new();
	let mut high_water: HashMap<RollingGroup<A>, RollingCoord<A>> = HashMap::new();
	let mut last_visible: HashMap<RollingGroup<A>, Vec<R>> = HashMap::new();

	for batch in batches {
		let snapshot = HashMap::new();
		let buckets = bucket_rolling(aggregate, pane, batch);
		let touched = apply_rolling_buckets::<A>(settings.size, &snapshot, buckets, &mut buffers, &mut high_water);
		for group in touched {
			if let Some(buffer) = buffers.get(&group) {
				let emit =
					combine_rolling(aggregate, settings, pane, &group, buffer).unwrap_or_default();
				last_visible.insert(group, emit.into_values().collect());
			}
		}
	}

	let outputs: Vec<R> = last_visible.into_values().flatten().collect();
	materialize_outputs(outputs.into_iter(), ctx.now(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn extract_carry<A>(aggregate: &A, row: &CoreRow) -> Option<(CarryGroup<A>, CarryCoord<A>, CarryContribution<A>)>
where
	A: WindowedOperator,
{
	let columns = Columns::from_row(row);
	let view = InProcessColumnsView::new(&columns);
	let row_view = view.row(0)?;
	let coord = aggregate.coord(&row_view)?;
	let (group, contribution) = with_oracle_ctx(|ctx| aggregate.extract(ctx, &row_view))?;
	Some((group, coord, contribution))
}
