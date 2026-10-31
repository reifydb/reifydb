// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	slice,
};

use reifydb_core::{
	common::{ChangeVersion, CommitVersion, WindowSizeDomain},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
	},
	operator_with::ApplyWith,
	value::batch::from_encoded_bytes,
};
use reifydb_flow_async::{
	operator::{
		host::TxnHostContext,
		state::seal::{
			coord::{Coord, IsZero},
			domain::SealDomain,
		},
	},
	transaction::FlowTransaction,
	window::{
		accumulator::{MergeAccumulator, WindowAccumulator},
		settings::WindowSettings,
		span::WindowSpan,
	},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_sdk::flow::operator::{
	column::{row::Row, sink::in_process::InProcessRowSink},
	mount::context::InProcessContext,
	view::in_process::InProcessColumnsView,
	windowed::operator::{CarryEmit, Contribution, Emit, WindowedOperator},
};
use reifydb_testing_chaos::operator::{
	event::{ChaosBatch, ChaosEvent, Row as CoreRow},
	view::MaterializedView,
};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

use super::{context::ChaosContext, materialize::materialize_history};
use crate::in_process::transaction::TestFlowTransaction;

fn with_oracle_ctx<R>(chaos: &ChaosContext, f: impl FnOnce(&mut InProcessContext<'_>) -> R) -> R {
	let mut txn = TestFlowTransaction::new(CommitVersion(1), Clock::Mock(MockClock::new(0)));
	for (dictionary, values) in &chaos.dictionaries {
		txn.catalog().cache().set_dictionary(dictionary.id, CommitVersion(1), Some(dictionary.clone()));
		txn.dictionary_allocators()
			.intern_batch(dictionary, values)
			.unwrap_or_else(|err| panic!("oracle dictionary seed failed: {err}"));
	}
	let mut host = TxnHostContext::new(&mut txn, OperatorId(1));
	let mut ctx = InProcessContext::new(&mut host, OperatorId(1));
	f(&mut ctx)
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
				ctx,
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

#[allow(clippy::type_complexity, clippy::too_many_arguments)]
fn apply_leg<A>(
	aggregate: &A,
	chaos: &ChaosContext,
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
	let Some((group, coord, contribution)) = extract_one(aggregate, chaos, row) else {
		return;
	};
	let span = WindowSpan::for_coord(coord, settings.fixed_size());
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
fn extract_one<A>(
	aggregate: &A,
	chaos: &ChaosContext,
	row: &CoreRow,
) -> Option<(Group<A>, TumblingCoord<A>, Contribution<A>)>
where
	A: WindowedOperator,
{
	let columns = from_encoded_bytes(&row.shape, &[row.number], slice::from_ref(&row.encoded))
		.unwrap_or_else(|err| panic!("oracle row batch failed: {err}"));
	let view = InProcessColumnsView::new(&columns);
	let coord = aggregate
		.coord(&view, &[0])
		.unwrap_or_else(|err| panic!("oracle coord read failed: {err}"))
		.pop()
		.flatten()?;
	let (group, contribution) = with_oracle_ctx(chaos, |ctx| aggregate.extract(ctx, &view, &[0]))
		.unwrap_or_else(|err| panic!("oracle extract read failed: {err}"))
		.pop()
		.flatten()?;
	Some((group, coord, contribution))
}

fn materialize_outputs<O: Row>(
	outputs: impl Iterator<Item = O>,
	now: DateTime,
	output_key_columns: &[String],
) -> MaterializedView {
	let mut sink = InProcessRowSink::new(<O as Row>::COLUMNS, 0).expect("output sink");
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
	let span = WindowSpan::new(end.saturating_sub_span(settings.fixed_size()), end);
	aggregate.build_output(group, span, &value)
}

fn rolling_pane<A>(settings: &WindowSettings<A::Coord>) -> <RollingCoord<A> as Coord>::Span
where
	A: WindowedOperator,
{
	settings.pane.expect("a rolling window needs a pane")
}

struct OraclePanes<A: WindowedOperator> {
	panes: BTreeMap<RollingCoord<A>, A::Accumulator>,
	rows: BTreeMap<RowNumber, (RollingCoord<A>, RollingContribution<A>)>,
	newest: RollingCoord<A>,
}

struct RollingRules<C: Coord> {
	size: C::Span,
	lateness: C::Span,
	retract: C::Span,
}

impl<C: Coord> RollingRules<C> {
	fn own_cutoff(&self, newest: C) -> Option<C> {
		newest.checked_sub_span(self.size)
	}

	fn admits(&self, newest: Option<C>, pane: C, retraction: bool) -> bool {
		let Some(newest) = newest else {
			return true;
		};
		if self.own_cutoff(newest).is_some_and(|cutoff| pane <= cutoff) {
			return false;
		}
		let line = if retraction {
			self.retract
		} else {
			self.lateness
		};
		pane >= newest.saturating_sub_span(line)
	}
}

#[allow(clippy::type_complexity)]
fn rolling_windows<A>(
	aggregate: &A,
	settings: &WindowSettings<DateTime>,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
) -> Vec<(RollingGroup<A>, BTreeMap<RollingCoord<A>, A::Accumulator>)>
where
	A: WindowedOperator<Coord = DateTime>,
{
	let pane = rolling_pane::<A>(settings);
	let rules = RollingRules::<DateTime> {
		size: settings.fixed_size(),
		lateness: settings.lateness,
		retract: settings.immutable.unwrap_or(settings.lateness),
	};
	let mut groups: HashMap<RollingGroup<A>, OraclePanes<A>> = HashMap::new();

	for batch in batches {
		let newest = |groups: &HashMap<RollingGroup<A>, OraclePanes<A>>, group: &RollingGroup<A>| {
			groups.get(group).map(|state| state.newest)
		};
		let mut admitted: Vec<(RollingGroup<A>, RollingCoord<A>, RowNumber, Leg<RollingContribution<A>>)> =
			Vec::new();
		for event in &batch.events {
			match event {
				ChaosEvent::Insert {
					row,
					..
				} => {
					if let Some((group, coord, contribution)) =
						extract_rolling(aggregate, ctx, pane, row) && rules.admits(
						newest(&groups, &group),
						coord,
						false,
					) {
						admitted.push((group, coord, row.number, Leg::Add(contribution)));
					}
				}
				ChaosEvent::Remove {
					row,
					..
				} => {
					if let Some((group, coord, contribution)) =
						extract_rolling(aggregate, ctx, pane, row) && rules.admits(
						newest(&groups, &group),
						coord,
						true,
					) {
						admitted.push((group, coord, row.number, Leg::Remove(contribution)));
					}
				}
				ChaosEvent::Update {
					pre,
					post,
					..
				} => {
					let before = extract_rolling(aggregate, ctx, pane, pre);
					let after = extract_rolling(aggregate, ctx, pane, post);
					match (before, after) {
						(Some(before), Some(after)) if before.0 == after.0 => {
							let current = newest(&groups, &before.0);
							if rules.admits(current, before.1, true)
								&& rules.admits(current, after.1, false)
							{
								admitted.push((
									before.0,
									before.1,
									pre.number,
									Leg::Remove(before.2),
								));
								admitted.push((
									after.0,
									after.1,
									post.number,
									Leg::Add(after.2),
								));
							}
						}
						(before, after) => {
							if let Some((group, coord, contribution)) = before
								&& rules.admits(newest(&groups, &group), coord, true)
							{
								admitted.push((
									group,
									coord,
									pre.number,
									Leg::Remove(contribution),
								));
							}
							if let Some((group, coord, contribution)) = after
								&& rules.admits(newest(&groups, &group), coord, false)
							{
								admitted.push((
									group,
									coord,
									post.number,
									Leg::Add(contribution),
								));
							}
						}
					}
				}
			}
		}
		let mut touched: BTreeSet<RollingGroup<A>> = BTreeSet::new();
		let before: HashMap<RollingGroup<A>, RollingCoord<A>> =
			groups.iter().map(|(group, state)| (group.clone(), state.newest)).collect();
		for (group, coord, row, leg) in admitted {
			let state = groups.entry(group.clone()).or_insert_with(|| OraclePanes {
				panes: BTreeMap::new(),
				rows: BTreeMap::new(),
				newest: coord,
			});
			let changed = match leg {
				Leg::Add(contribution) => {
					state.panes.entry(coord).or_default().add(&contribution);
					state.rows.insert(row, (coord, contribution));
					Some(coord)
				}
				Leg::Remove(_) => match state.rows.remove(&row) {
					Some((held, contribution))
						if rules.admits(before.get(&group).copied(), held, true) =>
					{
						let accumulator = state.panes.entry(held).or_default();
						accumulator.remove(&contribution);
						if accumulator.is_empty() {
							state.panes.remove(&held);
						}
						Some(held)
					}
					_ => None,
				},
			};
			if let Some(at) = changed {
				state.newest = state.newest.max(at);
				touched.insert(group);
			}
		}
		for group in touched {
			if let Some(state) = groups.get_mut(&group)
				&& let Some(cutoff) = rules.own_cutoff(state.newest)
			{
				state.panes.retain(|coord, _| *coord > cutoff);
				state.rows.retain(|_, (coord, _)| *coord > cutoff);
			}
		}
	}

	let drain_ms = i64::try_from(ctx.drain_at_ms).expect("chaos ledger time fits in i64 milliseconds");
	let drain = DateTime::from_epoch_millis(drain_ms).expect("rolling oracle drain time");
	let clock_cutoff = drain.checked_sub_span(rules.size).map(|cutoff| cutoff.floor_to(pane));
	groups.into_iter()
		.map(|(group, mut state)| {
			let cutoff = match (clock_cutoff, rules.own_cutoff(state.newest)) {
				(Some(clock), Some(own)) => Some(clock.max(own)),
				(clock, own) => clock.or(own),
			};
			if let Some(cutoff) = cutoff {
				state.panes.retain(|coord, _| *coord > cutoff);
				state.rows.retain(|_, (coord, _)| *coord > cutoff);
			}
			(group, state.panes)
		})
		.collect()
}

pub fn rolling_accumulator_oracle<A>(
	aggregate: &A,
	with: &ApplyWith,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedView
where
	A: Emit + WindowedOperator<Coord = DateTime>,
	A::Accumulator: MergeAccumulator,
	A::Output: Row,
{
	let settings = <DateTime as SealDomain>::window_settings_of(with).expect("rolling oracle window settings");
	let pane = rolling_pane::<A>(&settings);
	let outputs: Vec<A::Output> = rolling_windows(aggregate, &settings, ctx, batches)
		.into_iter()
		.filter_map(|(group, panes)| combine_rolling(aggregate, &settings, pane, &group, &panes))
		.collect();
	materialize_outputs(outputs.into_iter(), ctx.now(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn extract_rolling<A>(
	aggregate: &A,
	chaos: &ChaosContext,
	pane: <RollingCoord<A> as Coord>::Span,
	row: &CoreRow,
) -> Option<(RollingGroup<A>, RollingCoord<A>, RollingContribution<A>)>
where
	A: WindowedOperator,
{
	let columns = from_encoded_bytes(&row.shape, &[row.number], slice::from_ref(&row.encoded))
		.unwrap_or_else(|err| panic!("oracle row batch failed: {err}"));
	let view = InProcessColumnsView::new(&columns);
	let coord = aggregate
		.coord(&view, &[0])
		.unwrap_or_else(|err| panic!("oracle coord read failed: {err}"))
		.pop()
		.flatten()?;
	let (group, contribution) = with_oracle_ctx(chaos, |ctx| aggregate.extract(ctx, &view, &[0]))
		.unwrap_or_else(|err| panic!("oracle extract read failed: {err}"))
		.pop()
		.flatten()?;
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

fn bucket_carry<A>(
	aggregate: &A,
	chaos: &ChaosContext,
	settings: &WindowSettings<A::Coord>,
	batch: &ChaosBatch,
) -> CarryBuckets<A>
where
	A: WindowedOperator,
{
	let mut buckets: CarryBuckets<A> = BTreeMap::new();
	fan_out(batch, |row, is_add| push_carry(aggregate, chaos, settings, row, is_add, &mut buckets));
	buckets
}

fn push_carry<A>(
	aggregate: &A,
	chaos: &ChaosContext,
	settings: &WindowSettings<A::Coord>,
	row: &CoreRow,
	is_add: bool,
	buckets: &mut CarryBuckets<A>,
) where
	A: WindowedOperator,
{
	if let Some((group, coord, contribution)) = extract_carry(aggregate, chaos, row) {
		let span = WindowSpan::for_coord(coord, settings.fixed_size());
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
	let sealed_by_horizon =
		<A::Coord as SealDomain>::SIZE_DOMAIN == WindowSizeDomain::Time || !settings.lateness.is_zero();
	let retention = if sealed_by_horizon {
		None
	} else {
		settings.immutable
	};

	for batch in batches {
		let snapshot: HashMap<CarryGroup<A>, CarryCoord<A>> = HashMap::new();
		let buckets = bucket_carry(aggregate, ctx, settings, batch);

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

			if let (Some(retention), Some(hw)) = (retention, meta.high_water) {
				while let Some((&first, carry_out)) = meta.windows.iter().next() {
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
	settings: &WindowSettings<DateTime>,
	ctx: &ChaosContext,
	batches: &[ChaosBatch],
	output_key_columns: &[String],
) -> MaterializedView
where
	A: Emit<Output = BTreeMap<SK, R>> + WindowedOperator<Coord = DateTime>,
	A::Accumulator: MergeAccumulator,
	SK: Ord,
	R: Row,
{
	let pane = rolling_pane::<A>(settings);
	let outputs: Vec<R> = rolling_windows(aggregate, settings, ctx, batches)
		.into_iter()
		.flat_map(|(group, panes)| {
			combine_rolling(aggregate, settings, pane, &group, &panes).unwrap_or_default().into_values()
		})
		.collect();
	materialize_outputs(outputs.into_iter(), ctx.now(), output_key_columns)
}

#[allow(clippy::type_complexity)]
fn extract_carry<A>(
	aggregate: &A,
	chaos: &ChaosContext,
	row: &CoreRow,
) -> Option<(CarryGroup<A>, CarryCoord<A>, CarryContribution<A>)>
where
	A: WindowedOperator,
{
	let columns = from_encoded_bytes(&row.shape, &[row.number], slice::from_ref(&row.encoded))
		.unwrap_or_else(|err| panic!("oracle row batch failed: {err}"));
	let view = InProcessColumnsView::new(&columns);
	let coord = aggregate
		.coord(&view, &[0])
		.unwrap_or_else(|err| panic!("oracle coord read failed: {err}"))
		.pop()
		.flatten()?;
	let (group, contribution) = with_oracle_ctx(chaos, |ctx| aggregate.extract(ctx, &view, &[0]))
		.unwrap_or_else(|err| panic!("oracle extract read failed: {err}"))
		.pop()
		.flatten()?;
	Some((group, coord, contribution))
}
