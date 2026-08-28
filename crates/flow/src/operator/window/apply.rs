// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use std::collections::{BTreeMap, HashMap, HashSet};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::change::{Change, Diff},
	key::operator_state::GroupId,
	state::timer::TimerKind,
	value::column::columns::Columns,
};
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, duration::Duration},
};
use tracing::{Span, instrument};

use super::operator::WindowOperator;
use crate::{
	operator::{
		aggregation::{
			accumulator::{RowAccumulator, WindowSlotKey},
			engine::{
				EngineBuckets, WindowGroups, finish_tumbling_engine, intern_window_groups,
				route_into_buckets, slot_coord, window_group_key,
			},
		},
		host::HostContext,
		state::{
			reaper::{drain, enqueue},
			seal::{coord::Coord, ledger::FiredAt, policy::SealPolicy, sweep::SealSweep},
		},
		state_access::get,
	},
	window::{
		coord::{EventCoord, RowSpan},
		engine::{AccumulatorEvent, ExpiryAnchor, tumbling::TumblingEngine},
		kind::{
			ordinal_window_span,
			session::{SessionKind, SessionTracker},
			tumbling::TumblingOverRows,
		},
		meta::{EngineMeta, EngineMetaKey},
		span::WindowSpan,
	},
};

#[allow(clippy::too_many_arguments)]
#[instrument(name = "flow::operator::window::route", level = "trace", skip_all, fields(rows = columns.row_count()))]
fn route_engine_columns(
	operator: &WindowOperator,
	columns: &Columns,
	is_add: bool,
	window_size: Duration,
	buckets: &mut EngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	arrival: &mut Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: &mut HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
) -> Result<()> {
	let timestamps = operator.row_times(columns, columns.row_count())?;
	route_into_buckets(
		&operator.core,
		columns,
		is_add,
		|row_idx| {
			let ts = timestamps[row_idx];
			(WindowSpan::for_coord(ts, window_size), ts)
		},
		buckets,
		group_values,
		arrival,
		window_max_ts,
	)
}

#[allow(clippy::too_many_arguments)]
fn push_count_event(
	buckets: &mut EngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	arrival: &mut Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: &mut HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
	hash: Hash128,
	gvals: &[Value],
	span: WindowSpan<DateTime>,
	coord: WindowSlotKey,
	event: AccumulatorEvent<Vec<Option<Value>>>,
	event_ts: DateTime,
) {
	let now = event_ts;
	let key = (hash, span);
	let event = match event {
		AccumulatorEvent::Add(c) => AccumulatorEvent::Add((coord, c)),
		AccumulatorEvent::Remove(c) => AccumulatorEvent::Remove((coord, c)),
	};
	if matches!(event, AccumulatorEvent::Add(_)) {
		let entry = window_max_ts.entry(key).or_default();
		*entry = (*entry).max(now);
	}
	if !buckets.contains_key(&key) {
		arrival.push(key);
	}
	buckets.entry(key).or_default().push(event);
	group_values.entry(hash).or_insert_with(|| gvals.to_vec());
}

fn route_count_tumbling(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: &Change,
	buckets: &mut EngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	arrival: &mut Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: &mut HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
) -> Result<()> {
	let rows = TumblingOverRows::holding(RowSpan::of(operator.size_count().unwrap_or(1)));
	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => {
				let groups = operator.core.compute_groups(post)?;
				let slot_cols = operator.core.evaluate_slot_inputs(post)?;
				let times = operator.row_times(post, post.row_count())?;
				for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
					let ordinal = operator.get_and_increment_global_count(host, *hash)?;
					let window_id = rows.window_id(ordinal);
					operator.store_row_index(host, *hash, post.row_numbers()[row_idx], window_id)?;
					let contribution = operator.core.build_contribution(post, &slot_cols, row_idx);
					let coord = slot_coord(true, times[row_idx], post.row_numbers()[row_idx].0);
					push_count_event(
						buckets,
						group_values,
						arrival,
						window_max_ts,
						*hash,
						gvals,
						ordinal_window_span(window_id),
						coord,
						AccumulatorEvent::Add(contribution),
						times[row_idx],
					);
				}
			}
			Diff::Remove {
				pre,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let slot_cols = operator.core.evaluate_slot_inputs(pre)?;
				let times = operator.row_times(pre, pre.row_count())?;
				for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
					let contribution = operator.core.build_contribution(pre, &slot_cols, row_idx);
					let coord = slot_coord(true, times[row_idx], pre.row_numbers()[row_idx].0);
					for window_id in
						operator.lookup_row_index(host, *hash, pre.row_numbers()[row_idx])?
					{
						push_count_event(
							buckets,
							group_values,
							arrival,
							window_max_ts,
							*hash,
							gvals,
							ordinal_window_span(window_id),
							coord,
							AccumulatorEvent::Remove(contribution.clone()),
							times[row_idx],
						);
					}
					operator.drop_row_index(host, *hash, pre.row_numbers()[row_idx])?;
				}
			}
			Diff::Update {
				pre,
				post,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let pre_cols = operator.core.evaluate_slot_inputs(pre)?;
				let post_cols = operator.core.evaluate_slot_inputs(post)?;
				let times = operator.row_times(post, post.row_count())?;
				for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
					let row_number = pre.row_numbers()[row_idx];
					let existing = operator.lookup_row_index(host, *hash, row_number)?;
					if existing.is_empty() {
						let ordinal = operator.get_and_increment_global_count(host, *hash)?;
						let window_id = rows.window_id(ordinal);
						operator.store_row_index(
							host,
							*hash,
							post.row_numbers()[row_idx],
							window_id,
						)?;
						let contribution =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord =
							slot_coord(true, times[row_idx], post.row_numbers()[row_idx].0);
						push_count_event(
							buckets,
							group_values,
							arrival,
							window_max_ts,
							*hash,
							gvals,
							ordinal_window_span(window_id),
							coord,
							AccumulatorEvent::Add(contribution),
							times[row_idx],
						);
					} else {
						let pre_contrib =
							operator.core.build_contribution(pre, &pre_cols, row_idx);
						let post_contrib =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord =
							slot_coord(true, times[row_idx], pre.row_numbers()[row_idx].0);
						for window_id in existing {
							push_count_event(
								buckets,
								group_values,
								arrival,
								window_max_ts,
								*hash,
								gvals,
								ordinal_window_span(window_id),
								coord,
								AccumulatorEvent::Remove(pre_contrib.clone()),
								times[row_idx],
							);
							push_count_event(
								buckets,
								group_values,
								arrival,
								window_max_ts,
								*hash,
								gvals,
								ordinal_window_span(window_id),
								coord,
								AccumulatorEvent::Add(post_contrib.clone()),
								times[row_idx],
							);
						}
					}
				}
			}
		}
	}
	Ok(())
}

#[instrument(name = "flow::operator::window::tumbling", level = "trace", skip_all)]
pub fn apply_tumbling_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	let window_size = operator.size_duration().unwrap_or_default();
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<DateTime>), DateTime> = HashMap::new();

	if operator.is_count_based() {
		route_count_tumbling(
			operator,
			host,
			&change,
			&mut buckets,
			&mut group_values,
			&mut arrival,
			&mut window_max_ts,
		)?;
	} else {
		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => route_engine_columns(
					operator,
					post,
					true,
					window_size,
					&mut buckets,
					&mut group_values,
					&mut arrival,
					&mut window_max_ts,
				)?,
				Diff::Remove {
					pre,
					..
				} => route_engine_columns(
					operator,
					pre,
					false,
					window_size,
					&mut buckets,
					&mut group_values,
					&mut arrival,
					&mut window_max_ts,
				)?,
				Diff::Update {
					pre,
					post,
					..
				} => {
					route_engine_columns(
						operator,
						pre,
						false,
						window_size,
						&mut buckets,
						&mut group_values,
						&mut arrival,
						&mut window_max_ts,
					)?;
					route_engine_columns(
						operator,
						post,
						true,
						window_size,
						&mut buckets,
						&mut group_values,
						&mut arrival,
						&mut window_max_ts,
					)?;
				}
			}
		}
	}

	gate_and_arm_seals(
		operator,
		host,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		SealPolicy::tumbling(window_size, operator.lateness().unwrap_or_else(Duration::zero)),
		ExpiryAnchor::WindowStart,
	)?;

	let groups = intern_batch(&arrival);

	let engine_config = operator.engine_config();
	let engine_immutable = operator.immutable();
	let count_based = operator.is_count_based();
	let expiry_anchor = if count_based {
		ExpiryAnchor::Unindexed
	} else {
		ExpiryAnchor::WindowStart
	};
	let diffs = finish_tumbling_engine(
		&mut operator.core,
		host,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&groups,
		&kinds,
		engine_config,
		engine_immutable,
		expiry_anchor,
		count_based,
	)?;
	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

#[instrument(name = "flow::operator::window::intern", level = "trace", skip_all, fields(windows = arrival.len()))]
fn intern_batch(arrival: &[(Hash128, WindowSpan<DateTime>)]) -> WindowGroups {
	let windows: Vec<(Hash128, u64)> = arrival.iter().map(|(hash, span)| (*hash, span.start.to_order())).collect();
	intern_window_groups(&windows)
}

fn sliding_insert_anchors(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	hash: Hash128,
	event_ts: DateTime,
	is_count: bool,
) -> Result<Vec<u64>> {
	let coord = if is_count {
		operator.get_and_increment_global_count(host, hash)?.value()
	} else {
		event_ts.to_order()
	};
	Ok(operator.sliding_window_anchors(coord))
}

#[instrument(name = "flow::operator::window::sliding", level = "trace", skip_all)]
pub fn apply_sliding_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let is_count = operator.is_count_based();
	let window_size = operator.size_duration().unwrap_or_default();

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<DateTime>), DateTime> = HashMap::new();

	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => {
				let groups = operator.core.compute_groups(post)?;
				let timestamps = if is_count {
					Vec::new()
				} else {
					operator.row_times(post, post.row_count())?
				};
				let slot_cols = operator.core.evaluate_slot_inputs(post)?;
				for row_idx in 0..post.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let event_ts = if is_count {
						DateTime::default()
					} else {
						timestamps[row_idx]
					};
					let window_ids =
						sliding_insert_anchors(operator, host, *hash, event_ts, is_count)?;
					let contribution = operator.core.build_contribution(post, &slot_cols, row_idx);
					let coord = slot_coord(is_count, event_ts, post.row_numbers()[row_idx].0);
					for wid in &window_ids {
						operator.store_row_index(
							host,
							*hash,
							post.row_numbers()[row_idx],
							*wid,
						)?;
						push_count_event(
							&mut buckets,
							&mut group_values,
							&mut arrival,
							&mut window_max_ts,
							*hash,
							gvals,
							operator.sliding_window_span(*wid),
							coord,
							AccumulatorEvent::Add(contribution.clone()),
							event_ts,
						);
					}
				}
			}
			Diff::Remove {
				pre,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let timestamps = if is_count {
					Vec::new()
				} else {
					operator.row_times(pre, pre.row_count())?
				};
				let slot_cols = operator.core.evaluate_slot_inputs(pre)?;
				for row_idx in 0..pre.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let event_ts = if is_count {
						DateTime::default()
					} else {
						timestamps[row_idx]
					};
					let contribution = operator.core.build_contribution(pre, &slot_cols, row_idx);
					let coord = slot_coord(is_count, event_ts, pre.row_numbers()[row_idx].0);
					for wid in operator.lookup_row_index(host, *hash, pre.row_numbers()[row_idx])? {
						push_count_event(
							&mut buckets,
							&mut group_values,
							&mut arrival,
							&mut window_max_ts,
							*hash,
							gvals,
							operator.sliding_window_span(wid),
							coord,
							AccumulatorEvent::Remove(contribution.clone()),
							event_ts,
						);
					}
					operator.drop_row_index(host, *hash, pre.row_numbers()[row_idx])?;
				}
			}
			Diff::Update {
				pre,
				post,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let timestamps = if is_count {
					Vec::new()
				} else {
					operator.row_times(post, post.row_count())?
				};
				let pre_cols = operator.core.evaluate_slot_inputs(pre)?;
				let post_cols = operator.core.evaluate_slot_inputs(post)?;
				for row_idx in 0..pre.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let row_number = pre.row_numbers()[row_idx];
					let event_ts = if is_count {
						DateTime::default()
					} else {
						timestamps[row_idx]
					};
					let existing = operator.lookup_row_index(host, *hash, row_number)?;
					if existing.is_empty() {
						let window_ids = sliding_insert_anchors(
							operator, host, *hash, event_ts, is_count,
						)?;
						let contribution =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord = slot_coord(is_count, event_ts, row_number.0);
						for wid in &window_ids {
							operator.store_row_index(
								host,
								*hash,
								post.row_numbers()[row_idx],
								*wid,
							)?;
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								operator.sliding_window_span(*wid),
								coord,
								AccumulatorEvent::Add(contribution.clone()),
								event_ts,
							);
						}
					} else {
						let pre_contrib =
							operator.core.build_contribution(pre, &pre_cols, row_idx);
						let post_contrib =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord = slot_coord(is_count, event_ts, row_number.0);
						for wid in existing {
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								operator.sliding_window_span(wid),
								coord,
								AccumulatorEvent::Remove(pre_contrib.clone()),
								event_ts,
							);
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								operator.sliding_window_span(wid),
								coord,
								AccumulatorEvent::Add(post_contrib.clone()),
								event_ts,
							);
						}
					}
				}
			}
		}
	}

	gate_and_arm_seals(
		operator,
		host,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		SealPolicy::tumbling(window_size, operator.lateness().unwrap_or_else(Duration::zero)),
		ExpiryAnchor::WindowStart,
	)?;

	let groups = intern_batch(&arrival);

	let engine_config = operator.engine_config();
	let engine_immutable = operator.immutable();
	let count_based = operator.is_count_based();
	let expiry_anchor = if count_based {
		ExpiryAnchor::Unindexed
	} else {
		ExpiryAnchor::WindowStart
	};
	let diffs = finish_tumbling_engine(
		&mut operator.core,
		host,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&groups,
		&kinds,
		engine_config,
		engine_immutable,
		expiry_anchor,
		true,
	)?;
	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

fn session_assign(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	hash: Hash128,
	event_ts: DateTime,
	kind: &SessionKind,
	trackers: &mut HashMap<Hash128, SessionTracker>,
) -> Result<Option<u64>> {
	let mut tracker = match trackers.get(&hash) {
		Some(&tracker) => tracker,
		None => operator.load_session_tracker(host, hash)?,
	};
	let assignment = kind.assign(&mut tracker, EventCoord::of(&event_ts));
	if assignment.session_id().is_some() {
		trackers.insert(hash, tracker);
	}
	Ok(assignment.session_id())
}

#[instrument(name = "flow::operator::window::session", level = "trace", skip_all)]
pub fn apply_session_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let kind = operator.session_kind();

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<DateTime>), DateTime> = HashMap::new();
	let mut trackers: HashMap<Hash128, SessionTracker> = HashMap::new();

	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => {
				let groups = operator.core.compute_groups(post)?;
				let timestamps = operator.row_times(post, post.row_count())?;
				let slot_cols = operator.core.evaluate_slot_inputs(post)?;
				for row_idx in 0..post.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let event_ts = timestamps[row_idx];
					if let Some(session_id) =
						session_assign(operator, host, *hash, event_ts, &kind, &mut trackers)?
					{
						operator.store_row_index(
							host,
							*hash,
							post.row_numbers()[row_idx],
							session_id,
						)?;
						let contribution =
							operator.core.build_contribution(post, &slot_cols, row_idx);
						let coord = slot_coord(false, event_ts, post.row_numbers()[row_idx].0);
						push_count_event(
							&mut buckets,
							&mut group_values,
							&mut arrival,
							&mut window_max_ts,
							*hash,
							gvals,
							ordinal_window_span(session_id),
							coord,
							AccumulatorEvent::Add(contribution),
							event_ts,
						);
					}
				}
			}
			Diff::Remove {
				pre,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let timestamps = operator.row_times(pre, pre.row_count())?;
				let slot_cols = operator.core.evaluate_slot_inputs(pre)?;
				for row_idx in 0..pre.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let event_ts = timestamps[row_idx];
					let contribution = operator.core.build_contribution(pre, &slot_cols, row_idx);
					let coord = slot_coord(false, event_ts, pre.row_numbers()[row_idx].0);
					for session_id in
						operator.lookup_row_index(host, *hash, pre.row_numbers()[row_idx])?
					{
						push_count_event(
							&mut buckets,
							&mut group_values,
							&mut arrival,
							&mut window_max_ts,
							*hash,
							gvals,
							ordinal_window_span(session_id),
							coord,
							AccumulatorEvent::Remove(contribution.clone()),
							event_ts,
						);
					}
					operator.drop_row_index(host, *hash, pre.row_numbers()[row_idx])?;
				}
			}
			Diff::Update {
				pre,
				post,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let timestamps = operator.row_times(post, post.row_count())?;
				let pre_cols = operator.core.evaluate_slot_inputs(pre)?;
				let post_cols = operator.core.evaluate_slot_inputs(post)?;
				for row_idx in 0..pre.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let event_ts = timestamps[row_idx];
					let existing =
						operator.lookup_row_index(host, *hash, pre.row_numbers()[row_idx])?;
					if existing.is_empty() {
						if let Some(session_id) = session_assign(
							operator,
							host,
							*hash,
							event_ts,
							&kind,
							&mut trackers,
						)? {
							operator.store_row_index(
								host,
								*hash,
								post.row_numbers()[row_idx],
								session_id,
							)?;
							let contribution = operator
								.core
								.build_contribution(post, &post_cols, row_idx);
							let coord = slot_coord(
								false,
								event_ts,
								post.row_numbers()[row_idx].0,
							);
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								ordinal_window_span(session_id),
								coord,
								AccumulatorEvent::Add(contribution),
								event_ts,
							);
						}
					} else {
						let pre_contrib =
							operator.core.build_contribution(pre, &pre_cols, row_idx);
						let post_contrib =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord = slot_coord(false, event_ts, pre.row_numbers()[row_idx].0);
						for session_id in existing {
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								ordinal_window_span(session_id),
								coord,
								AccumulatorEvent::Remove(pre_contrib.clone()),
								event_ts,
							);
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								ordinal_window_span(session_id),
								coord,
								AccumulatorEvent::Add(post_contrib.clone()),
								event_ts,
							);
						}
					}
				}
			}
		}
	}

	for (hash, tracker) in &trackers {
		operator.save_session_tracker(host, *hash, tracker)?;
	}

	gate_and_arm_seals(
		operator,
		host,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		operator.session_policy(),
		ExpiryAnchor::LastEvent,
	)?;

	let groups = intern_batch(&arrival);

	let engine_config = operator.engine_config();
	let engine_immutable = operator.immutable();
	let diffs = finish_tumbling_engine(
		&mut operator.core,
		host,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&groups,
		&kinds,
		engine_config,
		engine_immutable,
		ExpiryAnchor::LastEvent,
		true,
	)?;
	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

#[instrument(name = "flow::operator::window::gate_seals", level = "trace", skip_all)]
fn gate_and_arm_seals(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	buckets: &mut EngineBuckets,
	arrival: &mut Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: &HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
	policy: SealPolicy,
	anchor: ExpiryAnchor,
) -> Result<()> {
	if policy.is_inert() || operator.is_count_based() {
		return Ok(());
	}
	let gate = operator.seal_gate(host, policy)?;
	let mut sealed: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut rearm: Vec<(Hash128, u64, Option<u64>, u64)> = Vec::new();
	let mut dropped = 0u64;
	{
		for (key, events) in buckets.iter() {
			let group = GroupId::of(&window_group_key(key.0, key.1.start.to_order()));
			let prior_last = get::<_, EngineMeta>(host, &EngineMetaKey(group))?.map(|m| m.last_event_time);
			let batch_last = window_max_ts.get(key).map(|ts| ts.to_order());
			let last = prior_last.max(batch_last);
			let window_start = key.1.start.to_order();
			let Some(horizon) = anchor.of(window_start, last) else {
				continue;
			};
			let prior_horizon = anchor.of(window_start, prior_last);
			if !gate.admits(horizon) {
				dropped += events.len() as u64;
				sealed.push(*key);
			} else {
				rearm.push((key.0, window_start, prior_horizon, horizon));
			}
		}
	}

	for (hash, window_start, prior_horizon, horizon) in rearm {
		gate.arm(host, &window_group_key(hash, window_start), prior_horizon, horizon)?;
	}

	if sealed.is_empty() {
		return Ok(());
	}
	for key in &sealed {
		buckets.remove(key);
	}
	let sealed: HashSet<(Hash128, WindowSpan<DateTime>)> = sealed.into_iter().collect();
	arrival.retain(|key| !sealed.contains(key));
	operator.note_sealed_drops(dropped);
	Ok(())
}

#[tracing::instrument(name = "flow::window::seal", level = "debug", skip_all, fields(operator = operator.core.operator.0, expired = tracing::field::Empty))]
#[instrument(name = "flow::operator::window::seal", level = "trace", skip_all)]
fn seal_due_windows(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	fired: FiredAt,
	policy: SealPolicy,
) -> Result<Vec<Diff>> {
	if policy.is_inert() {
		return Ok(Vec::new());
	}
	operator.advance_seal_ledger(host, fired)?;
	let Some(threshold) = SealSweep::new(policy).horizon(fired) else {
		return Ok(Vec::new());
	};
	let config = operator.engine_config();
	let expired = {
		let mut engine =
			operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
				Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::new(config))
			});
		let res = engine.expire(host, threshold.to_order())?;
		for window in &res {
			enqueue(host, window.group_id)?;
		}
		if !res.is_empty() {
			let due = fired.at().saturating_add(policy.admissible().duration());
			host.arm_timer(due, TimerKind::Maintenance, &EncodedKey::new(Vec::new()))?;
		}
		*operator.core.tumbling_engine_slot() = Some(engine);
		res
	};
	Span::current().record("expired", expired.len());
	Ok(Vec::new())
}

pub fn reap_sealed_groups(operator: &mut WindowOperator, host: &mut dyn HostContext) -> Result<usize> {
	let config = operator.engine_config();
	let budget = config.expire_batch();
	let mut engine = operator
		.core
		.tumbling_engine_slot()
		.take()
		.unwrap_or_else(|| Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::new(config)));
	let drained = drain(host, &mut *engine, budget)?;
	*operator.core.tumbling_engine_slot() = Some(engine);
	Ok(drained.freed)
}

pub fn seal_session_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	fired: FiredAt,
) -> Result<Vec<Diff>> {
	let policy = operator.session_policy();
	seal_due_windows(operator, host, fired, policy)
}

pub fn seal_engine_windows(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	fired: FiredAt,
) -> Result<Vec<Diff>> {
	let Some(window_size) = operator.size_duration() else {
		return Ok(Vec::new());
	};
	let policy = SealPolicy::tumbling(window_size, operator.lateness().unwrap_or_else(Duration::zero));
	seal_due_windows(operator, host, fired, policy)
}

#[cfg(test)]
mod tests {
	use reifydb_value::{factory::time::at_millis, value::duration::Duration};

	use super::SealPolicy;
	use crate::operator::state::seal::{
		coord::Coord,
		policy::{is_sealed, seal_horizon},
	};

	#[test]
	fn the_armed_seal_instant_reproduces_the_pre_timer_boundary() {
		// The armed instant and the sealing gate must be the same expression, or a bucket is
		// dropped before its timer fires or rebuilt from a late row after it did. The wheel fires
		// at watermark >= at, so reproducing the strict gate needs the +1 seal_instant carries.
		let cutoff_ms = 19u64;
		let cutoff = Duration::from_milliseconds(cutoff_ms as i64).expect("representable span");
		let policy = SealPolicy::tumbling(cutoff, Duration::from_milliseconds_const(0));
		let last = 10u64;
		let order = |millis: u64| at_millis(millis).to_order();
		let sealed = |wm: u64| policy.seal_instant_from_order(order(last)).at().to_order() <= order(wm);
		let pre_timer_gate = |wm: u64| wm.saturating_sub(last) > cutoff_ms;

		for wm in 0..100u64 {
			assert_eq!(
				sealed(wm),
				pre_timer_gate(wm),
				"timer seal diverges from the gate at watermark {wm}"
			);
		}
		assert!(!sealed(last + cutoff_ms), "watermark exactly cutoff past the last event is still mutable");
		assert!(sealed(last + cutoff_ms + 1), "one past the cutoff is sealed");
	}

	#[test]
	fn seal_horizon_saturates_for_young_watermarks() {
		// The epoch is the domain floor; wrapping past it declares every window sealed.
		let lateness = Duration::from_milliseconds_const(10);

		assert_eq!(
			seal_horizon(at_millis(3), lateness),
			at_millis(0),
			"young watermark saturates to the epoch"
		);
		assert!(
			!is_sealed(at_millis(0), seal_horizon(at_millis(3), lateness)),
			"the epoch is not below itself"
		);
		assert!(
			is_sealed(at_millis(4), seal_horizon(at_millis(20), lateness)),
			"anchor below watermark - lateness is sealed"
		);
	}
}
