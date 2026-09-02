// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use std::collections::{BTreeMap, HashMap, HashSet};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::WindowKind,
	interface::change::{Change, Diff},
	key::operator::state::GroupId,
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
			reaper::{self, drain, enqueue},
			seal::{coord::Coord, gate::rearm_seal, ledger::FiredAt, rule::SealRule, sweep::SealSweep},
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
					let contribution = operator.core.build_contribution(
						post,
						&slot_cols,
						row_idx,
						times[row_idx],
					);
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
					let contribution = operator.core.build_contribution(
						pre,
						&slot_cols,
						row_idx,
						times[row_idx],
					);
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
						let contribution = operator.core.build_contribution(
							post,
							&post_cols,
							row_idx,
							times[row_idx],
						);
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
						let pre_contrib = operator.core.build_contribution(
							pre,
							&pre_cols,
							row_idx,
							times[row_idx],
						);
						let post_contrib = operator.core.build_contribution(
							post,
							&post_cols,
							row_idx,
							times[row_idx],
						);
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

	let rule = SealRule::tumbling(window_size, operator.lateness().unwrap_or_else(Duration::zero));
	drop_sealed_events(
		operator,
		host,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		rule,
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
	let armed_before = armed_engine_seal(operator, host, rule)?;
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
	rearm_engine_seal(operator, host, rule, armed_before)?;
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
				let timestamps = if is_count && !operator.core.needs_event_time() {
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
					let contribution = operator.core.build_contribution(
						post,
						&slot_cols,
						row_idx,
						timestamps.get(row_idx).copied().unwrap_or_default(),
					);
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
				let timestamps = if is_count && !operator.core.needs_event_time() {
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
					let contribution = operator.core.build_contribution(
						pre,
						&slot_cols,
						row_idx,
						timestamps.get(row_idx).copied().unwrap_or_default(),
					);
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
				let timestamps = if is_count && !operator.core.needs_event_time() {
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
						let contribution = operator.core.build_contribution(
							post,
							&post_cols,
							row_idx,
							timestamps.get(row_idx).copied().unwrap_or_default(),
						);
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
						let pre_contrib = operator.core.build_contribution(
							pre,
							&pre_cols,
							row_idx,
							timestamps.get(row_idx).copied().unwrap_or_default(),
						);
						let post_contrib = operator.core.build_contribution(
							post,
							&post_cols,
							row_idx,
							timestamps.get(row_idx).copied().unwrap_or_default(),
						);
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

	let rule = SealRule::tumbling(window_size, operator.lateness().unwrap_or_else(Duration::zero));
	drop_sealed_events(
		operator,
		host,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		rule,
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
	let armed_before = armed_engine_seal(operator, host, rule)?;
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
	rearm_engine_seal(operator, host, rule, armed_before)?;
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
						let contribution = operator.core.build_contribution(
							post,
							&slot_cols,
							row_idx,
							timestamps[row_idx],
						);
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
					let contribution = operator.core.build_contribution(
						pre,
						&slot_cols,
						row_idx,
						timestamps[row_idx],
					);
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
							let contribution = operator.core.build_contribution(
								post,
								&post_cols,
								row_idx,
								timestamps[row_idx],
							);
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
						let pre_contrib = operator.core.build_contribution(
							pre,
							&pre_cols,
							row_idx,
							timestamps[row_idx],
						);
						let post_contrib = operator.core.build_contribution(
							post,
							&post_cols,
							row_idx,
							timestamps[row_idx],
						);
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

	let rule = operator.session_rule();
	drop_sealed_events(operator, host, &mut buckets, &mut arrival, &window_max_ts, rule, ExpiryAnchor::LastEvent)?;

	let groups = intern_batch(&arrival);

	let engine_config = operator.engine_config();
	let engine_immutable = operator.immutable();
	let armed_before = armed_engine_seal(operator, host, rule)?;
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
	rearm_engine_seal(operator, host, rule, armed_before)?;
	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

#[instrument(name = "flow::operator::window::gate_seals", level = "trace", skip_all)]
fn drop_sealed_events(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	buckets: &mut EngineBuckets,
	arrival: &mut Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: &HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
	rule: SealRule,
	anchor: ExpiryAnchor,
) -> Result<()> {
	if rule.is_inert() || operator.is_count_based() {
		return Ok(());
	}
	let gate = operator.seal_gate(host, rule)?;
	let mut sealed: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
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
			if !gate.admits(horizon) {
				dropped += events.len() as u64;
				sealed.push(*key);
			}
		}
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

fn engine_arms_seal(operator: &WindowOperator, rule: SealRule) -> bool {
	!rule.is_inert() && !operator.is_count_based()
}

fn engine_earliest_expiry(operator: &mut WindowOperator, host: &mut dyn HostContext) -> Result<Option<u64>> {
	let config = operator.engine_config();
	let mut engine = operator
		.core
		.tumbling_engine_slot()
		.take()
		.unwrap_or_else(|| Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::new(config)));
	let earliest = engine.earliest_expiry(host)?;
	*operator.core.tumbling_engine_slot() = Some(engine);
	Ok(earliest)
}

fn armed_engine_seal(operator: &mut WindowOperator, host: &mut dyn HostContext, rule: SealRule) -> Result<Option<u64>> {
	if !engine_arms_seal(operator, rule) {
		return Ok(None);
	}
	engine_earliest_expiry(operator, host)
}

fn rearm_engine_seal(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	rule: SealRule,
	before: Option<u64>,
) -> Result<()> {
	if !engine_arms_seal(operator, rule) {
		return Ok(());
	}
	let after = engine_earliest_expiry(operator, host)?;
	rearm_seal(host, rule, &EncodedKey::new(Vec::new()), before, after)
}

#[tracing::instrument(name = "flow::window::seal", level = "debug", skip_all, fields(operator = operator.core.operator.0, expired = tracing::field::Empty))]
#[instrument(name = "flow::operator::window::seal", level = "trace", skip_all)]
fn seal_due_windows(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	fired: FiredAt,
	rule: SealRule,
) -> Result<Vec<Diff>> {
	if rule.is_inert() {
		return Ok(Vec::new());
	}
	operator.advance_seal_ledger(host, fired)?;
	let Some(threshold) = SealSweep::new(rule).horizon(fired) else {
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
			let due = fired.at().saturating_add(rule.admissible().duration());
			host.arm_timer(due, TimerKind::Maintenance, &EncodedKey::new(Vec::new()))?;
		}
		*operator.core.tumbling_engine_slot() = Some(engine);
		res
	};
	rearm_engine_seal(operator, host, rule, None)?;
	Span::current().record("expired", expired.len());
	Ok(Vec::new())
}

fn maintenance_rule(operator: &WindowOperator) -> Option<SealRule> {
	match operator.kind {
		WindowKind::Session {
			..
		} => Some(operator.session_rule()),
		_ => operator
			.size_duration()
			.map(|size| SealRule::tumbling(size, operator.lateness().unwrap_or_else(Duration::zero))),
	}
}

pub fn reap_sealed_groups(operator: &mut WindowOperator, host: &mut dyn HostContext, fired: FiredAt) -> Result<usize> {
	let config = operator.engine_config();
	let budget = config.expire_batch();
	let mut engine = operator
		.core
		.tumbling_engine_slot()
		.take()
		.unwrap_or_else(|| Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::new(config)));
	let drained = drain(host, reaper::WINDOW, &mut *engine, budget)?;
	*operator.core.tumbling_engine_slot() = Some(engine);
	if !drained.queue_is_empty()
		&& let Some(rule) = maintenance_rule(operator)
	{
		let due = fired.at().saturating_add(rule.admissible().duration());
		host.arm_timer(due, TimerKind::Maintenance, &EncodedKey::new(Vec::new()))?;
	}
	Ok(drained.freed)
}

pub fn seal_session_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	fired: FiredAt,
) -> Result<Vec<Diff>> {
	let rule = operator.session_rule();
	seal_due_windows(operator, host, fired, rule)
}

pub fn seal_engine_windows(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	fired: FiredAt,
) -> Result<Vec<Diff>> {
	let Some(window_size) = operator.size_duration() else {
		return Ok(Vec::new());
	};
	let rule = SealRule::tumbling(window_size, operator.lateness().unwrap_or_else(Duration::zero));
	seal_due_windows(operator, host, fired, rule)
}

#[cfg(test)]
mod tests {
	use reifydb_value::{factory::time::at_millis, value::duration::Duration};

	use super::SealRule;
	use crate::operator::state::seal::{
		coord::Coord,
		rule::{is_sealed, seal_horizon},
	};

	#[test]
	fn the_armed_seal_instant_reproduces_the_pre_timer_boundary() {
		// The armed instant and the sealing gate must be the same expression, or a bucket is
		// dropped before its timer fires or rebuilt from a late row after it did. The wheel fires
		// at watermark >= at, so reproducing the strict gate needs the +1 seal_instant carries.
		let cutoff_ms = 19u64;
		let cutoff = Duration::from_milliseconds(cutoff_ms as i64).expect("representable span");
		let rule = SealRule::tumbling(cutoff, Duration::from_milliseconds_const(0));
		let last = 10u64;
		let order = |millis: u64| at_millis(millis).to_order();
		let sealed = |wm: u64| rule.seal_instant_from_order(order(last)).at().to_order() <= order(wm);
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

#[cfg(test)]
mod reap_tests {
	use std::sync::Arc;

	use reifydb_core::{
		common::{CommitVersion, WindowSize},
		interface::catalog::flow::OperatorId,
		key::operator::state::{KeyspaceId, keyspace_inner_range},
	};
	use reifydb_routine_abi::registry::Routines;
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{factory::time::at_millis, value::duration::Duration};

	use super::*;
	use crate::{
		context::FlowContext,
		operator::{
			host::TxnHostContext,
			window::operator::{WindowConfig, WindowOperator},
		},
		timer::Timer,
		transaction::{
			ChangeCoordinate, FlowTransaction,
			deferred::DeferredTransaction,
			mock::FlowTxn,
			state::{StateExtension, StateRange},
		},
	};

	fn window(operator: u64) -> WindowOperator {
		WindowOperator::new(WindowConfig {
			parent_schema: None,
			operator: OperatorId(operator),
			kind: WindowKind::Tumbling {
				size: WindowSize::Duration(Duration::from_seconds(10).unwrap()),
			},
			group_by: Vec::new(),
			aggregations: Vec::new(),
			runtime_context: RuntimeContext::testing(0, 1),
			routines: Routines::empty(),
			lateness: None,
			immutable: None,
			ctx: Arc::new(FlowContext::default()),
		})
	}

	fn txn_at(engine: &TestEngine, coordinate: u64) -> DeferredTransaction {
		let mut txn = engine.flow_txn().at(CommitVersion(coordinate)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_nanos(coordinate)),
			version: CommitVersion(coordinate),
		});
		txn
	}

	fn armed_timers(txn: &mut DeferredTransaction, operator: OperatorId) -> usize {
		txn.state_range(
			operator,
			StateRange::forward(keyspace_inner_range(GroupId::ROOT, KeyspaceId::TIMER_WHEEL), "test"),
		)
		.unwrap()
		.items
		.len()
	}

	fn fired() -> FiredAt {
		FiredAt::of(&Timer {
			due: at_millis(1_000),
			kind: TimerKind::Maintenance,
			key: EncodedKey::new(Vec::new()),
		})
	}

	fn reap_with_queued(groups: u128) -> usize {
		let engine = TestEngine::new();
		let mut operator = window(1);
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			for n in 0..groups {
				enqueue(&mut host, GroupId(n + 1)).unwrap();
			}
			reap_sealed_groups(&mut operator, &mut host, fired()).unwrap();
		}
		armed_timers(&mut txn, id)
	}

	#[test]
	fn a_reap_backlog_larger_than_one_budget_arms_another_maintenance_pass() {
		// one maintenance pass drains at most expire_batch groups; if the pass does not re-arm
		// itself the remainder is never revisited and the reap queue grows without bound
		let budget = window(1).engine_config().expire_batch() as u128;

		assert!(
			reap_with_queued(budget + 5) > 0,
			"a queue deeper than one budget must leave a maintenance timer armed to finish it"
		);
	}

	#[test]
	fn a_reap_queue_that_empties_in_one_pass_arms_nothing_further() {
		// re-arming unconditionally would spin the maintenance timer forever on an empty queue
		assert_eq!(reap_with_queued(2), 0, "a queue the pass drained to empty must not schedule another pass");
	}
}

#[cfg(test)]
mod seal_arm_tests {
	use std::sync::Arc;

	use reifydb_core::{
		common::{CommitVersion, WindowSize},
		interface::catalog::flow::OperatorId,
		key::{
			EncodableKey,
			operator::{
				keyspace::timer::TimerWheelKey,
				state::{KeyspaceId, OperatorStateKey, keyspace_inner_range},
			},
		},
		state::typed::SuffixBytes,
	};
	use reifydb_routine_abi::registry::Routines;
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{factory::time::at_millis, value::duration::Duration};

	use super::*;
	use crate::{
		context::FlowContext,
		operator::{
			host::TxnHostContext,
			window::operator::{WindowConfig, WindowOperator},
		},
		timer::{Timer, wheel::TimerWheel},
		transaction::{
			ChangeCoordinate, FlowTransaction,
			deferred::DeferredTransaction,
			mock::FlowTxn,
			state::{StateExtension, StateRange},
		},
	};

	const SIZE_MS: u64 = 10_000;

	fn window() -> WindowOperator {
		WindowOperator::new(WindowConfig {
			parent_schema: None,
			operator: OperatorId(1),
			kind: WindowKind::Tumbling {
				size: WindowSize::Duration(Duration::from_milliseconds(SIZE_MS as i64).unwrap()),
			},
			group_by: Vec::new(),
			aggregations: Vec::new(),
			runtime_context: RuntimeContext::testing(0, 1),
			routines: Routines::empty(),
			lateness: None,
			immutable: None,
			ctx: Arc::new(FlowContext::default()),
		})
	}

	fn rule() -> SealRule {
		SealRule::tumbling(Duration::from_milliseconds(SIZE_MS as i64).unwrap(), Duration::zero())
	}

	fn txn_at(engine: &TestEngine, coordinate: u64) -> DeferredTransaction {
		let mut txn = engine.flow_txn().at(CommitVersion(coordinate)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_nanos(coordinate)),
			version: CommitVersion(coordinate),
		});
		txn
	}

	fn seal_timers(txn: &mut DeferredTransaction, operator: OperatorId) -> Vec<DateTime> {
		txn.state_range(
			operator,
			StateRange::forward(keyspace_inner_range(GroupId::ROOT, KeyspaceId::TIMER_WHEEL), "test"),
		)
		.unwrap()
		.items
		.iter()
		.filter_map(|item| {
			let decoded = OperatorStateKey::decode(&item.key).expect("a wheel row must decode");
			let suffix =
				TimerWheelKey::from_suffix_bytes(&decoded.suffix).expect("a wheel row must decode");
			(suffix.kind.0 == TimerKind::Seal).then_some(suffix.due.0)
		})
		.collect()
	}

	fn index_windows(operator: &mut WindowOperator, host: &mut dyn HostContext, starts: &[u64]) {
		let config = operator.engine_config();
		let mut engine =
			operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
				Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::new(config))
			});
		for (n, start_ms) in starts.iter().enumerate() {
			let start = at_millis(*start_ms);
			engine.reindex_window(
				host,
				&Hash128::from(n as u128),
				start,
				GroupId(n as u128 + 1),
				&EncodedKey::new(Vec::new()),
				None,
				Some(start.to_order()),
			)
			.unwrap();
		}
		*operator.core.tumbling_engine_slot() = Some(engine);
	}

	fn take_one_due(txn: &mut DeferredTransaction, operator: OperatorId, watermark: u64) -> Timer {
		let mut timers = TimerWheel::take_due(operator, txn, at_millis(watermark), 16, None).unwrap().timers;
		assert_eq!(timers.len(), 1, "exactly one timer may stand for the whole operator");
		timers.remove(0)
	}

	#[test]
	fn a_batch_of_many_windows_leaves_exactly_one_seal_timer() {
		// The sweep discards the timer key entirely, so any row past the earliest buys nothing.
		let engine = TestEngine::new();
		let mut operator = window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		let starts: Vec<u64> = (0..64).map(|n| n * SIZE_MS).collect();
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			index_windows(&mut operator, &mut host, &starts);
			rearm_engine_seal(&mut operator, &mut host, rule(), None).unwrap();
		}

		assert_eq!(
			seal_timers(&mut txn, id),
			vec![at_millis(SIZE_MS + 1)],
			"64 indexed windows must leave one seal timer, armed at the earliest window's seal instant"
		);
	}

	#[test]
	fn a_sweep_rearms_for_the_windows_it_left_behind() {
		// The wheel consumes the fired row, so without a re-arm every window left behind stays open.
		let engine = TestEngine::new();
		let mut operator = window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			index_windows(&mut operator, &mut host, &[0, SIZE_MS, 2 * SIZE_MS]);
			rearm_engine_seal(&mut operator, &mut host, rule(), None).unwrap();
		}
		let timer = take_one_due(&mut txn, id, SIZE_MS + 1);
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			seal_engine_windows(&mut operator, &mut host, FiredAt::of(&timer)).unwrap();
		}

		assert_eq!(
			seal_timers(&mut txn, id),
			vec![at_millis(2 * SIZE_MS + 1)],
			"the sweep took the window at 0 and must leave the window at one span armed, not nothing"
		);
	}

	#[test]
	fn an_index_drained_to_empty_leaves_no_seal_timer_behind() {
		// A timer on an empty index fires forever, advancing the seal ledger past windows nothing holds.
		let engine = TestEngine::new();
		let mut operator = window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			index_windows(&mut operator, &mut host, &[0]);
			rearm_engine_seal(&mut operator, &mut host, rule(), None).unwrap();
		}
		let timer = take_one_due(&mut txn, id, SIZE_MS + 1);
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			seal_engine_windows(&mut operator, &mut host, FiredAt::of(&timer)).unwrap();
		}

		assert!(seal_timers(&mut txn, id).is_empty(), "an empty expiry index must hold no seal timer");
	}
}
