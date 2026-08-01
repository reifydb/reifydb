// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use std::collections::{BTreeMap, HashMap, HashSet};

use reifydb_core::{
	interface::change::{Change, Diff},
	key::operator_group_state::{GroupId, IntoGroupStateKey},
	state::store::StateStore,
	value::column::columns::Columns,
};
use reifydb_flow::{
	transaction::FlowTransaction,
	window::{
		coord::EventCoord,
		driver::{gate::disarm_seal, sweep::SealSweep},
		engine::{AccumulatorEvent, ExpiryAnchor, WindowStateKey, tumbling::TumblingEngine},
		kind::{
			ordinal_window_span,
			session::{SessionKind, SessionTracker},
			tumbling::TumblingOverRows,
		},
		ledger::FiredAt,
		meta::EngineMetaKey,
		policy::SealPolicy,
		span::{WindowCoord, WindowSpan},
	},
};
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, duration::Duration},
};
use tracing::Span;

use super::operator::WindowOperator;
use crate::operator::{
	aggregation::{
		accumulator::{RowAccumulator, WindowSlotKey},
		engine::{
			EngineBuckets, WindowGroups, finish_tumbling_engine, intern_window_groups, route_into_buckets,
			slot_coord, window_group_key,
		},
	},
	stateful::utils,
	store::OperatorStateStore,
};

#[allow(clippy::too_many_arguments)]
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
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	change: &Change,
	buckets: &mut EngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	arrival: &mut Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: &mut HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
) -> Result<()> {
	let rows = TumblingOverRows::holding(operator.size_count().unwrap_or(1));
	let now = operator.core.current_timestamp();
	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => {
				let groups = operator.core.compute_groups(post)?;
				let slot_cols = operator.core.evaluate_slot_inputs(post)?;
				for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
					let ordinal = operator.get_and_increment_global_count(txn, *hash)?;
					let window_id = rows.window_id(ordinal);
					operator.store_row_index(txn, *hash, post.row_numbers()[row_idx], window_id)?;
					let contribution = operator.core.build_contribution(post, &slot_cols, row_idx);
					let coord = slot_coord(true, now, post.row_numbers()[row_idx].0);
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
						now,
					);
				}
			}
			Diff::Remove {
				pre,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let slot_cols = operator.core.evaluate_slot_inputs(pre)?;
				for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
					let contribution = operator.core.build_contribution(pre, &slot_cols, row_idx);
					let coord = slot_coord(true, now, pre.row_numbers()[row_idx].0);
					for window_id in
						operator.lookup_row_index(txn, *hash, pre.row_numbers()[row_idx])?
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
							now,
						);
					}
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
				for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
					let row_number = pre.row_numbers()[row_idx];
					let existing = operator.lookup_row_index(txn, *hash, row_number)?;
					if existing.is_empty() {
						let ordinal = operator.get_and_increment_global_count(txn, *hash)?;
						let window_id = rows.window_id(ordinal);
						operator.store_row_index(
							txn,
							*hash,
							post.row_numbers()[row_idx],
							window_id,
						)?;
						let contribution =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord = slot_coord(true, now, post.row_numbers()[row_idx].0);
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
							now,
						);
					} else {
						let pre_contrib =
							operator.core.build_contribution(pre, &pre_cols, row_idx);
						let post_contrib =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord = slot_coord(true, now, pre.row_numbers()[row_idx].0);
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
								now,
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
								now,
							);
						}
					}
				}
			}
		}
	}
	Ok(())
}

pub fn apply_tumbling_engine(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let window_size = operator.size_duration().unwrap_or_default();
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<DateTime>), DateTime> = HashMap::new();

	if operator.is_count_based() {
		route_count_tumbling(
			operator,
			txn,
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
		txn,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		SealPolicy::tumbling(window_size, operator.grace()),
		ExpiryAnchor::WindowStart,
	)?;

	let groups = intern_batch(operator, txn, &arrival)?;

	let diffs = finish_tumbling_engine(
		&operator.core,
		txn,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&groups,
		&kinds,
		operator.engine_config(),
		operator.grace(),
		if operator.is_count_based() {
			ExpiryAnchor::Unindexed
		} else {
			ExpiryAnchor::WindowStart
		},
	)?;
	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

fn intern_batch(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	arrival: &[(Hash128, WindowSpan<DateTime>)],
) -> Result<WindowGroups> {
	let windows: Vec<(Hash128, u64)> = arrival.iter().map(|(hash, span)| (*hash, span.start.to_order())).collect();
	intern_window_groups(operator.core.operator, txn, &windows)
}

fn sliding_insert_anchors(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	hash: Hash128,
	event_ts: DateTime,
	is_count: bool,
) -> Result<Vec<u64>> {
	let coord = if is_count {
		operator.get_and_increment_global_count(txn, hash)?.value()
	} else {
		event_ts.to_order()
	};
	Ok(operator.sliding_window_anchors(coord))
}

pub fn apply_sliding_engine(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
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
						sliding_insert_anchors(operator, txn, *hash, event_ts, is_count)?;
					let contribution = operator.core.build_contribution(post, &slot_cols, row_idx);
					let coord = slot_coord(is_count, event_ts, post.row_numbers()[row_idx].0);
					for wid in &window_ids {
						operator.store_row_index(
							txn,
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
					for wid in operator.lookup_row_index(txn, *hash, pre.row_numbers()[row_idx])? {
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
					let existing = operator.lookup_row_index(txn, *hash, row_number)?;
					if existing.is_empty() {
						let window_ids = sliding_insert_anchors(
							operator, txn, *hash, event_ts, is_count,
						)?;
						let contribution =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord = slot_coord(is_count, event_ts, row_number.0);
						for wid in &window_ids {
							operator.store_row_index(
								txn,
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
		txn,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		SealPolicy::tumbling(window_size, operator.grace()),
		ExpiryAnchor::WindowStart,
	)?;

	let groups = intern_batch(operator, txn, &arrival)?;

	let diffs = finish_tumbling_engine(
		&operator.core,
		txn,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&groups,
		&kinds,
		operator.engine_config(),
		operator.grace(),
		if operator.is_count_based() {
			ExpiryAnchor::Unindexed
		} else {
			ExpiryAnchor::WindowStart
		},
	)?;
	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

fn session_assign(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	hash: Hash128,
	event_ts: DateTime,
	kind: &SessionKind,
	trackers: &mut HashMap<Hash128, SessionTracker>,
	closes: &mut Vec<(Hash128, u64)>,
) -> Result<Option<u64>> {
	let mut tracker = match trackers.get(&hash) {
		Some(&tracker) => tracker,
		None => operator.load_session_tracker(txn, hash)?,
	};
	let assignment = kind.assign(&mut tracker, EventCoord::of(&event_ts));
	if let Some(closed) = assignment.closed() {
		closes.push((hash, closed));
	}
	if assignment.session_id().is_some() {
		trackers.insert(hash, tracker);
	}
	Ok(assignment.session_id())
}

pub fn apply_session_engine(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let kind = operator.session_kind();

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<DateTime>), DateTime> = HashMap::new();
	let mut closes: Vec<(Hash128, u64)> = Vec::new();
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
					if let Some(session_id) = session_assign(
						operator,
						txn,
						*hash,
						event_ts,
						&kind,
						&mut trackers,
						&mut closes,
					)? {
						operator.store_row_index(
							txn,
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
						operator.lookup_row_index(txn, *hash, pre.row_numbers()[row_idx])?
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
						operator.lookup_row_index(txn, *hash, pre.row_numbers()[row_idx])?;
					if existing.is_empty() {
						if let Some(session_id) = session_assign(
							operator,
							txn,
							*hash,
							event_ts,
							&kind,
							&mut trackers,
							&mut closes,
						)? {
							operator.store_row_index(
								txn,
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
		operator.save_session_tracker(txn, *hash, tracker)?;
	}

	gate_and_arm_seals(
		operator,
		txn,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		operator.session_policy(),
		ExpiryAnchor::LastEvent,
	)?;

	let groups = intern_batch(operator, txn, &arrival)?;

	let diffs = finish_tumbling_engine(
		&operator.core,
		txn,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&groups,
		&kinds,
		operator.engine_config(),
		operator.grace(),
		ExpiryAnchor::LastEvent,
	)?;

	let mut disarm: Vec<(Hash128, u64, u64)> = Vec::new();
	{
		let operator_id = operator.core.operator;
		let mut closing: Vec<(Hash128, u64, GroupId)> = Vec::with_capacity(closes.len());
		for (hash, session_id) in &closes {
			if let Some(group) = txn.lookup_group(operator_id, &window_group_key(*hash, *session_id))? {
				closing.push((*hash, *session_id, group));
			}
		}
		let mut engine = operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
			Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::group_scoped(
				operator.engine_config(),
			))
		});
		let mut store = OperatorStateStore::new(txn, operator_id);
		for (hash, session_id, group) in &closing {
			let accumulator_key = WindowStateKey::new(*group, utils::empty_key()).into_group_state_key();
			let meta = operator.core.engine_meta().get(&mut store, &EngineMetaKey(*group))?;
			let prior_last = meta.as_ref().map(|m| m.last_event_time).unwrap_or(0);
			if prior_last > 0 {
				disarm.push((*hash, *session_id, prior_last));
			}
			engine.reindex_window(
				&mut store,
				hash,
				<DateTime as WindowCoord>::from_order(*session_id),
				*group,
				&utils::empty_key(),
				(prior_last > 0).then_some(prior_last),
				None,
			)?;
			store.state_remove(&accumulator_key)?;
			operator.core.engine_meta().remove(&mut store, &EngineMetaKey(*group))?;
		}
		*operator.core.tumbling_engine_slot() = Some(engine);
	}

	if !operator.is_count_based() {
		let operator_id = operator.core.operator;
		let policy = operator.session_policy();
		let mut store = OperatorStateStore::new(txn, operator_id);
		for (hash, session_id, prior_last) in disarm {
			disarm_seal(&mut store, policy, &window_group_key(hash, session_id), prior_last)?;
		}
	}

	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

fn gate_and_arm_seals(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	buckets: &mut EngineBuckets,
	arrival: &mut Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: &HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
	policy: SealPolicy,
	anchor: ExpiryAnchor,
) -> Result<()> {
	if policy.is_inert() || operator.is_count_based() {
		return Ok(());
	}
	let gate = operator.seal_gate(txn, policy)?;
	let operator_id = operator.core.operator;
	let mut known: Vec<Option<GroupId>> = Vec::with_capacity(buckets.len());
	for (hash, span) in buckets.keys() {
		known.push(txn.lookup_group(operator_id, &window_group_key(*hash, span.start.to_order()))?);
	}
	let mut sealed: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut rearm: Vec<(Hash128, u64, Option<u64>, u64)> = Vec::new();
	let mut dropped = 0u64;
	{
		let mut store = OperatorStateStore::new(txn, operator_id);
		for ((key, events), group) in buckets.iter().zip(known) {
			let prior_last = match group {
				Some(group) => operator
					.core
					.engine_meta()
					.get(&mut store, &EngineMetaKey(group))?
					.map(|m| m.last_event_time),
				None => None,
			};
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

	{
		let mut store = OperatorStateStore::new(txn, operator_id);
		for (hash, window_start, prior_horizon, horizon) in rearm {
			gate.arm(&mut store, &window_group_key(hash, window_start), prior_horizon, horizon)?;
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

#[tracing::instrument(name = "flow::window::seal", level = "debug", skip_all, fields(operator = operator.core.operator.0, expired = tracing::field::Empty))]
fn seal_due_windows(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	fired: FiredAt,
	policy: SealPolicy,
) -> Result<Vec<Diff>> {
	if policy.is_inert() {
		return Ok(Vec::new());
	}
	operator.advance_seal_ledger(txn, fired)?;
	let Some(threshold) = SealSweep::new(policy).horizon(fired) else {
		return Ok(Vec::new());
	};
	let expired = {
		let mut store = OperatorStateStore::new(txn, operator.core.operator);
		let mut engine = operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
			Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::group_scoped(
				operator.engine_config(),
			))
		});
		let res = engine.expire(&mut store, threshold.to_order())?;
		engine.flush(&mut store)?;
		*operator.core.tumbling_engine_slot() = Some(engine);
		res
	};
	Span::current().record("expired", expired.len());
	Ok(Vec::new())
}

pub fn seal_session_engine(operator: &WindowOperator, txn: &mut FlowTransaction, fired: FiredAt) -> Result<Vec<Diff>> {
	seal_due_windows(operator, txn, fired, operator.session_policy())
}

pub fn seal_engine_windows(operator: &WindowOperator, txn: &mut FlowTransaction, fired: FiredAt) -> Result<Vec<Diff>> {
	let Some(window_size) = operator.size_duration() else {
		return Ok(Vec::new());
	};
	seal_due_windows(operator, txn, fired, SealPolicy::tumbling(window_size, operator.grace()))
}

#[cfg(test)]
mod tests {
	use reifydb_flow::window::{
		engine::{is_sealed, seal_horizon},
		span::WindowCoord,
	};
	use reifydb_value::value::duration::Duration;

	use super::SealPolicy;

	#[test]
	fn the_armed_seal_instant_reproduces_the_pre_timer_boundary() {
		// The armed instant and the sealing gate must be the same expression, or a bucket is
		// dropped before its timer fires or rebuilt from a late row after it did. The wheel fires
		// at watermark >= at, so reproducing the strict gate needs the +1 seal_instant carries.
		let cutoff_ms = 19u64;
		let cutoff = Duration::from_milliseconds(cutoff_ms as i64).expect("representable span");
		let policy = SealPolicy::tumbling(cutoff, Duration::from_milliseconds_const(0));
		let last = 10u64;
		let sealed = |wm: u64| policy.seal_instant_from_order(last).at().to_order() <= wm;
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
		// A watermark smaller than seal_after must not wrap; nothing is sealed yet.
		assert_eq!(seal_horizon(3, 10), 0, "young watermark saturates to zero horizon");
		assert!(!is_sealed(0, seal_horizon(3, 10)), "anchor zero is not below a zero horizon");
		assert!(is_sealed(4, seal_horizon(20, 10)), "anchor below watermark - seal_after is sealed");
	}
}
