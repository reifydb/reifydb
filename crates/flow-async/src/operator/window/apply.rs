// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use std::{
	collections::{HashMap, HashSet, hash_map::Entry},
	slice::from_ref,
};

use arrow_array::RecordBatch;
use arrow_ord::cmp::eq;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::WindowKind,
	interface::change::{Change, Diff},
	internal,
	key::operator::state::{GroupId, GroupStateKey, IntoGroupStateKey},
	state::timer::{StateStore, TimerKind},
	value::batch::filter,
};
use reifydb_value::{
	Result,
	error::Error,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, duration::Duration, row_number::RowNumber},
};
use tracing::{Span, instrument};

use super::{
	bind::SlidingBound,
	operator::{WindowOperator, required_row_numbers},
};
use crate::{
	operator::{
		aggregation::{
			accumulator::{RowAccumulator, WindowSlotKey},
			core::{BatchGroups, SlotRow},
			engine::{
				EngineBuckets, GatedMeta, Routed, Routing, SessionBounds, Stamp, WindowGroups,
				distinct_row_runs, finish_tumbling_engine, group_of, intern_window_groups,
				route_into_buckets, slot_coord,
			},
		},
		host::HostContext,
		state::{
			reaper::{drain, drain_groups, enqueue, enqueue_retired},
			seal::{
				coord::Coord,
				gate::{SealGate, rearm_seal, seal_move},
				ledger::FiredAt,
				rule::{SealRule, SealedThrough},
				sweep::SealSweep,
			},
		},
		state_access::get_classified,
		time_at_or_after,
	},
	timer::Timer,
	window::{
		engine::{AccumulatorEvent, ExpiryAnchor, tumbling::TumblingEngine},
		kind::{
			ordinal_window_span,
			session::open::{Insertion, Member, OpenSessions, Removal},
		},
		meta::{EngineMeta, EngineMetaKey, RowIndexKey, SessionKey, SessionMemberKey},
		prefetch::Prefetched,
		span::WindowSpan,
	},
};

#[instrument(name = "flow::operator::window::route", level = "trace", skip_all, fields(rows = columns.num_rows()))]
fn route_engine_columns(
	operator: &WindowOperator,
	columns: &RecordBatch,
	is_add: bool,
	window_size: Duration,
	routing: &mut Routing,
) -> Result<()> {
	let timestamps = operator.row_times(columns, columns.num_rows())?;
	route_into_buckets(
		&operator.core,
		columns,
		is_add,
		|row_idx| {
			let ts = timestamps[row_idx];
			(WindowSpan::for_coord(ts, window_size), ts)
		},
		routing,
	)
}

fn first_open_window(gate: &SealGate, window_size: Duration) -> DateTime {
	let floor = WindowSpan::for_coord(gate.horizon(), window_size).start;
	match gate.admits(floor.to_order()) {
		true => floor,
		false => floor.add_span(window_size),
	}
}

fn drop_straddling(
	pre: &RecordBatch,
	post: &RecordBatch,
	open_from: DateTime,
) -> Result<(RecordBatch, RecordBatch, usize)> {
	let kept = eq(&time_at_or_after(pre, open_from)?, &time_at_or_after(post, open_from)?)
		.map_err(|err| Error(Box::new(internal!(err.to_string()))))?;
	let dropped = kept.false_count();
	if dropped == 0 {
		return Ok((pre.clone(), post.clone(), 0));
	}
	Ok((filter(pre, kept.values())?, filter(post, kept.values())?, dropped))
}

fn push_count_event(
	routing: &mut Routing,
	hash: Hash128,
	gvals: &[Value],
	span: WindowSpan<DateTime>,
	coord: WindowSlotKey,
	event: AccumulatorEvent<SlotRow>,
	event_ts: DateTime,
) {
	let event = match event {
		AccumulatorEvent::Add(c) => AccumulatorEvent::Add((coord, c)),
		AccumulatorEvent::Remove(c) => AccumulatorEvent::Remove((coord, c)),
	};
	routing.push(hash, gvals, span, event, event_ts);
}

struct DiffGroups {
	pre: BatchGroups,
	post: BatchGroups,
}

fn count_routing_groups(operator: &WindowOperator, change: &Change) -> Result<Vec<DiffGroups>> {
	let mut grouped = Vec::with_capacity(change.diffs.len());
	for diff in change.diffs.iter() {
		grouped.push(DiffGroups {
			pre: diff.pre().map(|pre| operator.core.compute_groups(pre)).transpose()?.unwrap_or_default(),
			post: diff
				.post()
				.map(|post| operator.core.compute_groups(post))
				.transpose()?
				.unwrap_or_default(),
		});
	}
	Ok(grouped)
}

fn row_index_keys(operator: &WindowOperator, change: &Change, grouped: &[DiffGroups]) -> Result<Vec<GroupStateKey>> {
	let mut keys = Vec::new();
	for (diff, groups) in change.diffs.iter().zip(grouped) {
		let pre_rows = match diff.pre() {
			Some(pre) => required_row_numbers(pre)?,
			None => &[],
		};
		for (hash, row_number) in groups.pre.hashes.iter().zip(pre_rows) {
			keys.push((&RowIndexKey(operator.partition_group(*hash), *row_number)).into_group_state_key());
		}
		let post_rows = match diff.post() {
			Some(post) => required_row_numbers(post)?,
			None => &[],
		};
		for (hash, row_number) in groups.post.hashes.iter().zip(post_rows) {
			keys.push((&RowIndexKey(operator.partition_group(*hash), *row_number)).into_group_state_key());
		}
	}
	Ok(keys)
}

#[instrument(name = "flow::operator::window::tumbling", level = "trace", skip_all)]
pub fn apply_tumbling_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	let window_size = operator.size_duration().unwrap_or_default();
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");

	let mut routing = Routing::default();
	let rule = SealRule::tumbling(window_size, operator.lateness().unwrap_or_else(Duration::zero));
	let open_from = match change.diffs.iter().any(|diff| matches!(diff, Diff::Update { .. })) {
		true => Some(first_open_window(&operator.seal_gate(host, rule)?, window_size)),
		false => None,
	};
	let mut straddled = 0u64;

	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => route_engine_columns(operator, post, true, window_size, &mut routing)?,
			Diff::Remove {
				pre,
				..
			} => route_engine_columns(operator, pre, false, window_size, &mut routing)?,
			Diff::Update {
				pre,
				post,
				..
			} => {
				let open_from =
					open_from.expect("the seal gate is read for every change that holds an update");
				for (pre, post) in distinct_row_runs(pre, post)? {
					let (pre, post, dropped) = drop_straddling(&pre, &post, open_from)?;
					straddled += dropped as u64;
					route_engine_columns(operator, &pre, false, window_size, &mut routing)?;
					route_engine_columns(operator, &post, true, window_size, &mut routing)?;
				}
			}
		}
	}
	if straddled > 0 {
		operator.note_sealed_drops(straddled * 2);
	}

	let Routed {
		mut buckets,
		group_values,
		mut arrival,
		window_max_ts,
	} = routing.finish();
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
	let expiry_anchor = ExpiryAnchor::WindowStart;
	let armed_before = engine_earliest_expiry(operator, host)?;
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
		false,
		Stamp::SpanStart,
	)?;
	rearm_engine_seal(operator, host, rule, armed_before)?;
	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

#[instrument(name = "flow::operator::window::intern", level = "trace", skip_all, fields(windows = arrival.len()))]
fn intern_batch(arrival: &[(Hash128, WindowSpan<DateTime>)]) -> WindowGroups {
	let windows: Vec<(Hash128, u64)> = arrival.iter().map(|(hash, span)| (*hash, span.start.to_order())).collect();
	intern_window_groups(&windows)
}

fn route_time_sliding(
	operator: &WindowOperator,
	bound: &SlidingBound,
	columns: &RecordBatch,
	is_add: bool,
	routing: &mut Routing,
) -> Result<()> {
	let groups = operator.core.compute_groups(columns)?;
	let row_numbers = required_row_numbers(columns)?;
	let timestamps = operator.row_times(columns, columns.num_rows())?;
	let slot_cols = operator.core.evaluate_slot_inputs(columns)?;
	let views = operator.core.slot_views(columns, &slot_cols)?;
	for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
		let event_ts = timestamps[row_idx];
		let contribution = views.contribution(row_idx, event_ts);
		let coord = slot_coord(false, event_ts, row_numbers[row_idx].0);
		for wid in bound.anchors(event_ts.to_order()) {
			let event = if is_add {
				AccumulatorEvent::Add(contribution.clone())
			} else {
				AccumulatorEvent::Remove(contribution.clone())
			};
			push_count_event(routing, *hash, gvals, bound.span(wid), coord, event, event_ts);
		}
	}
	Ok(())
}

#[instrument(name = "flow::operator::window::sliding", level = "trace", skip_all)]
pub fn apply_sliding_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let window_size = operator.size_duration().unwrap_or_default();
	let bound = operator.sliding_bound();

	let mut routing = Routing::default();

	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => route_time_sliding(operator, &bound, post, true, &mut routing)?,
			Diff::Remove {
				pre,
				..
			} => route_time_sliding(operator, &bound, pre, false, &mut routing)?,
			Diff::Update {
				pre,
				post,
				..
			} => {
				for (pre, post) in distinct_row_runs(pre, post)? {
					route_time_sliding(operator, &bound, &pre, false, &mut routing)?;
					route_time_sliding(operator, &bound, &post, true, &mut routing)?;
				}
			}
		}
	}

	let Routed {
		mut buckets,
		group_values,
		mut arrival,
		window_max_ts,
	} = routing.finish();
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
	let expiry_anchor = ExpiryAnchor::WindowStart;
	let armed_before = engine_earliest_expiry(operator, host)?;
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
		Stamp::SpanStart,
	)?;
	rearm_engine_seal(operator, host, rule, armed_before)?;
	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

type SealedWindows = HashSet<(Hash128, WindowSpan<DateTime>)>;

struct GroupSessions {
	loaded_due: Option<u64>,
	open: OpenSessions<DateTime>,
}

struct IndexEvent {
	hash: Hash128,
	row_number: RowNumber,
	session_id: u64,
	added: bool,
}

fn withdraw_sealed_admissions(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	events: &[IndexEvent],
	sealed: &SealedWindows,
) -> Result<()> {
	let mut first_touch: HashMap<(Hash128, RowNumber, u64), bool> = HashMap::new();
	for event in events {
		if sealed.contains(&(event.hash, ordinal_window_span(event.session_id))) {
			first_touch.entry((event.hash, event.row_number, event.session_id)).or_insert(event.added);
		}
	}
	for ((hash, row_number, session_id), added) in first_touch {
		if added {
			operator.withdraw_row_index(host, hash, row_number, session_id)?;
			operator.drop_session_member(host, hash, session_id, row_number)?;
		}
	}
	Ok(())
}

fn earliest_due(open: &OpenSessions<DateTime>, gap: Duration) -> Option<u64> {
	open.sessions()
		.iter()
		.map(|session| session.last)
		.min()
		.or(open.sealed_last())
		.map(|last| last.saturating_add(gap).to_order())
}

fn group_sessions<'a>(
	operator: &mut WindowOperator,
	store: &mut dyn StateStore,
	groups: &'a mut HashMap<Hash128, GroupSessions>,
	hash: Hash128,
	horizon: DateTime,
	gap: Duration,
) -> Result<&'a mut OpenSessions<DateTime>> {
	if let Entry::Vacant(entry) = groups.entry(hash) {
		let mut open = operator.load_open_sessions(store, hash)?;
		let loaded_due = earliest_due(&open, gap);
		for closed in open.close(horizon) {
			for member in &closed.members {
				operator.drop_session_member(store, hash, closed.id, member.row)?;
				operator.withdraw_row_index(store, hash, member.row, closed.id)?;
			}
		}
		entry.insert(GroupSessions {
			loaded_due,
			open,
		});
	}
	Ok(&mut groups.get_mut(&hash).expect("the group was loaded above").open)
}

struct SessionPass<'a> {
	routing: &'a mut Routing,
	groups: &'a mut HashMap<Hash128, GroupSessions>,
	index_events: &'a mut Vec<IndexEvent>,
	horizon: DateTime,
	gap: Duration,
}

impl SessionPass<'_> {
	fn move_members(
		&mut self,
		operator: &mut WindowOperator,
		store: &mut dyn StateStore,
		hash: Hash128,
		gvals: &[Value],
		from: u64,
		to: u64,
		members: &[Member<DateTime>],
	) -> Result<()> {
		for member in members {
			let slot = operator.load_session_member(store, hash, from, member.row)?;
			let coord = slot_coord(false, member.at, member.row.0);
			push_count_event(
				self.routing,
				hash,
				gvals,
				ordinal_window_span(from),
				coord,
				AccumulatorEvent::Remove(slot.clone()),
				member.at,
			);
			push_count_event(
				self.routing,
				hash,
				gvals,
				ordinal_window_span(to),
				coord,
				AccumulatorEvent::Add(slot.clone()),
				member.at,
			);
			operator.withdraw_row_index(store, hash, member.row, from)?;
			operator.store_row_index(store, hash, member.row, to)?;
			operator.drop_session_member(store, hash, from, member.row)?;
			operator.store_session_member(store, hash, to, member.row, &slot)?;
		}
		Ok(())
	}

	fn admit(
		&mut self,
		operator: &mut WindowOperator,
		store: &mut dyn StateStore,
		hash: Hash128,
		gvals: &[Value],
		at: DateTime,
		row: RowNumber,
		contribution: SlotRow,
	) -> Result<bool> {
		let open = group_sessions(operator, store, self.groups, hash, self.horizon, self.gap)?;
		let target = match open.insert(at, row, self.horizon) {
			Insertion::Refused => return Ok(false),
			Insertion::Opened(id) | Insertion::Extended(id) => id,
			Insertion::Merged {
				survivor,
				absorbed,
			} => {
				for session in &absorbed {
					self.move_members(
						operator,
						store,
						hash,
						gvals,
						session.id,
						survivor,
						&session.members,
					)?;
				}
				survivor
			}
		};
		operator.store_row_index(store, hash, row, target)?;
		operator.store_session_member(store, hash, target, row, &contribution)?;
		self.index_events.push(IndexEvent {
			hash,
			row_number: row,
			session_id: target,
			added: true,
		});
		push_count_event(
			self.routing,
			hash,
			gvals,
			ordinal_window_span(target),
			slot_coord(false, at, row.0),
			AccumulatorEvent::Add(contribution),
			at,
		);
		Ok(true)
	}

	fn retract(
		&mut self,
		operator: &mut WindowOperator,
		store: &mut dyn StateStore,
		hash: Hash128,
		gvals: &[Value],
		at: DateTime,
		row: RowNumber,
		contribution: SlotRow,
	) -> Result<()> {
		for session_id in operator.lookup_row_index(store, hash, row)? {
			let open = group_sessions(operator, store, self.groups, hash, self.horizon, self.gap)?;
			let Some(removal) = open.remove(session_id, row)? else {
				continue;
			};
			push_count_event(
				self.routing,
				hash,
				gvals,
				ordinal_window_span(session_id),
				slot_coord(false, at, row.0),
				AccumulatorEvent::Remove(contribution.clone()),
				at,
			);
			self.index_events.push(IndexEvent {
				hash,
				row_number: row,
				session_id,
				added: false,
			});
			operator.drop_session_member(store, hash, session_id, row)?;
			if let Removal::Split {
				kept,
				moved,
			} = removal
			{
				self.move_members(operator, store, hash, gvals, kept, moved.id, &moved.members)?;
			}
		}
		operator.drop_row_index(store, hash, row)
	}

	fn revalue(
		&mut self,
		operator: &mut WindowOperator,
		store: &mut dyn StateStore,
		hash: Hash128,
		gvals: &[Value],
		at: DateTime,
		row: RowNumber,
		pre: SlotRow,
		post: SlotRow,
	) -> Result<()> {
		for session_id in operator.lookup_row_index(store, hash, row)? {
			let open = group_sessions(operator, store, self.groups, hash, self.horizon, self.gap)?;
			if !open.sessions().iter().any(|session| session.id == session_id) {
				continue;
			}
			let coord = slot_coord(false, at, row.0);
			let span = ordinal_window_span(session_id);
			push_count_event(
				self.routing,
				hash,
				gvals,
				span,
				coord,
				AccumulatorEvent::Remove(pre.clone()),
				at,
			);
			push_count_event(
				self.routing,
				hash,
				gvals,
				span,
				coord,
				AccumulatorEvent::Add(post.clone()),
				at,
			);
			operator.store_session_member(store, hash, session_id, row, &post)?;
		}
		Ok(())
	}
}

#[instrument(name = "flow::operator::window::session", level = "trace", skip_all)]
pub fn apply_session_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let rule = operator.session_rule();
	let gap = operator.session_gap();
	let horizon = operator.seal_gate(host, rule)?.horizon();

	let mut routing = Routing::default();
	let mut groups: HashMap<Hash128, GroupSessions> = HashMap::new();
	let mut index_events: Vec<IndexEvent> = Vec::new();
	let mut refused = 0u64;

	let grouped = count_routing_groups(operator, &change)?;
	let keys = row_index_keys(operator, &change, &grouped)?;
	let mut store = Prefetched::read(host, keys)?;
	{
		let mut pass = SessionPass {
			routing: &mut routing,
			groups: &mut groups,
			index_events: &mut index_events,
			horizon,
			gap,
		};
		for (diff, diff_groups) in change.diffs.iter().zip(&grouped) {
			match diff {
				Diff::Insert {
					post,
					..
				} => {
					let post_rows = required_row_numbers(post)?;
					let timestamps = operator.row_times(post, post.num_rows())?;
					let slot_cols = operator.core.evaluate_slot_inputs(post)?;
					let views = operator.core.slot_views(post, &slot_cols)?;
					for row_idx in 0..post.num_rows() {
						let (hash, gvals) = diff_groups.post.row(row_idx);
						let at = timestamps[row_idx];
						let contribution = views.contribution(row_idx, at);
						if !pass.admit(
							operator,
							&mut store,
							*hash,
							gvals,
							at,
							post_rows[row_idx],
							contribution,
						)? {
							refused += 1;
						}
					}
				}
				Diff::Remove {
					pre,
					..
				} => {
					let pre_rows = required_row_numbers(pre)?;
					let timestamps = operator.row_times(pre, pre.num_rows())?;
					let slot_cols = operator.core.evaluate_slot_inputs(pre)?;
					let views = operator.core.slot_views(pre, &slot_cols)?;
					for row_idx in 0..pre.num_rows() {
						let (hash, gvals) = diff_groups.pre.row(row_idx);
						let at = timestamps[row_idx];
						let contribution = views.contribution(row_idx, at);
						pass.retract(
							operator,
							&mut store,
							*hash,
							gvals,
							at,
							pre_rows[row_idx],
							contribution,
						)?;
					}
				}
				Diff::Update {
					pre,
					post,
					..
				} => {
					let pre_rows = required_row_numbers(pre)?;
					let pre_timestamps = operator.row_times(pre, pre.num_rows())?;
					let timestamps = operator.row_times(post, post.num_rows())?;
					let pre_cols = operator.core.evaluate_slot_inputs(pre)?;
					let post_cols = operator.core.evaluate_slot_inputs(post)?;
					let pre_views = operator.core.slot_views(pre, &pre_cols)?;
					let post_views = operator.core.slot_views(post, &post_cols)?;
					for row_idx in 0..pre.num_rows() {
						let (hash, gvals) = diff_groups.pre.row(row_idx);
						let (post_hash, post_gvals) = diff_groups.post.row(row_idx);
						let row = pre_rows[row_idx];
						let (pre_at, at) = (pre_timestamps[row_idx], timestamps[row_idx]);
						let pre_contrib = pre_views.contribution(row_idx, pre_at);
						let post_contrib = post_views.contribution(row_idx, at);
						let indexed =
							!operator.lookup_row_index(&mut store, *hash, row)?.is_empty();
						if indexed && post_hash == hash && pre_at == at {
							pass.revalue(
								operator,
								&mut store,
								*hash,
								gvals,
								at,
								row,
								pre_contrib,
								post_contrib,
							)?;
							continue;
						}
						if indexed {
							pass.retract(
								operator,
								&mut store,
								*hash,
								gvals,
								pre_at,
								row,
								pre_contrib,
							)?;
						}
						if !pass.admit(
							operator,
							&mut store,
							*post_hash,
							post_gvals,
							at,
							row,
							post_contrib,
						)? {
							refused += 1;
						}
					}
				}
			}
		}
	}

	store.write()?;

	operator.note_refused_rows(refused);

	let Routed {
		mut buckets,
		group_values,
		mut arrival,
		window_max_ts,
		..
	} = routing.finish();
	let (gated, sealed) = drop_sealed_events(
		operator,
		host,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		rule,
		ExpiryAnchor::LastEvent,
	)?;
	if !sealed.is_empty() {
		withdraw_sealed_admissions(operator, host, &index_events, &sealed)?;
	}
	let mut bounds: SessionBounds = HashMap::new();
	let mut moves = Vec::with_capacity(groups.len());
	let mut emptied = Vec::new();
	let mut saving = Vec::with_capacity(groups.len());
	for (hash, group) in &groups {
		for session in group.open.sessions() {
			bounds.insert((*hash, session.id), (session.start, session.last));
		}
		if group.open.sessions().is_empty() && group.open.sealed_last().is_none() {
			emptied.push(*hash);
		} else {
			saving.push((*hash, &group.open));
		}
		moves.push(seal_move(rule, tracker_key(*hash), group.loaded_due, earliest_due(&group.open, gap)));
	}
	operator.save_open_sessions(host, &saving)?;
	operator.drop_session_trackers(host, &emptied)?;
	host.rearm_timers(TimerKind::Seal, &moves)?;

	let interned = intern_batch(&arrival);
	let gone: Vec<GroupId> = arrival
		.iter()
		.filter(|(hash, span)| !bounds.contains_key(&(*hash, span.start.to_order())))
		.map(|(hash, span)| group_of(&interned, *hash, span.start.to_order()))
		.collect();

	let engine_config = operator.engine_config();
	let engine_immutable = operator.immutable();
	let armed_before = engine_earliest_expiry(operator, host)?;
	let diffs = finish_tumbling_engine(
		&mut operator.core,
		host,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&interned,
		&kinds,
		engine_config,
		engine_immutable,
		ExpiryAnchor::LastEvent,
		true,
		Stamp::Session(gap, &bounds, &gated),
	)?;
	if !gone.is_empty() {
		let engine_config = operator.engine_config();
		let mut engine = operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
			Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::new(engine_config))
		});
		drain_groups(host, &gone, &mut *engine, usize::MAX)?;
		*operator.core.tumbling_engine_slot() = Some(engine);
	}
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
) -> Result<(GatedMeta, SealedWindows)> {
	let mut gated = GatedMeta::new();
	let gate = operator.seal_gate(host, rule)?;
	if matches!(anchor, ExpiryAnchor::LastEvent) && !buckets.is_empty() {
		let keys = buckets
			.keys()
			.map(|key| {
				(&EngineMetaKey(GroupId::window(key.0, key.1.start.to_order()))).into_group_state_key()
			})
			.collect();
		let mut store = Prefetched::read(host, keys)?;
		for key in buckets.keys() {
			let group = GroupId::window(key.0, key.1.start.to_order());
			gated.insert(
				*key,
				get_classified::<_, EngineMeta>(&mut store, from_ref(&EngineMetaKey(group)))?.remove(0),
			);
		}
	}
	let mut sealed: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut dropped = 0u64;
	{
		for (key, events) in buckets.iter() {
			let window_start = key.1.start.to_order();
			let horizon = match anchor {
				ExpiryAnchor::LastEvent => {
					let prior_last = gated
						.get(key)
						.expect("the gate reads the engine meta of every bucket")
						.as_ref()
						.map(|meta| meta.last_event_time);
					let batch_last = window_max_ts.get(key).map(|ts| ts.to_order());
					anchor.of(window_start, prior_last.max(batch_last))
				}
				ExpiryAnchor::WindowStart | ExpiryAnchor::Unindexed => anchor.of(window_start, None),
			};
			let Some(horizon) = horizon else {
				continue;
			};
			if !gate.admits(horizon) {
				dropped += events.len() as u64;
				sealed.push(*key);
			}
		}
	}

	if sealed.is_empty() {
		return Ok((gated, SealedWindows::new()));
	}
	for key in &sealed {
		buckets.remove(key);
	}
	let sealed: SealedWindows = sealed.into_iter().collect();
	arrival.retain(|key| !sealed.contains(key));
	operator.note_sealed_drops(dropped);
	Ok((gated, sealed))
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

fn rearm_engine_seal(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	rule: SealRule,
	before: Option<u64>,
) -> Result<()> {
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
	runs: usize,
) -> Result<Vec<Diff>> {
	operator.advance_seal_ledger(host, fired)?;
	let Some(threshold) = SealSweep::new(rule).horizon(fired) else {
		return Ok(Vec::new());
	};
	let config = operator.engine_config();
	let budget = config.expire_batch().saturating_mul(runs);
	let expired = {
		let mut engine =
			operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
				Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::new(config))
			});
		let res = engine.expire_within(host, threshold.to_order(), budget)?;
		let retires = host.state_retires_groups() && !matches!(operator.kind, WindowKind::Session { .. });
		if retires {
			let groups: Vec<GroupId> = res.iter().map(|window| window.group_id).collect();
			enqueue_retired(host, &groups)?;
		} else {
			for window in &res {
				enqueue(host, &[window.group_id])?;
			}
		}
		if !res.is_empty() && !retires {
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
	let drained = drain(host, &mut *engine, budget)?;
	*operator.core.tumbling_engine_slot() = Some(engine);
	if !drained.queue_is_empty()
		&& let Some(rule) = maintenance_rule(operator)
	{
		let due = fired.at().saturating_add(rule.admissible().duration());
		host.arm_timer(due, TimerKind::Maintenance, &EncodedKey::new(Vec::new()))?;
	}
	Ok(drained.freed)
}

pub fn seal_session_engines(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	timers: &[Timer],
) -> Result<Vec<Diff>> {
	let Some(latest) = timers.iter().max_by_key(|timer| timer.due) else {
		return Ok(Vec::new());
	};
	let rule = operator.session_rule();
	let diffs = seal_due_windows(operator, host, FiredAt::of(latest), rule, timers.len())?;
	retire_session_trackers(operator, host, timers, FiredAt::of(latest))?;
	Ok(diffs)
}

fn tracker_key(hash: Hash128) -> EncodedKey {
	EncodedKey::new(hash.0.to_be_bytes())
}

fn retire_session_trackers(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	timers: &[Timer],
	fired: FiredAt,
) -> Result<()> {
	let mut hashes: Vec<Hash128> = timers
		.iter()
		.filter(|timer| !timer.key.as_slice().is_empty())
		.map(|timer| {
			Hash128(u128::from_be_bytes(
				timer.key
					.as_slice()
					.try_into()
					.expect("a session tracker timer is keyed by its group hash"),
			))
		})
		.collect();
	hashes.sort();
	hashes.dedup();
	if hashes.is_empty() {
		return Ok(());
	}
	let rule = operator.session_rule();
	let gap = operator.session_gap();
	let watermark = host.flow_watermark()?;
	let horizon = SealGate::new(rule, Some(SealedThrough::from_order(fired.at().to_order())), watermark).horizon();
	let loaded = operator.load_open_sessions_of(host, &hashes)?;
	let mut removes: Vec<GroupStateKey> = Vec::new();
	let mut saves = Vec::new();
	let mut moves = Vec::new();
	for (hash, mut open) in hashes.into_iter().zip(loaded) {
		let group = operator.partition_group(hash);
		for closed in open.close(horizon) {
			for member in &closed.members {
				removes.push((&SessionMemberKey(GroupId::window(hash, closed.id), member.row))
					.into_group_state_key());
				removes.push((&RowIndexKey(group, member.row)).into_group_state_key());
			}
		}
		if open.sessions().is_empty() {
			removes.push((&SessionKey(group)).into_group_state_key());
		} else {
			moves.push(seal_move(rule, tracker_key(hash), None, earliest_due(&open, gap)));
			saves.push((hash, open));
		}
	}
	let saving: Vec<(Hash128, &OpenSessions<DateTime>)> = saves.iter().map(|(hash, open)| (*hash, open)).collect();
	operator.save_open_sessions(host, &saving)?;
	if !removes.is_empty() {
		host.state_remove(&removes)?;
	}
	host.rearm_timers(TimerKind::Seal, &moves)
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
	seal_due_windows(operator, host, fired, rule, 1)
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
		let order = |millis: u64| {
			at_millis(i64::try_from(millis).expect("test order offset fits in i64 milliseconds")).to_order()
		};
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
	fn seal_horizon_goes_pre_epoch_for_young_watermarks() {
		// Wrapping past the epoch instead of going negative would seal every window early
		let lateness = Duration::from_milliseconds_const(10);

		assert_eq!(
			seal_horizon(at_millis(3), lateness),
			at_millis(-7),
			"young watermark minus lateness is a real pre-epoch instant"
		);
		assert!(
			!is_sealed(at_millis(0), seal_horizon(at_millis(3), lateness)),
			"the epoch is still after a pre-epoch horizon"
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
		key::operator::state::KeyspaceId,
	};
	use reifydb_flow::context::FlowContext;
	use reifydb_routine_abi::registry::Routines;
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{factory::time::at_millis, value::duration::Duration};

	use super::*;
	use crate::{
		operator::{
			host::TxnHostContext,
			state::test_utils::test::whole_keyspace,
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
		.expect("the window operator must build")
	}

	fn txn_at(engine: &TestEngine, coordinate: u64) -> DeferredTransaction {
		let mut txn = engine.flow_txn().at(CommitVersion(coordinate)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_nanos(
				i64::try_from(coordinate).expect("test coordinate fits in i64 nanoseconds"),
			)),
		});
		txn
	}

	fn armed_timers(txn: &mut DeferredTransaction, operator: OperatorId) -> usize {
		txn.state_range(
			operator,
			StateRange::forward(whole_keyspace(GroupId::ROOT, KeyspaceId::TIMER_WHEEL), "test"),
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
				enqueue(&mut host, &[GroupId::hashed(Hash128(n + 1))]).unwrap();
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

	use reifydb_codec::row::pod::EncodedPodRow;
	use reifydb_core::{
		common::{ChangeVersion, CommitVersion, WindowSize},
		interface::catalog::flow::OperatorId,
		key::{
			any::TaggedKey,
			operator::{
				keyspace::{expiry::RETIRED_AT_FLUSH, timer::TimerWheelKey},
				state::KeyspaceId,
			},
		},
		state::typed::SuffixBytes,
	};
	use reifydb_flow::context::FlowContext;
	use reifydb_routine_abi::registry::Routines;
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{factory::time::at_millis, value::duration::Duration};

	use super::*;
	use crate::{
		operator::{
			aggregation::core::slot_rows::slot_row,
			host::TxnHostContext,
			state::test_utils::test::whole_keyspace,
			window::operator::{WindowConfig, WindowOperator},
		},
		timer::{Timer, wheel::TimerWheel},
		transaction::{
			ChangeCoordinate, FlowTransaction,
			deferred::DeferredTransaction,
			mock::FlowTxn,
			state::{StateExtension, StateRange},
		},
		window::{
			engine::WindowReindex,
			kind::session::open::{OpenSession, SessionIds},
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
		.expect("the window operator must build")
	}

	fn rule() -> SealRule {
		SealRule::tumbling(Duration::from_milliseconds(SIZE_MS as i64).unwrap(), Duration::zero())
	}

	fn txn_at(engine: &TestEngine, coordinate: u64) -> DeferredTransaction {
		let mut txn = engine.flow_txn().at(CommitVersion(coordinate)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_nanos(
				i64::try_from(coordinate).expect("test coordinate fits in i64 nanoseconds"),
			)),
		});
		txn
	}

	fn seal_timers(txn: &mut DeferredTransaction, operator: OperatorId) -> Vec<DateTime> {
		txn.state_range(
			operator,
			StateRange::forward(whole_keyspace(GroupId::ROOT, KeyspaceId::TIMER_WHEEL), "test"),
		)
		.unwrap()
		.items
		.iter()
		.filter_map(|item| {
			let TaggedKey::OperatorState(decoded) = &item.key else {
				panic!("a wheel row must decode");
			};
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
		let slot_key = EncodedKey::new(Vec::new());
		let windows: Vec<(Hash128, DateTime)> = starts
			.iter()
			.enumerate()
			.map(|(n, start_ms)| {
				let start = at_millis(
					i64::try_from(*start_ms).expect("test window start fits in i64 milliseconds"),
				);
				(Hash128::from(n as u128), start)
			})
			.collect();
		let entries: Vec<WindowReindex<'_, Hash128, DateTime>> = windows
			.iter()
			.map(|(group, start)| WindowReindex {
				group,
				window_start: *start,
				id: GroupId::window(*group, start.to_order()),
				slot_key: &slot_key,
				prior: None,
				new: Some(start.to_order()),
			})
			.collect();
		engine.reindex_windows(host, &entries).unwrap();
		*operator.core.tumbling_engine_slot() = Some(engine);
	}

	fn take_one_due(txn: &mut DeferredTransaction, operator: OperatorId, watermark: u64) -> Timer {
		let mut timers = TimerWheel::take_due(
			operator,
			txn,
			at_millis(i64::try_from(watermark).expect("test watermark fits in i64 milliseconds")),
			16,
			None,
		)
		.unwrap()
		.timers;
		assert_eq!(timers.len(), 1, "exactly one timer may stand for the whole operator");
		timers.remove(0)
	}

	fn session_window() -> WindowOperator {
		WindowOperator::new(WindowConfig {
			parent_schema: None,
			operator: OperatorId(1),
			kind: WindowKind::Session {
				gap: Duration::from_milliseconds(1_000).unwrap(),
			},
			group_by: Vec::new(),
			aggregations: Vec::new(),
			runtime_context: RuntimeContext::testing(0, 1),
			routines: Routines::empty(),
			lateness: None,
			immutable: None,
			ctx: Arc::new(FlowContext::default()),
		})
		.expect("the session operator must build")
	}

	fn tracker_timer(hash: Hash128, due_ms: i64) -> Timer {
		Timer {
			due: at_millis(due_ms),
			kind: TimerKind::Seal,
			key: tracker_key(hash),
		}
	}

	fn queued_bodies(txn: &mut DeferredTransaction, operator: OperatorId) -> Vec<Vec<u8>> {
		txn.state_range(
			operator,
			StateRange::forward(whole_keyspace(GroupId::ROOT, KeyspaceId::REAP_QUEUE), "test"),
		)
		.unwrap()
		.items
		.iter()
		.map(|item| EncodedPodRow::from(item.bytes.clone()).body().to_vec())
		.collect()
	}

	fn maintenance_timers(txn: &mut DeferredTransaction, operator: OperatorId) -> usize {
		txn.state_range(
			operator,
			StateRange::forward(whole_keyspace(GroupId::ROOT, KeyspaceId::TIMER_WHEEL), "test"),
		)
		.unwrap()
		.items
		.iter()
		.filter(|item| {
			let TaggedKey::OperatorState(decoded) = &item.key else {
				panic!("a wheel row must decode");
			};
			TimerWheelKey::from_suffix_bytes(&decoded.suffix).expect("a wheel row must decode").kind.0
				== TimerKind::Maintenance
		})
		.count()
	}

	#[test]
	fn a_closed_tumbling_window_is_left_for_the_flush_and_arms_no_maintenance() {
		// a maintenance timer would drain the group inline, putting the cost back on the flow's hot path
		let engine = TestEngine::new();
		let mut operator = window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		let mut host = TxnHostContext::new(&mut txn, id);
		index_windows(&mut operator, &mut host, &[0, SIZE_MS]);

		seal_engine_windows(
			&mut operator,
			&mut host,
			FiredAt::of(&Timer {
				due: at_millis(1_000_000),
				kind: TimerKind::Seal,
				key: EncodedKey::new(Vec::new()),
			}),
		)
		.unwrap();
		drop(host);

		assert_eq!(
			queued_bodies(&mut txn, id),
			vec![RETIRED_AT_FLUSH.to_vec(), RETIRED_AT_FLUSH.to_vec()],
			"every closed window must carry the marker, or the flush never deletes it"
		);
		assert_eq!(maintenance_timers(&mut txn, id), 0);
	}

	#[test]
	fn a_closed_session_still_reaps_inline_because_the_flush_cannot_reach_its_root_rows() {
		// a session member has a sibling in the root group; deleting by group alone would leak that row for
		// good
		let engine = TestEngine::new();
		let mut operator = session_window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		let mut host = TxnHostContext::new(&mut txn, id);
		index_windows(&mut operator, &mut host, &[0, 1]);

		seal_session_engines(&mut operator, &mut host, &[tracker_timer(Hash128::from(1u128), 1_000_000)])
			.unwrap();
		drop(host);

		let bodies = queued_bodies(&mut txn, id);
		assert_eq!(bodies.len(), 2, "precondition: both sessions must have closed");
		assert!(bodies.iter().all(|body| body.is_empty()), "a session must never carry the flush marker");
		assert_eq!(maintenance_timers(&mut txn, id), 1, "the inline reap needs its timer");
	}

	#[test]
	fn a_seal_run_keeps_a_group_with_a_session_ahead_of_the_horizon_and_retires_one_behind_it() {
		// Retiring a group whose session is still ahead of the horizon drops a live session and its later rows.
		let engine = TestEngine::new();
		let mut operator = session_window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		let (alive, closing) = (Hash128::from(1u128), Hash128::from(2u128));
		let gap = Duration::from_milliseconds_const(1_000);
		let one_session = |millis: i64| {
			let at = at_millis(millis);
			OpenSessions::resumed(
				gap,
				SessionIds::StartTime,
				vec![OpenSession {
					id: at.to_order(),
					start: at,
					last: at,
					members: vec![Member {
						at,
						row: RowNumber(1),
					}],
				}],
				None,
			)
		};
		let mut host = TxnHostContext::new(&mut txn, id);
		let (alive_sessions, closing_sessions) = (one_session(2_500), one_session(1_100));
		operator.save_open_sessions(&mut host, &[(alive, &alive_sessions), (closing, &closing_sessions)])
			.unwrap();

		seal_session_engines(
			&mut operator,
			&mut host,
			&[tracker_timer(alive, 3_001), tracker_timer(closing, 3_101)],
		)
		.unwrap();

		let alive_left = operator.load_open_sessions(&mut host, alive).unwrap();
		let closing_left = operator.load_open_sessions(&mut host, closing).unwrap();
		assert_eq!(alive_left.sessions().len(), 1, "a session ahead of the horizon must survive the seal run");
		assert!(
			closing_left.sessions().is_empty(),
			"a session behind the horizon must close and its group retire"
		);
		assert_eq!(closing_left.sealed_last(), None, "a retired group keeps no state behind");
	}

	#[test]
	fn a_seal_run_closes_one_expire_budget_of_windows_per_timer_it_carries() {
		// A run capped at one budget closes fewer windows than its timers would alone, leaving late rows
		// admitted.
		let engine = TestEngine::new();
		let mut operator = session_window();
		let id = operator.core.operator;
		let mut txn = txn_at(&engine, 100);
		let due = operator.engine_config().expire_batch() + 44;
		let starts: Vec<u64> = (0..due as u64).collect();
		let mut host = TxnHostContext::new(&mut txn, id);
		index_windows(&mut operator, &mut host, &starts);

		seal_session_engines(
			&mut operator,
			&mut host,
			&[
				tracker_timer(Hash128::from(1u128), 1_000_000),
				tracker_timer(Hash128::from(2u128), 1_000_000),
			],
		)
		.unwrap();

		drop(host);
		assert!(
			seal_timers(&mut txn, id).is_empty(),
			"a run of two timers must close two budgets of due windows in its one pass"
		);
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
			vec![at_millis(
				i64::try_from(SIZE_MS + 1).expect("test window bound fits in i64 milliseconds")
			)],
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
			vec![at_millis(
				i64::try_from(2 * SIZE_MS + 1).expect("test window bound fits in i64 milliseconds"),
			)],
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

	fn accumulators(txn: &mut DeferredTransaction, operator: OperatorId, group: GroupId) -> usize {
		txn.state_range(operator, StateRange::forward(whole_keyspace(group, KeyspaceId::ACCUMULATOR), "test"))
			.unwrap()
			.items
			.len()
	}

	#[test]
	fn a_window_that_nets_to_empty_in_one_batch_is_still_reaped_once_it_seals() {
		// a window that publishes nothing must still be indexed, otherwise its accumulator is never reaped
		let engine = TestEngine::new();
		let mut operator = window();
		let id = operator.core.operator;
		let hash = Hash128(7);
		let start = at_millis(0);
		let span = WindowSpan {
			start,
			end: at_millis(SIZE_MS as i64),
		};
		let group = GroupId::window(hash, start.to_order());
		let mut txn = txn_at(&engine, 100);
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			let mut routing = Routing::default();
			for event in [
				AccumulatorEvent::Add(slot_row(Vec::new())),
				AccumulatorEvent::Remove(slot_row(Vec::new())),
			] {
				push_count_event(
					&mut routing,
					hash,
					&[],
					span,
					slot_coord(false, start, 1),
					event,
					start,
				);
			}
			let Routed {
				buckets,
				group_values,
				arrival,
				window_max_ts,
				..
			} = routing.finish();
			let groups = intern_batch(&arrival);
			let change = Change::from_flow(id, ChangeVersion::from(CommitVersion(100)), Vec::new(), start);
			let config = operator.engine_config();
			let diffs = finish_tumbling_engine(
				&mut operator.core,
				&mut host,
				&change,
				buckets,
				&group_values,
				arrival,
				window_max_ts,
				&groups,
				&[],
				config,
				None,
				ExpiryAnchor::WindowStart,
				false,
				Stamp::SpanStart,
			)
			.unwrap();
			assert!(diffs.is_empty(), "precondition: a window that nets to empty publishes nothing");
		}
		assert_eq!(accumulators(&mut txn, id, group), 1, "precondition: the empty accumulator is stored");

		let seal = FiredAt::of(&Timer {
			due: at_millis(SIZE_MS as i64 + 1),
			kind: TimerKind::Seal,
			key: EncodedKey::new(Vec::new()),
		});
		{
			let mut host = TxnHostContext::new(&mut txn, id);
			seal_engine_windows(&mut operator, &mut host, seal).unwrap();
			reap_sealed_groups(&mut operator, &mut host, seal).unwrap();
		}

		assert_eq!(accumulators(&mut txn, id, group), 0, "a sealed window must not keep its accumulator");
	}
}
