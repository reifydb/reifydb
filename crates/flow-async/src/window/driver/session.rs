// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	fmt::Debug,
	hash::Hash,
	mem::replace,
	slice::from_ref,
};

use reifydb_codec::{key::encoded::EncodedKey, row::operator::state::StateCodec};
use reifydb_core::{
	internal_err,
	key::operator::state::{GroupId, GroupStateKey, IntoGroupStateKey},
	state::timer::{StateStore, TimerMove},
};
use reifydb_value::{Result, util::hash::Hash128, value::row_number::RowNumber};

use crate::{
	operator::{
		state::seal::{coord::Coord, domain::SealDomain, rule::is_sealed},
		state_access::{get_classified, remove, set},
	},
	window::{
		accumulator::{AccumulatorOp, Contributions, WindowAccumulator},
		driver::{
			hooks::{WindowEmits, WindowHooks},
			publish::publish,
			route::{Routed, Routing},
			rows::{WindowColumns, WindowDiff, WindowRows},
		},
		engine::{EmitKind, ExpiryAnchor, WindowReindex, tumbling::TumblingEngine},
		kind::session::open::{Insertion, Member, OpenSession, OpenSessions, Removal},
		meta::{
			EngineMeta, EngineMetaKey, OpenSessionsState, RowIndexKey, SessionKey, SessionMemberKey,
			SessionState, WindowMeta,
		},
		mint::Mint,
		prefetch::Prefetched,
		span::{WindowAnchor, WindowSpan},
	},
};

pub enum SessionOp<G, C, X> {
	Admit {
		group: G,
		at: C,
		row: RowNumber,
		contribution: X,
	},
	Retract {
		group: G,
		at: C,
		row: RowNumber,
		contribution: X,
	},
	Revise {
		row: RowNumber,
		pre: (G, C, X),
		post: (G, C, X),
	},
}

pub struct SessionRows<G, C, X> {
	pub ops: Vec<SessionOp<G, C, X>>,
	pub refused: u64,
}

impl<G: Clone, C: Copy, X: Clone> SessionRows<G, C, X> {
	fn single(&mut self, columns: &WindowColumns<G, C, X>, admit: bool) {
		for row in 0..columns.len() {
			let Some(at) = columns.coords()[row] else {
				continue;
			};
			let Some(number) = columns.rows()[row] else {
				self.refused += 1;
				continue;
			};
			let group = columns.groups()[row].clone();
			let contribution = columns.contributions()[row].clone();
			self.ops.push(match admit {
				true => SessionOp::Admit {
					group,
					at,
					row: number,
					contribution,
				},
				false => SessionOp::Retract {
					group,
					at,
					row: number,
					contribution,
				},
			});
		}
	}

	fn update(&mut self, diff: usize, pre: &WindowColumns<G, C, X>, post: &WindowColumns<G, C, X>) -> Result<()> {
		let number = |columns: &WindowColumns<G, C, X>, row: usize| -> Result<RowNumber> {
			match columns.rows()[row] {
				Some(number) => Ok(number),
				None => internal_err!(
					"session update diff {diff} has a row without a row number at batch position {}",
					columns.index()[row]
				),
			}
		};
		let side = |columns: &WindowColumns<G, C, X>, row: usize| -> Option<(G, C, X)> {
			columns.coords()[row]
				.map(|at| (columns.groups()[row].clone(), at, columns.contributions()[row].clone()))
		};
		let (mut p, mut q) = (0, 0);
		while p < pre.len() || q < post.len() {
			let position = match (pre.index().get(p), post.index().get(q)) {
				(Some(&a), Some(&b)) => a.min(b),
				(Some(&a), None) => a,
				(None, Some(&b)) => b,
				(None, None) => break,
			};
			let before = (pre.index().get(p) == Some(&position)).then_some(p);
			let after = (post.index().get(q) == Some(&position)).then_some(q);
			let row = match (before, after) {
				(Some(row), _) => number(pre, row)?,
				(None, Some(row)) => number(post, row)?,
				(None, None) => break,
			};
			let old = before.and_then(|row| side(pre, row));
			let new = after.and_then(|row| side(post, row));
			match (old, new) {
				(Some(pre), Some(post)) => self.ops.push(SessionOp::Revise {
					row,
					pre,
					post,
				}),
				(Some((group, at, contribution)), None) => self.ops.push(SessionOp::Retract {
					group,
					at,
					row,
					contribution,
				}),
				(None, Some((group, at, contribution))) => self.ops.push(SessionOp::Admit {
					group,
					at,
					row,
					contribution,
				}),
				(None, None) => {}
			}
			p += usize::from(before.is_some());
			q += usize::from(after.is_some());
		}
		Ok(())
	}
}

pub fn route_session<R>(rows: &mut R) -> Result<SessionRows<R::Group, R::Coord, R::Contribution>>
where
	R: WindowRows,
	R::Group: Clone,
	R::Coord: Copy,
	R::Contribution: Clone,
{
	let mut routed = SessionRows {
		ops: Vec::new(),
		refused: 0,
	};
	for diff in 0..rows.diff_count() {
		match rows.diff(diff)? {
			WindowDiff::Insert(post) => routed.single(&post, true),
			WindowDiff::Remove(pre) => routed.single(&pre, false),
			WindowDiff::Update {
				pre,
				post,
			} => routed.update(diff, &pre, &post)?,
		}
	}
	Ok(routed)
}

pub fn session_span<C: Coord>(id: u64) -> WindowSpan<C> {
	WindowSpan::new(C::from_order(id), C::from_order(id + 1))
}

pub fn tracker_key(hash: Hash128) -> EncodedKey {
	EncodedKey::new(hash.0.to_be_bytes())
}

pub fn tracker_hash(key: &[u8]) -> Result<Hash128> {
	match <[u8; 16]>::try_from(key) {
		Ok(bytes) => Ok(Hash128(u128::from_be_bytes(bytes))),
		Err(_) => internal_err!("a session tracker timer must be keyed by a 16 byte group hash, got {key:?}"),
	}
}

pub fn earliest_due<C: Coord>(open: &OpenSessions<C>, gap: C::Span) -> Option<u64> {
	open.sessions()
		.iter()
		.map(|session| session.last)
		.min()
		.or(open.sealed_last())
		.map(|last| last.add_span(gap).to_order())
}

pub fn load_open_sessions<C: Coord>(
	store: &mut dyn StateStore,
	hashes: &[Hash128],
	gap: C::Span,
) -> Result<Vec<OpenSessions<C>>> {
	let partitions: Vec<GroupId> = hashes.iter().map(|hash| GroupId::hashed(*hash)).collect();
	let states = WindowMeta::new().load_open_sessions_of(store, &partitions)?;
	Ok(states.into_iter().map(|state| open_sessions_of(gap, state)).collect())
}

fn open_sessions_of<C: Coord>(gap: C::Span, state: Option<OpenSessionsState>) -> OpenSessions<C> {
	let Some(state) = state else {
		return OpenSessions::new(gap);
	};
	let sessions = state
		.sessions
		.into_iter()
		.map(|(id, members)| {
			let members: Vec<Member<C>> = members
				.into_iter()
				.map(|(at, row)| Member {
					at: C::from_order(at),
					row: RowNumber(row),
				})
				.collect();
			OpenSession {
				id,
				start: members[0].at,
				last: members[members.len() - 1].at,
				members,
			}
		})
		.collect();
	OpenSessions::resumed(gap, sessions, state.sealed_last.map(C::from_order))
}

pub fn open_sessions_state<C: Coord>(open: &OpenSessions<C>) -> OpenSessionsState {
	OpenSessionsState {
		sessions: open
			.sessions()
			.iter()
			.map(|session| {
				(session.id, session.members.iter().map(|m| (m.at.to_order(), m.row.0)).collect())
			})
			.collect(),
		sealed_last: open.sealed_last().map(|at| at.to_order()),
	}
}

struct GroupSessions<C: Coord> {
	loaded_due: Option<u64>,
	open: OpenSessions<C>,
	touched: bool,
}

struct IndexEvent<G> {
	group: G,
	hash: Hash128,
	row: RowNumber,
	session: u64,
	added: bool,
}

pub(crate) struct SessionPass<'a, G, C: Coord, X: Contributions> {
	meta: WindowMeta,
	identities: &'a HashMap<G, (Hash128, EncodedKey)>,
	routing: Routing<G, C, X>,
	groups: HashMap<Hash128, GroupSessions<C>>,
	events: Vec<IndexEvent<G>>,
	horizon: C,
	gap: C::Span,
}

pub(crate) struct SessionPassed<G, C: Coord, X> {
	pub routed: Routed<G, C, X>,
	pub moves: Vec<(Hash128, Option<u64>, Option<u64>)>,
	pub saving: Vec<(GroupId, OpenSessionsState)>,
	pub emptied: Vec<GroupId>,
	pub bounds: HashMap<(Hash128, u64), (C, C)>,
}

impl<'a, G, C, X> SessionPass<'a, G, C, X>
where
	G: Clone + Eq + Ord + Hash + Debug,
	C: Coord + Hash,
	X: Contributions,
	X::Item: StateCodec,
{
	pub(crate) fn new(identities: &'a HashMap<G, (Hash128, EncodedKey)>, horizon: C, gap: C::Span) -> Self {
		Self {
			meta: WindowMeta::new(),
			identities,
			routing: Routing::sessions(gap),
			groups: HashMap::new(),
			events: Vec::new(),
			horizon,
			gap,
		}
	}

	fn hash(&self, group: &G) -> Result<Hash128> {
		match self.identities.get(group) {
			Some((hash, _)) => Ok(*hash),
			None => internal_err!("session group {group:?} has no identity"),
		}
	}

	pub(crate) fn load(&mut self, store: &mut dyn StateStore) -> Result<()> {
		let mut hashes: Vec<Hash128> = self.identities.values().map(|(hash, _)| *hash).collect();
		hashes.sort();
		hashes.dedup();
		if hashes.is_empty() {
			return Ok(());
		}
		let loaded = load_open_sessions::<C>(store, &hashes, self.gap)?;
		for (hash, open) in hashes.into_iter().zip(loaded) {
			let loaded_due = earliest_due(&open, self.gap);
			self.groups.insert(
				hash,
				GroupSessions {
					loaded_due,
					open,
					touched: false,
				},
			);
		}
		Ok(())
	}

	fn group(&mut self, store: &mut dyn StateStore, hash: Hash128) -> Result<&mut OpenSessions<C>> {
		let Some(group) = self.groups.get_mut(&hash) else {
			return internal_err!("session group {hash:?} was not loaded before the batch touched it");
		};
		if !group.touched {
			group.touched = true;
			for closed in group.open.close(self.horizon) {
				for member in &closed.members {
					self.meta.drop_session_member(
						store,
						GroupId::window(hash, closed.id),
						member.row,
					)?;
					Mint::new(&mut self.meta).withdraw_membership(
						store,
						GroupId::hashed(hash),
						member.row,
						closed.id,
					)?;
				}
			}
		}
		Ok(&mut group.open)
	}

	pub(crate) fn indexed(&mut self, store: &mut dyn StateStore, group: &G, row: RowNumber) -> Result<bool> {
		let hash = self.hash(group)?;
		Ok(!Mint::new(&mut self.meta).membership(store, GroupId::hashed(hash), row)?.is_empty())
	}

	fn push(&mut self, op: AccumulatorOp, group: &G, id: u64, at: C, contribution: &X::Item) {
		self.routing.push_span(op, group, at, session_span(id), contribution);
	}

	fn move_members(
		&mut self,
		store: &mut dyn StateStore,
		group: &G,
		hash: Hash128,
		from: u64,
		to: u64,
		members: &[Member<C>],
	) -> Result<()> {
		let partition = GroupId::hashed(hash);
		for member in members {
			let item: X::Item =
				self.meta.load_session_member(store, GroupId::window(hash, from), member.row)?;
			self.push(AccumulatorOp::Remove, group, from, member.at, &item);
			self.push(AccumulatorOp::Add, group, to, member.at, &item);
			let mut mint = Mint::new(&mut self.meta);
			mint.withdraw_membership(store, partition, member.row, from)?;
			mint.record_membership(store, partition, member.row, to)?;
			self.meta.drop_session_member(store, GroupId::window(hash, from), member.row)?;
			self.meta.store_session_member(store, GroupId::window(hash, to), member.row, &item)?;
		}
		Ok(())
	}

	pub(crate) fn admit(
		&mut self,
		store: &mut dyn StateStore,
		group: &G,
		at: C,
		row: RowNumber,
		contribution: X::Item,
	) -> Result<bool> {
		let hash = self.hash(group)?;
		let horizon = self.horizon;
		let target = match self.group(store, hash)?.insert(at, row, horizon) {
			Insertion::Refused => return Ok(false),
			Insertion::Opened(id) | Insertion::Extended(id) => id,
			Insertion::Merged {
				survivor,
				absorbed,
			} => {
				for session in &absorbed {
					self.move_members(store, group, hash, session.id, survivor, &session.members)?;
				}
				survivor
			}
		};
		Mint::new(&mut self.meta).record_membership(store, GroupId::hashed(hash), row, target)?;
		self.meta.store_session_member(store, GroupId::window(hash, target), row, &contribution)?;
		self.events.push(IndexEvent {
			group: group.clone(),
			hash,
			row,
			session: target,
			added: true,
		});
		self.push(AccumulatorOp::Add, group, target, at, &contribution);
		Ok(true)
	}

	pub(crate) fn retract(
		&mut self,
		store: &mut dyn StateStore,
		group: &G,
		at: C,
		row: RowNumber,
		contribution: X::Item,
	) -> Result<()> {
		let hash = self.hash(group)?;
		let partition = GroupId::hashed(hash);
		for session in Mint::new(&mut self.meta).membership(store, partition, row)? {
			let Some(removal) = self.group(store, hash)?.remove(session, row)? else {
				continue;
			};
			self.push(AccumulatorOp::Remove, group, session, at, &contribution);
			self.events.push(IndexEvent {
				group: group.clone(),
				hash,
				row,
				session,
				added: false,
			});
			self.meta.drop_session_member(store, GroupId::window(hash, session), row)?;
			if let Removal::Split {
				kept,
				moved,
			} = removal
			{
				self.move_members(store, group, hash, kept, moved.id, &moved.members)?;
			}
		}
		Mint::new(&mut self.meta).drop_membership(store, partition, row)
	}

	pub(crate) fn revalue(
		&mut self,
		store: &mut dyn StateStore,
		group: &G,
		at: C,
		row: RowNumber,
		pre: X::Item,
		post: X::Item,
	) -> Result<()> {
		let hash = self.hash(group)?;
		for session in Mint::new(&mut self.meta).membership(store, GroupId::hashed(hash), row)? {
			if !self.group(store, hash)?.sessions().iter().any(|open| open.id == session) {
				continue;
			}
			self.push(AccumulatorOp::Remove, group, session, at, &pre);
			self.push(AccumulatorOp::Add, group, session, at, &post);
			self.meta.store_session_member(store, GroupId::window(hash, session), row, &post)?;
		}
		Ok(())
	}

	pub(crate) fn withdraw_sealed(
		&mut self,
		store: &mut dyn StateStore,
		sealed: &HashSet<(G, WindowSpan<C>)>,
	) -> Result<()> {
		let mut first_touch: HashMap<(Hash128, RowNumber, u64), bool> = HashMap::new();
		for event in &self.events {
			if sealed.contains(&(event.group.clone(), session_span(event.session))) {
				first_touch.entry((event.hash, event.row, event.session)).or_insert(event.added);
			}
		}
		for ((hash, row, session), added) in first_touch {
			if added {
				Mint::new(&mut self.meta).withdraw_membership(
					store,
					GroupId::hashed(hash),
					row,
					session,
				)?;
				self.meta.drop_session_member(store, GroupId::window(hash, session), row)?;
			}
		}
		Ok(())
	}

	pub(crate) fn take_routed(&mut self) -> Routed<G, C, X> {
		replace(&mut self.routing, Routing::sessions(self.gap)).finish()
	}

	pub(crate) fn finish(self, routed: Routed<G, C, X>) -> SessionPassed<G, C, X> {
		let mut passed = SessionPassed {
			routed,
			moves: Vec::with_capacity(self.groups.len()),
			saving: Vec::with_capacity(self.groups.len()),
			emptied: Vec::new(),
			bounds: HashMap::new(),
		};
		for (hash, group) in self.groups.iter().filter(|(_, group)| group.touched) {
			for session in group.open.sessions() {
				passed.bounds.insert((*hash, session.id), (session.start, session.last));
			}
			let partition = GroupId::hashed(*hash);
			if group.open.sessions().is_empty() && group.open.sealed_last().is_none() {
				passed.emptied.push(partition);
			} else {
				passed.saving.push((partition, open_sessions_state(&group.open)));
			}
			passed.moves.push((*hash, group.loaded_due, earliest_due(&group.open, self.gap)));
		}
		passed
	}
}

pub type GatedMeta<G, C> = HashMap<(G, WindowSpan<C>), Option<EngineMeta>>;

pub fn gate_sessions<G, C, X>(
	store: &mut dyn StateStore,
	routed: &mut Routed<G, C, X>,
	identities: &HashMap<G, (Hash128, EncodedKey)>,
	horizon: C,
) -> Result<(GatedMeta<G, C>, HashSet<(G, WindowSpan<C>)>, u64)>
where
	G: Clone + Eq + Ord + Hash + Debug,
	C: Coord + Hash,
	X: Contributions,
{
	let id_of = |group: &G, span: &WindowSpan<C>| GroupId::window(identities[group].0, span.start.to_order());
	let mut gated = GatedMeta::new();
	if !routed.buckets.is_empty() {
		let keys = routed
			.buckets
			.keys()
			.map(|(group, span)| (&EngineMetaKey(id_of(group, span))).into_group_state_key())
			.collect();
		let mut prefetched = Prefetched::read(store, keys)?;
		for (group, span) in routed.buckets.keys() {
			let meta = get_classified::<_, EngineMeta>(
				&mut prefetched,
				from_ref(&EngineMetaKey(id_of(group, span))),
			)?
			.remove(0);
			gated.insert((group.clone(), *span), meta);
		}
	}
	let mut sealed = HashSet::new();
	let mut dropped = 0u64;
	for (key, events) in routed.buckets.iter() {
		let prior_last = gated[key].as_ref().map(|meta| meta.last_event_time);
		let batch_last = routed.window_max_coord.get(key).map(|coord| coord.to_order());
		let Some(last) = ExpiryAnchor::LastEvent.of(key.1.start.to_order(), prior_last.max(batch_last)) else {
			continue;
		};
		if is_sealed(C::from_order(last), horizon) {
			dropped += events.len() as u64;
			sealed.insert(key.clone());
		}
	}
	if !sealed.is_empty() {
		routed.buckets.retain(|key, _| !sealed.contains(key));
		routed.arrival.retain(|key| !sealed.contains(key));
	}
	Ok((gated, sealed, dropped))
}

pub fn tracker_moves<C: SealDomain>(
	seal_span: C::SealSpan,
	moves: Vec<(Hash128, Option<u64>, Option<u64>)>,
) -> Vec<TimerMove> {
	moves.into_iter()
		.filter_map(|(hash, before, after)| C::seal_move(seal_span, tracker_key(hash), before, after))
		.collect()
}

type Item<H> = <<<H as WindowHooks>::Accumulator as WindowAccumulator>::Contributions as Contributions>::Item;

#[allow(clippy::too_many_arguments)]
pub fn finish_session<H>(
	engine: &mut TumblingEngine<H::Group, H::Coord, H::Accumulator>,
	store: &mut dyn StateStore,
	routed: Routed<H::Group, H::Coord, <H::Accumulator as WindowAccumulator>::Contributions>,
	gated: &GatedMeta<H::Group, H::Coord>,
	bounds: &HashMap<(Hash128, u64), (H::Coord, H::Coord)>,
	identities: &HashMap<H::Group, (Hash128, EncodedKey)>,
	gap: <H::Coord as Coord>::Span,
	hooks: &mut H,
) -> Result<H::Out>
where
	H: WindowHooks,
	H::Group: Clone + Eq + Ord + Hash + Debug + StateCodec,
	H::Coord: WindowAnchor + Hash,
	Item<H>: StateCodec,
{
	let Routed {
		buckets,
		arrival,
		window_max_coord,
	} = routed;
	let anchor = ExpiryAnchor::LastEvent;
	let id_of = |group: &H::Group, start: H::Coord| GroupId::window(identities[group].0, start.to_order());
	let results = engine.apply(
		store,
		buckets,
		&arrival,
		|group, start| (id_of(group, start), identities[group].1.clone()),
		|| hooks.new_accumulator(),
	)?;
	let keys: Vec<GroupStateKey> =
		results.iter().map(|r| (&SessionKey(id_of(&r.group, r.span.start))).into_group_state_key()).collect();
	let gone = |group: &H::Group, start: u64| !bounds.contains_key(&(identities[group].0, start));
	let prior_of = |group: &H::Group, span: WindowSpan<H::Coord>| {
		gated.get(&(group.clone(), span)).cloned().flatten().map(|meta| meta.last_event_time)
	};
	let mut prefetched = Prefetched::read(store, keys)?;
	let mut reindexed = Vec::with_capacity(arrival.len());
	let mut spans = Vec::with_capacity(results.len());
	for r in &results {
		let id = id_of(&r.group, r.span.start);
		let window_start = r.span.start.to_order();
		let prior_last = prior_of(&r.group, r.span);
		let prior_index = prior_last.and_then(|last| anchor.of(window_start, Some(last)));
		match r.kind {
			EmitKind::Remove => {
				if gone(&r.group, window_start) {
					reindexed.push(WindowReindex {
						group: &r.group,
						window_start: r.span.start,
						id,
						slot_key: &identities[&r.group].1,
						prior: prior_index,
						new: None,
					});
				}
			}
			EmitKind::Insert | EmitKind::Update => {
				let batch_max =
					window_max_coord.get(&(r.group.clone(), r.span)).map(|at| at.to_order());
				let last_event_time = prior_last.max(batch_max);
				reindexed.push(WindowReindex {
					group: &r.group,
					window_start: r.span.start,
					id,
					slot_key: &identities[&r.group].1,
					prior: prior_index,
					new: anchor.of(window_start, last_event_time),
				});
				let Some(last_event_time) = last_event_time else {
					return internal_err!(
						"session {window_start} of group {:?} has neither a stored nor a routed event time",
						r.group
					);
				};
				set(
					&mut prefetched,
					vec![(
						EngineMetaKey(id),
						EngineMeta {
							last_event_time,
						},
					)],
				)?;
			}
		}
		let stored = get_classified::<_, SessionState>(&mut prefetched, from_ref(&SessionKey(id)))?.remove(0);
		let before = stored.map(|state| {
			(H::Coord::from_order(state.session_start), H::Coord::from_order(state.last_event_time))
		});
		let now = bounds.get(&(identities[&r.group].0, window_start)).copied().or(before);
		let pre = match r.kind {
			EmitKind::Insert => now,
			EmitKind::Update | EmitKind::Remove => before,
		};
		let (Some(pre), Some(now)) = (pre, now) else {
			return internal_err!(
				"session {window_start} of group {:?} published with no bounds record",
				r.group
			);
		};
		let Some(first_time) = now.0.as_time() else {
			return internal_err!(
				"session {window_start} of group {:?} starts at a coordinate with no time",
				r.group
			);
		};
		match r.kind {
			EmitKind::Remove => remove(&mut prefetched, from_ref(&SessionKey(id)))?,
			EmitKind::Insert | EmitKind::Update => set(
				&mut prefetched,
				vec![(
					SessionKey(id),
					SessionState {
						session_id: window_start,
						last_event_time: now.1.to_order(),
						session_start: now.0.to_order(),
						first_time,
					},
				)],
			)?,
		}
		let span = |(start, last): (H::Coord, H::Coord)| WindowSpan {
			start,
			end: last.add_span(gap),
		};
		spans.push((span(pre), span(now)));
	}
	let published: HashSet<(H::Group, WindowSpan<H::Coord>)> =
		results.iter().map(|r| (r.group.clone(), r.span)).collect();
	for (group, span) in arrival.iter().filter(|window| !published.contains(window)) {
		let id = id_of(group, span.start);
		let window_start = span.start.to_order();
		let prior_last = prior_of(group, *span);
		let prior_index = prior_last.and_then(|last| anchor.of(window_start, Some(last)));
		let batch_max = window_max_coord.get(&(group.clone(), *span)).map(|at| at.to_order());
		let last_event_time = prior_last.max(batch_max);
		reindexed.push(WindowReindex {
			group,
			window_start: span.start,
			id,
			slot_key: &identities[group].1,
			prior: prior_index,
			new: match gone(group, window_start) {
				true => None,
				false => anchor.of(window_start, last_event_time),
			},
		});
		let Some(last_event_time) = last_event_time else {
			return internal_err!(
				"session {window_start} of group {group:?} has neither a stored nor a routed event time"
			);
		};
		set(
			&mut prefetched,
			vec![(
				EngineMetaKey(id),
				EngineMeta {
					last_event_time,
				},
			)],
		)?;
	}
	engine.reindex_windows(&mut prefetched, &reindexed)?;
	prefetched.write()?;

	let mut emits = WindowEmits::default();
	for (r, (pre_span, now_span)) in results.into_iter().zip(spans) {
		let Some(pre_time) = pre_span.start.as_time() else {
			return internal_err!(
				"session of group {:?} was published at a coordinate with no time",
				r.group
			);
		};
		let Some(post_time) = now_span.start.as_time() else {
			return internal_err!("session of group {:?} starts at a coordinate with no time", r.group);
		};
		let pre = r.prior.as_ref().and_then(|prior| hooks.build(&r.group, pre_span, prior));
		let post = match r.kind {
			EmitKind::Remove => None,
			EmitKind::Insert | EmitKind::Update => hooks.build(&r.group, now_span, &r.value),
		};
		publish(&mut emits, r.row_number, pre_time, post_time, pre, post);
	}
	hooks.emit(emits)
}

pub fn retire_trackers<C: Coord>(
	store: &mut dyn StateStore,
	hashes: &[Hash128],
	horizon: C,
	gap: C::Span,
) -> Result<Vec<(Hash128, Option<u64>, Option<u64>)>> {
	let loaded = load_open_sessions::<C>(store, hashes, gap)?;
	let mut removes: Vec<GroupStateKey> = Vec::new();
	let mut saves = Vec::new();
	let mut moves = Vec::new();
	for (hash, mut open) in hashes.iter().copied().zip(loaded) {
		let partition = GroupId::hashed(hash);
		for closed in open.close(horizon) {
			for member in &closed.members {
				removes.push((&SessionMemberKey(GroupId::window(hash, closed.id), member.row))
					.into_group_state_key());
				removes.push((&RowIndexKey(partition, member.row)).into_group_state_key());
			}
		}
		if open.sessions().is_empty() {
			removes.push((&SessionKey(partition)).into_group_state_key());
		} else {
			moves.push((hash, None, earliest_due(&open, gap)));
			saves.push((partition, open_sessions_state(&open)));
		}
	}
	WindowMeta::new().save_open_sessions(store, saves)?;
	if !removes.is_empty() {
		store.state_remove(&removes)?;
	}
	Ok(moves)
}
