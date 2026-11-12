// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, fmt::Debug, hash::Hash};

use reifydb_codec::row::operator::state::StateCodec;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	internal_err,
	key::operator::state::{GroupId, GroupStateKey, IntoGroupStateKey},
	state::timer::TimerKind,
};
use reifydb_value::Result;

use crate::{
	operator::{
		drops::DropCounter,
		state::{
			reaper::{drain, drain_groups},
			seal::{domain::SealDomain, ledger::FiredAt, rule::is_sealed},
		},
	},
	window::{
		accumulator::{Contributions, WindowAccumulator},
		driver::{
			hooks::WindowHooks,
			route::{Routed, route_sliding, route_tumbling},
			rows::WindowRows,
			seal::{close_through, reap, seal},
			session::{
				SessionOp, SessionPass, SessionRows, finish_session, gate_sessions, retire_trackers,
				tracker_hash, tracker_moves,
			},
			store::WindowStore,
			tumbling::finish_tumbling,
		},
		engine::{ExpiryAnchor, config::WindowEngineConfig, tumbling::TumblingEngine},
		meta::{RowIndexKey, WindowMeta},
		prefetch::Prefetched,
		span::WindowAnchor,
	},
};

type HookContributions<H> = <<H as WindowHooks>::Accumulator as WindowAccumulator>::Contributions;

type HookItem<H> = <HookContributions<H> as Contributions>::Item;

pub struct WindowRunner<C: SealDomain> {
	size: C::Span,
	slide: Option<C::Span>,
	seal_span: Option<C::SealSpan>,
	expire_batch: usize,
	gap: Option<C::Span>,
	sealed_drops: DropCounter,
	refused: DropCounter,
	reap_pending: bool,
}

impl<C> WindowRunner<C>
where
	C: SealDomain + WindowAnchor + Hash,
{
	pub fn new(
		operator: OperatorId,
		size: C::Span,
		slide: Option<C::Span>,
		seal_span: Option<C::SealSpan>,
		config: &WindowEngineConfig,
	) -> Self {
		Self {
			size,
			slide,
			seal_span,
			expire_batch: config.expire_batch(),
			gap: None,
			sealed_drops: DropCounter::new(operator, "mutations targeting sealed windows"),
			refused: DropCounter::new(operator, "session rows refused by assignment"),
			reap_pending: true,
		}
	}

	pub fn sessions(
		operator: OperatorId,
		gap: C::Span,
		seal_span: Option<C::SealSpan>,
		config: &WindowEngineConfig,
	) -> Self {
		Self {
			gap: Some(gap),
			..Self::new(operator, gap, None, seal_span, config)
		}
	}

	pub fn route<R, X>(&self, rows: &mut R) -> Result<Routed<R::Group, C, X>>
	where
		R: WindowRows<Coord = C>,
		X: Contributions<Item = R::Contribution>,
		R::Group: Clone + Ord + Hash,
	{
		match self.slide {
			Some(slide) => route_sliding(rows, self.size, slide),
			None => route_tumbling(rows, self.size),
		}
	}

	pub fn apply<H>(
		&mut self,
		engine: &mut TumblingEngine<H::Group, C, H::Accumulator>,
		store: &mut dyn WindowStore,
		mut routed: Routed<H::Group, C, HookContributions<H>>,
		hooks: &mut H,
	) -> Result<H::Out>
	where
		H: WindowHooks<Coord = C>,
		H::Group: Clone + Eq + Ord + Hash + Debug + StateCodec,
	{
		let Some(seal_span) = self.seal_span else {
			return finish_tumbling(engine, store, routed, hooks, ExpiryAnchor::WindowStart);
		};
		if !C::arms_timer()
			&& let Some(newest) = routed.buckets.keys().map(|(_, span)| span.start).max()
		{
			C::observe(store, newest, seal_span)?;
		}
		let frontier = C::frontier(store)?;
		let horizon = C::horizon(frontier, seal_span);
		let mut dropped = 0u64;
		routed.buckets.retain(|(_, span), events| {
			if is_sealed(span.start, horizon) {
				dropped += events.len() as u64;
				false
			} else {
				true
			}
		});
		let buckets = &routed.buckets;
		routed.arrival.retain(|window| buckets.contains_key(window));
		self.sealed_drops.note(dropped);
		if !C::arms_timer() {
			let out = finish_tumbling(engine, store, routed, hooks, ExpiryAnchor::WindowStart)?;
			self.close_inline(engine, store, frontier, seal_span)?;
			return Ok(out);
		}
		let before = engine.earliest_expiry(store)?;
		let out = finish_tumbling(engine, store, routed, hooks, ExpiryAnchor::WindowStart)?;
		let after = engine.earliest_expiry(store)?;
		C::rearm_seal(store, seal_span, before, after)?;
		Ok(out)
	}

	fn close_inline<G, A>(
		&mut self,
		engine: &mut TumblingEngine<G, C, A>,
		store: &mut dyn WindowStore,
		frontier: C,
		seal_span: C::SealSpan,
	) -> Result<()>
	where
		G: Clone + Eq + Ord + Hash + Debug + StateCodec,
		A: WindowAccumulator,
	{
		if close_through(engine, store, frontier, seal_span, usize::MAX, true)? {
			self.reap_pending = true;
		}
		if self.reap_pending {
			self.reap_pending = !drain(store, engine, self.expire_batch)?.queue_is_empty();
		}
		Ok(())
	}

	pub fn on_timer<G, A>(
		&mut self,
		engine: &mut TumblingEngine<G, C, A>,
		store: &mut dyn WindowStore,
		kind: TimerKind,
		fired: FiredAt,
	) -> Result<()>
	where
		G: Clone + Eq + Ord + Hash + Debug + StateCodec,
		A: WindowAccumulator,
	{
		let Some(seal_span) = self.seal_span else {
			return Ok(());
		};
		match kind {
			TimerKind::Seal => seal(engine, store, fired, seal_span, self.expire_batch, true),
			TimerKind::Maintenance => reap(engine, store, fired, seal_span, self.expire_batch).map(|_| ()),
			other => internal_err!("a window runner fired a {:?} timer it never arms", other),
		}
	}

	pub fn apply_session<H>(
		&mut self,
		engine: &mut TumblingEngine<H::Group, C, H::Accumulator>,
		store: &mut dyn WindowStore,
		rows: SessionRows<H::Group, C, HookItem<H>>,
		hooks: &mut H,
	) -> Result<H::Out>
	where
		H: WindowHooks<Coord = C>,
		H::Group: Clone + Eq + Ord + Hash + Debug + StateCodec,
		HookItem<H>: StateCodec,
	{
		let (Some(seal_span), Some(gap)) = (self.seal_span, self.gap) else {
			return internal_err!("a session window runner needs a seal span and a gap");
		};
		let horizon = C::horizon(C::frontier(store)?, seal_span);
		let SessionRows {
			ops,
			mut refused,
		} = rows;
		let mut identities = HashMap::new();
		let mut keys: Vec<GroupStateKey> = Vec::with_capacity(ops.len());
		let mut note = |group: &H::Group, row| {
			let hash = identities.entry(group.clone()).or_insert_with(|| hooks.group_identity(group)).0;
			keys.push((&RowIndexKey(GroupId::hashed(hash), row)).into_group_state_key());
		};
		for op in &ops {
			match op {
				SessionOp::Admit {
					group,
					row,
					..
				}
				| SessionOp::Retract {
					group,
					row,
					..
				} => note(group, *row),
				SessionOp::Revise {
					row,
					pre,
					post,
				} => {
					note(&pre.0, *row);
					note(&post.0, *row);
				}
			}
		}
		let mut pass = SessionPass::<_, C, HookContributions<H>>::new(&identities, horizon, gap);
		{
			let mut prefetched = Prefetched::read(store, keys)?;
			pass.load(&mut prefetched)?;
			for op in ops {
				match op {
					SessionOp::Admit {
						group,
						at,
						row,
						contribution,
					} => {
						if !pass.admit(&mut prefetched, &group, at, row, contribution)? {
							refused += 1;
						}
					}
					SessionOp::Retract {
						group,
						at,
						row,
						contribution,
					} => pass.retract(&mut prefetched, &group, at, row, contribution)?,
					SessionOp::Revise {
						row,
						pre: (pre_group, pre_at, pre),
						post: (post_group, at, post),
					} => {
						let indexed = pass.indexed(&mut prefetched, &pre_group, row)?;
						if indexed && pre_group == post_group && pre_at == at {
							pass.revalue(&mut prefetched, &pre_group, at, row, pre, post)?;
							continue;
						}
						if indexed {
							pass.retract(&mut prefetched, &pre_group, pre_at, row, pre)?;
						}
						if !pass.admit(&mut prefetched, &post_group, at, row, post)? {
							refused += 1;
						}
					}
				}
			}
			prefetched.write()?;
		}
		self.refused.note(refused);
		let mut routed = pass.take_routed();
		let (gated, sealed, dropped) = gate_sessions(store, &mut routed, &identities, horizon)?;
		self.sealed_drops.note(dropped);
		if !sealed.is_empty() {
			pass.withdraw_sealed(store, &sealed)?;
		}
		let passed = pass.finish(routed);
		let mut meta = WindowMeta::new();
		meta.save_open_sessions(store, passed.saving)?;
		meta.drop_sessions(store, &passed.emptied)?;
		store.rearm_timers(TimerKind::Seal, &tracker_moves::<C>(seal_span, passed.moves))?;
		let gone: Vec<GroupId> = passed
			.routed
			.arrival
			.iter()
			.map(|(group, span)| (identities[group].0, span.start.to_order()))
			.filter(|window| !passed.bounds.contains_key(window))
			.map(|(hash, start)| GroupId::window(hash, start))
			.collect();
		let before = engine.earliest_expiry(store)?;
		let out =
			finish_session(engine, store, passed.routed, &gated, &passed.bounds, &identities, gap, hooks)?;
		if !gone.is_empty() {
			drain_groups(store, &gone, engine, usize::MAX)?;
		}
		let after = engine.earliest_expiry(store)?;
		C::rearm_seal(store, seal_span, before, after)?;
		Ok(out)
	}

	pub fn on_session_timers<G, A>(
		&mut self,
		engine: &mut TumblingEngine<G, C, A>,
		store: &mut dyn WindowStore,
		keys: &[&[u8]],
		fired: FiredAt,
	) -> Result<()>
	where
		G: Clone + Eq + Ord + Hash + Debug + StateCodec,
		A: WindowAccumulator,
	{
		let (Some(seal_span), Some(gap)) = (self.seal_span, self.gap) else {
			return internal_err!("a session window runner needs a seal span and a gap");
		};
		seal(engine, store, fired, seal_span, self.expire_batch.saturating_mul(keys.len()), false)?;
		let mut hashes = keys
			.iter()
			.filter(|key| !key.is_empty())
			.map(|key| tracker_hash(key))
			.collect::<Result<Vec<_>>>()?;
		hashes.sort();
		hashes.dedup();
		if hashes.is_empty() {
			return Ok(());
		}
		let horizon = C::horizon(C::from_order(fired.at().to_order()), seal_span);
		let moves = retire_trackers::<C>(store, &hashes, horizon, gap)?;
		store.rearm_timers(TimerKind::Seal, &tracker_moves::<C>(seal_span, moves))
	}
}
