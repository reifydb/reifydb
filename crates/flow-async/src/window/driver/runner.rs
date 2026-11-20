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
use reifydb_value::{error::ValueError, value::row_number::RowNumber};

use crate::{
	operator::{
		drops::DropCounter,
		state::{
			forget_pass::{drain, drain_groups},
			seal::{domain::SealDomain, ledger::FiredAt, rule::is_sealed},
		},
	},
	window::{
		accumulator::{Contributions, WindowAccumulator},
		driver::{
			hooks::WindowHooks,
			route::{Routed, route_sliding, route_tumbling},
			rows::WindowRows,
			seal::{close_through, forget_pass, seal},
			session::{
				SessionOp, SessionPass, SessionRows, finish_session, forget_trackers, gate_sessions,
				tracker_hash, tracker_moves,
			},
			store::WindowStore,
			tumbling::finish_tumbling,
		},
		engine::{SealAnchor, config::WindowEngineConfig, tumbling::TumblingEngine},
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
	due_batch: usize,
	gap: Option<C::Span>,
	sealed_drops: DropCounter,
	refused: DropCounter,
	forget_pass_pending: bool,
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
			due_batch: config.due_batch(),
			gap: None,
			sealed_drops: DropCounter::new(operator, "mutations targeting sealed windows"),
			refused: DropCounter::new(operator, "session rows refused by assignment"),
			forget_pass_pending: true,
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

	pub fn route<R, X>(&self, rows: &mut R) -> Result<Routed<R::Group, C, X>, ValueError>
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
	) -> Result<H::Out, ValueError>
	where
		H: WindowHooks<Coord = C>,
		H::Group: Clone + Eq + Ord + Hash + Debug + StateCodec,
	{
		let Some(seal_span) = self.seal_span else {
			return finish_tumbling(engine, store, routed, hooks, SealAnchor::WindowStart);
		};
		if !C::arms_timer()
			&& let Some(newest) = routed.buckets.keys().map(|(_, span)| span.start).max()
		{
			C::observe(store, newest, seal_span)?;
		}
		let operator_watermark = C::operator_watermark(store)?;
		let first_admitted = C::first_admitted(operator_watermark, seal_span);
		let mut dropped = 0u64;
		routed.buckets.retain(|(_, span), events| {
			if is_sealed(span.start, first_admitted) {
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
			let out = finish_tumbling(engine, store, routed, hooks, SealAnchor::WindowStart)?;
			self.close_inline(engine, store, operator_watermark, seal_span)?;
			return Ok(out);
		}
		let before = engine.earliest_seal(store)?;
		let out = finish_tumbling(engine, store, routed, hooks, SealAnchor::WindowStart)?;
		let after = engine.earliest_seal(store)?;
		C::rearm_seal(store, seal_span, before, after)?;
		Ok(out)
	}

	fn close_inline<G, A>(
		&mut self,
		engine: &mut TumblingEngine<G, C, A>,
		store: &mut dyn WindowStore,
		operator_watermark: C,
		seal_span: C::SealSpan,
	) -> Result<(), ValueError>
	where
		G: Clone + Eq + Ord + Hash + Debug + StateCodec,
		A: WindowAccumulator,
	{
		if close_through(engine, store, operator_watermark, seal_span, usize::MAX, true)? {
			self.forget_pass_pending = true;
		}
		if self.forget_pass_pending {
			self.forget_pass_pending = !drain(store, engine, self.due_batch)?.queue_is_empty();
		}
		Ok(())
	}

	pub fn on_timer<G, A>(
		&mut self,
		engine: &mut TumblingEngine<G, C, A>,
		store: &mut dyn WindowStore,
		kind: TimerKind,
		fired: FiredAt,
	) -> Result<(), ValueError>
	where
		G: Clone + Eq + Ord + Hash + Debug + StateCodec,
		A: WindowAccumulator,
	{
		let Some(seal_span) = self.seal_span else {
			return Ok(());
		};
		match kind {
			TimerKind::Seal => seal(engine, store, fired, seal_span, self.due_batch, true),
			TimerKind::ForgetPass | TimerKind::Tick => {
				forget_pass(engine, store, fired, self.due_batch).map(|_| ())
			}
			other => internal_err!("a window runner fired a {:?} timer it never arms", other),
		}
	}

	pub fn apply_session<H>(
		&mut self,
		engine: &mut TumblingEngine<H::Group, C, H::Accumulator>,
		store: &mut dyn WindowStore,
		rows: SessionRows<H::Group, C, HookItem<H>>,
		hooks: &mut H,
	) -> Result<H::Out, ValueError>
	where
		H: WindowHooks<Coord = C>,
		H::Group: Clone + Eq + Ord + Hash + Debug + StateCodec,
		HookItem<H>: StateCodec,
	{
		let (Some(seal_span), Some(gap)) = (self.seal_span, self.gap) else {
			return internal_err!("a session window runner needs a seal span and a gap");
		};
		let first_admitted = C::first_admitted(C::operator_watermark(store)?, seal_span);
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
		let mut pass = SessionPass::<_, C, HookContributions<H>>::new(&identities, first_admitted, gap);
		{
			let mut prefetched = Prefetched::read(store, keys)?;
			pass.load(&mut prefetched)?;
			let retracted: Vec<(&H::Group, RowNumber)> = ops
				.iter()
				.filter_map(|op| match op {
					SessionOp::Admit {
						..
					} => None,
					SessionOp::Retract {
						group,
						row,
					} => Some((group, *row)),
					SessionOp::Revise {
						row,
						pre,
						post,
					} => (pre.0 != post.0 || pre.1 != post.1).then_some((&pre.0, *row)),
				})
				.collect();
			let members = pass.member_keys(&mut prefetched, &retracted)?;
			prefetched.read_ahead(members)?;
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
						row,
					} => pass.retract(&mut prefetched, &group, row)?,
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
							pass.retract(&mut prefetched, &pre_group, row)?;
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
		let (gated, sealed, dropped) = gate_sessions(store, &mut routed, &identities, first_admitted)?;
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
		let before = engine.earliest_seal(store)?;
		let out =
			finish_session(engine, store, passed.routed, &gated, &passed.bounds, &identities, gap, hooks)?;
		if !gone.is_empty() {
			drain_groups(store, &gone, engine, usize::MAX)?;
		}
		let after = engine.earliest_seal(store)?;
		C::rearm_seal(store, seal_span, before, after)?;
		Ok(out)
	}

	pub fn on_session_timers<G, A>(
		&mut self,
		engine: &mut TumblingEngine<G, C, A>,
		store: &mut dyn WindowStore,
		keys: &[&[u8]],
		fired: FiredAt,
	) -> Result<(), ValueError>
	where
		G: Clone + Eq + Ord + Hash + Debug + StateCodec,
		A: WindowAccumulator,
	{
		let (Some(seal_span), Some(gap)) = (self.seal_span, self.gap) else {
			return internal_err!("a session window runner needs a seal span and a gap");
		};
		seal(engine, store, fired, seal_span, self.due_batch.saturating_mul(keys.len()), false)?;
		let mut hashes = keys
			.iter()
			.filter(|key| !key.is_empty())
			.map(|key| tracker_hash(key))
			.collect::<Result<Vec<_>, ValueError>>()?;
		hashes.sort();
		hashes.dedup();
		if hashes.is_empty() {
			return Ok(());
		}
		let first_admitted = C::first_admitted(C::from_order(fired.at().to_order()), seal_span);
		let moves = forget_trackers::<C>(store, &hashes, first_admitted, gap)?;
		store.rearm_timers(TimerKind::Seal, &tracker_moves::<C>(seal_span, moves))
	}
}
