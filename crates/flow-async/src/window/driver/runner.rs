// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, hash::Hash};

use reifydb_codec::row::operator::state::StateCodec;
use reifydb_core::{interface::catalog::flow::OperatorId, internal_err, state::timer::TimerKind};
use reifydb_value::Result;

use crate::{
	operator::{
		drops::DropCounter,
		state::{
			reaper::drain,
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
			store::WindowStore,
			tumbling::finish_tumbling,
		},
		engine::{ExpiryAnchor, config::WindowEngineConfig, tumbling::TumblingEngine},
		span::WindowAnchor,
	},
};

type HookContributions<H> = <<H as WindowHooks>::Accumulator as WindowAccumulator>::Contributions;

pub struct WindowRunner<C: SealDomain> {
	size: C::Span,
	slide: Option<C::Span>,
	seal_span: Option<C::SealSpan>,
	expire_batch: usize,
	sealed_drops: DropCounter,
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
			sealed_drops: DropCounter::new(operator, "mutations targeting sealed windows"),
			reap_pending: true,
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
}
