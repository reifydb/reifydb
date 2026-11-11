// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, hash::Hash};

use reifydb_codec::row::operator::state::StateCodec;
use reifydb_core::key::operator::state::GroupId;
use reifydb_value::Result;

use crate::{
	operator::state::{
		reaper::{DrainOutcome, drain, enqueue, enqueue_retired},
		seal::{
			domain::SealDomain,
			ledger::{FiredAt, SealLedger},
		},
	},
	window::{
		accumulator::WindowAccumulator, driver::store::WindowStore, engine::tumbling::TumblingEngine,
		span::WindowAnchor,
	},
};

pub fn close_through<G, C, A>(
	engine: &mut TumblingEngine<G, C, A>,
	store: &mut dyn WindowStore,
	frontier: C,
	seal_span: C::SealSpan,
	budget: usize,
	may_retire: bool,
) -> Result<bool>
where
	G: Clone + Eq + Ord + Hash + Debug + StateCodec,
	C: SealDomain + WindowAnchor + Hash,
	A: WindowAccumulator,
{
	let horizon = C::horizon(frontier, seal_span);
	if horizon <= C::from_order(0) {
		return Ok(false);
	}
	let expired = engine.expire_within(store, horizon.to_order().saturating_sub(1), budget)?;
	let groups: Vec<GroupId> = expired.iter().map(|window| window.group_id).collect();
	if may_retire && store.state_retires_groups() {
		enqueue_retired(store, &groups)?;
		return Ok(false);
	}
	enqueue(store, &groups)?;
	Ok(!groups.is_empty())
}

pub fn seal<G, C, A>(
	engine: &mut TumblingEngine<G, C, A>,
	store: &mut dyn WindowStore,
	fired: FiredAt,
	seal_span: C::SealSpan,
	budget: usize,
	may_retire: bool,
) -> Result<()>
where
	G: Clone + Eq + Ord + Hash + Debug + StateCodec,
	C: SealDomain + WindowAnchor + Hash,
	A: WindowAccumulator,
{
	SealLedger::advance(store, fired)?;
	let frontier = C::from_order(fired.at().to_order());
	if C::horizon(frontier, seal_span) <= C::from_order(0) {
		return Ok(());
	}
	if close_through(engine, store, frontier, seal_span, budget, may_retire)? {
		C::arm_reap(store, fired.at(), seal_span)?;
	}
	let earliest = engine.earliest_expiry(store)?;
	C::rearm_seal(store, seal_span, None, earliest)
}

pub fn reap<G, C, A>(
	engine: &mut TumblingEngine<G, C, A>,
	store: &mut dyn WindowStore,
	fired: FiredAt,
	seal_span: C::SealSpan,
	budget: usize,
) -> Result<DrainOutcome>
where
	G: Clone + Eq + Ord + Hash + Debug + StateCodec,
	C: SealDomain + WindowAnchor + Hash,
	A: WindowAccumulator,
{
	let drained = drain(store, engine, budget)?;
	if !drained.queue_is_empty() {
		C::arm_reap(store, fired.at(), seal_span)?;
	}
	Ok(drained)
}
