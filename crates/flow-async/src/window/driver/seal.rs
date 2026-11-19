// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{fmt::Debug, hash::Hash};

use reifydb_codec::row::operator::state::StateCodec;
use reifydb_core::key::operator::state::GroupId;
use reifydb_value::error::ValueError;

use crate::{
	operator::state::{
		forget_pass::{DrainOutcome, drain, enqueue, enqueue_forgotten},
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
	operator_watermark: C,
	seal_span: C::SealSpan,
	budget: usize,
	may_forget: bool,
) -> Result<bool, ValueError>
where
	G: Clone + Eq + Ord + Hash + Debug + StateCodec,
	C: SealDomain + WindowAnchor + Hash,
	A: WindowAccumulator,
{
	let first_admitted = C::first_admitted(operator_watermark, seal_span);
	if first_admitted <= C::from_order(0) {
		return Ok(false);
	}
	let due = engine.seal_due_within(store, first_admitted.to_order().saturating_sub(1), budget)?;
	let groups: Vec<GroupId> = due.iter().map(|window| window.group_id).collect();
	if may_forget && store.state_forgets_groups() {
		enqueue_forgotten(store, &groups)?;
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
	may_forget: bool,
) -> Result<(), ValueError>
where
	G: Clone + Eq + Ord + Hash + Debug + StateCodec,
	C: SealDomain + WindowAnchor + Hash,
	A: WindowAccumulator,
{
	SealLedger::advance(store, fired)?;
	let operator_watermark = C::from_order(fired.at().to_order());
	if C::first_admitted(operator_watermark, seal_span) <= C::from_order(0) {
		return Ok(());
	}
	if close_through(engine, store, operator_watermark, seal_span, budget, may_forget)? {
		C::arm_forget_pass(store, fired.at(), seal_span)?;
	}
	let earliest = engine.earliest_seal(store)?;
	C::rearm_seal(store, seal_span, None, earliest)
}

pub fn forget_pass<G, C, A>(
	engine: &mut TumblingEngine<G, C, A>,
	store: &mut dyn WindowStore,
	fired: FiredAt,
	seal_span: C::SealSpan,
	budget: usize,
) -> Result<DrainOutcome, ValueError>
where
	G: Clone + Eq + Ord + Hash + Debug + StateCodec,
	C: SealDomain + WindowAnchor + Hash,
	A: WindowAccumulator,
{
	let drained = drain(store, engine, budget)?;
	if !drained.queue_is_empty() {
		C::arm_forget_pass(store, fired.at(), seal_span)?;
	}
	Ok(drained)
}
