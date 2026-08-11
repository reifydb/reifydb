// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use dashmap::DashMap;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
};
use reifydb_value::{Result, reifydb_assertions, value::datetime::DateTime};
use tracing::{info, warn};

use crate::transaction::{
	group::{decode_payload, encode_payload},
	interface::FlowTransaction,
};

const PERSIST_BUCKET_MS: u64 = 1_000;

const IMPLAUSIBLE_JUMP_MS: u64 = 3_600_000;

fn source_watermark_key() -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::SOURCE_WATERMARK, vec![])
}

#[derive(Default)]
struct SourceState {
	hydrated: bool,
	value: Option<u64>,
}

#[derive(Clone, Default)]
pub struct SourceWatermarks {
	inner: Arc<DashMap<OperatorId, SourceState>>,
}

impl SourceWatermarks {
	pub fn advance(&self, source: OperatorId, txn: &mut impl FlowTransaction, at: DateTime) -> Result<()> {
		let coordinate = at.to_millis();
		let mut state = self.inner.entry(source).or_default();
		Self::hydrate_once(&mut state, source, txn)?;
		let persist = match state.value {
			Some(previous) => {
				if coordinate <= previous {
					return Ok(());
				}
				coordinate / PERSIST_BUCKET_MS > previous / PERSIST_BUCKET_MS
			}
			None => true,
		};
		if let Some(previous) = state.value
			&& coordinate > previous.saturating_add(IMPLAUSIBLE_JUMP_MS)
		{
			warn!(
				source = source.0,
				from_ms = previous,
				to_ms = coordinate,
				delta_ms = coordinate - previous,
				"source watermark jumped by more than an hour in one step; a row stamped from a \
				 clock rather than from its own event time moves the watermark to now and can seal \
				 every open window at once"
			);
		}
		state.value = Some(coordinate);
		if persist {
			txn.state_set(source, &source_watermark_key(), encode_payload(&coordinate, at)?)?;
		}
		Ok(())
	}

	pub fn source_watermark(&self, source: OperatorId, txn: &mut impl FlowTransaction) -> Result<DateTime> {
		Ok(DateTime::from_millis(self.raw(source, txn)?))
	}

	pub fn flow_watermark(&self, sources: &[OperatorId], txn: &mut impl FlowTransaction) -> Result<DateTime> {
		reifydb_assertions! {
			assert!(
				!sources.is_empty(),
				"a flow watermark was read with no sources; the min-merge over nothing would \
				 pin the watermark at zero and hold every horizon open, so the caller failed \
				 to wire the flow's source list"
			);
		}
		let mut merged: Option<u64> = None;
		let mut per_source: Vec<(OperatorId, u64)> = Vec::with_capacity(sources.len());
		for source in sources {
			let value = self.raw(*source, txn)?;
			per_source.push((*source, value));
			merged = Some(match merged {
				Some(current) => current.min(value),
				None => value,
			});
		}
		let merged = merged.unwrap_or(0);
		if merged == 0 && per_source.iter().any(|(_, value)| *value > 0) {
			let pinning: Vec<u64> = per_source
				.iter()
				.filter(|(_, value)| *value == 0)
				.map(|(source, _)| source.0)
				.collect();
			info!(
				sources = sources.len(),
				pinned_by = ?pinning,
				"flow watermark merged to the epoch while other sources have advanced; the min-merge \
				 holds every horizon open until each listed source reports, so no window can seal"
			);
		}
		Ok(DateTime::from_millis(merged))
	}

	fn raw(&self, source: OperatorId, txn: &mut impl FlowTransaction) -> Result<u64> {
		let mut state = self.inner.entry(source).or_default();
		Self::hydrate_once(&mut state, source, txn)?;
		Ok(state.value.unwrap_or(0))
	}

	fn hydrate_once(state: &mut SourceState, source: OperatorId, txn: &mut impl FlowTransaction) -> Result<()> {
		if state.hydrated {
			return Ok(());
		}
		state.hydrated = true;
		if let Some(row) = txn.state_get(source, &source_watermark_key())? {
			state.value = Some(decode_payload::<u64>(&row)?);
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::actors::pending::PendingLayers;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::{factory::time::at_millis, value::identity::IdentityId};

	use super::*;
	use crate::transaction::{
		DeferredParams, DepFlowTransaction,
		substrate::{FlowSubstrate, apply_operator_state},
	};

	const SOURCE_A: OperatorId = OperatorId(1);
	const SOURCE_B: OperatorId = OperatorId(2);

	fn deferred(engine: &TestEngine, clock: MockClock) -> DepFlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		DepFlowTransaction::deferred_from_parts(DeferredParams {
			version,
			pending: PendingLayers::empty(),
			query: parent.multi.begin_query().unwrap(),
			state_query: parent.multi.begin_query().unwrap(),
			catalog: Catalog::testing(),
			interceptors: Interceptors::new(),
			clock: Clock::Mock(clock),
			substrate: FlowSubstrate {
				operators: engine.inner().operator_state(),
				..FlowSubstrate::default()
			},
		})
	}

	fn commit_pending(engine: &TestEngine, txn: &mut impl FlowTransaction) {
		// Persists the pending writes so a cold instance resolves them as a restarted process would.
		let pending = txn.take_pending();
		apply_operator_state(&engine.inner().operator_state(), &pending);
	}

	#[test]
	fn the_source_watermark_never_moves_backwards() {
		// The per-source watermark is a running max over #time. Late rows arrive with older stamps
		// routinely; dragging it backwards would move derived cutoffs back and re-open horizons
		// that have already sealed.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine, MockClock::from_millis(0));
		let watermarks = SourceWatermarks::default();

		watermarks.advance(SOURCE_A, &mut txn, at_millis(5_000)).unwrap();
		watermarks.advance(SOURCE_A, &mut txn, at_millis(3_000)).unwrap();

		assert_eq!(watermarks.source_watermark(SOURCE_A, &mut txn).unwrap(), at_millis(5_000));
	}

	#[test]
	fn the_flow_watermark_tracks_the_slowest_source() {
		// The flow watermark is the min across sources so a fast source can never seal a slow
		// source's state.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine, MockClock::from_millis(0));
		let watermarks = SourceWatermarks::default();
		let sources = [SOURCE_A, SOURCE_B];

		watermarks.advance(SOURCE_A, &mut txn, at_millis(10_000)).unwrap();
		watermarks.advance(SOURCE_B, &mut txn, at_millis(2_000)).unwrap();
		assert_eq!(watermarks.flow_watermark(&sources, &mut txn).unwrap(), at_millis(2_000));

		watermarks.advance(SOURCE_A, &mut txn, at_millis(20_000)).unwrap();
		assert_eq!(
			watermarks.flow_watermark(&sources, &mut txn).unwrap(),
			at_millis(2_000),
			"the fast source must not advance the flow watermark past the slow one"
		);

		watermarks.advance(SOURCE_B, &mut txn, at_millis(12_000)).unwrap();
		assert_eq!(watermarks.flow_watermark(&sources, &mut txn).unwrap(), at_millis(12_000));
	}

	#[test]
	fn a_restart_resumes_from_the_bucketed_persisted_watermark() {
		// Persistence is bucketed at 1s to bound write amplification, so an advance inside the same
		// second stays in RAM. Hydrating up to one bucket stale is conservative: retention seals
		// later than the live value, never earlier.
		let engine = TestEngine::new();
		let warm = SourceWatermarks::default();

		let mut txn = deferred(&engine, MockClock::from_millis(0));
		warm.advance(SOURCE_A, &mut txn, at_millis(5_400)).unwrap();
		warm.advance(SOURCE_A, &mut txn, at_millis(5_900)).unwrap();
		commit_pending(&engine, &mut txn);

		let mut cold_txn = deferred(&engine, MockClock::from_millis(0));
		let cold = SourceWatermarks::default();
		assert_eq!(
			cold.source_watermark(SOURCE_A, &mut cold_txn).unwrap(),
			at_millis(5_400),
			"the same-second advance must not have persisted"
		);

		let mut txn = deferred(&engine, MockClock::from_millis(0));
		warm.advance(SOURCE_A, &mut txn, at_millis(6_100)).unwrap();
		commit_pending(&engine, &mut txn);

		let mut cold_txn = deferred(&engine, MockClock::from_millis(0));
		let cold = SourceWatermarks::default();
		assert_eq!(
			cold.source_watermark(SOURCE_A, &mut cold_txn).unwrap(),
			at_millis(6_100),
			"crossing the 1s bucket must persist"
		);
	}

	#[test]
	fn an_empty_source_hydrates_to_zero_not_to_now() {
		// A source that has never produced a row hydrates to zero, never to the clock. Hydrating to
		// now would compute cutoffs over the whole backlog on restart and seal state before the
		// first row is processed.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine, MockClock::from_millis(500_000));
		let watermarks = SourceWatermarks::default();

		assert_eq!(watermarks.source_watermark(SOURCE_A, &mut txn).unwrap(), at_millis(0));
		assert_eq!(watermarks.flow_watermark(&[SOURCE_A], &mut txn).unwrap(), at_millis(0));
	}

	#[test]
	fn the_watermark_is_the_min_merge_of_stamped_arrivals_never_the_clock() {
		// Replay determinism: the flow watermark derives from stamped row time in every domain -
		// processing time is event time over arrival stamps. The clock starts far ahead of the
		// data and moves again mid-test, so a clock read anywhere in the merge shows up as
		// 100_000 or 150_000 where 5_000 is expected.
		let engine = TestEngine::new();
		let clock = MockClock::from_millis(100_000);
		let mut txn = deferred(&engine, clock.clone());
		let watermarks = SourceWatermarks::default();
		let sources = [SOURCE_A, SOURCE_B];

		watermarks.advance(SOURCE_A, &mut txn, at_millis(10_000)).unwrap();
		watermarks.advance(SOURCE_B, &mut txn, at_millis(5_000)).unwrap();
		assert_eq!(
			watermarks.flow_watermark(&sources, &mut txn).unwrap(),
			at_millis(5_000),
			"the watermark must be the min-merge of the arrival-derived sources, not the clock"
		);

		clock.advance_millis(50_000);

		assert_eq!(
			watermarks.flow_watermark(&sources, &mut txn).unwrap(),
			at_millis(5_000),
			"with no new data the watermark must hold however far the clock runs"
		);
	}

	#[test]
	fn the_source_watermark_key_round_trips() {
		// A drifted encoding would make hydration read an absent key and silently restart every
		// watermark at zero, which reads as a healthy cold start rather than as lost state.
		let key = source_watermark_key();
		let (group, keyspace, suffix) =
			OperatorStateKey::decode_inner(key.as_slice()).expect("the key must decode as inner state");

		assert_eq!(group, GroupId::ROOT);
		assert_eq!(keyspace, Keyspace::SOURCE_WATERMARK);
		assert!(suffix.is_empty());
	}
}
