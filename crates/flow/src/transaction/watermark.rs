// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use dashmap::DashMap;
use reifydb_core::{
	common::TimeDomain,
	interface::catalog::flow::OperatorId,
	key::operator_group_state::{GroupId, GroupStateKey, Keyspace, OperatorGroupStateKey},
};
use reifydb_value::{Result, reifydb_assertions, value::datetime::DateTime};

use super::{
	FlowTransaction,
	group::{decode_payload, encode_payload},
};

const PERSIST_BUCKET_MS: u64 = 1_000;

fn source_watermark_key() -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::SOURCE_WATERMARK, vec![])
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
	pub fn advance(&self, source: OperatorId, txn: &mut FlowTransaction, at: DateTime) -> Result<()> {
		let coordinate = at.to_millis();
		let now = txn.clock().now();
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
		state.value = Some(coordinate);
		if persist {
			txn.state_set(source, &source_watermark_key(), encode_payload(&coordinate, now)?)?;
		}
		Ok(())
	}

	pub fn source_watermark(&self, source: OperatorId, txn: &mut FlowTransaction) -> Result<DateTime> {
		Ok(DateTime::from_millis(self.raw(source, txn)?))
	}

	pub fn flow_watermark(
		&self,
		domain: TimeDomain,
		sources: &[OperatorId],
		txn: &mut FlowTransaction,
	) -> Result<DateTime> {
		match domain {
			TimeDomain::Processing => Ok(txn.clock().now()),
			TimeDomain::Event => {
				reifydb_assertions! {
					assert!(
						!sources.is_empty(),
						"an event-time flow watermark was read with no sources; the \
						 min-merge over nothing would pin the watermark at zero and hold \
						 every horizon open, so the caller failed to wire the flow's \
						 source list"
					);
				}
				let mut merged: Option<u64> = None;
				for source in sources {
					let value = self.raw(*source, txn)?;
					merged = Some(match merged {
						Some(current) => current.min(value),
						None => value,
					});
				}
				Ok(DateTime::from_millis(merged.unwrap_or(0)))
			}
		}
	}

	fn raw(&self, source: OperatorId, txn: &mut FlowTransaction) -> Result<u64> {
		let mut state = self.inner.entry(source).or_default();
		Self::hydrate_once(&mut state, source, txn)?;
		Ok(state.value.unwrap_or(0))
	}

	fn hydrate_once(state: &mut SourceState, source: OperatorId, txn: &mut FlowTransaction) -> Result<()> {
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
	use reifydb_core::actors::pending::PendingWrite;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::identity::IdentityId;

	use super::*;

	const SOURCE_A: OperatorId = OperatorId(1);
	const SOURCE_B: OperatorId = OperatorId(2);

	fn deferred(engine: &TestEngine, clock: MockClock) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		FlowTransaction::deferred(&parent, version, Catalog::testing(), Interceptors::new(), Clock::Mock(clock))
	}

	fn commit_pending(engine: &TestEngine, txn: &mut FlowTransaction) {
		// Persists the pending writes so a cold instance resolves them as a restarted process would.
		let pending = txn.take_pending();
		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		for (k, pw) in pending.iter_sorted() {
			match pw {
				PendingWrite::Set(v) => cmd.set(k, v.clone()).unwrap(),
				PendingWrite::Remove {
					announce: true,
				} => cmd.remove(k).unwrap(),
				PendingWrite::Remove {
					announce: false,
				} => cmd.remove_silent(k).unwrap(),
			};
		}
		cmd.commit_unchecked().unwrap();
	}

	fn at(millis: u64) -> DateTime {
		DateTime::from_millis(millis)
	}

	#[test]
	fn the_source_watermark_never_moves_backwards() {
		// The per-source watermark is a running max over #time. Late rows arrive with older stamps
		// routinely; dragging it backwards would move derived cutoffs back and re-open horizons
		// that have already sealed.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine, MockClock::from_millis(0));
		let watermarks = SourceWatermarks::default();

		watermarks.advance(SOURCE_A, &mut txn, at(5_000)).unwrap();
		watermarks.advance(SOURCE_A, &mut txn, at(3_000)).unwrap();

		assert_eq!(watermarks.source_watermark(SOURCE_A, &mut txn).unwrap(), at(5_000));
	}

	#[test]
	fn the_flow_watermark_tracks_the_slowest_source() {
		// The flow watermark is the min across sources so a fast source can never seal a slow
		// source's state.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine, MockClock::from_millis(0));
		let watermarks = SourceWatermarks::default();
		let sources = [SOURCE_A, SOURCE_B];

		watermarks.advance(SOURCE_A, &mut txn, at(10_000)).unwrap();
		watermarks.advance(SOURCE_B, &mut txn, at(2_000)).unwrap();
		assert_eq!(watermarks.flow_watermark(TimeDomain::Event, &sources, &mut txn).unwrap(), at(2_000));

		watermarks.advance(SOURCE_A, &mut txn, at(20_000)).unwrap();
		assert_eq!(
			watermarks.flow_watermark(TimeDomain::Event, &sources, &mut txn).unwrap(),
			at(2_000),
			"the fast source must not advance the flow watermark past the slow one"
		);

		watermarks.advance(SOURCE_B, &mut txn, at(12_000)).unwrap();
		assert_eq!(watermarks.flow_watermark(TimeDomain::Event, &sources, &mut txn).unwrap(), at(12_000));
	}

	#[test]
	fn a_restart_resumes_from_the_bucketed_persisted_watermark() {
		// Persistence is bucketed at 1s to bound write amplification, so an advance inside the same
		// second stays in RAM. Hydrating up to one bucket stale is conservative: retention seals
		// later than the live value, never earlier.
		let engine = TestEngine::new();
		let warm = SourceWatermarks::default();

		let mut txn = deferred(&engine, MockClock::from_millis(0));
		warm.advance(SOURCE_A, &mut txn, at(5_400)).unwrap();
		warm.advance(SOURCE_A, &mut txn, at(5_900)).unwrap();
		commit_pending(&engine, &mut txn);

		let mut cold_txn = deferred(&engine, MockClock::from_millis(0));
		let cold = SourceWatermarks::default();
		assert_eq!(
			cold.source_watermark(SOURCE_A, &mut cold_txn).unwrap(),
			at(5_400),
			"the same-second advance must not have persisted"
		);

		let mut txn = deferred(&engine, MockClock::from_millis(0));
		warm.advance(SOURCE_A, &mut txn, at(6_100)).unwrap();
		commit_pending(&engine, &mut txn);

		let mut cold_txn = deferred(&engine, MockClock::from_millis(0));
		let cold = SourceWatermarks::default();
		assert_eq!(
			cold.source_watermark(SOURCE_A, &mut cold_txn).unwrap(),
			at(6_100),
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

		assert_eq!(watermarks.source_watermark(SOURCE_A, &mut txn).unwrap(), at(0));
		assert_eq!(watermarks.flow_watermark(TimeDomain::Event, &[SOURCE_A], &mut txn).unwrap(), at(0));
	}

	#[test]
	fn event_silence_holds_while_processing_silence_advances() {
		// Both halves in one test so neither can be satisfied by breaking the other: an event
		// watermark is data-driven and holds while no data arrives, a processing watermark is the
		// wall clock so an idle flow keeps draining.
		let engine = TestEngine::new();
		let clock = MockClock::from_millis(100_000);
		let mut txn = deferred(&engine, clock.clone());
		let watermarks = SourceWatermarks::default();
		let sources = [SOURCE_A];

		watermarks.advance(SOURCE_A, &mut txn, at(5_000)).unwrap();
		assert_eq!(watermarks.flow_watermark(TimeDomain::Event, &sources, &mut txn).unwrap(), at(5_000));
		assert_eq!(watermarks.flow_watermark(TimeDomain::Processing, &sources, &mut txn).unwrap(), at(100_000));

		clock.advance_millis(50_000);

		assert_eq!(
			watermarks.flow_watermark(TimeDomain::Event, &sources, &mut txn).unwrap(),
			at(5_000),
			"an event watermark must not advance without data"
		);
		assert_eq!(
			watermarks.flow_watermark(TimeDomain::Processing, &sources, &mut txn).unwrap(),
			at(150_000),
			"a processing watermark must keep draining while idle"
		);
	}

	#[test]
	fn the_watermark_key_round_trips_beside_the_node_watermark() {
		// A drifted encoding would make hydration read an absent key and silently restart every
		// watermark at zero; a tag collision with NODE_WATERMARK would let the two overwrite each
		// other on node-scope state.
		let key = source_watermark_key();
		let (group, keyspace, suffix) = OperatorGroupStateKey::decode_inner(key.as_slice())
			.expect("the key must decode as inner state");

		assert_eq!(group, GroupId::NODE_SCOPE);
		assert_eq!(keyspace, Keyspace::SOURCE_WATERMARK);
		assert!(suffix.is_empty());
		assert_ne!(Keyspace::SOURCE_WATERMARK, Keyspace::NODE_WATERMARK);
	}
}
