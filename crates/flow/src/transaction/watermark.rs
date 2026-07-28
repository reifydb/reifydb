// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use dashmap::DashMap;
use reifydb_core::{
	common::TimeDomain,
	interface::catalog::flow::FlowNodeId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey, StateKey},
};
use reifydb_value::{Result, reifydb_assertions, value::datetime::DateTime};

use super::{
	FlowTransaction,
	group::{decode_payload, encode_payload},
};

const PERSIST_BUCKET_MS: u64 = 1_000;

fn source_watermark_key() -> StateKey {
	OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::SOURCE_WATERMARK, vec![])
}

#[derive(Default)]
struct SourceState {
	hydrated: bool,
	value: Option<u64>,
}

#[derive(Clone, Default)]
pub struct SourceWatermarks {
	inner: Arc<DashMap<FlowNodeId, SourceState>>,
}

impl SourceWatermarks {
	pub fn advance(&self, source: FlowNodeId, txn: &mut FlowTransaction, at: DateTime) -> Result<()> {
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

	pub fn source_watermark(&self, source: FlowNodeId, txn: &mut FlowTransaction) -> Result<DateTime> {
		Ok(DateTime::from_millis(self.raw(source, txn)?))
	}

	pub fn flow_watermark(
		&self,
		domain: TimeDomain,
		sources: &[FlowNodeId],
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

	fn raw(&self, source: FlowNodeId, txn: &mut FlowTransaction) -> Result<u64> {
		let mut state = self.inner.entry(source).or_default();
		Self::hydrate_once(&mut state, source, txn)?;
		Ok(state.value.unwrap_or(0))
	}

	fn hydrate_once(state: &mut SourceState, source: FlowNodeId, txn: &mut FlowTransaction) -> Result<()> {
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

	const SOURCE_A: FlowNodeId = FlowNodeId(1);
	const SOURCE_B: FlowNodeId = FlowNodeId(2);

	fn deferred(engine: &TestEngine, clock: MockClock) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		FlowTransaction::deferred(&parent, version, Catalog::testing(), Interceptors::new(), Clock::Mock(clock))
	}

	// Persist a deferred transaction's pending writes so a cold instance resolves them the way a
	// restarted process would.
	fn commit_pending(engine: &TestEngine, txn: &mut FlowTransaction) {
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

	// Intent: the per-source watermark is a running MAX over #time. Late rows arrive with older
	// stamps as a matter of course; if one dragged the watermark backwards, cutoffs derived from
	// it would move backwards and re-open horizons that already sealed, breaking monotonic
	// retention decisions (locked decision C3).
	// Mutation: track last-seen instead of max and the 3s advance overwrites the 5s value.
	#[test]
	fn the_source_watermark_never_moves_backwards() {
		let engine = TestEngine::new();
		let mut txn = deferred(&engine, MockClock::from_millis(0));
		let watermarks = SourceWatermarks::default();

		watermarks.advance(SOURCE_A, &mut txn, at(5_000)).unwrap();
		watermarks.advance(SOURCE_A, &mut txn, at(3_000)).unwrap();

		assert_eq!(watermarks.source_watermark(SOURCE_A, &mut txn).unwrap(), at(5_000));
	}

	// Intent: the flow watermark is the MIN across sources so a fast source can never seal a
	// slow source's state (locked decision C3). This is the invariant that protects a lagging
	// feed's windows from a sibling that is racing ahead.
	// Mutation: merge with max and the fast source drives the flow watermark to 20s while the
	// slow source still sits at 2s.
	#[test]
	fn the_flow_watermark_tracks_the_slowest_source() {
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

	// Intent: a restart resumes from the persisted watermark, and persistence is bucketed at 1s
	// (PERSIST_BUCKET_MS) to bound write amplification: an advance inside the same second stays
	// in RAM. Hydrating up to one bucket stale is conservative - retention seals LATER than the
	// live value, never earlier. Both halves of that contract are pinned here.
	// Mutation: hydrate to zero and the first cold read returns 0; persist every advance and the
	// first cold read returns 5.9s instead of 5.4s.
	#[test]
	fn a_restart_resumes_from_the_bucketed_persisted_watermark() {
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

	// Intent: a source that has never produced a row hydrates to ZERO, never to the clock.
	// Hydrating to now would compute cutoffs over the whole backlog on restart and seal state
	// before the first row is processed - the exact failure the plan forbids.
	// Mutation: default the missing value to txn.clock().now() and both reads return 500s.
	#[test]
	fn an_empty_source_hydrates_to_zero_not_to_now() {
		let engine = TestEngine::new();
		let mut txn = deferred(&engine, MockClock::from_millis(500_000));
		let watermarks = SourceWatermarks::default();

		assert_eq!(watermarks.source_watermark(SOURCE_A, &mut txn).unwrap(), at(0));
		assert_eq!(watermarks.flow_watermark(TimeDomain::Event, &[SOURCE_A], &mut txn).unwrap(), at(0));
	}

	// Intent: both halves of the silence contract in one suite so neither can be satisfied by
	// breaking the other (locked decisions C2/C4). An event flow's watermark is data-driven and
	// HOLDS while no data arrives; a processing flow's "now" is the wall clock, so an idle
	// processing flow keeps draining.
	// Mutation: derive the event value from the clock and the hold assertions fail; freeze the
	// processing value at the last coordinate and the drain assertions fail.
	#[test]
	fn event_silence_holds_while_processing_silence_advances() {
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

	// Intent: the watermark key lives beside NODE_WATERMARK with the identical inner-key
	// encoding (extend_u8 INVERTS the tag byte on the wire; decode_inner un-inverts it, which is
	// the convention this key deliberately picks). A drifted encoding would make hydration read
	// an absent key and silently restart every watermark at zero; a tag collision with
	// NODE_WATERMARK would let the two overwrite each other on node-scope state.
	#[test]
	fn the_watermark_key_round_trips_beside_the_node_watermark() {
		let key = source_watermark_key();
		let (group, keyspace, suffix) =
			OperatorStateKey::decode_inner(key.as_slice()).expect("the key must decode as inner state");

		assert_eq!(group, GroupId::NODE_SCOPE);
		assert_eq!(keyspace, Keyspace::SOURCE_WATERMARK);
		assert!(suffix.is_empty());
		assert_ne!(Keyspace::SOURCE_WATERMARK, Keyspace::NODE_WATERMARK);
	}
}
