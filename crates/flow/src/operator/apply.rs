// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	key::operator::keyspace::expiry::CustomManagedDue,
	metrics::heap::OperatorSample,
	operator_with::{ApplyWith, WithSpan},
	state::timer::TimerKind,
	value::column::columns::Columns,
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};

use crate::{
	operator::{
		BoxedHostOperator, HostOperator,
		host::HostContext,
		max_input_time, stamp_output_time,
		state::{
			expiry::{expiry_drop, expiry_due, managed_due_group},
			reaper::{StoreReaper, drain, drain_group, enqueue},
			seal::rule::{SEAL_GATE_STEP, SealRule},
		},
	},
	timer::Timer,
};

pub struct ApplyOperator {
	parent_schema: Option<Columns>,
	operator: OperatorId,
	inner: BoxedHostOperator,
	seal_span: Option<Duration>,
}

impl ApplyOperator {
	pub fn new(
		parent_schema: Option<Columns>,
		operator: OperatorId,
		inner: BoxedHostOperator,
		with: &ApplyWith,
	) -> Self {
		Self {
			parent_schema,
			operator,
			inner,
			seal_span: engine_seal_span(with),
		}
	}

	pub(crate) fn output_schema(&self) -> Option<Columns> {
		self.parent_schema.clone()
	}
}

impl HostOperator for ApplyOperator {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		self.inner.capabilities()
	}

	fn seal_span(&self) -> Option<Duration> {
		self.seal_span
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let inherited = max_input_time(&change);
		let mut out = self.inner.apply(host, change)?;
		stamp_output_time(&mut out, inherited);
		Ok(out)
	}

	fn on_timer(&mut self, host: &mut dyn HostContext, timer: Timer) -> Result<Option<Change>> {
		if timer.kind == TimerKind::Reclaim {
			reclaim(host, timer.due)?;
			return Ok(None);
		}
		let due = timer.due;
		let mut out = self.inner.on_timer(host, timer)?;
		if let Some(change) = out.as_mut() {
			stamp_output_time(change, Some(due));
		}
		Ok(out)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.inner.sample()
	}

	fn output_schema(&self) -> Option<Columns> {
		self.output_schema()
	}
}

pub fn engine_seal_span(with: &ApplyWith) -> Option<Duration> {
	let span = match &with.window {
		Some(kind) => {
			let lateness = match with.lateness {
				Some(WithSpan::Duration(d)) => d,
				_ => Duration::zero(),
			};
			SealRule::for_window(kind, lateness).map(|rule| rule.admissible().duration())
		}
		None => match with.lateness {
			Some(WithSpan::Duration(d)) => Some(d),
			_ => None,
		},
	};
	span.filter(|span| !span.is_zero())
}

const RECLAIM_BATCH: usize = 256;

fn reclaim(host: &mut dyn HostContext, fired: DateTime) -> Result<()> {
	let mut retry = !drain(host, &mut StoreReaper, RECLAIM_BATCH)?.queue_is_empty();
	let due = expiry_due::<CustomManagedDue, Vec<u8>>(host, fired.to_order(), None, RECLAIM_BATCH)?;
	retry |= due.len() == RECLAIM_BATCH;
	for (key, _) in due {
		let group = managed_due_group(&key)?;
		enqueue(host, group)?;
		retry |= drain_group(host, group, &mut StoreReaper, RECLAIM_BATCH)?.still_queued;
		expiry_drop(host, &key)?;
	}
	if retry {
		host.arm_timer(fired.saturating_add(SEAL_GATE_STEP), TimerKind::Reclaim, &EncodedKey::new(Vec::new()))?;
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_core::common::{WindowKind, WindowSize};

	use super::*;

	struct Stub;

	impl HostOperator for Stub {
		fn id(&self) -> OperatorId {
			OperatorId(1)
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
			Ok(change)
		}
	}

	fn seconds(seconds: i64) -> Duration {
		Duration::from_milliseconds_const(seconds * 1_000)
	}

	fn operator(with: ApplyWith) -> ApplyOperator {
		ApplyOperator::new(None, OperatorId(1), Box::new(Stub), &with)
	}

	#[test]
	fn a_windowed_apply_holds_by_size_plus_lateness() {
		// The engine span for a windowed apply must come from SealRule::for_window, not the guest.
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(seconds(60)),
			}),
			lateness: Some(WithSpan::Duration(seconds(30))),
			immutable: None,
		};

		assert_eq!(operator(with).seal_span(), Some(seconds(90)));
	}

	#[test]
	fn an_apply_without_a_window_holds_by_its_lateness() {
		// An apply with no window still seals by its own lateness, otherwise a bare apply never holds.
		let with = ApplyWith {
			window: None,
			lateness: Some(WithSpan::Duration(seconds(30))),
			immutable: None,
		};

		assert_eq!(operator(with).seal_span(), Some(seconds(30)));
	}

	#[test]
	fn an_apply_without_a_window_or_lateness_holds_nothing() {
		// Nothing in `with` gives the engine a span, so the watermark must pass through untouched.
		let with = ApplyWith::default();

		assert_eq!(operator(with).seal_span(), None);
	}

	#[test]
	fn a_slot_window_holds_nothing_in_the_engine() {
		// A count-sized window seals in a row coordinate the engine has no duration for.
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Count(10),
			}),
			lateness: Some(WithSpan::Count(2)),
			immutable: None,
		};

		assert_eq!(operator(with).seal_span(), None);
	}

	#[test]
	fn a_rolling_apply_holds_by_the_rolling_rule() {
		// Rolling must go through the rolling arm of for_window, which adds lag to size before lateness.
		let with = ApplyWith {
			window: Some(WindowKind::Rolling {
				size: WindowSize::Duration(seconds(120)),
				lag: Some(seconds(10)),
				pane: None,
			}),
			lateness: Some(WithSpan::Duration(seconds(5))),
			immutable: None,
		};

		assert_eq!(operator(with).seal_span(), Some(seconds(135)));
	}

	#[test]
	fn a_zero_span_reports_none() {
		// A zero span must read as no hold, matching what the guest mount reports today.
		let with = ApplyWith {
			window: None,
			lateness: Some(WithSpan::Duration(Duration::zero())),
			immutable: None,
		};

		assert_eq!(operator(with).seal_span(), None);
	}
}

#[cfg(test)]
mod reclaim_tests {
	use std::sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	};

	use reifydb_codec::row::pod::EncodedPodRow;
	use reifydb_core::{
		common::CommitVersion,
		key::{
			any::TaggedKey,
			operator::{
				keyspace::timer::TimerWheelKey,
				state::{
					GroupId, GroupStateKey, KeyspaceId, keyspace_inner_range, managed_key_in,
					unmanaged_key_in,
				},
			},
		},
		state::{timer::StateStore, typed::SuffixBytes},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{factory::time::secs, util::hash::Hash128};

	use super::*;
	use crate::{
		operator::host::TxnHostContext,
		transaction::{
			ChangeCoordinate, FlowTransaction,
			deferred::DeferredTransaction,
			mock::FlowTxn,
			state::{StateExtension, StateRange},
		},
	};

	const OP: OperatorId = OperatorId(1);

	struct Probe {
		timers: Arc<AtomicUsize>,
	}

	impl HostOperator for Probe {
		fn id(&self) -> OperatorId {
			OP
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change> {
			Ok(change)
		}

		fn on_timer(&mut self, _host: &mut dyn HostContext, _timer: Timer) -> Result<Option<Change>> {
			self.timers.fetch_add(1, Ordering::SeqCst);
			Ok(None)
		}
	}

	fn managed(lateness_secs: u64) -> (ApplyOperator, Arc<AtomicUsize>) {
		let timers = Arc::new(AtomicUsize::new(0));
		let with = ApplyWith {
			lateness: Some(WithSpan::Duration(secs(lateness_secs))),
			..ApplyWith::default()
		};
		let probe = Probe {
			timers: Arc::clone(&timers),
		};
		(ApplyOperator::new(None, OP, Box::new(probe), &with), timers)
	}

	fn txn(engine: &TestEngine) -> DeferredTransaction {
		engine.flow_txn().at(CommitVersion(7)).deferred()
	}

	fn group(n: u128) -> GroupId {
		GroupId::hashed(Hash128(n))
	}

	fn managed_key(group: GroupId) -> GroupStateKey {
		managed_key_in(group, b"k").expect("a fixture id fits the keyspace").into()
	}

	fn write_at(
		txn: &mut DeferredTransaction,
		millis: u64,
		seal_span: Option<Duration>,
		key: &GroupStateKey,
	) -> Result<()> {
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_millis(millis)),
		});
		TxnHostContext::with_seal_span(txn, OP, seal_span).state_set(key, EncodedPodRow::new(&[1]))
	}

	fn keys(txn: &mut DeferredTransaction, group: GroupId, keyspace: KeyspaceId) -> usize {
		txn.state_range(OP, StateRange::forward(keyspace_inner_range(group, keyspace), "test"))
			.unwrap()
			.items
			.len()
	}

	fn due_entries(txn: &mut DeferredTransaction) -> usize {
		keys(txn, GroupId::ROOT, KeyspaceId::CUSTOM_MANAGED_DUE)
	}

	fn reclaim_timers(txn: &mut DeferredTransaction) -> Vec<u64> {
		let mut dues: Vec<u64> = txn
			.state_range(
				OP,
				StateRange::forward(
					keyspace_inner_range(GroupId::ROOT, KeyspaceId::TIMER_WHEEL),
					"test",
				),
			)
			.unwrap()
			.items
			.iter()
			.filter_map(|item| {
				let TaggedKey::OperatorState(decoded) = &item.key else {
					panic!("a wheel row must decode");
				};
				let suffix = TimerWheelKey::from_suffix_bytes(&decoded.suffix)
					.expect("a wheel row must decode");
				(suffix.kind.0 == TimerKind::Reclaim).then_some(suffix.due.0.to_millis())
			})
			.collect();
		dues.sort();
		dues
	}

	fn fire(
		operator: &mut ApplyOperator,
		txn: &mut DeferredTransaction,
		kind: TimerKind,
		millis: u64,
	) -> Option<Change> {
		let mut host = TxnHostContext::with_seal_span(txn, OP, operator.seal_span());
		operator.on_timer(
			&mut host,
			Timer {
				due: DateTime::from_millis(millis),
				kind,
				key: EncodedKey::new(Vec::new()),
			},
		)
		.unwrap()
	}

	#[test]
	fn a_managed_write_arms_at_the_next_whole_second_after_lateness_never_earlier() {
		// A due rounded down frees a group while a row inside its lateness can still arrive.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let span = Some(secs(2));

		write_at(&mut txn, 9_999, span, &managed_key(group(1))).unwrap();
		write_at(&mut txn, 10_000, span, &managed_key(group(2))).unwrap();

		assert_eq!(reclaim_timers(&mut txn), vec![12_000, 13_000], "9.999s + 2s + 1ms is exactly 12s");
		assert_eq!(due_entries(&mut txn), 2);
	}

	#[test]
	fn writes_in_one_bucket_share_one_timer_and_one_due_entry_per_group() {
		// A row per write grows the wheel and the due index with the write rate, not the group count.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let span = Some(secs(2));

		write_at(&mut txn, 10_100, span, &managed_key(group(1))).unwrap();
		write_at(&mut txn, 10_900, span, &managed_key(group(1))).unwrap();
		write_at(&mut txn, 10_500, span, &managed_key(group(2))).unwrap();

		assert_eq!(reclaim_timers(&mut txn), vec![13_000]);
		assert_eq!(due_entries(&mut txn), 2, "one due entry per group per bucket");
	}

	#[test]
	fn root_and_unmanaged_writes_arm_nothing() {
		// ROOT holds state no group owns, so arming it would free that state on the first fire.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let span = Some(secs(2));
		let unmanaged: GroupStateKey =
			unmanaged_key_in(group(1), b"k").expect("a fixture id fits the keyspace").into();

		write_at(&mut txn, 10_000, span, &managed_key(GroupId::ROOT)).unwrap();
		write_at(&mut txn, 10_000, span, &unmanaged).unwrap();

		assert!(reclaim_timers(&mut txn).is_empty());
		assert_eq!(due_entries(&mut txn), 0);
	}

	#[test]
	fn a_managed_write_without_a_seal_span_fails() {
		// Without a span nothing would ever free the group, which is the leak this stage closes.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);

		let err = write_at(&mut txn, 10_000, None, &managed_key(group(1)))
			.expect_err("a managed write needs a seal span");

		assert!(err.to_string().contains("no seal span"), "got: {err}");
	}

	#[test]
	fn a_reclaim_fire_frees_the_groups_due_by_it_and_keeps_the_rest() {
		// Freeing a later group drops state its lateness still protects.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let (mut operator, guest_timers) = managed(2);
		let span = operator.seal_span();

		write_at(&mut txn, 10_000, span, &managed_key(group(1))).unwrap();
		write_at(&mut txn, 11_500, span, &managed_key(group(2))).unwrap();
		write_at(&mut txn, 10_000, span, &managed_key(GroupId::ROOT)).unwrap();

		let emitted = fire(&mut operator, &mut txn, TimerKind::Reclaim, 13_000);

		assert!(emitted.is_none(), "a reclaim fire emits nothing");
		assert_eq!(keys(&mut txn, group(1), KeyspaceId::CUSTOM_MANAGED), 0, "group 1 was due at 13s");
		assert_eq!(keys(&mut txn, group(2), KeyspaceId::CUSTOM_MANAGED), 1, "group 2 is due at 14s");
		assert_eq!(keys(&mut txn, GroupId::ROOT, KeyspaceId::CUSTOM_MANAGED), 1, "ROOT is never freed");
		assert_eq!(due_entries(&mut txn), 1, "only group 2's entry is left");
		assert_eq!(guest_timers.load(Ordering::SeqCst), 0, "the guest never sees a reclaim fire");
	}

	#[test]
	fn a_capped_reclaim_fire_arms_a_retry_that_frees_the_rest() {
		// A cap with no retry holds every group past the batch until some unrelated write arms again.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let (mut operator, _) = managed(2);
		let span = operator.seal_span();
		for n in 1..=257 {
			write_at(&mut txn, 10_000, span, &managed_key(group(n))).unwrap();
		}

		fire(&mut operator, &mut txn, TimerKind::Reclaim, 13_000);

		assert_eq!(due_entries(&mut txn), 1, "one fire frees at most 256 groups");
		assert_eq!(reclaim_timers(&mut txn), vec![13_000, 13_001], "the capped fire arms its retry 1ms later");

		fire(&mut operator, &mut txn, TimerKind::Reclaim, 13_001);

		assert_eq!(due_entries(&mut txn), 0);
		assert_eq!((1..=257).map(|n| keys(&mut txn, group(n), KeyspaceId::CUSTOM_MANAGED)).sum::<usize>(), 0);
		assert_eq!(reclaim_timers(&mut txn), vec![13_000, 13_001], "a fire that finishes arms no retry");
	}

	#[test]
	fn other_timer_kinds_still_reach_the_guest() {
		// An intercept wider than the one kind would silently stop every guest timer.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let (mut operator, guest_timers) = managed(2);

		fire(&mut operator, &mut txn, TimerKind::Seal, 13_000);
		fire(&mut operator, &mut txn, TimerKind::Maintenance, 13_000);

		assert_eq!(guest_timers.load(Ordering::SeqCst), 2);
	}
}
