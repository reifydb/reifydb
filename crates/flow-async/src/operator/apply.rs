// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, sync::Arc};

use arrow_schema::{FieldRef, Schema, SchemaRef};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	key::operator::keyspace::due::CustomManagedDue,
	metrics::heap::OperatorSample,
	operator_with::{ApplyWith, WithSpan},
	state::timer::TimerKind,
	value::{batch::filter, column::builder::ColumnBuilder},
};
use reifydb_value::{
	error::ValueError,
	value::{
		constraint::TypeConstraint,
		datetime::DateTime,
		duration::Duration,
		value_type::{
			ValueType,
			field::{from_field, to_field},
		},
	},
};

use crate::{
	operator::{
		BoxedHostOperator, HostOperator, TimedOutput,
		drops::{DropCounter, seal_drops},
		host::HostContext,
		join::kept::dictionary_columns,
		stamp_output_created,
		state::{
			due::{due_drop, due_earliest, due_entries, managed_due_group, managed_latest_key},
			forget_pass::{StoreForgetter, drain, drain_group, enqueue},
			seal::{
				check::{SEALED_INSERTS, SEALED_REMOVES, SEALED_UPDATES, first_admitted, kept_from},
				ledger::{FiredAt, SealLedger},
				rule::{SEAL_GATE_STEP, SealRule},
			},
		},
	},
	timer::Timer,
};

pub struct ApplyOperator {
	schema: Option<SchemaRef>,
	operator: OperatorId,
	inner: BoxedHostOperator,
	seal_span: Option<Duration>,
	forget: Option<Duration>,
	input_seal: Option<Duration>,
	sealed_inserts: DropCounter,
	sealed_updates: DropCounter,
	sealed_removes: DropCounter,
}

impl ApplyOperator {
	pub fn new(
		schema: Option<SchemaRef>,
		operator: OperatorId,
		inner: BoxedHostOperator,
		with: &ApplyWith,
	) -> Result<Self, ValueError> {
		Ok(Self {
			schema,
			operator,
			inner,
			seal_span: engine_seal_span(with)?,
			forget: engine_forget(with),
			input_seal: input_seal(with)?,
			sealed_inserts: DropCounter::new(operator, SEALED_INSERTS),
			sealed_updates: DropCounter::new(operator, SEALED_UPDATES),
			sealed_removes: DropCounter::new(operator, SEALED_REMOVES),
		})
	}

	fn drop_sealed(&self, host: &mut dyn HostContext, mut change: Change) -> Result<Option<Change>, ValueError> {
		let Some(seal) = self.input_seal else {
			return Ok(Some(change));
		};
		if change.diffs.is_empty() {
			return Ok(Some(change));
		}
		let first_admitted = first_admitted(host, seal)?;
		let mut kept = Vec::with_capacity(change.diffs.len());
		for diff in change.diffs.drain(..) {
			if let Some(diff) = self.drop_sealed_diff(first_admitted, diff)? {
				kept.push(diff);
			}
		}
		if kept.is_empty() {
			return Ok(None);
		}
		change.diffs = kept.into();
		Ok(Some(change))
	}

	fn drop_sealed_diff(&self, first_admitted: DateTime, diff: Diff) -> Result<Option<Diff>, ValueError> {
		let (timed, drops) = match &diff {
			Diff::Insert {
				post,
				..
			} => (post, &self.sealed_inserts),
			Diff::Update {
				post,
				..
			} => (post, &self.sealed_updates),
			Diff::Remove {
				pre,
				..
			} => (pre, &self.sealed_removes),
		};
		let kept = kept_from(first_admitted, timed)?;
		if kept.dropped == 0 {
			return Ok(Some(diff));
		}
		drops.note(kept.dropped);
		if kept.dropped as usize == timed.num_rows() {
			return Ok(None);
		}
		let mask = kept.mask.values();
		Ok(Some(match diff {
			Diff::Insert {
				post,
				origin,
			} => Diff::Insert {
				post: filter(&post, mask)?,
				origin,
			},
			Diff::Update {
				pre,
				post,
				origin,
			} => Diff::Update {
				pre: filter(&pre, mask)?,
				post: filter(&post, mask)?,
				origin,
			},
			Diff::Remove {
				pre,
				origin,
			} => Diff::Remove {
				pre: filter(&pre, mask)?,
				origin,
			},
		}))
	}

	pub(crate) fn output_schema(&self) -> Option<SchemaRef> {
		self.schema.clone()
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

	fn forget(&self) -> Option<Duration> {
		self.forget
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change, ValueError> {
		let changed_at = change.changed_at;
		let version = change.version;
		let Some(change) = self.drop_sealed(host, change)? else {
			return Ok(Change::from_flow(self.operator, version, Vec::new(), changed_at));
		};
		let mut out = self.inner.apply(host, change)?;
		stamp_output_created(&mut out.diffs, changed_at)?;
		Ok(out)
	}

	fn on_timer(&mut self, host: &mut dyn HostContext, timer: Timer) -> Result<Option<TimedOutput>, ValueError> {
		if timer.kind == TimerKind::ForgetPass {
			SealLedger::advance(host, FiredAt::of(&timer))?;
			fire_forget_pass(host, timer.due)?;
			return Ok(None);
		}
		let due = timer.due;
		let mut out = self.inner.on_timer(host, timer)?;
		if let Some(output) = out.as_mut() {
			stamp_output_created(&mut output.diffs, due)?;
		}
		Ok(out)
	}

	fn sample(&self) -> Option<OperatorSample> {
		Some(self.inner.sample().unwrap_or_default().with_drops(seal_drops(
			self.input_seal.is_some(),
			&self.sealed_inserts,
			&self.sealed_updates,
			&self.sealed_removes,
		)))
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		self.output_schema()
	}
}

pub fn declared_schema<'a>(columns: impl IntoIterator<Item = (&'a str, &'a TypeConstraint)>) -> Option<SchemaRef> {
	let fields: Vec<FieldRef> = columns
		.into_iter()
		.map(|(name, constraint)| ColumnBuilder::with_capacity(constraint.get_type(), 0).finish(name).0)
		.collect();
	(!fields.is_empty()).then(|| Arc::new(Schema::new(fields)))
}

pub fn output_schema_of(
	parent: Option<SchemaRef>,
	declared: Option<SchemaRef>,
) -> Result<Option<SchemaRef>, ValueError> {
	let Some(declared) = declared else {
		return Ok(parent);
	};
	let dictionaries: BTreeSet<_> = match &parent {
		Some(parent) => dictionary_columns(parent)?.into_values().collect(),
		None => BTreeSet::new(),
	};
	let mut dictionaries = dictionaries.into_iter();
	let (Some(dictionary), None) = (dictionaries.next(), dictionaries.next()) else {
		return Ok(Some(declared));
	};
	let fields = declared
		.fields()
		.iter()
		.map(|field| {
			let mut field_type = from_field(field)?;
			let untagged_id = field_type.dictionary_id.is_none()
				&& field_type
					.value_type
					.as_ref()
					.is_some_and(|t| *t.inner_type() == ValueType::DictionaryId);
			if !untagged_id {
				return Ok(field.clone());
			}
			field_type.dictionary_id = Some(dictionary);
			Ok(Arc::new(to_field(field.name(), &field_type)))
		})
		.collect::<Result<Vec<FieldRef>, ValueError>>()?;
	Ok(Some(Arc::new(Schema::new(fields))))
}

pub fn engine_seal_span(with: &ApplyWith) -> Result<Option<Duration>, ValueError> {
	let span = match &with.window {
		Some(kind) => {
			let drop_span = match with.drop {
				Some(WithSpan::Duration(d)) => d,
				_ => Duration::zero(),
			};
			SealRule::for_window(kind, drop_span).map(|rule| rule.seal_span().duration())
		}
		None => with.effective_drop()?,
	};
	Ok(span.filter(|span| !span.is_zero()))
}

fn input_seal(with: &ApplyWith) -> Result<Option<Duration>, ValueError> {
	if with.window.is_some() {
		return Ok(None);
	}
	with.effective_drop()
}

pub fn engine_forget(with: &ApplyWith) -> Option<Duration> {
	with.forget
}

const FORGET_PASS_BATCH: usize = 256;

fn fire_forget_pass(host: &mut dyn HostContext, fired: DateTime) -> Result<(), ValueError> {
	let mut retry = !drain(host, &mut StoreForgetter, FORGET_PASS_BATCH)?.queue_is_empty();
	let due = due_entries::<CustomManagedDue, Vec<u8>>(host, fired.to_order(), None, FORGET_PASS_BATCH)?;
	retry |= due.len() == FORGET_PASS_BATCH;
	for (key, _) in due {
		let group = managed_due_group(&key)?;
		enqueue(host, &[group])?;
		retry |= drain_group(host, group, &mut StoreForgetter, FORGET_PASS_BATCH)?.still_queued;
		due_drop(host, &key)?;
		due_drop(host, &managed_latest_key(group))?;
	}
	let retry_at = fired.saturating_add(SEAL_GATE_STEP);
	let next = match retry {
		true => Some(retry_at),
		false => due_earliest::<CustomManagedDue>(host)?.map(|order| DateTime::from_order(order).max(retry_at)),
	};
	if let Some(at) = next {
		host.arm_timer(at, TimerKind::ForgetPass, &EncodedKey::new(Vec::new()))?;
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_core::common::{OnEmpty, WindowKind, WindowSize};

	use super::*;

	struct Stub;

	impl HostOperator for Stub {
		fn id(&self) -> OperatorId {
			OperatorId(1)
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change, ValueError> {
			Ok(change)
		}
	}

	fn seconds(seconds: i64) -> Duration {
		Duration::from_milliseconds_const(seconds * 1_000)
	}

	fn operator(with: ApplyWith) -> ApplyOperator {
		ApplyOperator::new(None, OperatorId(1), Box::new(Stub), &with).unwrap()
	}

	#[test]
	fn a_windowed_apply_holds_by_size_plus_drop() {
		// The engine span for a windowed apply must come from SealRule::for_window, not the guest.
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(seconds(60)),
			}),
			drop: Some(WithSpan::Duration(seconds(30))),
			immutable: None,
			forget: None,
		};

		assert_eq!(operator(with).seal_span(), Some(seconds(90)));
	}

	#[test]
	fn an_apply_without_a_window_holds_by_its_drop() {
		// An apply with no window still seals by its own drop, otherwise a bare apply never holds.
		let with = ApplyWith {
			window: None,
			drop: Some(WithSpan::Duration(seconds(30))),
			immutable: None,
			forget: None,
		};

		assert_eq!(operator(with).seal_span(), Some(seconds(30)));
	}

	#[test]
	fn an_apply_without_a_window_or_drop_holds_nothing() {
		// Nothing in `with` gives the engine a span, so the source watermark must pass through untouched.
		let with = ApplyWith::default();

		assert_eq!(operator(with).seal_span(), None);
	}

	#[test]
	fn a_slot_window_holds_nothing_in_the_engine() {
		// A count-sized window seals in a row coordinate the engine has no duration for.
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Slots(10),
			}),
			drop: Some(WithSpan::Slots(2)),
			immutable: None,
			forget: None,
		};

		assert_eq!(operator(with).seal_span(), None);
	}

	#[test]
	fn a_rolling_apply_holds_by_the_rolling_rule() {
		// Rolling must go through the rolling arm of for_window, which adds lag to size before drop.
		let with = ApplyWith {
			window: Some(WindowKind::Rolling {
				size: WindowSize::Duration(seconds(120)),
				lag: Some(seconds(10)),
				pane: None,
				on_empty: OnEmpty::Remove,
			}),
			drop: Some(WithSpan::Duration(seconds(5))),
			immutable: None,
			forget: None,
		};

		assert_eq!(operator(with).seal_span(), Some(seconds(135)));
	}

	#[test]
	fn a_zero_span_reports_none() {
		// A zero span must read as no hold, matching what the guest mount reports today.
		let with = ApplyWith {
			window: None,
			drop: Some(WithSpan::Duration(Duration::zero())),
			immutable: None,
			forget: None,
		};

		assert_eq!(operator(with).seal_span(), None);
	}

	#[test]
	fn forget_feeds_the_forget_pass_and_never_the_hold() {
		// A forget that also held the output would delay every downstream seal by the state bound.
		let with = ApplyWith {
			window: None,
			drop: Some(WithSpan::Duration(seconds(30))),
			immutable: None,
			forget: Some(seconds(120)),
		};
		let operator = operator(with);

		assert_eq!(operator.seal_span(), Some(seconds(30)));
		assert_eq!(operator.forget(), Some(seconds(120)));
	}

	#[test]
	fn the_drop_falls_back_to_the_forget_and_never_the_forget_to_the_drop() {
		// A drop that armed the forget pass would free state the user never asked to forget.
		let forget_alone = operator(ApplyWith {
			forget: Some(seconds(30)),
			..ApplyWith::default()
		});
		let drop_alone = operator(ApplyWith {
			drop: Some(WithSpan::Duration(seconds(30))),
			..ApplyWith::default()
		});

		assert_eq!(forget_alone.input_seal, Some(seconds(30)));
		assert_eq!(forget_alone.seal_span(), Some(seconds(30)));
		assert_eq!(forget_alone.forget(), Some(seconds(30)));
		assert_eq!(drop_alone.input_seal, Some(seconds(30)));
		assert_eq!(drop_alone.seal_span(), Some(seconds(30)));
		assert_eq!(drop_alone.forget(), None);
	}

	#[test]
	fn a_zero_forget_alone_gives_a_zero_drop_and_no_hold() {
		// Zero must stay a real bound for the forget pass and the input cut while the hold reads it as none.
		let with = ApplyWith {
			window: None,
			drop: None,
			immutable: None,
			forget: Some(Duration::zero()),
		};
		let operator = operator(with);

		assert_eq!(operator.seal_span(), None);
		assert_eq!(operator.input_seal, Some(Duration::zero()));
		assert_eq!(operator.forget(), Some(Duration::zero()));
	}
}

#[cfg(test)]
mod forget_pass_tests {
	use std::sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	};

	use reifydb_codec::row::pod::EncodedPodRow;
	use reifydb_core::{
		common::{CommitVersion, WindowKind, WindowSize},
		key::{
			any::TaggedKey,
			operator::{
				keyspace::timer::TimerWheelKey,
				state::{GroupId, GroupStateKey, KeyspaceId, managed_key_in, unmanaged_key_in},
			},
		},
		state::{timer::StateStore, typed::SuffixBytes},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{factory::time::secs, util::hash::Hash128};

	use super::*;
	use crate::{
		operator::{host::TxnHostContext, state::test_utils::test::whole_keyspace},
		transaction::{
			FlowTransaction,
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

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change, ValueError> {
			Ok(change)
		}

		fn on_timer(
			&mut self,
			_host: &mut dyn HostContext,
			_timer: Timer,
		) -> Result<Option<TimedOutput>, ValueError> {
			self.timers.fetch_add(1, Ordering::SeqCst);
			Ok(None)
		}
	}

	fn managed(forget_secs: u64) -> (ApplyOperator, Arc<AtomicUsize>) {
		let timers = Arc::new(AtomicUsize::new(0));
		let with = ApplyWith {
			forget: Some(secs(forget_secs)),
			..ApplyWith::default()
		};
		let probe = Probe {
			timers: Arc::clone(&timers),
		};
		(ApplyOperator::new(None, OP, Box::new(probe), &with).unwrap(), timers)
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
		millis: i64,
		forget: Option<Duration>,
		key: &GroupStateKey,
	) -> Result<(), ValueError> {
		txn.set_batch_time(Some(DateTime::from_millis(millis)));
		TxnHostContext::with_forget(txn, OP, forget).state_set(vec![(key.clone(), EncodedPodRow::new(&[1]))])
	}

	fn keys(txn: &mut DeferredTransaction, group: GroupId, keyspace: KeyspaceId) -> usize {
		txn.state_range(OP, StateRange::forward(whole_keyspace(group, keyspace), "test")).unwrap().items.len()
	}

	fn due_entries(txn: &mut DeferredTransaction) -> usize {
		keys(txn, GroupId::ROOT, KeyspaceId::CUSTOM_MANAGED_DUE)
	}

	fn latest_entries(txn: &mut DeferredTransaction) -> usize {
		keys(txn, GroupId::ROOT, KeyspaceId::CUSTOM_MANAGED_LATEST)
	}

	fn forget_pass_timers(txn: &mut DeferredTransaction) -> Vec<i64> {
		let mut dues: Vec<i64> = txn
			.state_range(
				OP,
				StateRange::forward(whole_keyspace(GroupId::ROOT, KeyspaceId::TIMER_WHEEL), "test"),
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
				(suffix.kind.0 == TimerKind::ForgetPass).then_some(suffix.due.0.to_millis())
			})
			.collect();
		dues.sort();
		dues
	}

	fn fire(
		operator: &mut ApplyOperator,
		txn: &mut DeferredTransaction,
		kind: TimerKind,
		millis: i64,
	) -> Option<TimedOutput> {
		let mut host = TxnHostContext::with_forget(txn, OP, operator.forget());
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
	fn a_managed_write_arms_at_the_next_whole_second_after_forget_never_earlier() {
		// A due rounded down frees a group while a row inside its forget can still arrive.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let (mut operator, _) = managed(2);
		let span = operator.forget();

		write_at(&mut txn, 9_999, span, &managed_key(group(1))).unwrap();
		write_at(&mut txn, 10_000, span, &managed_key(group(2))).unwrap();

		assert_eq!(
			forget_pass_timers(&mut txn),
			vec![12_000],
			"9.999s + 2s + 1ms is exactly 12s, the earliest due"
		);
		assert_eq!(due_entries(&mut txn), 2);

		fire(&mut operator, &mut txn, TimerKind::ForgetPass, 12_000);

		assert_eq!(forget_pass_timers(&mut txn), vec![13_000], "10s + 2s + 1ms rounds up to 13s, armed next");
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

		assert_eq!(forget_pass_timers(&mut txn), vec![13_000]);
		assert_eq!(due_entries(&mut txn), 2, "one due entry per group per bucket");
	}

	#[test]
	fn a_rewrite_inside_the_forget_moves_the_groups_due_entry_to_the_last_write() {
		// A due entry left from the first write frees state the rewrite still needs.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let (mut operator, _) = managed(2);
		let span = operator.forget();

		write_at(&mut txn, 10_000, span, &managed_key(group(1))).unwrap();
		write_at(&mut txn, 12_000, span, &managed_key(group(1))).unwrap();

		assert_eq!(due_entries(&mut txn), 1, "the rewrite replaces the first due entry instead of adding one");
		assert_eq!(latest_entries(&mut txn), 1);

		fire(&mut operator, &mut txn, TimerKind::ForgetPass, 13_000);

		assert_eq!(
			keys(&mut txn, group(1), KeyspaceId::CUSTOM_MANAGED),
			1,
			"the first write's due must not free the rewritten group"
		);

		fire(&mut operator, &mut txn, TimerKind::ForgetPass, 15_000);

		assert_eq!(
			keys(&mut txn, group(1), KeyspaceId::CUSTOM_MANAGED),
			0,
			"the rewrite's due frees the group"
		);
		assert_eq!(due_entries(&mut txn), 0);
		assert_eq!(latest_entries(&mut txn), 0, "a freed group leaves no latest entry behind");
	}

	#[test]
	fn a_late_write_never_moves_a_groups_forget_pass_earlier() {
		// A write stamped with an older row time must not free state a newer write still needs.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let (mut operator, _) = managed(2);
		let span = operator.forget();

		write_at(&mut txn, 12_000, span, &managed_key(group(1))).unwrap();
		write_at(&mut txn, 10_000, span, &managed_key(group(1))).unwrap();

		assert_eq!(due_entries(&mut txn), 1, "the late write keeps one due entry for the group");
		assert_eq!(latest_entries(&mut txn), 1);

		fire(&mut operator, &mut txn, TimerKind::ForgetPass, 13_000);

		assert_eq!(
			keys(&mut txn, group(1), KeyspaceId::CUSTOM_MANAGED),
			1,
			"the late write's due must not free the group the newer write keeps"
		);

		fire(&mut operator, &mut txn, TimerKind::ForgetPass, 15_000);

		assert_eq!(
			keys(&mut txn, group(1), KeyspaceId::CUSTOM_MANAGED),
			0,
			"the newer write's due frees the group"
		);
		assert_eq!(due_entries(&mut txn), 0);
		assert_eq!(latest_entries(&mut txn), 0);
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

		assert!(forget_pass_timers(&mut txn).is_empty());
		assert_eq!(due_entries(&mut txn), 0);
	}

	#[test]
	fn a_managed_write_without_a_forget_fails() {
		// Without a forget nothing would ever free the group, which is the leak this stage closes.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);

		let err = write_at(&mut txn, 10_000, None, &managed_key(group(1)))
			.expect_err("a managed write needs a forget");

		assert!(err.to_string().contains("no forget"), "got: {err}");
	}

	#[test]
	fn a_windowed_apply_with_only_a_drop_has_no_forget_and_its_managed_write_fails() {
		// A drop standing in for the forget would free managed state at the late-arrival line nobody chose for
		// it.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Duration(secs(60)),
			}),
			drop: Some(WithSpan::Duration(secs(10))),
			immutable: None,
			forget: None,
		};

		let forget = engine_forget(&with);
		assert_eq!(forget, None, "a drop alone must never become a forget line");

		let err = write_at(&mut txn, 10_000, forget, &managed_key(group(1)))
			.expect_err("a managed write under a drop alone needs a forget");
		assert!(err.to_string().contains("no forget"), "got: {err}");
	}

	#[test]
	fn a_forget_pass_fire_frees_the_groups_due_by_it_and_keeps_the_rest() {
		// Freeing a later group drops state its forget still protects.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let (mut operator, guest_timers) = managed(2);
		let span = operator.forget();

		write_at(&mut txn, 10_000, span, &managed_key(group(1))).unwrap();
		write_at(&mut txn, 11_500, span, &managed_key(group(2))).unwrap();
		write_at(&mut txn, 10_000, span, &managed_key(GroupId::ROOT)).unwrap();

		let emitted = fire(&mut operator, &mut txn, TimerKind::ForgetPass, 13_000);

		assert!(emitted.is_none(), "a forget pass fire emits nothing");
		assert_eq!(keys(&mut txn, group(1), KeyspaceId::CUSTOM_MANAGED), 0, "group 1 was due at 13s");
		assert_eq!(keys(&mut txn, group(2), KeyspaceId::CUSTOM_MANAGED), 1, "group 2 is due at 14s");
		assert_eq!(keys(&mut txn, GroupId::ROOT, KeyspaceId::CUSTOM_MANAGED), 1, "ROOT is never freed");
		assert_eq!(due_entries(&mut txn), 1, "only group 2's entry is left");
		assert_eq!(guest_timers.load(Ordering::SeqCst), 0, "the guest never sees a forget pass fire");
	}

	#[test]
	fn a_capped_forget_pass_fire_arms_a_retry_that_frees_the_rest() {
		// A cap with no retry holds every group past the batch until some unrelated write arms again.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let (mut operator, _) = managed(2);
		let span = operator.forget();
		for n in 1..=257 {
			write_at(&mut txn, 10_000, span, &managed_key(group(n))).unwrap();
		}

		fire(&mut operator, &mut txn, TimerKind::ForgetPass, 13_000);

		assert_eq!(due_entries(&mut txn), 1, "one fire frees at most 256 groups");
		assert_eq!(
			forget_pass_timers(&mut txn),
			vec![13_001],
			"the capped fire moves its one timer to the retry 1ms later"
		);

		fire(&mut operator, &mut txn, TimerKind::ForgetPass, 13_001);

		assert_eq!(due_entries(&mut txn), 0);
		assert_eq!((1..=257).map(|n| keys(&mut txn, group(n), KeyspaceId::CUSTOM_MANAGED)).sum::<usize>(), 0);
		assert_eq!(forget_pass_timers(&mut txn), vec![13_001], "a fire that finishes arms nothing new");
	}

	#[test]
	fn each_group_is_forgotten_at_its_own_due_through_one_timer() {
		// Arming only the latest due forgets the earlier group late; never re-arming after a fire forgets the
		// later group never.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let (mut operator, _) = managed(2);
		let span = operator.forget();

		write_at(&mut txn, 10_000, span, &managed_key(group(1))).unwrap();
		write_at(&mut txn, 12_500, span, &managed_key(group(2))).unwrap();

		assert_eq!(forget_pass_timers(&mut txn), vec![13_000], "one timer, at the earliest due");

		fire(&mut operator, &mut txn, TimerKind::ForgetPass, 13_000);

		assert_eq!(keys(&mut txn, group(1), KeyspaceId::CUSTOM_MANAGED), 0, "group 1 was due at 13s");
		assert_eq!(keys(&mut txn, group(2), KeyspaceId::CUSTOM_MANAGED), 1, "group 2 is due at 15s");
		assert_eq!(forget_pass_timers(&mut txn), vec![15_000], "the fire re-arms the next earliest due");

		fire(&mut operator, &mut txn, TimerKind::ForgetPass, 15_000);

		assert_eq!(keys(&mut txn, group(2), KeyspaceId::CUSTOM_MANAGED), 0, "group 2 is forgotten at its due");
		assert_eq!(due_entries(&mut txn), 0);
	}

	#[test]
	fn other_timer_kinds_still_reach_the_guest() {
		// An intercept wider than the one kind would silently stop every guest timer.
		let engine = TestEngine::new();
		let mut txn = txn(&engine);
		let (mut operator, guest_timers) = managed(2);

		fire(&mut operator, &mut txn, TimerKind::Seal, 13_000);
		fire(&mut operator, &mut txn, TimerKind::Tick, 13_000);

		assert_eq!(guest_timers.load(Ordering::SeqCst), 2);
	}
}

#[cfg(test)]
mod output_schema_tests {
	use std::iter::empty;

	use reifydb_value::value::{dictionary::DictionaryId, value_type::field::FieldType};

	use super::*;

	const MINTS: DictionaryId = DictionaryId(1);
	const OTHER: DictionaryId = DictionaryId(2);

	fn parent(dictionaries: &[DictionaryId]) -> Option<SchemaRef> {
		let fields: Vec<FieldRef> = dictionaries
			.iter()
			.enumerate()
			.map(|(index, dictionary)| {
				let mut builder = ColumnBuilder::with_capacity(ValueType::DictionaryId, 0);
				builder.set_dictionary_id(*dictionary);
				builder.finish(&format!("in{index}")).0
			})
			.chain([ColumnBuilder::with_capacity(ValueType::Uint8, 0).finish("amount").0])
			.collect();
		Some(Arc::new(Schema::new(fields)))
	}

	fn declared() -> Option<SchemaRef> {
		let id = TypeConstraint::unconstrained(ValueType::DictionaryId);
		let maybe = TypeConstraint::unconstrained(ValueType::Option(Box::new(ValueType::DictionaryId)));
		let n = TypeConstraint::unconstrained(ValueType::Uint8);
		declared_schema([("base", &id), ("quote", &maybe), ("n", &n)])
	}

	fn shape(schema: Option<SchemaRef>) -> Vec<(String, FieldType)> {
		schema.expect("a schema")
			.fields()
			.iter()
			.map(|field| (field.name().clone(), from_field(field).expect("the field reads back")))
			.collect()
	}

	fn dictionaries(schema: Option<SchemaRef>) -> Vec<(String, Option<DictionaryId>)> {
		shape(schema).into_iter().map(|(name, field)| (name, field.dictionary_id)).collect()
	}

	#[test]
	fn an_apply_that_declares_nothing_keeps_its_parent_schema() {
		// an operator with no declaration makes no claim, so downstream must still see the input it passes on.
		assert_eq!(output_schema_of(parent(&[MINTS]), None).unwrap(), parent(&[MINTS]));
		assert_eq!(declared_schema(empty()), None);
	}

	#[test]
	fn a_declared_id_takes_the_one_dictionary_its_input_ids_share() {
		// a declared id with no dictionary can never be decoded or joined as an id downstream.
		let out = output_schema_of(parent(&[MINTS, MINTS]), declared()).unwrap();
		assert_eq!(
			dictionaries(out.clone()),
			vec![("base".into(), Some(MINTS)), ("quote".into(), Some(MINTS)), ("n".into(), None)]
		);
		let types: Vec<_> = shape(out).into_iter().map(|(_, field)| field.value_type).collect();
		assert_eq!(
			types,
			vec![
				Some(ValueType::DictionaryId),
				Some(ValueType::Option(Box::new(ValueType::DictionaryId))),
				Some(ValueType::Uint8)
			],
			"the declared types, optionality included, are what the apply emits"
		);
	}

	#[test]
	fn a_declared_id_stays_untagged_when_its_input_has_no_single_dictionary() {
		// guessing between two dictionaries would decode an id against the wrong values.
		let untagged = vec![("base".into(), None), ("quote".into(), None), ("n".into(), None)];
		assert_eq!(dictionaries(output_schema_of(parent(&[MINTS, OTHER]), declared()).unwrap()), untagged);
		assert_eq!(dictionaries(output_schema_of(parent(&[]), declared()).unwrap()), untagged);
		assert_eq!(dictionaries(output_schema_of(None, declared()).unwrap()), untagged);
	}

	#[test]
	fn a_declared_id_that_already_names_a_dictionary_keeps_it() {
		// the declaration is the stronger claim, so the input's dictionary never overrides it.
		let mut builder = ColumnBuilder::with_capacity(ValueType::DictionaryId, 0);
		builder.set_dictionary_id(OTHER);
		let declared = Some(Arc::new(Schema::new(vec![builder.finish("base").0])));
		assert_eq!(
			dictionaries(output_schema_of(parent(&[MINTS]), declared).unwrap()),
			vec![("base".into(), Some(OTHER))]
		);
	}
}

#[cfg(test)]
mod seal_tests {
	use std::sync::{Arc, Mutex};

	use arrow_array::{Int32Array, RecordBatch, UInt64Array};
	use reifydb_core::{
		common::{ChangeVersion, CommitVersion, WindowKind, WindowSize},
		interface::change::Diff,
		metrics::heap::DropCounts,
		value::{batch::batch, column::factory::int4},
	};
	use reifydb_test_harness::engine::TestEngine;
	use reifydb_value::{
		factory::time::{at_millis, secs},
		value::system_columns::{SystemColumn, with_system_column},
	};

	use super::*;
	use crate::{
		operator::{host::TxnHostContext, time_column},
		transaction::{FlowTransaction, deferred::DeferredTransaction, mock::FlowTxn},
	};

	const OP: OperatorId = OperatorId(1);

	#[derive(Default)]
	struct Seen {
		calls: usize,
		diffs: Vec<Diff>,
	}

	struct Recorder {
		seen: Arc<Mutex<Seen>>,
	}

	impl HostOperator for Recorder {
		fn id(&self) -> OperatorId {
			OP
		}

		fn capabilities(&self) -> &[OperatorCapability] {
			OperatorCapability::STANDARD
		}

		fn apply(&mut self, _host: &mut dyn HostContext, change: Change) -> Result<Change, ValueError> {
			let mut seen = self.seen.lock().unwrap();
			seen.calls += 1;
			seen.diffs.extend(change.diffs.iter().cloned());
			Ok(Change::from_flow(OP, change.version, Vec::new(), change.changed_at))
		}
	}

	fn recorder(with: ApplyWith) -> (ApplyOperator, Arc<Mutex<Seen>>) {
		let seen = Arc::new(Mutex::new(Seen::default()));
		let inner = Recorder {
			seen: Arc::clone(&seen),
		};
		(ApplyOperator::new(None, OP, Box::new(inner), &with).unwrap(), seen)
	}

	fn drop_span(seconds: u64) -> ApplyWith {
		ApplyWith {
			drop: Some(WithSpan::Duration(secs(seconds))),
			..ApplyWith::default()
		}
	}

	fn txn(engine: &TestEngine, watermark_millis: i64) -> DeferredTransaction {
		let mut txn = engine.flow_txn().at(CommitVersion(7)).deferred();
		txn.set_flow_watermark(at_millis(watermark_millis));
		txn
	}

	fn rows(keys: &[i32], times: &[Option<i64>]) -> RecordBatch {
		let columns = batch(vec![int4("k", keys.to_vec())]).unwrap();
		let numbers: Vec<u64> = keys.iter().map(|k| *k as u64).collect();
		let columns =
			with_system_column(columns, SystemColumn::RowNumbers, Arc::new(UInt64Array::from(numbers)))
				.unwrap();
		let times = time_column(times.iter().map(|time| time.map(at_millis)));
		with_system_column(columns, SystemColumn::Time, times).unwrap()
	}

	fn apply(op: &mut ApplyOperator, txn: &mut DeferredTransaction, diffs: Vec<Diff>) {
		let mut host = TxnHostContext::with_forget(txn, OP, op.forget());
		let change = Change::from_flow(
			OperatorId(1_000),
			ChangeVersion::from(CommitVersion(7)),
			diffs,
			at_millis(0),
		);
		op.apply(&mut host, change).unwrap();
	}

	fn keys_seen(seen: &Arc<Mutex<Seen>>) -> Vec<i32> {
		let seen = seen.lock().unwrap();
		let mut keys = Vec::new();
		for diff in &seen.diffs {
			let batch = match diff {
				Diff::Insert {
					post,
					..
				} => post,
				Diff::Update {
					post,
					..
				} => post,
				Diff::Remove {
					pre,
					..
				} => pre,
			};
			let column = batch.column_by_name("k").unwrap().as_any().downcast_ref::<Int32Array>().unwrap();
			keys.extend(column.values().iter().copied());
		}
		keys
	}

	#[test]
	fn an_insert_past_the_seal_never_reaches_the_guest() {
		// the guest cannot tell a new row from one whose state it already forgot, so it must never see it
		let engine = TestEngine::new();
		let mut txn = txn(&engine, 30_000);
		let (mut op, seen) = recorder(drop_span(10));

		apply(&mut op, &mut txn, vec![Diff::insert(rows(&[1], &[Some(5_000)]))]);

		assert_eq!(seen.lock().unwrap().calls, 0, "a change emptied by the seal must not call the guest");
		assert_eq!(op.sealed_inserts.total(), 1);
	}

	#[test]
	fn a_forget_alone_drops_input_and_holds_output_by_the_forget() {
		// A missing drop defaults to the forget, otherwise rows reach forgotten state and readers see open
		// output.
		let engine = TestEngine::new();
		let mut txn = txn(&engine, 30_000);
		let (mut op, seen) = recorder(ApplyWith {
			forget: Some(secs(10)),
			..ApplyWith::default()
		});

		apply(&mut op, &mut txn, vec![Diff::insert(rows(&[1, 2], &[Some(19_999), Some(20_000)]))]);

		assert_eq!(keys_seen(&seen), vec![2]);
		assert_eq!(op.sealed_inserts.total(), 1);
		assert_eq!(op.seal_span(), Some(secs(10)));
		assert_eq!(op.forget(), Some(secs(10)));
	}

	#[test]
	fn a_drop_below_the_forget_cuts_and_holds_by_the_drop_and_forgets_by_the_forget() {
		// A cut or hold by the forget would admit rows an hour late and stall every reader by the state bound.
		let engine = TestEngine::new();
		let mut txn = txn(&engine, 30_000);
		let (mut op, seen) = recorder(ApplyWith {
			drop: Some(WithSpan::Duration(secs(10))),
			forget: Some(secs(3_600)),
			..ApplyWith::default()
		});

		apply(&mut op, &mut txn, vec![Diff::insert(rows(&[1, 2], &[Some(19_999), Some(20_000)]))]);

		assert_eq!(keys_seen(&seen), vec![2]);
		assert_eq!(op.sealed_inserts.total(), 1);
		assert_eq!(op.seal_span(), Some(secs(10)));
		assert_eq!(op.forget(), Some(secs(3_600)));
	}

	#[test]
	fn a_drop_alone_cuts_and_holds_by_the_drop_and_forgets_nothing() {
		// A drop that fell through to the forget pass would free state the user never asked to forget.
		let engine = TestEngine::new();
		let mut txn = txn(&engine, 30_000);
		let (mut op, seen) = recorder(drop_span(10));

		apply(&mut op, &mut txn, vec![Diff::insert(rows(&[1, 2], &[Some(19_999), Some(20_000)]))]);

		assert_eq!(keys_seen(&seen), vec![2]);
		assert_eq!(op.sealed_inserts.total(), 1);
		assert_eq!(op.seal_span(), Some(secs(10)));
		assert_eq!(op.forget(), None);
	}

	#[test]
	fn a_zero_drop_drops_every_late_row_and_keeps_the_watermark_instant() {
		// Zero must mean no late rows like a window drop, otherwise every late row slips past the seal.
		let engine = TestEngine::new();
		let mut txn = txn(&engine, 30_000);
		let (mut op, seen) = recorder(drop_span(0));

		apply(&mut op, &mut txn, vec![Diff::insert(rows(&[1, 2, 3], &[Some(29_999), Some(30_000), None]))]);

		assert_eq!(keys_seen(&seen), vec![2, 3]);
		assert_eq!(op.sealed_inserts.total(), 1);
	}

	#[test]
	fn a_mixed_batch_keeps_rows_inside_the_seal_and_rows_without_a_time() {
		// dropping a row with no time, or one inside the drop, loses live data
		let engine = TestEngine::new();
		let mut txn = txn(&engine, 30_000);
		let (mut op, seen) = recorder(drop_span(10));

		apply(&mut op, &mut txn, vec![Diff::insert(rows(&[1, 2, 3], &[Some(5_000), Some(25_000), None]))]);

		assert_eq!(keys_seen(&seen), vec![2, 3]);
		assert_eq!(op.sealed_inserts.total(), 1);
	}

	#[test]
	fn each_seal_reason_counts_only_its_own_kind() {
		// a remove or update past the seal must be dropped and counted under its own reason
		let engine = TestEngine::new();
		let mut txn = txn(&engine, 30_000);
		let (mut op, seen) = recorder(drop_span(10));
		let old = rows(&[1], &[Some(5_000)]);

		apply(
			&mut op,
			&mut txn,
			vec![
				Diff::insert(rows(&[2, 3], &[Some(1_000), Some(2_000)])),
				Diff::update(old.clone(), old.clone()),
				Diff::remove(old),
			],
		);

		assert_eq!(seen.lock().unwrap().calls, 0);
		assert_eq!(op.sealed_inserts.total(), 2);
		assert_eq!(op.sealed_updates.total(), 1);
		assert_eq!(op.sealed_removes.total(), 1);
	}

	#[test]
	fn the_apply_sample_reports_its_seal_drops_by_reason() {
		// a drop the sample does not carry never reaches system metrics, so late input stays invisible

		let engine = TestEngine::new();
		let mut txn = txn(&engine, 30_000);
		let (mut op, _) = recorder(drop_span(10));
		let old = rows(&[1], &[Some(5_000)]);

		apply(
			&mut op,
			&mut txn,
			vec![
				Diff::insert(rows(&[2, 3], &[Some(1_000), Some(2_000)])),
				Diff::update(old.clone(), old),
				Diff::remove(rows(&[1, 4, 5], &[Some(5_000), Some(5_000), Some(5_000)])),
			],
		);

		assert_eq!(
			op.sample().expect("the operator reports a sample").drops,
			DropCounts {
				inserts_past_seal: Some(2),
				updates_past_seal: Some(1),
				removes_past_seal: Some(3),
				..DropCounts::default()
			},
			"the sample must carry each seal count under its own reason"
		);
	}

	#[test]
	fn a_windowed_apply_leaves_the_seal_to_its_window() {
		// a session row older than the gap can still sit in an open session, so a row-time seal drops live
		// input
		let engine = TestEngine::new();
		let mut txn = txn(&engine, 12_100);
		let with = ApplyWith {
			window: Some(WindowKind::Session {
				gap: Duration::from_milliseconds_const(1_000),
			}),
			..ApplyWith::default()
		};
		let (mut op, seen) = recorder(with);

		apply(&mut op, &mut txn, vec![Diff::remove(rows(&[2], &[Some(10_800)]))]);

		assert_eq!(
			keys_seen(&seen),
			vec![2],
			"the remove of a row in a session open until 12.6s must reach the guest"
		);
	}

	#[test]
	fn an_apply_with_a_forget_and_no_drop_drops_by_its_forget() {
		// a missing drop defaults to the forget, otherwise input reaches state already forgotten
		let engine = TestEngine::new();
		let mut txn = txn(&engine, 30_000);
		let with = ApplyWith {
			forget: Some(secs(10)),
			..ApplyWith::default()
		};
		let (mut op, seen) = recorder(with);

		apply(&mut op, &mut txn, vec![Diff::insert(rows(&[1, 2], &[Some(5_000), Some(25_000)]))]);

		assert_eq!(keys_seen(&seen), vec![2]);
	}

	#[test]
	fn a_slot_window_with_a_forget_drops_nothing() {
		// a slot window seals in slots, so a time seal would drop rows of an open window
		let engine = TestEngine::new();
		let mut txn = txn(&engine, 30_000);
		let with = ApplyWith {
			window: Some(WindowKind::Tumbling {
				size: WindowSize::Slots(10),
			}),
			forget: Some(secs(10)),
			..ApplyWith::default()
		};
		let (mut op, seen) = recorder(with);

		apply(&mut op, &mut txn, vec![Diff::insert(rows(&[1], &[Some(5_000)]))]);

		assert_eq!(keys_seen(&seen), vec![1]);
	}

	#[test]
	fn an_apply_without_a_seal_reports_no_seal_drop_reasons() {
		// a zero row must mean nothing dropped yet, never an operator that cannot drop at all
		let (op, _) = recorder(ApplyWith::default());

		assert_eq!(op.sample().expect("an apply reports a sample").drops, DropCounts::default());
	}

	#[test]
	fn an_apply_without_drop_or_forget_keeps_an_old_row() {
		// with no seal declared nothing is ever sealed, so an old row must still reach the guest
		let engine = TestEngine::new();
		let mut txn = txn(&engine, 9_000_000);
		let (mut op, seen) = recorder(ApplyWith::default());

		apply(&mut op, &mut txn, vec![Diff::insert(rows(&[1], &[Some(1_000)]))]);

		assert_eq!(keys_seen(&seen), vec![1]);
	}

	#[test]
	fn a_forget_pass_keeps_the_seal_when_the_flow_watermark_falls_back() {
		// without the ledger a row the forget pass already freed passes again once the watermark falls back
		let engine = TestEngine::new();
		let mut txn = txn(&engine, 40_000);
		let (mut op, seen) = recorder(drop_span(10));
		{
			let mut host = TxnHostContext::with_forget(&mut txn, OP, op.forget());
			op.on_timer(
				&mut host,
				Timer {
					due: at_millis(40_000),
					kind: TimerKind::ForgetPass,
					key: EncodedKey::new(Vec::new()),
				},
			)
			.unwrap();
		}
		txn.set_flow_watermark(at_millis(20_000));

		apply(&mut op, &mut txn, vec![Diff::insert(rows(&[1, 2], &[Some(25_000), Some(35_000)]))]);

		assert_eq!(keys_seen(&seen), vec![2]);
	}
}
