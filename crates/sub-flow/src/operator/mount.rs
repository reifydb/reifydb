// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	backtrace::Backtrace,
	panic::{AssertUnwindSafe, catch_unwind},
};

use reifydb_core::{
	common::ChangeVersion,
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	metrics::heap::OperatorSample,
};
use reifydb_flow_async::{
	operator::{BoxedHostOperator, HostOperator, host::HostContext},
	timer::Timer,
};
use reifydb_runtime::fatal::{
	describe_payload, fatal,
	report::{FatalKind, FatalReport},
};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		MountedOperator, OperatorMetadata, timer::Timer as SdkTimer, view::in_process::InProcessChangeView,
	},
};
use reifydb_value::Result;

use crate::operator::context::in_process::InProcessContext;

fn run_or_abort<R>(operator: OperatorId, stage: &'static str, f: impl FnOnce() -> SdkResult<R>) -> R {
	match catch_unwind(AssertUnwindSafe(f)) {
		Ok(Ok(value)) => value,
		Ok(Err(e)) => {
			fatal(FatalReport::new(FatalKind::Error, format!("guest operator returned an error: {:?}", e))
				.component("flow operator")
				.with("operator", operator.0.to_string())
				.with("stage", stage)
				.backtrace(Backtrace::force_capture().to_string()))
		}
		Err(payload) => fatal(FatalReport::new(FatalKind::Panic, describe_payload(&payload))
			.component("flow operator")
			.with("operator", operator.0.to_string())
			.with("stage", stage)
			.backtrace(Backtrace::force_capture().to_string())),
	}
}

pub fn mount<C: MountedOperator + OperatorMetadata + 'static>(
	logic: C,
	operator: OperatorId,
	capabilities: &'static [OperatorCapability],
) -> BoxedHostOperator {
	Box::new(GuestAdapter {
		logic,
		operator,
		capabilities,
	})
}

struct GuestAdapter<C> {
	logic: C,
	operator: OperatorId,
	capabilities: &'static [OperatorCapability],
}

impl<C: MountedOperator + 'static> HostOperator for GuestAdapter<C> {
	fn id(&self) -> OperatorId {
		self.operator
	}

	fn capabilities(&self) -> &[OperatorCapability] {
		self.capabilities
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change> {
		let version = change.version;
		let changed_at = change.changed_at;
		let mut ctx = InProcessContext::new(host, self.operator);
		{
			let view = InProcessChangeView::new(&change);
			let logic = &mut self.logic;
			run_or_abort(self.operator, "apply", || logic.apply(&mut ctx, view));
		}
		let diffs = ctx.take_diffs();
		Ok(Change::from_flow(self.operator, version, diffs, changed_at))
	}

	fn on_timer(&mut self, host: &mut dyn HostContext, timer: Timer) -> Result<Option<Change>> {
		let due = timer.due;
		let version = ChangeVersion::from(host.version());
		let mut ctx = InProcessContext::new(host, self.operator);
		{
			let logic = &mut self.logic;
			run_or_abort(self.operator, "on_timer", || {
				logic.on_timer(
					&mut ctx,
					SdkTimer {
						due,
						kind: timer.kind,
						key: timer.key.as_ref(),
					},
				)
			});
		}
		let diffs = ctx.take_diffs();
		if diffs.is_empty() {
			return Ok(None);
		}
		Ok(Some(Change::from_flow(self.operator, version, diffs, due)))
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.logic.sample()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
	use reifydb_core::{
		common::CommitVersion,
		key::operator::state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey, unmanaged_key_in},
		state::timer::{StateStore, TimerKind},
	};
	use reifydb_flow_async::{
		operator::host::{HostContext, TxnHostContext},
		transaction::{ChangeCoordinate, FlowTransaction},
	};
	use reifydb_sdk::flow::operator::context::GuestEmitContext;
	use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
	use reifydb_value::value::datetime::DateTime;

	use super::{InProcessContext, OperatorId};

	const NODE: OperatorId = OperatorId(1);

	fn key(name: &str) -> EncodedKey {
		EncodedKey::new(name.as_bytes())
	}

	#[test]
	fn a_dylib_read_of_an_absent_group_writes_nothing() {
		// a probe that wrote would resurrect groups the reaper had already erased
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().at(CommitVersion(7)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_millis(0)),
		});
		let mut host = TxnHostContext::new(&mut txn, NODE);
		let group = GroupId::of(&key("absent"));
		let absent = OperatorStateKey::inner_encoded(group, KeyspaceId::ACCUMULATOR, []);

		assert!(host.state_get(&absent).unwrap().is_none());

		assert!(host.group_sweep(group, false, None).unwrap().is_empty(), "the probe must leave no row");
	}

	fn stored_key(id: &str) -> GroupStateKey {
		// the guest owns only this keyspace, and its id column is what the round trip must hand back intact
		unmanaged_key_in(GroupId::ROOT, id.as_bytes()).expect("a fixture id fits the keyspace").into()
	}

	#[test]
	fn a_dylib_batch_read_hands_back_the_key_the_guest_wrote() {
		// Handing back an operator-scoped key unstripped makes the guest's own lookups miss.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().at(CommitVersion(7)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_millis(0)),
		});
		let mut host = TxnHostContext::new(&mut txn, NODE);

		let written = stored_key("entry");
		host.state_set(&written, EncodedPodRow::new(&[7])).unwrap();

		let from_get_many: Vec<GroupStateKey> =
			host.state_get_many(&[written.clone()]).unwrap().into_iter().map(|(key, _)| key).collect();
		assert_eq!(from_get_many, vec![written.clone()], "state_get_many must return the key that was written");

		let from_range: Vec<GroupStateKey> =
			host.group_sweep(GroupId::ROOT, false, None).unwrap().into_iter().map(|(key, _)| key).collect();
		assert_eq!(from_range, vec![written.clone()], "a group sweep must return the key that was written");

		let mut visited = Vec::new();
		host.state_get_many_visit(&[written.clone()], &mut |key, _| {
			visited.push(key);
			Ok(())
		})
		.unwrap();
		assert_eq!(visited, vec![written], "state_get_many_visit must visit the key that was written");
	}

	#[test]
	fn an_in_process_guest_cannot_arm_or_disarm_the_reclaim_timer() {
		// A guest that arms the engine's reclaim kind would free managed state on its own schedule.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().at(CommitVersion(7)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_millis(0)),
		});
		let mut host = TxnHostContext::new(&mut txn, NODE);
		let mut ctx = InProcessContext::new(&mut host, NODE);
		let due = DateTime::from_millis(5_000);

		let armed = ctx.arm_timer(due, TimerKind::Reclaim, &key("r")).expect_err("arming Reclaim must fail");
		let disarmed =
			ctx.disarm_timer(due, TimerKind::Reclaim, &key("r")).expect_err("disarming Reclaim must fail");

		assert!(armed.to_string().contains("FLOW_074"), "expected FLOW_074, got: {armed}");
		assert!(disarmed.to_string().contains("FLOW_074"), "expected FLOW_074, got: {disarmed}");
		ctx.arm_timer(due, TimerKind::Seal, &key("s")).expect("a guest Seal timer must still arm");
		ctx.disarm_timer(due, TimerKind::Seal, &key("s")).expect("and still disarm");
	}
}
