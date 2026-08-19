// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	backtrace::Backtrace,
	panic::{AssertUnwindSafe, catch_unwind},
};

use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	metrics::heap::OperatorSample,
};
use reifydb_flow::{
	operator::{BoxedHostOperator, HostOperator, host::HostContext},
	timer::Timer,
};
use reifydb_runtime::fatal::{
	describe_payload, fatal,
	report::{FatalKind, FatalReport},
};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{GuestOperator, timer::Timer as SdkTimer, view::in_process::InProcessChangeView},
};
use reifydb_value::{Result, value::duration::Duration};

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

pub fn mount<C: GuestOperator + 'static>(
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

impl<C: GuestOperator + 'static> HostOperator for GuestAdapter<C> {
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
		let version = host.version();
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

	fn lateness_span(&self) -> Option<Duration> {
		self.logic.lateness().filter(|span| !span.is_zero())
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.logic.sample()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::{
		key::encoded::{EncodedKey, EncodedKeyRange},
		row::pod::EncodedPodRow,
	};
	use reifydb_core::{
        common::CommitVersion,
        key::operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
        state::timer::StateStore,
	};
	use reifydb_flow::{
		operator::{
			HostOperator,
			host::{HostContext, TxnHostContext},
		},
		transaction::{ChangeCoordinate, FlowTransaction},
	};
	use reifydb_sdk::{
		error::Result as SdkResult,
		flow::operator::{GuestOperator, context::GuestContext, view::ChangeView},
	};
	use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
	use reifydb_value::{
		config::Config,
		value::{datetime::DateTime, duration::Duration},
	};

	use super::{OperatorId, mount};

	const NODE: OperatorId = OperatorId(1);

	fn key(name: &str) -> EncodedKey {
		EncodedKey::new(name.as_bytes())
	}

	#[test]
	fn a_dylib_read_resolves_a_group_without_creating_one() {
		// A read must never intern, or groups already reclaimed resurrect and the dictionary never shrinks.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().at(CommitVersion(7)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_millis(0)),
			version: CommitVersion(7),
		});
		let mut host = TxnHostContext::new(&mut txn, NODE);

		assert_eq!(host.lookup_groups(&[key("absent")]).unwrap(), vec![None]);

		let interned: Vec<GroupId> =
			host.intern_groups(&[key("absent")]).unwrap().into_iter().map(|(group, _)| group).collect();
		assert_eq!(
			interned,
			vec![GroupId::FIRST],
			"the earlier read must not have consumed an id from the counter"
		);
	}

	fn stored_key(suffix: &str) -> GroupStateKey {
		OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::ACCUMULATOR, suffix.as_bytes())
	}

	#[test]
	fn a_dylib_batch_read_hands_back_the_key_the_guest_wrote() {
		// Handing back an operator-scoped key unstripped makes the guest's own lookups miss.
		let engine = TestEngine::new();
		let mut txn = engine.flow_txn().at(CommitVersion(7)).deferred();
		txn.set_change_coordinate(ChangeCoordinate {
			at: Some(DateTime::from_millis(0)),
			version: CommitVersion(7),
		});
		let mut host = TxnHostContext::new(&mut txn, NODE);

		let written = stored_key("entry");
		host.state_set(&written, EncodedPodRow::new(&[7])).unwrap();

		let from_get_many: Vec<GroupStateKey> =
			host.state_get_many(&[written.clone()]).unwrap().into_iter().map(|(key, _)| key).collect();
		assert_eq!(from_get_many, vec![written.clone()], "state_get_many must return the key that was written");

		let from_range: Vec<GroupStateKey> =
			host.state_range(EncodedKeyRange::all()).unwrap().into_iter().map(|(key, _)| key).collect();
		assert_eq!(from_range, vec![written.clone()], "state_range must return the key that was written");

		let mut visited = Vec::new();
		host.state_get_many_visit(&[written.clone()], &mut |key, _| {
			visited.push(key);
			Ok(())
		})
		.unwrap();
		assert_eq!(visited, vec![written], "state_get_many_visit must visit the key that was written");
	}

	struct SealProbe(Option<i64>);

	impl GuestOperator for SealProbe {
		fn create(_operator_id: OperatorId, _config: &Config) -> SdkResult<Self> {
			Ok(Self(None))
		}

		fn apply(&mut self, _ctx: &mut impl GuestContext, _change: impl ChangeView) -> SdkResult<()> {
			Ok(())
		}

		fn lateness(&self) -> Option<Duration> {
			self.0.map(Duration::from_milliseconds_const)
		}
	}

	#[test]
	fn a_mounted_guest_forwards_its_lateness_span() {
		// A mount that swallows the lateness span claims a frontier covering buckets still immutable.
		let mounted = mount(SealProbe(Some(65_000)), NODE, &[]);

		assert_eq!(HostOperator::lateness_span(&*mounted), Some(Duration::from_milliseconds(65_000).unwrap()));
	}

	#[test]
	fn a_mounted_guest_reports_no_lateness_span_for_a_zero_span() {
		// A zero span seals instantly, claiming a frontier over buckets that are still immutable.
		let mounted = mount(SealProbe(Some(0)), NODE, &[]);

		assert_eq!(HostOperator::lateness_span(&*mounted), None);
	}
}
