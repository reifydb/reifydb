// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicUsize, Ordering},
};

use reifydb::{
	core::{
		common::CommitVersion,
		interface::{
			catalog::flow::OperatorId,
			change::{Change, Diffs},
			flow::OperatorCapability,
		},
	},
	sdk::{
		error::Result as SdkResult,
		flow::operator::{
			GuestOperator, OperatorMetadata, column::operator::OperatorColumn, context::GuestContext,
			view::ChangeView,
		},
	},
	testing::flow::harness::Harness,
	value::{config::Config, value::datetime::DateTime},
};

const NODE: OperatorId = OperatorId(1);

struct Counting(Arc<AtomicUsize>);

impl OperatorMetadata for Counting {
	const NAME: &'static str = "counting";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Records that it was reached; exists to prove the harness drives a guest";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl GuestOperator for Counting {
	fn create(_node: OperatorId, _config: &Config) -> SdkResult<Self> {
		Ok(Counting(Arc::new(AtomicUsize::new(0))))
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext, _change: impl ChangeView) -> SdkResult<()> {
		self.0.fetch_add(1, Ordering::SeqCst);
		Ok(())
	}
}

#[test]
fn a_guest_operator_is_driven_through_the_published_testing_surface() {
	// The counter is the assertion, never the Ok: mounting can succeed while the guest is never invoked.
	let calls = Arc::new(AtomicUsize::new(0));
	let mut harness = Harness::guest(Counting(calls.clone()), NODE, OperatorCapability::STANDARD);

	let changed_at = DateTime::from_epoch_millis(0).expect("the epoch is representable");
	let out = harness
		.apply(Change::from_flow(NODE, CommitVersion(1), Diffs::new(), changed_at))
		.expect("the published surface must carry a change to the guest");

	assert_eq!(calls.load(Ordering::SeqCst), 1, "the guest must be reached exactly once per applied change");
	assert_eq!(out.row_count(), 0, "a guest that emits nothing must not have rows invented for it");
}
