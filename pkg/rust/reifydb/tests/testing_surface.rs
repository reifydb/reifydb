// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The harness is only useful to an out-of-tree operator author if it is reachable the way the other
// test surfaces are, so this drives it through `reifydb::testing::flow` and nowhere else. Reaching
// past that - into `reifydb::sub_flow` - would work today and is exactly what must not become the
// documented path, because it makes a test-only harness part of a subsystem's public shape.

use reifydb::{
	abi::operator::capabilities::OperatorCapability,
	core::interface::catalog::flow::FlowNodeId,
	sdk::{
		config::Config,
		error::Result as SdkResult,
		operator::{
			OperatorLogic, OperatorMetadata, column::operator::OperatorColumn, context::OperatorContext,
			view::ChangeView,
		},
	},
	testing::flow::harness::Harness,
	value::value::duration::Duration,
};

const NODE: FlowNodeId = FlowNodeId(1);

struct Inert;

impl OperatorMetadata for Inert {
	const NAME: &'static str = "inert";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Holds nothing; exists to prove the harness is reachable";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD_WITH_RECLAIM;
}

impl OperatorLogic for Inert {
	fn create(_node: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(Inert)
	}

	fn apply(&mut self, _ctx: &mut impl OperatorContext, _change: impl ChangeView) -> SdkResult<()> {
		Ok(())
	}
}

#[test]
fn a_guest_operator_reaches_the_sweep_through_the_published_testing_surface() {
	// The assertion is the grid rather than a reclaimed group: a guest that holds no state has
	// nothing to retire, so counting retirements here would pass for the wrong reason. What this
	// pins is that the declared ttl crossed the package boundary and reached the substrate as a
	// real retention scale - the step that decides whether a node is swept at all.
	let ttl = Duration::from_seconds(60).expect("60s is representable");

	let harness =
		Harness::guest(Inert, NODE, OperatorCapability::STANDARD_WITH_RECLAIM, Some(ttl)).with_activity_grid();
	assert!(
		harness.activity_grid().event_grid().is_some(),
		"a declared ttl must grid the node, or the driver skips it and counts it perpetual"
	);

	let ungridded =
		Harness::guest(Inert, NODE, OperatorCapability::STANDARD_WITH_RECLAIM, None).with_activity_grid();
	assert!(
		ungridded.activity_grid().event_grid().is_none(),
		"and without one it must not, which is the whole difference between the two cohorts"
	);
}
