// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	abi::operator::capabilities::OperatorCapability,
	core::interface::catalog::flow::OperatorId,
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

const NODE: OperatorId = OperatorId(1);

struct Inert;

impl OperatorMetadata for Inert {
	const NAME: &'static str = "inert";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Holds nothing; exists to prove the harness is reachable";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl OperatorLogic for Inert {
	fn create(_node: OperatorId, _config: &Config) -> SdkResult<Self> {
		Ok(Inert)
	}

	fn apply(&mut self, _ctx: &mut impl OperatorContext, _change: impl ChangeView) -> SdkResult<()> {
		Ok(())
	}
}

#[test]
fn a_guest_operator_reaches_the_sweep_through_the_published_testing_surface() {
	// Drives the harness through `reifydb::testing::flow` only; reaching into `reifydb::sub_flow`
	// works today and must not become the path. The grid is the assertion, not a reclaimed group:
	// a guest holding no state has nothing to retire and would pass for the wrong reason.
	let ttl = Duration::from_seconds(60).expect("60s is representable");

	let harness =
		Harness::guest(Inert, NODE, OperatorCapability::STANDARD, Some(ttl)).with_activity_grid();
	assert!(
		harness.activity_grid().event_grid().is_some(),
		"a declared ttl must grid the node, or the driver skips it and counts it perpetual"
	);

	let ungridded =
		Harness::guest(Inert, NODE, OperatorCapability::STANDARD, None).with_activity_grid();
	assert!(
		ungridded.activity_grid().event_grid().is_none(),
		"and without one it must not, which is the whole difference between the two cohorts"
	);
}
