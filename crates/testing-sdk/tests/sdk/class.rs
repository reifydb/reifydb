// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	operator_with::{ApplyWith, WithSpan},
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		ManagedMount, ManagedOperator, NostateMount, NostateOperator, OperatorMetadata,
		column::operator::OperatorColumn,
		context::{GuestContext, Managed, Nostate},
		extern_c::binding::operator::ExternCOperatorAdapter,
		view::ChangeView,
	},
};
use reifydb_testing_sdk::harness::ExternCOperatorHarnessBuilder;
use reifydb_value::{config::ExtensionParams, factory::time::secs};

struct NostateProbe;

impl OperatorMetadata for NostateProbe {
	const NAME: &'static str = "nostate_probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Holds no state";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl NostateOperator for NostateProbe {
	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(NostateProbe)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext<Nostate>, _change: impl ChangeView) -> Result<()> {
		Ok(())
	}
}

struct ManagedProbe;

impl OperatorMetadata for ManagedProbe {
	const NAME: &'static str = "managed_probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Holds managed state";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl ManagedOperator for ManagedProbe {
	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(ManagedProbe)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext<Managed>, _change: impl ChangeView) -> Result<()> {
		Ok(())
	}
}

fn lateness_of(seconds: u64) -> ApplyWith {
	ApplyWith {
		lateness: Some(WithSpan::Duration(secs(seconds))),
		..ApplyWith::default()
	}
}

#[test]
fn a_nostate_operator_refuses_any_with() {
	// A nostate operator that takes a with block lets a view declare a seal nothing honours.
	let Err(err) = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<NostateMount<NostateProbe>>>::new()
		.with(lateness_of(60))
		.build()
	else {
		panic!("create must refuse a with block");
	};
	assert!(err.to_string().contains("FLOW_071"), "expected FLOW_071, got: {err}");
}

#[test]
fn a_managed_operator_refuses_a_missing_lateness() {
	// A managed operator must never start without lateness, or its state has no bound.
	let Err(err) = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<ManagedMount<ManagedProbe>>>::new()
		.with(ApplyWith::default())
		.build()
	else {
		panic!("create must refuse a missing lateness");
	};
	assert!(err.to_string().contains("FLOW_072"), "expected FLOW_072, got: {err}");
}

#[test]
fn a_managed_operator_refuses_a_window() {
	// A window on a managed operator must be refused, never silently ignored.
	let with = ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(secs(60)),
		}),
		..lateness_of(60)
	};
	let Err(err) = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<ManagedMount<ManagedProbe>>>::new()
		.with(with)
		.build()
	else {
		panic!("create must refuse a window");
	};
	assert!(err.to_string().contains("FLOW_067"), "expected FLOW_067, got: {err}");
}

#[test]
fn a_managed_operator_builds_with_a_duration_lateness() {
	// The managed checks must never refuse the with block they exist to require.
	ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<ManagedMount<ManagedProbe>>>::new()
		.with(lateness_of(60))
		.build()
		.expect("a duration lateness is all a managed operator needs");
}
