// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{OperatorClass, WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	operator_with::{ApplyWith, WithSpan},
};
use reifydb_sdk::{
	error::Result,
	flow::operator::{
		ManagedMount, ManagedOperator, NostateMount, NostateOperator, OperatorMetadata, UnmanagedMount,
		UnmanagedOperator,
		column::operator::OperatorColumn,
		context::{GuestContext, Managed, Nostate, Unmanaged},
		extern_c::binding::{exports::create_descriptor, operator::ExternCOperatorAdapter},
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

struct UnmanagedProbe;

impl OperatorMetadata for UnmanagedProbe {
	const NAME: &'static str = "unmanaged_probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Holds unmanaged state";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl UnmanagedOperator for UnmanagedProbe {
	const UNMANAGED_BECAUSE: &'static str = "frees its own rows";

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self> {
		Ok(UnmanagedProbe)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext<Unmanaged>, _change: impl ChangeView) -> Result<()> {
		Ok(())
	}
}

fn lateness_of(seconds: u64) -> ApplyWith {
	ApplyWith {
		lateness: Some(WithSpan::Duration(secs(seconds))),
		..ApplyWith::default()
	}
}

fn retention_of(seconds: u64) -> ApplyWith {
	ApplyWith {
		retention: Some(secs(seconds)),
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
fn a_managed_operator_builds_with_a_zero_lateness() {
	// Zero lateness is a real bound now: no output hold, and the state is freed at the next gate step.
	ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<ManagedMount<ManagedProbe>>>::new()
		.with(lateness_of(0))
		.build()
		.expect("a zero lateness bounds the state and must build");
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

#[test]
fn a_managed_operator_builds_with_a_retention_alone() {
	// Retention is the bound the reclaim reads, so it must satisfy the managed check without any lateness.
	ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<ManagedMount<ManagedProbe>>>::new()
		.with(retention_of(60))
		.build()
		.expect("a retention alone bounds the state and must build");
}

#[test]
fn a_managed_operator_refuses_a_retention_below_its_lateness() {
	// A retention under the lateness frees a group while a timer inside the hold can still fire for it.
	let with = ApplyWith {
		retention: Some(secs(30)),
		..lateness_of(60)
	};
	let Err(err) = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<ManagedMount<ManagedProbe>>>::new()
		.with(with)
		.build()
	else {
		panic!("create must refuse a retention below the lateness");
	};
	assert!(err.to_string().contains("FLOW_081"), "expected FLOW_081, got: {err}");
}

#[test]
fn an_unmanaged_operator_refuses_a_retention() {
	// Nothing reclaims unmanaged state, so an accepted retention is a bound the author believes holds.
	let Err(err) = ExternCOperatorHarnessBuilder::<ExternCOperatorAdapter<UnmanagedMount<UnmanagedProbe>>>::new()
		.with(retention_of(60))
		.build()
	else {
		panic!("create must refuse a retention");
	};
	assert!(err.to_string().contains("FLOW_080"), "expected FLOW_080, got: {err}");
}

#[test]
fn only_an_unmanaged_descriptor_carries_a_reason_and_each_carries_its_class() {
	// A reason dropped or published for another class makes the census name the wrong owner.
	let unmanaged = create_descriptor::<UnmanagedMount<UnmanagedProbe>>();
	assert_eq!(OperatorClass::from_u8(unmanaged.class), Some(OperatorClass::Unmanaged));
	assert!(!unmanaged.unmanaged_because.ptr.is_null());
	// SAFETY: the reason is non-null and points at a `&'static str` of exactly `len` bytes.
	let reason =
		unsafe { std::slice::from_raw_parts(unmanaged.unmanaged_because.ptr, unmanaged.unmanaged_because.len) };
	assert_eq!(reason, b"frees its own rows");
	assert_eq!((unmanaged.window.takes_window, unmanaged.window.kinds), (0, 0));

	let managed = create_descriptor::<ManagedMount<ManagedProbe>>();
	assert_eq!(OperatorClass::from_u8(managed.class), Some(OperatorClass::Managed));
	assert!(managed.unmanaged_because.ptr.is_null());

	let nostate = create_descriptor::<NostateMount<NostateProbe>>();
	assert_eq!(OperatorClass::from_u8(nostate.class), Some(OperatorClass::Nostate));
	assert!(nostate.unmanaged_because.ptr.is_null());
}
