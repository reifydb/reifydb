// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use reifydb_core::{
	common::{
		ChangeVersion, CommitVersion, OperatorClass, WindowKind, WindowRequirements, WindowSize,
		WindowSizeDomain,
	},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
		flow::OperatorCapability,
	},
	operator_with::{ApplyWith, WithSpan},
	value::{batch::batch, column::factory::int4},
};
use reifydb_sdk::{
	error::SdkError,
	flow::operator::{
		ManagedMount, ManagedOperator, NostateMount, NostateOperator, OperatorMetadata, UnmanagedMount,
		UnmanagedOperator,
		column::operator::OperatorColumn,
		context::{GuestContext, Managed, Nostate, Unmanaged},
		extern_c::binding::exports::create_descriptor,
		view::ChangeView,
	},
};
use reifydb_testing_sdk::in_process::harness::InProcessOperatorHarnessBuilder;
use reifydb_value::{
	config::ExtensionParams,
	factory::time::{at_millis, secs},
	value::{
		container::temporal_array::datetime_array,
		system_columns::{SystemColumn, with_system_column},
	},
};

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
	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self, SdkError> {
		Ok(NostateProbe)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext<Nostate>, _change: impl ChangeView) -> Result<(), SdkError> {
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
	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self, SdkError> {
		Ok(ManagedProbe)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext<Managed>, _change: impl ChangeView) -> Result<(), SdkError> {
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
	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
	};

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self, SdkError> {
		Ok(UnmanagedProbe)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext<Unmanaged>, _change: impl ChangeView) -> Result<(), SdkError> {
		Ok(())
	}
}

struct UnmanagedWindowProbe;

impl OperatorMetadata for UnmanagedWindowProbe {
	const NAME: &'static str = "unmanaged_window_probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Holds unmanaged state and takes a tumbling window";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl UnmanagedOperator for UnmanagedWindowProbe {
	const UNMANAGED_BECAUSE: &'static str = "frees its own rows";
	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: true,
		kinds: &["tumbling"],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
	};

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> Result<Self, SdkError> {
		Ok(UnmanagedWindowProbe)
	}

	fn apply(&mut self, _ctx: &mut impl GuestContext<Unmanaged>, _change: impl ChangeView) -> Result<(), SdkError> {
		Ok(())
	}
}

fn tumbling_of(seconds: u64) -> ApplyWith {
	ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(secs(seconds)),
		}),
		..ApplyWith::default()
	}
}

fn drop_of(seconds: u64) -> ApplyWith {
	ApplyWith {
		drop: Some(WithSpan::Duration(secs(seconds))),
		..ApplyWith::default()
	}
}

fn forget_of(seconds: u64) -> ApplyWith {
	ApplyWith {
		forget: Some(secs(seconds)),
		..ApplyWith::default()
	}
}

#[test]
fn a_nostate_operator_refuses_any_with() {
	// A nostate operator that takes a with block lets a view declare a seal nothing honours.
	let Err(err) = InProcessOperatorHarnessBuilder::<NostateMount<NostateProbe>>::new().with(drop_of(60)).build()
	else {
		panic!("create must refuse a with block");
	};
	assert!(err.to_string().contains("FLOW_071"), "expected FLOW_071, got: {err}");
}

#[test]
fn a_managed_operator_refuses_a_missing_forget() {
	// A managed operator must never start without forget, or its state has no bound.
	let Err(err) =
		InProcessOperatorHarnessBuilder::<ManagedMount<ManagedProbe>>::new().with(ApplyWith::default()).build()
	else {
		panic!("create must refuse a missing forget");
	};
	assert!(err.to_string().contains("FLOW_072"), "expected FLOW_072, got: {err}");
}

#[test]
fn a_managed_operator_builds_with_a_zero_forget() {
	// Zero forget is a real bound now: no output hold, and the state is freed at the next gate step.
	InProcessOperatorHarnessBuilder::<ManagedMount<ManagedProbe>>::new()
		.with(forget_of(0))
		.build()
		.expect("a zero forget bounds the state and must build");
}

#[test]
fn a_managed_operator_refuses_a_window() {
	// A window on a managed operator must be refused, never silently ignored.
	let with = ApplyWith {
		window: Some(WindowKind::Tumbling {
			size: WindowSize::Duration(secs(60)),
		}),
		..forget_of(60)
	};
	let Err(err) = InProcessOperatorHarnessBuilder::<ManagedMount<ManagedProbe>>::new().with(with).build() else {
		panic!("create must refuse a window");
	};
	assert!(err.to_string().contains("FLOW_067"), "expected FLOW_067, got: {err}");
}

#[test]
fn a_managed_operator_with_only_a_drop_is_refused() {
	// A drop alone frees no state, so a managed operator must never start on it.
	let Err(err) = InProcessOperatorHarnessBuilder::<ManagedMount<ManagedProbe>>::new().with(drop_of(60)).build()
	else {
		panic!("create must refuse a drop without a forget");
	};
	assert!(err.to_string().contains("FLOW_072"), "expected FLOW_072, got: {err}");
}

#[test]
fn a_managed_operator_builds_with_a_drop_and_a_longer_forget() {
	// The managed checks must never refuse a forget that outlives the drop declared next to it.
	let with = ApplyWith {
		forget: Some(secs(3_600)),
		..drop_of(10)
	};
	InProcessOperatorHarnessBuilder::<ManagedMount<ManagedProbe>>::new()
		.with(with)
		.build()
		.expect("a drop of 10s under a forget of 1h must build");
}

#[test]
fn a_managed_operator_builds_with_a_forget_alone() {
	// Forget is the bound the forget pass reads, so it must satisfy the managed check without any drop.
	InProcessOperatorHarnessBuilder::<ManagedMount<ManagedProbe>>::new()
		.with(forget_of(60))
		.build()
		.expect("a forget alone bounds the state and must build");
}

#[test]
fn a_managed_operator_refuses_a_forget_below_its_drop() {
	// A forget under the drop frees a group while a timer inside the hold can still fire for it.
	let with = ApplyWith {
		forget: Some(secs(30)),
		..drop_of(60)
	};
	let Err(err) = InProcessOperatorHarnessBuilder::<ManagedMount<ManagedProbe>>::new().with(with).build() else {
		panic!("create must refuse a forget below the drop");
	};
	assert!(err.to_string().contains("FLOW_081"), "expected FLOW_081, got: {err}");
}

#[test]
fn an_unmanaged_operator_refuses_a_forget() {
	// Nothing forgets unmanaged state, so an accepted forget is a bound the author believes holds.
	let Err(err) =
		InProcessOperatorHarnessBuilder::<UnmanagedMount<UnmanagedProbe>>::new().with(forget_of(60)).build()
	else {
		panic!("create must refuse a forget");
	};
	assert!(err.to_string().contains("FLOW_080"), "expected FLOW_080, got: {err}");
}

#[test]
fn an_unmanaged_operator_accepts_a_drop_and_refuses_a_forget_next_to_it() {
	// A drop only refuses late arrivals, so it must build, while a forget next to it must never pass.
	InProcessOperatorHarnessBuilder::<UnmanagedMount<UnmanagedProbe>>::new()
		.with(drop_of(60))
		.build()
		.expect("an unmanaged operator must accept a drop");
	let with = ApplyWith {
		forget: Some(secs(120)),
		..drop_of(60)
	};
	let Err(err) = InProcessOperatorHarnessBuilder::<UnmanagedMount<UnmanagedProbe>>::new().with(with).build()
	else {
		panic!("create must refuse a forget next to a drop");
	};
	assert!(err.to_string().contains("FLOW_080"), "expected FLOW_080, got: {err}");
}

fn timed_row(number: u64) -> RecordBatch {
	let user = batch(vec![int4("k", vec![number as i32])]).unwrap();
	let numbers: ArrayRef = Arc::new(UInt64Array::from_iter_values([number]));
	let user = with_system_column(user, SystemColumn::RowNumbers, numbers).unwrap();
	let times: ArrayRef = Arc::new(datetime_array([at_millis(1_000)]));
	with_system_column(user, SystemColumn::Time, times).unwrap()
}

#[test]
fn an_apply_of_many_diffs_reads_the_operator_watermark_once() {
	// A read per diff costs a host round trip for every diff of every change on the extern-C binding.
	let mut harness = InProcessOperatorHarnessBuilder::<UnmanagedMount<UnmanagedProbe>>::new()
		.with(drop_of(60))
		.counting()
		.build()
		.expect("harness");
	let diffs = vec![Diff::insert(timed_row(1)), Diff::insert(timed_row(2)), Diff::insert(timed_row(3))];
	let change = Change::from_flow(OperatorId(1), ChangeVersion::from(CommitVersion(1)), diffs, at_millis(1_000));

	harness.apply(change).expect("apply");

	assert_eq!(harness.calls(), &[("state_get", 1), ("flow_watermark", 1)]);
}

#[test]
fn an_unmanaged_operator_builds_with_the_window_it_declares() {
	// The harness skips the create-time checks, so the mount is the last guard before a flow dies at start.
	InProcessOperatorHarnessBuilder::<UnmanagedMount<UnmanagedWindowProbe>>::new()
		.with(tumbling_of(60))
		.build()
		.expect("an unmanaged operator declaring a tumbling window must build with one");
}

#[test]
fn an_unmanaged_operator_refuses_a_window_it_does_not_declare() {
	// A mount that took any window would let an operator with no window logic silently receive one.
	let Err(err) =
		InProcessOperatorHarnessBuilder::<UnmanagedMount<UnmanagedProbe>>::new().with(tumbling_of(60)).build()
	else {
		panic!("create must refuse a window the operator does not declare");
	};
	assert!(err.to_string().contains("FLOW_067"), "expected FLOW_067, got: {err}");
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
