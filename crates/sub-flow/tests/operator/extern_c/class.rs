// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::Cell, ptr};

use reifydb_core::{
	common::{OperatorClass, WindowRequirements, WindowSizeDomain},
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	key::operator::state::unmanaged_key,
	operator_with::ApplyWith,
};
use reifydb_flow_async::operator::{HostOperator, host::TxnHostContext};
use reifydb_sdk::{
	error::Result as SdkResult,
	flow::operator::{
		OperatorMetadata, UnmanagedMount, UnmanagedOperator,
		column::operator::OperatorColumn,
		context::{ClassState, GuestContext, Unmanaged},
		extern_c::binding::exports::{create_descriptor, create_operator_instance},
		view::ChangeView,
	},
};
use reifydb_sub_flow::operator::extern_c::ExternCOperatorHandle;
use reifydb_test_harness::{
	engine::TestEngine,
	operator::{change::trigger, transaction::FlowTxn},
};
use reifydb_value::config::ExtensionParams;

thread_local! {
	static WRITE_ACCEPTED: Cell<Option<bool>> = const { Cell::new(None) };
}

struct WriteProbe;

impl OperatorMetadata for WriteProbe {
	const NAME: &'static str = "write_probe";
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Writes one unmanaged key and records whether the host took it";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD;
}

impl UnmanagedOperator for WriteProbe {
	const UNMANAGED_BECAUSE: &'static str = "test operator";
	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
		throttles: false,
	};

	fn create(_operator_id: OperatorId, _params: &ExtensionParams, _with: &ApplyWith) -> SdkResult<Self> {
		Ok(WriteProbe)
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Unmanaged>, _change: impl ChangeView) -> SdkResult<()> {
		// A guest error aborts the process at the boundary, so the outcome must travel out of band.
		let key = unmanaged_key(&0u64.to_be_bytes()).expect("an eight byte suffix fits the keyspace");
		let accepted = ctx.state().set(&key, &1i64).is_ok();
		WRITE_ACCEPTED.with(|cell| cell.set(Some(accepted)));
		Ok(())
	}
}

#[test]
fn the_handle_hands_its_own_class_to_the_host_state_checks() {
	// A handle that forwards any class but its own lets a guest write keyspaces its class must never reach.
	let engine = TestEngine::new();
	let mut txn = engine.flow_txn().deferred();
	let id = OperatorId(7);

	for (class, expected) in
		[(OperatorClass::Unmanaged, true), (OperatorClass::Managed, false), (OperatorClass::Nostate, false)]
	{
		// SAFETY: both param pointers are null with zero length, and the handle frees the instance on drop.
		let instance = unsafe {
			create_operator_instance::<UnmanagedMount<WriteProbe>>(ptr::null(), 0, ptr::null(), 0, id.0)
		};
		let mut handle = ExternCOperatorHandle::new(
			create_descriptor::<UnmanagedMount<WriteProbe>>(),
			class,
			instance,
			id,
		);

		handle.apply(&mut TxnHostContext::new(&mut txn, id), trigger()).expect("the probe never fails");

		assert_eq!(
			WRITE_ACCEPTED.with(Cell::take),
			Some(expected),
			"an unmanaged key written under class {class:?}"
		);
	}
}
