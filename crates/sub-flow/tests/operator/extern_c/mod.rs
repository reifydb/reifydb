// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{marker::PhantomData, ops::Index, ptr};

use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::OperatorId, change::Change},
};
use reifydb_flow_async::operator::{HostOperator, host::TxnHostContext};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_sdk::flow::operator::{
	MountedOperator, OperatorMetadata,
	context::ClassValue,
	extern_c::binding::exports::{create_descriptor, create_operator_instance},
};
use reifydb_sub_flow::operator::extern_c::ExternCOperatorHandle;
use reifydb_testing_sdk::in_process::transaction::TestFlowTransaction;
use reifydb_value::Result;

const OPERATOR: OperatorId = OperatorId(1);

pub struct Harness<C> {
	handle: ExternCOperatorHandle,
	txn: TestFlowTransaction,
	history: Vec<Change>,
	_operator: PhantomData<C>,
}

pub struct HarnessBuilder<C>(PhantomData<C>);

impl<C: MountedOperator + OperatorMetadata + 'static> Harness<C> {
	pub fn builder() -> HarnessBuilder<C> {
		HarnessBuilder(PhantomData)
	}

	pub fn apply(&mut self, change: Change) -> Result<Change> {
		let output = self.handle.apply(&mut TxnHostContext::new(&mut self.txn, OPERATOR), change)?;
		self.history.push(output.clone());
		Ok(output)
	}
}

impl<C: MountedOperator + OperatorMetadata + 'static> HarnessBuilder<C> {
	pub fn build(self) -> Result<Harness<C>> {
		// SAFETY: both param pointers are null with zero length, and the handle frees the instance on drop.
		let instance = unsafe { create_operator_instance::<C>(ptr::null(), 0, ptr::null(), 0, OPERATOR.0) };
		assert!(!instance.is_null(), "the guest refused to create the operator");
		Ok(Harness {
			handle: ExternCOperatorHandle::new(
				create_descriptor::<C>(),
				<C::Class as ClassValue>::CLASS,
				instance,
				OPERATOR,
			),
			txn: TestFlowTransaction::new(CommitVersion(1), Clock::Mock(MockClock::new(0))),
			history: Vec::new(),
			_operator: PhantomData,
		})
	}
}

impl<C> Index<usize> for Harness<C> {
	type Output = Change;

	fn index(&self, index: usize) -> &Change {
		&self.history[index]
	}
}

mod class;
mod error_abort;
mod row_number_registry;
mod window_count;
