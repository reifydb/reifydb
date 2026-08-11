// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::any::Any;

use reifydb_value::Result;

use super::DepFlowTransaction;

pub type PersistFn<T = DepFlowTransaction> = Box<dyn FnOnce(&mut T, Box<dyn Any>) -> Result<()> + Send>;

pub struct OperatorStateSlot<T = DepFlowTransaction> {
	pub value: Box<dyn Any + Send>,
	pub dirty: bool,
	pub persist: PersistFn<T>,
}

pub struct CarriedOperatorState {
	pub value: Box<dyn Any + Send>,
}
