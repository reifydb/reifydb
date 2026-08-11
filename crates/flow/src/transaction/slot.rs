// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::any::Any;

use reifydb_value::Result;

use crate::transaction::interface::FlowTransaction;

pub type PersistFn<T> = Box<dyn FnOnce(&mut T, Box<dyn Any>) -> Result<()> + Send>;

pub struct OperatorStateSlot<T: FlowTransaction> {
	pub value: Box<dyn Any + Send>,
	pub dirty: bool,
	pub persist: PersistFn<T>,
}

pub struct CarriedOperatorState {
	pub value: Box<dyn Any + Send>,
}
