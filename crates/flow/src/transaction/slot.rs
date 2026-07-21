// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::any::Any;

use reifydb_value::{Result, byte_size::ByteSize};

use super::FlowTransaction;

pub type PersistFn = Box<dyn FnOnce(&mut FlowTransaction, Box<dyn Any>) -> Result<()> + Send>;

pub type UsageFn = fn(&dyn Any) -> ByteSize;

pub fn zero_usage(_value: &dyn Any) -> ByteSize {
	ByteSize::ZERO
}

pub struct OperatorStateSlot {
	pub value: Box<dyn Any + Send>,
	pub dirty: bool,
	pub persist: PersistFn,
	pub usage: UsageFn,
	pub charged: ByteSize,
}

pub struct CarriedOperatorState {
	pub value: Box<dyn Any + Send>,
	pub usage: UsageFn,
}
