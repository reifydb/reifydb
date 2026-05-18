// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::any::Any;

use reifydb_type::Result;

use super::FlowTransaction;

pub type PersistFn = Box<dyn FnOnce(&mut FlowTransaction, Box<dyn Any>) -> Result<()> + Send>;

pub struct OperatorStateSlot {
	pub value: Box<dyn Any + Send>,
	pub dirty: bool,
	pub persist: PersistFn,
}
