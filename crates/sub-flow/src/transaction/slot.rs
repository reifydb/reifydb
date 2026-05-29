// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::any::Any;

use reifydb_value::Result;

use super::FlowTransaction;

pub type PersistFn = Box<dyn FnOnce(&mut FlowTransaction, Box<dyn Any>) -> Result<()> + Send>;

pub struct OperatorStateSlot {
	pub value: Box<dyn Any + Send>,
	pub dirty: bool,
	pub persist: PersistFn,
}
