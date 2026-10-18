// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;

use crate::{Result, vm::vm::Vm};

impl<'a> Vm<'a> {
	pub(crate) fn pop_as_column(&mut self) -> Result<(FieldRef, ArrayRef)> {
		self.stack.pop()?.into_column()
	}
}
